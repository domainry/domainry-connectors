// Package oauth2 contains the Runtime-transported OAuth token refresh used by
// official Providers. Secret values stay in HTTPRequest runtime-only fields.
package oauth2

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
)

type ClientAuthentication string

const (
	ClientAuthenticationForm  ClientAuthentication = "form"
	ClientAuthenticationBasic ClientAuthentication = "basic"
)

type RefreshRequest struct {
	Endpoint     string
	RefreshToken string
	ClientID     string
	ClientSecret string
	// Scope is public protocol metadata used only by authorization servers
	// that require scope replay during refresh.
	Scope string
	// ClientSecretOptional is reserved for providers whose authorization
	// server explicitly supports public clients. The strict default protects
	// confidential-client integrations from silently dropping authentication.
	ClientSecretOptional bool
	ClientAuthentication ClientAuthentication
	ErrorPrefix          string
}

type Token struct {
	AccessToken  string
	RefreshToken string
}

func Refresh(ctx context.Context, transport connector.Transport, request RefreshRequest) (Token, error) {
	if transport == nil {
		return Token{}, errors.New("OAuth transport is required")
	}
	prefix := strings.TrimSpace(request.ErrorPrefix)
	if prefix == "" {
		prefix = "oauth2"
	}
	if err := validateRequest(request); err != nil {
		return Token{}, connector.PermanentError(prefix+".refresh_configuration_invalid", err)
	}
	headers := map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/x-www-form-urlencoded"}}
	secretHeaders := map[string][]string{}
	secretForm := map[string]string{"refresh_token": request.RefreshToken}
	switch request.ClientAuthentication {
	case ClientAuthenticationForm:
		secretForm["client_id"] = request.ClientID
		if request.ClientSecret != "" {
			secretForm["client_secret"] = request.ClientSecret
		}
	case ClientAuthenticationBasic:
		secretHeaders["Authorization"] = []string{oauthBasicAuthorization(request.ClientID, request.ClientSecret)}
	}
	publicForm := url.Values{"grant_type": {"refresh_token"}}
	if scope := strings.TrimSpace(request.Scope); scope != "" {
		publicForm.Set("scope", scope)
	}
	response, err := transport.RoundTripHTTP(ctx, connector.HTTPRequest{
		Method: http.MethodPost, URL: strings.TrimSpace(request.Endpoint), Headers: headers,
		SecretHeaders: secretHeaders, Body: []byte(publicForm.Encode()), SecretForm: secretForm,
	})
	if err != nil {
		return Token{}, connector.RetryableError(prefix+".refresh_network_error", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := errors.New("OAuth token endpoint returned HTTP " + strconv.Itoa(response.StatusCode))
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			return Token{}, connector.RetryableError(prefix+".refresh_http_"+strconv.Itoa(response.StatusCode), cause)
		}
		return Token{}, connector.PermanentError(prefix+".refresh_rejected", cause)
	}
	var payload struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.Unmarshal(response.Body, &payload); err != nil || strings.TrimSpace(payload.AccessToken) == "" {
		return Token{}, connector.PermanentError(prefix+".refresh_response_invalid", errors.New("OAuth token response is invalid"))
	}
	return Token{AccessToken: strings.TrimSpace(payload.AccessToken), RefreshToken: strings.TrimSpace(payload.RefreshToken)}, nil
}

func oauthBasicAuthorization(clientID, clientSecret string) string {
	credential := base64.StdEncoding.EncodeToString([]byte(url.QueryEscape(clientID) + ":" + url.QueryEscape(clientSecret)))
	return "Basic " + credential
}

func validateRequest(request RefreshRequest) error {
	parsed, err := url.Parse(strings.TrimSpace(request.Endpoint))
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return errors.New("OAuth token endpoint must use HTTPS or loopback HTTP")
	}
	if strings.TrimSpace(request.RefreshToken) == "" || strings.TrimSpace(request.ClientID) == "" {
		return errors.New("OAuth refresh credentials are required")
	}
	if strings.TrimSpace(request.ClientSecret) == "" && (!request.ClientSecretOptional || request.ClientAuthentication != ClientAuthenticationForm) {
		return errors.New("OAuth client secret is required")
	}
	if request.ClientAuthentication != ClientAuthenticationForm && request.ClientAuthentication != ClientAuthenticationBasic {
		return errors.New("OAuth client authentication is unsupported")
	}
	return nil
}

func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
