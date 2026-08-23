package oauth2

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
)

const tokenResponseLimit int64 = 1 << 20

type ClientCredentialsRequest struct {
	Endpoint             string
	ClientID             string
	ClientSecret         string
	Scope                string
	ErrorPrefix          string
	ClientAuthentication ClientAuthentication
}

func ClientCredentials(ctx context.Context, transport connector.Transport, request ClientCredentialsRequest) (Token, error) {
	if transport == nil {
		return Token{}, errors.New("OAuth transport is required")
	}
	prefix := strings.TrimSpace(request.ErrorPrefix)
	if prefix == "" {
		prefix = "oauth2"
	}
	parsed, err := url.Parse(strings.TrimSpace(request.Endpoint))
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) || strings.TrimSpace(request.ClientID) == "" || strings.TrimSpace(request.ClientSecret) == "" {
		return Token{}, connector.PermanentError(prefix+".client_credentials_configuration_invalid", errors.New("valid endpoint and client credentials are required"))
	}
	form := url.Values{"grant_type": {"client_credentials"}}
	if scope := strings.TrimSpace(request.Scope); scope != "" {
		form.Set("scope", scope)
	}
	httpRequest := connector.HTTPRequest{Method: http.MethodPost, URL: parsed.String(), Headers: map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/x-www-form-urlencoded"}}, Body: []byte(form.Encode()), MaxResponseBytes: tokenResponseLimit}
	switch request.ClientAuthentication {
	case "", ClientAuthenticationForm:
		httpRequest.SecretForm = map[string]string{"client_id": request.ClientID, "client_secret": request.ClientSecret}
	case ClientAuthenticationBasic:
		httpRequest.SecretHeaders = map[string][]string{"Authorization": {oauthBasicAuthorization(request.ClientID, request.ClientSecret)}}
	default:
		return Token{}, connector.PermanentError(prefix+".client_credentials_configuration_invalid", errors.New("unsupported token endpoint authentication method"))
	}
	response, err := transport.RoundTripHTTP(ctx, httpRequest)
	if err != nil {
		return Token{}, connector.RetryableError(prefix+".client_credentials_network_error", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := errors.New("OAuth token endpoint returned HTTP " + strconv.Itoa(response.StatusCode))
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			return Token{}, connector.RetryableError(prefix+".client_credentials_http_"+strconv.Itoa(response.StatusCode), cause)
		}
		return Token{}, connector.PermanentError(prefix+".client_credentials_rejected", cause)
	}
	var payload struct {
		AccessToken string `json:"access_token"`
	}
	if json.Unmarshal(response.Body, &payload) != nil || strings.TrimSpace(payload.AccessToken) == "" {
		return Token{}, connector.PermanentError(prefix+".client_credentials_response_invalid", errors.New("OAuth token response is invalid"))
	}
	return Token{AccessToken: strings.TrimSpace(payload.AccessToken)}, nil
}
