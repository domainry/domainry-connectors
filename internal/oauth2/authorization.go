package oauth2

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"unicode/utf8"

	connector "github.com/domainry/domainry-connector-sdk"
)

// AuthorizationURL only builds a URL. Integration must authorize the caller and
// persist state, exact callback, scopes and PKCE before returning this URL.
func AuthorizationURL(endpoint string, request connector.OAuthAuthorizationRequest, parameters url.Values) (string, error) {
	if err := validateAuthorization(request); err != nil {
		return "", err
	}
	parsed, err := oauthEndpoint(endpoint)
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	// Provider parameters cannot weaken the fixed authorization-code/PKCE flow.
	for key, values := range parameters {
		query[key] = append([]string(nil), values...)
	}
	query.Set("response_type", "code")
	query.Set("client_id", request.ClientID)
	query.Set("redirect_uri", request.RedirectURI)
	query.Set("scope", strings.Join(request.Scopes, " "))
	query.Set("state", request.State)
	query.Set("code_challenge", request.CodeChallenge)
	query.Set("code_challenge_method", "S256")
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func validateAuthorization(request connector.OAuthAuthorizationRequest) error {
	if strings.TrimSpace(request.ClientID) == "" {
		return errors.New("OAuth client ID is required")
	}
	if _, err := oauthEndpoint(request.RedirectURI); err != nil {
		return errors.New("OAuth callback must use HTTPS or loopback HTTP without userinfo or fragment")
	}
	if len(request.State) < 32 || len(request.State) > 512 || strings.TrimSpace(request.State) != request.State {
		return errors.New("OAuth state is invalid")
	}
	challenge, err := base64.RawURLEncoding.DecodeString(request.CodeChallenge)
	if err != nil || len(challenge) != 32 {
		return errors.New("OAuth S256 code challenge is required")
	}
	if _, err := normalizeScopes(request.Scopes); err != nil {
		return err
	}
	return nil
}

func oauthEndpoint(raw string) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" || parsed.RawQuery != "" || strings.TrimSpace(raw) != raw || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return nil, errors.New("OAuth endpoint is invalid")
	}
	return parsed, nil
}

func normalizeScopes(scopes []string) ([]string, error) {
	if len(scopes) == 0 || len(scopes) > 64 {
		return nil, errors.New("OAuth explicit scopes are required")
	}
	seen := map[string]bool{}
	for _, scope := range scopes {
		if scope == "" || len(scope) > 512 || !utf8.ValidString(scope) {
			return nil, errors.New("OAuth scope is invalid")
		}
		for _, ch := range []byte(scope) {
			if ch < 0x21 || ch > 0x7e || ch == '"' || ch == '\\' {
				return nil, errors.New("OAuth scope is invalid")
			}
		}
		if seen[scope] {
			return nil, errors.New("OAuth scope is duplicated")
		}
		seen[scope] = true
	}
	result := append([]string(nil), scopes...)
	sort.Strings(result)
	return result, nil
}

// ExchangeAuthorizationCode performs exactly one token request. Even retryable
// transport failures may have consumed the code, so ambiguous outcomes use the
// SDK's uncertain classification. No response body or credential enters errors.
func ExchangeAuthorizationCode(ctx context.Context, transport connector.Transport, endpoint string, request connector.OAuthCodeExchangeRequest, prefix string) (connector.OAuthTokens, error) {
	invalid := func() (connector.OAuthTokens, error) {
		return connector.OAuthTokens{}, connector.PermanentError(prefix+".authorization_configuration_invalid", errors.New("OAuth code exchange configuration is invalid"))
	}
	if transport == nil {
		return invalid()
	}
	if _, err := oauthEndpoint(endpoint); err != nil {
		return invalid()
	}
	if _, err := oauthEndpoint(request.RedirectURI); err != nil {
		return invalid()
	}
	scopes, err := normalizeScopes(request.RequestedScopes)
	if err != nil {
		return invalid()
	}
	if strings.TrimSpace(request.ClientID) == "" || strings.TrimSpace(request.ClientSecret) == "" || strings.TrimSpace(request.Code) == "" || len(request.Code) > 16384 || len(request.CodeVerifier) < 43 || len(request.CodeVerifier) > 128 {
		return invalid()
	}
	for _, ch := range []byte(request.CodeVerifier) {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || strings.ContainsRune("-._~", rune(ch))) {
			return invalid()
		}
	}
	response, err := transport.RoundTripHTTP(ctx, connector.HTTPRequest{
		Method: http.MethodPost, URL: endpoint, Headers: map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/x-www-form-urlencoded"}},
		Body:       []byte(url.Values{"grant_type": {"authorization_code"}, "redirect_uri": {request.RedirectURI}}.Encode()),
		SecretForm: map[string]string{"client_id": request.ClientID, "client_secret": request.ClientSecret, "code": request.Code, "code_verifier": request.CodeVerifier}, MaxResponseBytes: 1 << 20,
	})
	uncertain := func() (connector.OAuthTokens, error) {
		return connector.OAuthTokens{}, connector.UncertainError(prefix+".authorization_exchange_unknown", errors.New("OAuth authorization code may have been consumed; start a new authorization"))
	}
	if err != nil {
		return uncertain()
	}
	if len(response.Body) > 1<<20 {
		return uncertain()
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var rejection struct {
			Error string `json:"error"`
		}
		if (response.StatusCode == 400 || response.StatusCode == 401) && json.Unmarshal(response.Body, &rejection) == nil {
			switch rejection.Error {
			case "invalid_request", "invalid_client", "invalid_grant", "unauthorized_client", "unsupported_grant_type", "invalid_scope", "access_denied":
				return connector.OAuthTokens{}, connector.PermanentError(prefix+".authorization_rejected", errors.New("OAuth authorization was rejected"))
			}
		}
		return uncertain()
	}
	var payload struct {
		AccessToken  string      `json:"access_token"`
		RefreshToken string      `json:"refresh_token"`
		TokenType    string      `json:"token_type"`
		Scope        *string     `json:"scope"`
		ExpiresIn    json.Number `json:"expires_in"`
	}
	if json.Unmarshal(response.Body, &payload) != nil || strings.TrimSpace(payload.AccessToken) == "" || !strings.EqualFold(payload.TokenType, "Bearer") {
		return uncertain()
	}
	var expires int64
	if payload.ExpiresIn != "" {
		expires, err = payload.ExpiresIn.Int64()
		if err != nil || expires <= 0 || expires > 31536000 {
			return uncertain()
		}
	}
	if payload.Scope != nil {
		scopes, err = normalizeScopes(strings.Fields(*payload.Scope))
		if err != nil {
			return uncertain()
		}
	}
	return connector.OAuthTokens{AccessToken: payload.AccessToken, RefreshToken: payload.RefreshToken, TokenType: "Bearer", GrantedScopes: scopes, ExpiresInSeconds: expires}, nil
}
