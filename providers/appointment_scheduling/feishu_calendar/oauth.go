package feishucalendar

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/internal/oauth2"
)

const (
	defaultOAuthBaseURL = "https://accounts.feishu.cn"
	oauthTokenPath      = "/oauth/v3/token"
)

func (p *provider) AuthorizationURL(request connector.OAuthAuthorizationRequest) (string, error) {
	if request.Connection.ConnectorKey != ConnectorKey || request.Connection.ProviderKey != ProviderKey {
		return "", errors.New("Feishu Calendar OAuth provider mismatch")
	}
	if err := p.ValidateConfig(request.Connection); err != nil {
		return "", err
	}
	return oauth2.AuthorizationURL(oauthBaseURL(request.Connection)+"/open-apis/authen/v1/authorize", request, nil)
}

func (p *provider) ExchangeAuthorizationCode(ctx context.Context, request connector.OAuthCodeExchangeRequest) (connector.OAuthTokens, error) {
	if request.Connection.ConnectorKey != ConnectorKey || request.Connection.ProviderKey != ProviderKey {
		return connector.OAuthTokens{}, connector.PermanentError("feishu_calendar.oauth.provider_mismatch", errors.New("Feishu Calendar OAuth provider mismatch"))
	}
	if err := p.ValidateConfig(request.Connection); err != nil {
		return connector.OAuthTokens{}, err
	}
	scopes, err := normalizeOAuthScopes(request.RequestedScopes)
	if err != nil || !validOAuthRedirect(request.RedirectURI) || strings.TrimSpace(request.ClientID) == "" || strings.TrimSpace(request.ClientSecret) == "" || strings.TrimSpace(request.Code) == "" || len(request.Code) > 16384 || !validCodeVerifier(request.CodeVerifier) {
		return connector.OAuthTokens{}, connector.PermanentError("feishu_calendar.oauth.authorization_configuration_invalid", errors.New("Feishu OAuth code exchange configuration is invalid"))
	}
	body := map[string]any{
		"grant_type":   "authorization_code",
		"redirect_uri": request.RedirectURI,
		"scope":        strings.Join(scopes, " "),
	}
	return p.exchangeOAuthToken(ctx, request.Connection, body, map[string]string{
		"client_id": request.ClientID, "client_secret": request.ClientSecret,
		"code": request.Code, "code_verifier": request.CodeVerifier,
	}, scopes, true)
}

func (p *provider) refreshUserToken(ctx context.Context, connection connector.Connection, secrets map[string]string) (map[string]string, error) {
	refreshToken := strings.TrimSpace(secrets["refresh_token"])
	clientID := strings.TrimSpace(secrets["client_id"])
	clientSecret := strings.TrimSpace(secrets["client_secret"])
	if refreshToken == "" || clientID == "" || clientSecret == "" {
		return nil, connector.PermanentError("feishu_calendar.oauth.refresh_configuration_invalid", errors.New("Feishu OAuth refresh credentials are required"))
	}
	tokens, err := p.exchangeOAuthToken(ctx, connection, map[string]any{"grant_type": "refresh_token"}, map[string]string{
		"client_id": clientID, "client_secret": clientSecret, "refresh_token": refreshToken,
	}, nil, false)
	if err != nil {
		return nil, err
	}
	updates := map[string]string{"access_token": tokens.AccessToken}
	if tokens.RefreshToken != "" {
		updates["refresh_token"] = tokens.RefreshToken
	}
	return updates, nil
}

func (p *provider) exchangeOAuthToken(ctx context.Context, connection connector.Connection, body map[string]any, secretJSON map[string]string, requestedScopes []string, authorizationCode bool) (connector.OAuthTokens, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return connector.OAuthTokens{}, connector.PermanentError("feishu_calendar.oauth.request_invalid", errors.New("Feishu OAuth request is invalid"))
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{
		Method: http.MethodPost, URL: oauthTokenURL(connection),
		Headers: map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/json; charset=utf-8"}},
		Body:    raw, SecretJSON: secretJSON, MaxResponseBytes: responseLimit,
	})
	unknown := func() (connector.OAuthTokens, error) {
		message := "Feishu OAuth token result is unknown"
		if authorizationCode {
			message = "Feishu authorization code may have been consumed; start a new authorization"
		}
		return connector.OAuthTokens{}, connector.UncertainError("feishu_calendar.oauth.token_result_unknown", errors.New(message))
	}
	if transportErr != nil {
		return unknown()
	}
	var payload struct {
		Code             int         `json:"code"`
		Error            string      `json:"error"`
		AccessToken      string      `json:"access_token"`
		RefreshToken     string      `json:"refresh_token"`
		TokenType        string      `json:"token_type"`
		Scope            string      `json:"scope"`
		ExpiresIn        json.Number `json:"expires_in"`
		ErrorDescription string      `json:"error_description"`
	}
	decoded := json.Unmarshal(response.Body, &payload) == nil
	if response.StatusCode < 200 || response.StatusCode >= 300 || !decoded || payload.Code != 0 || payload.Error != "" {
		if response.StatusCode == http.StatusTooManyRequests {
			return connector.OAuthTokens{}, connector.RetryableError("feishu_calendar.oauth.rate_limited", errors.New("Feishu OAuth token endpoint is rate limited"))
		}
		if response.StatusCode == http.StatusBadRequest || response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden || (decoded && (payload.Code != 0 || payload.Error != "")) {
			return connector.OAuthTokens{}, connector.PermanentError("feishu_calendar.oauth.token_rejected", errors.New("Feishu OAuth token request was rejected"))
		}
		return unknown()
	}
	accessToken := strings.TrimSpace(payload.AccessToken)
	if accessToken == "" || !strings.EqualFold(strings.TrimSpace(payload.TokenType), "Bearer") {
		return unknown()
	}
	expires := int64(0)
	if payload.ExpiresIn != "" {
		expires, err = payload.ExpiresIn.Int64()
		if err != nil || expires <= 0 || expires > 31536000 {
			return unknown()
		}
	}
	scopes := requestedScopes
	if strings.TrimSpace(payload.Scope) != "" {
		scopes, err = normalizeOAuthScopes(strings.Fields(payload.Scope))
		if err != nil {
			return unknown()
		}
	}
	return connector.OAuthTokens{
		AccessToken: accessToken, RefreshToken: strings.TrimSpace(payload.RefreshToken), TokenType: "Bearer",
		GrantedScopes: scopes, ExpiresInSeconds: expires,
	}, nil
}

type apiSession struct {
	provider   *provider
	connection connector.Connection
	secrets    map[string]string
	token      string
	updates    map[string]string
}

func (p *provider) newAPISession(connection connector.Connection, secrets map[string]string) *apiSession {
	copySecrets := map[string]string{}
	for key, value := range secrets {
		copySecrets[key] = value
	}
	return &apiSession{provider: p, connection: connection, secrets: copySecrets, updates: map[string]string{}}
}

func (s *apiSession) accessToken(ctx context.Context) (string, error) {
	if s.token != "" {
		return s.token, nil
	}
	token, err := s.provider.accessToken(ctx, s.connection, s.secrets)
	if err == nil {
		s.token = token
	}
	return token, err
}

func (s *apiSession) call(ctx context.Context, method, path string, query url.Values, body map[string]any, write bool) (connector.TypedResult[Response], error) {
	token, err := s.accessToken(ctx)
	if err != nil {
		return connector.TypedResult[Response]{SecretUpdates: cloneStringMap(s.updates)}, err
	}
	output, ref, _, err := s.provider.execute(ctx, s.connection, token, method, path, query, body, nil, write)
	if err == nil || !feishuAccessTokenRejected(err) || strings.TrimSpace(s.secrets["refresh_token"]) == "" {
		return connector.TypedResult[Response]{Output: output, ResponseRef: ref, SecretUpdates: cloneStringMap(s.updates)}, err
	}
	updates, refreshErr := s.provider.refreshUserToken(ctx, s.connection, s.secrets)
	if refreshErr != nil {
		return connector.TypedResult[Response]{ResponseRef: "oauth:refresh_failed", SecretUpdates: cloneStringMap(s.updates)}, refreshErr
	}
	for key, value := range updates {
		s.secrets[key] = value
		s.updates[key] = value
	}
	s.token = updates["access_token"]
	output, ref, _, err = s.provider.execute(ctx, s.connection, s.token, method, path, query, body, nil, write)
	return connector.TypedResult[Response]{Output: output, ResponseRef: ref, SecretUpdates: cloneStringMap(s.updates)}, err
}

func feishuAccessTokenRejected(err error) bool {
	code, ok := connector.ProviderErrorCodeOf(err)
	if !ok {
		return false
	}
	if code == "feishu_calendar.http_401" {
		return true
	}
	for providerCode := 99991661; providerCode <= 99991668; providerCode++ {
		if code == "feishu_calendar.provider_code_"+strconv.Itoa(providerCode) {
			return true
		}
	}
	return false
}

func normalizeOAuthScopes(scopes []string) ([]string, error) {
	if len(scopes) == 0 || len(scopes) > 64 {
		return nil, errors.New("explicit Feishu OAuth scopes are required")
	}
	seen := map[string]bool{}
	result := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		if scope == "" || len(scope) > 512 || strings.TrimSpace(scope) != scope || strings.ContainsAny(scope, " \t\r\n\"\\") || seen[scope] {
			return nil, errors.New("Feishu OAuth scope is invalid")
		}
		seen[scope] = true
		result = append(result, scope)
	}
	sort.Strings(result)
	return result, nil
}

func validCodeVerifier(value string) bool {
	if len(value) < 43 || len(value) > 128 {
		return false
	}
	for _, character := range []byte(value) {
		if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || strings.ContainsRune("-._~", rune(character))) {
			return false
		}
	}
	return true
}

func validOAuthRedirect(raw string) bool {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	return err == nil && parsed.Host != "" && parsed.User == nil && parsed.Fragment == "" && parsed.RawQuery == "" && (parsed.Scheme == "https" || parsed.Scheme == "http" && isLoopback(parsed.Hostname()))
}

func oauthBaseURL(connection connector.Connection) string {
	if value := strings.TrimRight(config(connection, "oauth_base_url", ""), "/"); value != "" {
		return value
	}
	return defaultOAuthBaseURL
}

func oauthTokenURL(connection connector.Connection) string {
	return oauthBaseURL(connection) + oauthTokenPath
}

func cloneStringMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func (*provider) OAuthConnectionTestScopes() ([][]string, bool) {
	return calendarReadScopeAlternatives(), true
}

func (*provider) OAuthOperationScopes(operationKey string) ([][]string, bool) {
	switch operationKey {
	case CalendarList.Key, CalendarEvents.Key, CalendarEvent.Key, CalendarAvailability.Key, TestConnection.Key:
		return calendarReadScopeAlternatives(), true
	case FetchMeetingContent.Key:
		return meetingContentScopeAlternatives(), true
	case CreateBooking.Key, EnqueueBooking.Key:
		return [][]string{{"calendar:calendar"}}, true
	default:
		return nil, false
	}
}

func calendarReadScopeAlternatives() [][]string {
	return [][]string{{"calendar:calendar:readonly"}, {"calendar:calendar"}}
}

func meetingContentScopeAlternatives() [][]string {
	return [][]string{
		{"calendar:calendar:readonly", "minutes:minutes:readonly", "vc:meeting:readonly", "vc:record:readonly"},
		{"calendar:calendar", "minutes:minutes:readonly", "vc:meeting:readonly", "vc:record:readonly"},
		{"calendar:calendar:readonly", "minutes:minutes.basic:read", "minutes:minutes.transcript:export", "vc:meeting:readonly", "vc:record:readonly"},
		{"calendar:calendar", "minutes:minutes.basic:read", "minutes:minutes.transcript:export", "vc:meeting:readonly", "vc:record:readonly"},
	}
}

func (*provider) ProviderAccountProbeEnabled() bool { return true }

var _ connector.OAuthAuthorizer = (*provider)(nil)
var _ connector.OAuthConnectionTestScopeProvider = (*provider)(nil)
var _ connector.OAuthOperationScopeProvider = (*provider)(nil)
