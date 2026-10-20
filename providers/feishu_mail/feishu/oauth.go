package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/mail"
	"github.com/domainry/domainry-connector-sdk/mailwrite"
	"github.com/domainry/domainry-connectors/internal/domain/mailattachment"
	internalfeishu "github.com/domainry/domainry-connectors/internal/feishu"
	"github.com/domainry/domainry-connectors/internal/oauth2"
)

const oauthTokenPath = "/open-apis/authen/v2/oauth/token"

const (
	mailProfileScope = "mail:user_mailbox:readonly"
	mailReadScope    = "mail:user_mailbox.message:readonly"
	mailSubjectScope = "mail:user_mailbox.message.subject:read"
	mailAddressScope = "mail:user_mailbox.message.address:read"
	mailBodyScope    = "mail:user_mailbox.message.body:read"
	mailSendScope    = "mail:user_mailbox.message:send"
)

func mailDetailScopes() []string {
	return []string{mailReadScope, mailSubjectScope, mailAddressScope, mailBodyScope}
}

func (p *provider) AuthorizationURL(request connector.OAuthAuthorizationRequest) (string, error) {
	if request.Connection.ConnectorKey != ConnectorKey || request.Connection.ProviderKey != ProviderKey {
		return "", errors.New("Feishu Mail OAuth provider mismatch")
	}
	if err := p.ValidateConfig(request.Connection); err != nil {
		return "", err
	}
	return oauth2.AuthorizationURL(oauthBaseURL(request.Connection)+"/open-apis/authen/v1/authorize", request, nil)
}

func (p *provider) ExchangeAuthorizationCode(ctx context.Context, request connector.OAuthCodeExchangeRequest) (connector.OAuthTokens, error) {
	if request.Connection.ConnectorKey != ConnectorKey || request.Connection.ProviderKey != ProviderKey {
		return connector.OAuthTokens{}, permanent("oauth.provider_mismatch", "Feishu Mail OAuth provider mismatch")
	}
	if err := p.ValidateConfig(request.Connection); err != nil {
		return connector.OAuthTokens{}, err
	}
	scopes, err := normalizeOAuthScopes(request.RequestedScopes)
	if err != nil || !validOAuthRedirect(request.RedirectURI) || strings.TrimSpace(request.ClientID) == "" || strings.TrimSpace(request.ClientSecret) == "" || strings.TrimSpace(request.Code) == "" || len(request.Code) > 16384 || !validCodeVerifier(request.CodeVerifier) {
		return connector.OAuthTokens{}, permanent("oauth.authorization_configuration_invalid", "Feishu OAuth code exchange configuration is invalid")
	}
	return p.exchangeOAuthToken(ctx, request.Connection, map[string]any{
		"grant_type": "authorization_code", "redirect_uri": request.RedirectURI, "scope": strings.Join(scopes, " "),
	}, map[string]string{
		"client_id": request.ClientID, "client_secret": request.ClientSecret, "code": request.Code, "code_verifier": request.CodeVerifier,
	}, scopes, true)
}

func (p *provider) refreshUserToken(ctx context.Context, connection connector.Connection, secrets map[string]string) (map[string]string, error) {
	refreshToken := strings.TrimSpace(secrets["refresh_token"])
	clientID := strings.TrimSpace(secrets["client_id"])
	clientSecret := strings.TrimSpace(secrets["client_secret"])
	if refreshToken == "" || clientID == "" || clientSecret == "" {
		return nil, permanent("oauth.refresh_configuration_invalid", "Feishu OAuth refresh credentials are required")
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
		return connector.OAuthTokens{}, permanent("oauth.request_invalid", "Feishu OAuth request is invalid")
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{
		Method: http.MethodPost, URL: baseURL(connection) + oauthTokenPath,
		Headers: map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/json; charset=utf-8"}},
		Body:    raw, SecretJSON: secretJSON, MaxResponseBytes: responseLimit,
	})
	unknown := func() (connector.OAuthTokens, error) {
		message := "Feishu OAuth token result is unknown"
		if authorizationCode {
			message = "Feishu authorization code may have been consumed; start a new authorization"
		}
		return connector.OAuthTokens{}, connector.UncertainError("feishu_mail.oauth.token_result_unknown", errors.New(message))
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
	if internalfeishu.IsRateLimitedResponse(response.StatusCode, response.Body) {
		return connector.OAuthTokens{}, internalfeishu.RateLimitError("feishu_mail.oauth")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 || !decoded || payload.Code != 0 || payload.Error != "" {
		if response.StatusCode == http.StatusBadRequest || response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden || decoded && (payload.Code != 0 || payload.Error != "") {
			return connector.OAuthTokens{}, permanent("oauth.token_rejected", "Feishu OAuth token request was rejected")
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
	return connector.OAuthTokens{AccessToken: accessToken, RefreshToken: strings.TrimSpace(payload.RefreshToken), TokenType: "Bearer", GrantedScopes: scopes, ExpiresInSeconds: expires}, nil
}

func (*provider) OAuthConnectionTestScopes() ([][]string, bool) {
	return [][]string{{mailReadScope}}, true
}

func (*provider) OAuthOperationScopes(operationKey string) ([][]string, bool) {
	switch operationKey {
	case mail.ListOperationKey, mail.SearchOperationKey, mail.ReadOperationKey, mailattachment.DownloadOperationKey, TestConnection.Key:
		return [][]string{mailDetailScopes()}, true
	case mailwrite.SendOperationKey:
		return [][]string{{mailProfileScope, mailSendScope}}, true
	case mailwrite.ReplyOperationKey:
		return [][]string{append([]string{mailProfileScope}, append(mailDetailScopes(), mailSendScope)...)}, true
	default:
		return nil, false
	}
}

func (*provider) ProviderAccountProbeEnabled() bool { return true }

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

var _ connector.OAuthAuthorizer = (*provider)(nil)
var _ connector.OAuthConnectionTestScopeProvider = (*provider)(nil)
var _ connector.OAuthOperationScopeProvider = (*provider)(nil)
