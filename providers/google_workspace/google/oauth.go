package google

import (
	"context"
	"errors"
	"net/url"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/internal/oauth2"
)

func (p *provider) AuthorizationURL(request connector.OAuthAuthorizationRequest) (string, error) {
	if request.Connection.ConnectorKey != ConnectorKey || request.Connection.ProviderKey != ProviderKey {
		return "", errors.New("Google OAuth provider mismatch")
	}
	return oauth2.AuthorizationURL("https://accounts.google.com/o/oauth2/v2/auth", request, url.Values{"access_type": {"offline"}, "prompt": {"consent"}, "include_granted_scopes": {"true"}})
}
func (p *provider) ExchangeAuthorizationCode(ctx context.Context, request connector.OAuthCodeExchangeRequest) (connector.OAuthTokens, error) {
	if request.Connection.ConnectorKey != ConnectorKey || request.Connection.ProviderKey != ProviderKey {
		return connector.OAuthTokens{}, connector.PermanentError("google.oauth.provider_mismatch", errors.New("Google OAuth provider mismatch"))
	}
	return oauth2.ExchangeAuthorizationCode(ctx, p.transport, tokenURL(request.Connection), request, "google.oauth")
}

var _ connector.OAuthAuthorizer = (*provider)(nil)

// UserInfo is the fixed read-only probe. Calendar, Gmail or Drive access alone
// must not be mistaken for permission to read this identity endpoint.
func (*provider) OAuthConnectionTestScopes() ([][]string, bool) {
	return [][]string{{"openid"}, {"https://www.googleapis.com/auth/userinfo.email"}, {"https://www.googleapis.com/auth/userinfo.profile"}}, true
}
