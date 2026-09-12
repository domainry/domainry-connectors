package microsoft

import (
	"context"
	"errors"
	"net/url"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/internal/oauth2"
)

func validOAuthTenant(connection connector.Connection) bool {
	tenant := config(connection, "tenant_id")
	if tenant == "" || len(tenant) > 253 {
		return false
	}
	for _, ch := range tenant {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '.') {
			return false
		}
	}
	return !strings.HasPrefix(tenant, ".") && !strings.HasSuffix(tenant, ".")
}
func (p *provider) AuthorizationURL(request connector.OAuthAuthorizationRequest) (string, error) {
	if request.Connection.ConnectorKey != ConnectorKey || request.Connection.ProviderKey != ProviderKey || !validOAuthTenant(request.Connection) {
		return "", errors.New("Microsoft OAuth provider or tenant mismatch")
	}
	return oauth2.AuthorizationURL("https://login.microsoftonline.com/"+url.PathEscape(config(request.Connection, "tenant_id"))+"/oauth2/v2.0/authorize", request, url.Values{"response_mode": {"query"}})
}
func (p *provider) ExchangeAuthorizationCode(ctx context.Context, request connector.OAuthCodeExchangeRequest) (connector.OAuthTokens, error) {
	if request.Connection.ConnectorKey != ConnectorKey || request.Connection.ProviderKey != ProviderKey || !validOAuthTenant(request.Connection) {
		return connector.OAuthTokens{}, connector.PermanentError("microsoft.oauth.provider_mismatch", errors.New("Microsoft OAuth provider or tenant mismatch"))
	}
	return oauth2.ExchangeAuthorizationCode(ctx, p.transport, tokenURL(request.Connection), request, "microsoft.oauth")
}

var _ connector.OAuthAuthorizer = (*provider)(nil)

// The delegated Graph /me probe requires a profile-reading grant. Graph scope
// responses may use the qualified or short spelling of the same permission.
func (*provider) OAuthConnectionTestScopes() ([][]string, bool) {
	var alternatives [][]string
	for _, scope := range []string{"User.Read", "User.ReadWrite", "User.ReadBasic.All", "User.Read.All", "User.ReadWrite.All", "Directory.Read.All", "Directory.ReadWrite.All"} {
		alternatives = append(alternatives, []string{scope}, []string{"https://graph.microsoft.com/" + scope})
	}
	return alternatives, true
}
