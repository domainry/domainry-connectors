package microsoft

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"net/url"
	"strings"
	"testing"
)

func TestOAuthAuthorizationCodeUsesRegistryAndPrivateHostTransport(t *testing.T) {
	transport := &recordingTransport{respond: func(request connector.HTTPRequest) (connector.HTTPResponse, error) {
		if request.SecretForm["code"] != "private-code" || request.SecretForm["client_secret"] != "private-secret" {
			t.Fatal("code exchange lost private material")
		}
		return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"access_token":"private-access","refresh_token":"private-refresh","token_type":"Bearer","expires_in":3600}`)}, nil
	}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	registry := connector.NewRegistry()
	if err = registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()
	frozen, _ := registry.Provider(ConnectorKey, ProviderKey)
	authorizer, ok := connector.ResolveOAuthAuthorizer(frozen)
	if !ok {
		t.Fatal("registry omitted OAuth")
	}
	verifier := strings.Repeat("v", 43)
	sum := sha256.Sum256([]byte(verifier))
	connection := connector.Connection{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Config: map[string]any{"tenant_id": "organizations"}}
	scopes := []string{"Calendars.Read", "Mail.Read", "offline_access"}
	request := connector.OAuthAuthorizationRequest{Connection: connection, ClientID: "client", RedirectURI: "http://127.0.0.1:9876/integration/oauth/callback", Scopes: scopes, State: strings.Repeat("s", 43), CodeChallenge: base64.RawURLEncoding.EncodeToString(sum[:])}
	raw, err := authorizer.AuthorizationURL(request)
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := url.Parse(raw)
	if parsed.Host != "login.microsoftonline.com" || parsed.Query().Get("code_challenge_method") != "S256" || parsed.Query().Get("state") != request.State {
		t.Fatal("authorization URL mismatch")
	}
	if len(transport.requests) != 0 {
		t.Fatal("building URL performed I/O")
	}
	exchange := connector.OAuthCodeExchangeRequest{Connection: connection, ClientID: "client", ClientSecret: "private-secret", Code: "private-code", CodeVerifier: verifier, RedirectURI: request.RedirectURI, RequestedScopes: scopes}
	tokens, err := authorizer.ExchangeAuthorizationCode(t.Context(), exchange)
	if err != nil || tokens.AccessToken != "private-access" || len(transport.requests) != 1 {
		t.Fatal("single code exchange failed", err)
	}
	rawRequest, _ := json.Marshal(transport.requests[0])
	if strings.Contains(string(rawRequest), "private-") || strings.Contains(string(rawRequest), verifier) {
		t.Fatal("public transport exposed code or token")
	}
	transport.respond = func(connector.HTTPRequest) (connector.HTTPResponse, error) {
		return connector.HTTPResponse{}, errors.New("synthetic dropped response")
	}
	_, err = authorizer.ExchangeAuthorizationCode(t.Context(), exchange)
	kind, _ := connector.ErrorClassificationOf(err)
	if kind != connector.ErrorUncertain || len(transport.requests) != 2 {
		t.Fatal("ambiguous code exchange retried or misclassified", err)
	}
	connection.ProviderKey = "other"
	request.Connection = connection
	if _, err = authorizer.AuthorizationURL(request); err == nil {
		t.Fatal("provider mix-up accepted")
	}
}

func TestDeclaredConnectionTestScopes(t *testing.T) {
	values, declared := (&provider{}).OAuthConnectionTestScopes()
	want := [][]string{{"User.Read"}, {"https://graph.microsoft.com/User.Read"}, {"User.ReadWrite"}, {"https://graph.microsoft.com/User.ReadWrite"}, {"User.ReadBasic.All"}, {"https://graph.microsoft.com/User.ReadBasic.All"}, {"User.Read.All"}, {"https://graph.microsoft.com/User.Read.All"}, {"User.ReadWrite.All"}, {"https://graph.microsoft.com/User.ReadWrite.All"}, {"Directory.Read.All"}, {"https://graph.microsoft.com/Directory.Read.All"}, {"Directory.ReadWrite.All"}, {"https://graph.microsoft.com/Directory.ReadWrite.All"}}
	gotRaw, _ := json.Marshal(values)
	wantRaw, _ := json.Marshal(want)
	if !declared || string(gotRaw) != string(wantRaw) {
		t.Fatalf("probe scope requirements=%s", gotRaw)
	}
	values[0][0] = "changed"
	again, _ := (&provider{}).OAuthConnectionTestScopes()
	if again[0][0] == "changed" {
		t.Fatal("caller mutated provider scope declaration")
	}
}
