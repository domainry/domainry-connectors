package oauth2

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/url"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
)

func authorizationRequest() connector.OAuthAuthorizationRequest {
	sum := sha256.Sum256([]byte(strings.Repeat("v", 43)))
	return connector.OAuthAuthorizationRequest{ClientID: "client", RedirectURI: "https://product.example.test/integration/oauth/callback", Scopes: []string{"calendar.read", "mail.read"}, State: strings.Repeat("s", 43), CodeChallenge: base64.RawURLEncoding.EncodeToString(sum[:])}
}
func exchangeRequest() connector.OAuthCodeExchangeRequest {
	return connector.OAuthCodeExchangeRequest{ClientID: "client", ClientSecret: "private-client-secret", Code: "private-code", CodeVerifier: strings.Repeat("v", 43), RedirectURI: authorizationRequest().RedirectURI, RequestedScopes: authorizationRequest().Scopes}
}
func TestAuthorizationURLKeepsFixedFlowAndPKCE(t *testing.T) {
	request := authorizationRequest()
	raw, err := AuthorizationURL("https://issuer.example.test/authorize", request, url.Values{"response_type": {"token"}, "code_challenge_method": {"plain"}, "client_id": {"wrong"}})
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := url.Parse(raw)
	q := parsed.Query()
	if q.Get("response_type") != "code" || q.Get("code_challenge_method") != "S256" || q.Get("code_challenge") != request.CodeChallenge || q.Get("client_id") != "client" || q.Get("state") != request.State {
		t.Fatal("authorization parameters not fixed")
	}
	for name, mutate := range map[string]func(*connector.OAuthAuthorizationRequest){"state": func(r *connector.OAuthAuthorizationRequest) { r.State = "" }, "challenge": func(r *connector.OAuthAuthorizationRequest) { r.CodeChallenge = "plain" }, "fragment": func(r *connector.OAuthAuthorizationRequest) { r.RedirectURI += "#code" }, "insecure": func(r *connector.OAuthAuthorizationRequest) { r.RedirectURI = "http://remote.example.test/callback" }, "userinfo": func(r *connector.OAuthAuthorizationRequest) {
		r.RedirectURI = "https://attacker@product.example.test/callback"
	}, "scope": func(r *connector.OAuthAuthorizationRequest) { r.Scopes = []string{"read write"} }, "empty-scopes": func(r *connector.OAuthAuthorizationRequest) { r.Scopes = nil }} {
		t.Run(name, func(t *testing.T) {
			copy := authorizationRequest()
			mutate(&copy)
			if _, err := AuthorizationURL("https://issuer.example.test/authorize", copy, nil); err == nil {
				t.Fatal("invalid authorization accepted")
			}
		})
	}
}
func TestCodeExchangeUsesPrivateTransportFieldsAndActualScopes(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"access_token":"private-access","refresh_token":"private-refresh","token_type":"Bearer","expires_in":3600,"scope":"calendar.read"}`)}}
	request := exchangeRequest()
	tokens, err := ExchangeAuthorizationCode(t.Context(), transport, "https://issuer.example.test/token", request, "probe.oauth")
	if err != nil || tokens.AccessToken != "private-access" || tokens.RefreshToken != "private-refresh" || tokens.ExpiresInSeconds != 3600 || len(tokens.GrantedScopes) != 1 || tokens.GrantedScopes[0] != "calendar.read" {
		t.Fatal("partial grant was not retained", err)
	}
	for key, want := range map[string]string{"client_id": request.ClientID, "client_secret": request.ClientSecret, "code": request.Code, "code_verifier": request.CodeVerifier} {
		if transport.request.SecretForm[key] != want {
			t.Fatalf("missing private field %s", key)
		}
	}
	raw, _ := json.Marshal(transport.request)
	if strings.Contains(string(raw), "private-") || strings.Contains(string(raw), request.CodeVerifier) {
		t.Fatal("public transport leaked OAuth material")
	}
	if transport.request.MaxResponseBytes != 1<<20 {
		t.Fatal("OAuth response unbounded")
	}
	public, _ := json.Marshal(tokens)
	if strings.Contains(string(public), "private-") {
		t.Fatal("public tokens leaked material")
	}
	transport.response.Body = []byte(`{"access_token":"access","token_type":"Bearer"}`)
	tokens, err = ExchangeAuthorizationCode(t.Context(), transport, "https://issuer.example.test/token", request, "probe.oauth")
	if err != nil || len(tokens.GrantedScopes) != 2 {
		t.Fatal("RFC 6749 omitted scope did not retain requested scope", err)
	}
}
func TestCodeExchangeSeparatesRejectionFromUnknownWithoutResponseLeaks(t *testing.T) {
	for _, test := range []struct {
		name           string
		status         int
		body           string
		classification connector.ErrorClassification
	}{
		{"rejected", 400, `{"error":"invalid_grant","error_description":"private-code"}`, connector.ErrorPermanent},
		{"malformed-success", 200, `private-access`, connector.ErrorUncertain},
		{"missing-token", 200, `{"token_type":"Bearer"}`, connector.ErrorUncertain},
		{"bad-type", 200, `{"access_token":"private-access","token_type":"mac"}`, connector.ErrorUncertain},
		{"bad-expiry", 200, `{"access_token":"private-access","token_type":"Bearer","expires_in":-1}`, connector.ErrorUncertain},
		{"server-error", 503, `{"token":"private-access"}`, connector.ErrorUncertain},
		{"redirect", 302, `private-access`, connector.ErrorUncertain},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: test.status, Body: []byte(test.body)}}
			_, err := ExchangeAuthorizationCode(t.Context(), transport, "https://issuer.example.test/token", exchangeRequest(), "probe.oauth")
			kind, _ := connector.ErrorClassificationOf(err)
			if err == nil || kind != test.classification || strings.Contains(err.Error(), "private-") {
				t.Fatalf("classification=%s err=%v", kind, err)
			}
		})
	}
	request := exchangeRequest()
	request.CodeVerifier = "too-short"
	transport := &recordingTransport{}
	if _, err := ExchangeAuthorizationCode(t.Context(), transport, "https://issuer.example.test/token", request, "probe.oauth"); err == nil || transport.request.Method != "" {
		t.Fatal("invalid PKCE reached transport")
	}
}
