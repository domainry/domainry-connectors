package oauth2

import (
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
)

func TestClientCredentialsKeepsCredentialsRuntimeOnly(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"access_token":"access"}`)}}
	token, err := ClientCredentials(t.Context(), transport, ClientCredentialsRequest{Endpoint: "http://localhost/token", ClientID: "client-id", ClientSecret: "client-secret", Scope: "shipment", ErrorPrefix: "fedex"})
	if err != nil || token.AccessToken != "access" {
		t.Fatalf("token=%+v err=%v", token, err)
	}
	public := transport.request.URL + string(transport.request.Body)
	if strings.Contains(public, "client-id") || strings.Contains(public, "client-secret") || transport.request.SecretForm["client_id"] != "client-id" || transport.request.SecretForm["client_secret"] != "client-secret" {
		t.Fatalf("request=%+v", transport.request)
	}
	if transport.request.MaxResponseBytes != tokenResponseLimit {
		t.Fatalf("response limit=%d", transport.request.MaxResponseBytes)
	}
}
