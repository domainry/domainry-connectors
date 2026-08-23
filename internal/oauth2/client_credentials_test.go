package oauth2

import (
	"encoding/base64"
	"encoding/json"
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

func TestClientCredentialsSupportsRuntimeOnlyClientSecretBasic(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"access_token":"access"}`)}}
	_, err := ClientCredentials(t.Context(), transport, ClientCredentialsRequest{Endpoint: "http://localhost/token", ClientID: "client id", ClientSecret: "client:secret", ErrorPrefix: "ups", ClientAuthentication: ClientAuthenticationBasic})
	if err != nil {
		t.Fatal(err)
	}
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("client+id:client%3Asecret"))
	if transport.request.SecretHeaders["Authorization"][0] != want || transport.request.SecretForm != nil {
		t.Fatalf("request=%+v", transport.request)
	}
	public, _ := json.Marshal(transport.request)
	if strings.Contains(string(public), "client") || strings.Contains(string(public), "Basic") {
		t.Fatalf("serialized request leaks credentials: %s", public)
	}
}

func TestClientCredentialsRejectsUnknownAuthentication(t *testing.T) {
	_, err := ClientCredentials(t.Context(), &recordingTransport{}, ClientCredentialsRequest{Endpoint: "http://localhost/token", ClientID: "client", ClientSecret: "secret", ClientAuthentication: "unknown"})
	if err == nil {
		t.Fatal("unknown client authentication accepted")
	}
}
