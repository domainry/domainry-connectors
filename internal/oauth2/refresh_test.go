package oauth2

import (
	"context"
	"errors"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
)

type recordingTransport struct {
	request  connector.HTTPRequest
	response connector.HTTPResponse
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.request = request
	return t.response, nil
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestRefreshKeepsCredentialsInRuntimeOnlyFields(t *testing.T) {
	for _, authentication := range []ClientAuthentication{ClientAuthenticationForm, ClientAuthenticationBasic} {
		t.Run(string(authentication), func(t *testing.T) {
			transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"access_token":"new-access","refresh_token":"new-refresh"}`)}}
			token, err := Refresh(t.Context(), transport, RefreshRequest{Endpoint: "http://localhost/token", RefreshToken: "refresh-secret", ClientID: "client-secret-id", ClientSecret: "client-secret", ClientAuthentication: authentication, ErrorPrefix: "provider"})
			if err != nil || token.AccessToken != "new-access" || token.RefreshToken != "new-refresh" {
				t.Fatalf("token=%+v error=%v", token, err)
			}
			serialized := string(transport.request.Body) + transport.request.URL
			if strings.Contains(serialized, "refresh-secret") || strings.Contains(serialized, "client-secret") {
				t.Fatalf("public request leaked credentials: %+v", transport.request)
			}
			if authentication == ClientAuthenticationForm && transport.request.SecretForm["client_secret"] != "client-secret" {
				t.Fatalf("secret form=%v", transport.request.SecretForm)
			}
			if authentication == ClientAuthenticationBasic && len(transport.request.SecretHeaders["Authorization"]) != 1 {
				t.Fatalf("secret headers=%v", transport.request.SecretHeaders)
			}
		})
	}
}

func TestRefreshAllowsExplicitPublicClientWithoutSecret(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"access_token":"new-access"}`)}}
	token, err := Refresh(t.Context(), transport, RefreshRequest{
		Endpoint: "http://localhost/token", RefreshToken: "refresh-secret", ClientID: "public-client",
		ClientSecretOptional: true, ClientAuthentication: ClientAuthenticationForm, ErrorPrefix: "google",
	})
	if err != nil || token.AccessToken != "new-access" {
		t.Fatalf("token=%+v error=%v", token, err)
	}
	if _, exists := transport.request.SecretForm["client_secret"]; exists || transport.request.SecretForm["client_id"] != "public-client" {
		t.Fatalf("secret form=%v", transport.request.SecretForm)
	}
}

func TestRefreshDoesNotWeakenConfidentialClientDefault(t *testing.T) {
	_, err := Refresh(t.Context(), &recordingTransport{}, RefreshRequest{
		Endpoint: "http://localhost/token", RefreshToken: "refresh-secret", ClientID: "client",
		ClientAuthentication: ClientAuthenticationForm, ErrorPrefix: "provider",
	})
	if err == nil {
		t.Fatal("missing confidential-client secret accepted")
	}
}
