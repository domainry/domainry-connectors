package oidc

import (
	"context"
	"encoding/json"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
	"net/http"
	"strings"
	"testing"
)

type recordingTransport struct {
	requests []connector.HTTPRequest
	respond  func(connector.HTTPRequest) (connector.HTTPResponse, error)
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, r connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, r)
	if t.respond != nil {
		return t.respond(r)
	}
	return connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"users":[{"id":"user-1"}]}`)}, nil
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func TestDiscoveryAndDirectoryUseRuntimeTransport(t *testing.T) {
	transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		if strings.Contains(r.URL, "openid-configuration") {
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"issuer":"http://127.0.0.1:8080","authorization_endpoint":"https://id.example/authorize","token_endpoint":"https://id.example/token","jwks_uri":"https://id.example/jwks"}`)}, nil
		}
		return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"users":[{"id":"user-1"}]}`)}, nil
	}}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	tester := adapter.(connector.ConnectionTester)
	tested, err := tester.TestConnection(t.Context(), connector.TestConnectionRequest{Connection: validConnection()})
	if err != nil || !tested.Connected {
		t.Fatalf("tested=%+v err=%v", tested, err)
	}
	payload, _ := json.Marshal(ListUsersInput{})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListUsers.Key, ContractSHA256: ListUsers.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_token": "runtime-token"}, Payload: payload})
	if err != nil {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	request := transport.requests[1]
	if request.Headers["Authorization"] != nil || request.SecretHeaders["Authorization"][0] != "Bearer runtime-token" {
		t.Fatalf("request=%+v", request)
	}
	raw, _ := json.Marshal(request)
	if strings.Contains(string(raw), "runtime-token") {
		t.Fatalf("request leaks token: %s", raw)
	}
}
func TestEndpointPolicyAndFailures(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	validator := adapter.(connector.ConfigValidator)
	for _, issuer := range []string{"http://remote.example", "https://user:password@id.example", "relative"} {
		connection := validConnection()
		connection.Config["issuer"] = issuer
		if validator.ValidateConfig(connection) == nil {
			t.Fatalf("accepted issuer=%q", issuer)
		}
	}
	connection := validConnection()
	connection.Config["list_users_url"] = "http://remote.example/users"
	payload, _ := json.Marshal(ListUsersInput{})
	_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListUsers.Key, ContractSHA256: ListUsers.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Secrets: map[string]string{"access_token": "token"}, Payload: payload})
	if err == nil {
		t.Fatal("accepted remote plaintext directory endpoint")
	}
}
func validConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"issuer": "http://127.0.0.1:8080", "client_id": "client", "redirect_url": "http://127.0.0.1:8081/callback", "list_users_url": "/users"}, SecretRefs: map[string]string{"client_secret": "secret:client", "access_token": "secret:access"}}
}
