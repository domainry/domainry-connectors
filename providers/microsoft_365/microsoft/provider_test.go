package microsoft

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
)

type recordingTransport struct {
	requests []connector.HTTPRequest
	respond  func(connector.HTTPRequest) (connector.HTTPResponse, error)
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, request)
	if t.respond != nil {
		return t.respond(request)
	}
	return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"value":[]}`)}, nil
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func TestTypedOperationsUseRuntimeHTTP(t *testing.T) {
	transport := &recordingTransport{}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	tests := []struct {
		operation, hash, path string
		input                 any
	}{{SyncCalendar.Key, SyncCalendar.ContractSHA256, "/me/events", SyncInput{}}, {SyncContacts.Key, SyncContacts.ContractSHA256, "/me/contacts", SyncInput{Limit: 50, SkipToken: "next"}}, {SyncOneDriveFileRefs.Key, SyncOneDriveFileRefs.ContractSHA256, "/me/drive/root/children", SyncInput{}}, {SyncOutlookMail.Key, SyncOutlookMail.ContractSHA256, "/me/messages", SyncInput{}}, {TestConnection.Key, TestConnection.ContractSHA256, "/me", struct{}{}}}
	for _, test := range tests {
		payload, _ := json.Marshal(test.input)
		_, callErr := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: test.operation, ContractSHA256: test.hash, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_token": "runtime-token"}, Payload: payload})
		if callErr != nil {
			t.Fatalf("operation=%s err=%v", test.operation, callErr)
		}
		request := transport.requests[len(transport.requests)-1]
		if !strings.Contains(request.URL, test.path) || request.SecretHeaders["Authorization"][0] != "Bearer runtime-token" || request.MaxResponseBytes != responseLimit {
			t.Fatalf("request=%+v", request)
		}
		raw, _ := json.Marshal(request)
		if strings.Contains(string(raw), "runtime-token") {
			t.Fatalf("request leaks token: %s", raw)
		}
	}
}
func TestUnauthorizedRefreshRotatesTokens(t *testing.T) {
	calls := 0
	transport := &recordingTransport{respond: func(request connector.HTTPRequest) (connector.HTTPResponse, error) {
		if strings.Contains(request.URL, "/oauth2/v2.0/token") {
			if request.SecretForm["refresh_token"] != "refresh" || request.SecretForm["client_id"] != "client" {
				t.Fatalf("refresh=%+v", request)
			}
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"access_token":"fresh","refresh_token":"rotated"}`)}, nil
		}
		calls++
		if request.SecretHeaders["Authorization"][0] == "Bearer stale" {
			return connector.HTTPResponse{StatusCode: 401, Body: []byte(`{}`)}, nil
		}
		return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"value":[]}`)}, nil
	}}
	adapter, _ := New(transport)
	payload, _ := json.Marshal(SyncInput{})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SyncCalendar.Key, ContractSHA256: SyncCalendar.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_token": "stale", "refresh_token": "refresh", "client_id": "client", "client_secret": "secret"}, Payload: payload})
	if err != nil || calls != 2 || result.SecretUpdates["access_token"] != "fresh" || result.SecretUpdates["refresh_token"] != "rotated" {
		t.Fatalf("result=%+v calls=%d err=%v", result, calls, err)
	}
}
func TestValidationAndReadFailures(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	validator := adapter.(connector.ConfigValidator)
	for _, config := range []map[string]any{{"tenant_id": ""}, {"tenant_id": "tenant", "graph_base_url": "http://remote.example"}, {"tenant_id": "tenant", "graph_base_url": "https://user:pass@graph.example"}, {"tenant_id": "tenant", "token_url": "ftp://token.example"}, {"tenant_id": "tenant", "timeout_seconds": 301}} {
		if validator.ValidateConfig(connector.Connection{Config: config}) == nil {
			t.Fatalf("accepted=%v", config)
		}
	}
	transport := &recordingTransport{respond: func(connector.HTTPRequest) (connector.HTTPResponse, error) {
		return connector.HTTPResponse{}, errors.New("reset")
	}}
	adapter, _ = New(transport)
	payload, _ := json.Marshal(SyncInput{})
	_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SyncCalendar.Key, ContractSHA256: SyncCalendar.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_token": "token"}, Payload: payload})
	classification, ok := connector.ErrorClassificationOf(err)
	if !ok || classification != connector.ErrorRetryable {
		t.Fatalf("classification=%q err=%v", classification, err)
	}
}
func validConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"tenant_id": "tenant", "graph_base_url": "http://127.0.0.1:8080/v1.0", "token_url": "http://127.0.0.1:8080/tenant/oauth2/v2.0/token", "scope": defaultScope, "timeout_seconds": 30}}
}
