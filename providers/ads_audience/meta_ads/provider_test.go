package metaads

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
)

type recordingTransport struct {
	requests []connector.HTTPRequest
	response connector.HTTPResponse
	err      error
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, request)
	return t.response, t.err
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestListCustomAudiencesUsesStrictQueryAndRuntimeOnlyToken(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"data":[{"id":"audience-1"}]}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(ListCustomAudiencesInput{Fields: "id", Limit: 10, After: "next", Before: "previous"})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListCustomAudiences.Key, ContractSHA256: ListCustomAudiences.ContractSHA256, Mode: connector.ModeCall, Connection: connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080", "ad_account_id": "42"}}, Secrets: map[string]string{"access_token": "meta-token"}, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	request := transport.requests[0]
	if request.SecretHeaders["Authorization"][0] != "Bearer meta-token" || strings.Contains(request.URL, "meta-token") {
		t.Fatalf("request=%+v", request)
	}
	if !strings.Contains(request.URL, "/act_42/customaudiences?") || !strings.Contains(request.URL, "fields=id") || !strings.Contains(request.URL, "limit=10") {
		t.Fatalf("url=%s", request.URL)
	}
	if len(result.Payload) == 0 {
		t.Fatal("empty result")
	}
}

func TestConnectionAndFailureClassification(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"id":"act_42"}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	connection := connector.Connection{Config: map[string]any{"base_url": "http://127.0.0.1:8080", "ad_account_id": "act_42"}}
	probe, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: connection, Secrets: map[string]string{"access_token": "token"}})
	if err != nil || !probe.Connected {
		t.Fatalf("probe=%+v error=%v", probe, err)
	}
	transport.response = connector.HTTPResponse{StatusCode: http.StatusTooManyRequests, Body: []byte(`{"error":{"code":4}}`)}
	payload, _ := json.Marshal(ListCustomAudiencesInput{})
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListCustomAudiences.Key, ContractSHA256: ListCustomAudiences.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Secrets: map[string]string{"access_token": "token"}, Payload: payload})
	classification, ok := connector.ErrorClassificationOf(err)
	if !ok || classification != connector.ErrorRetryable {
		t.Fatalf("classification=%q/%v error=%v", classification, ok, err)
	}
	if err := adapter.(connector.ConfigValidator).ValidateConfig(connector.Connection{Config: map[string]any{"base_url": "http://remote.example", "ad_account_id": "42"}}); err == nil {
		t.Fatal("remote HTTP endpoint accepted")
	}
}

func TestUnknownInputAndMissingSecretFailClosed(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"data":[]}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	connection := connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080", "ad_account_id": "42"}}
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListCustomAudiences.Key, ContractSHA256: ListCustomAudiences.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Secrets: map[string]string{"access_token": "token"}, Payload: []byte(`{"ignored":"value"}`)})
	if err == nil {
		t.Fatal("unknown input field accepted")
	}
	payload, _ := json.Marshal(ListCustomAudiencesInput{})
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListCustomAudiences.Key, ContractSHA256: ListCustomAudiences.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Payload: payload})
	if code, _ := connector.ProviderErrorCodeOf(err); code != "meta_ads.access_token_required" {
		t.Fatalf("error=%v", err)
	}
}
