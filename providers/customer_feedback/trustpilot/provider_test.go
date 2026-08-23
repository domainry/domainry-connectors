package trustpilot

import (
	"context"
	"encoding/json"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"net/http"
	"strings"
	"testing"
)

type recordingTransport struct {
	requests []connector.HTTPRequest
	response connector.HTTPResponse
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, request)
	return t.response, nil
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func TestListResponsesUsesStrictInputAndRuntimeOnlyAPIKey(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"reviews":[{"id":"review-1"}]}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	input, _ := json.Marshal(ListResponsesInput{Since: "2026-07-01T00:00:00Z", After: "2", PageSize: 25})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListResponses.Key, ContractSHA256: ListResponses.ContractSHA256, Mode: connector.ModeCall, Connection: connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080", "business_unit_id": "unit-1"}}, Secrets: map[string]string{"api_key": "secret-key"}, Payload: input})
	if err != nil {
		t.Fatal(err)
	}
	request := transport.requests[0]
	if request.SecretHeaders["Apikey"][0] != "secret-key" || strings.Contains(request.URL, "secret-key") || !strings.Contains(request.URL, "perPage=25") || !strings.Contains(request.URL, "page=2") {
		t.Fatalf("request=%+v", request)
	}
	var output map[string]any
	if json.Unmarshal(result.Payload, &output) != nil || output["items"] == nil {
		t.Fatalf("output=%s", result.Payload)
	}
}
func TestProfileProbeAndFailures(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"companyName":"One"}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	connection := connector.Connection{Config: map[string]any{"base_url": "http://127.0.0.1:8080", "business_unit_id": "unit-1"}}
	probe, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: connection, Secrets: map[string]string{"api_key": "key"}})
	if err != nil || !probe.Connected {
		t.Fatalf("probe=%+v error=%v", probe, err)
	}
	transport.response = connector.HTTPResponse{StatusCode: http.StatusInternalServerError, Body: []byte(`{}`)}
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: GetForm.Key, ContractSHA256: GetForm.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Secrets: map[string]string{"api_key": "key"}, Payload: []byte(`{}`)})
	classification, ok := connector.ErrorClassificationOf(err)
	if !ok || classification != connector.ErrorRetryable {
		t.Fatalf("classification=%q/%v error=%v", classification, ok, err)
	}
}
func TestInputConfigAndSecretFailClosed(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"reviews":[]}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	connection := connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080", "business_unit_id": "unit"}}
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListResponses.Key, ContractSHA256: ListResponses.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Secrets: map[string]string{"api_key": "key"}, Payload: []byte(`{"ignored":true}`)})
	if err == nil {
		t.Fatal("unknown input accepted")
	}
	input, _ := json.Marshal(ListResponsesInput{})
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListResponses.Key, ContractSHA256: ListResponses.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Payload: input})
	if code, _ := connector.ProviderErrorCodeOf(err); code != "trustpilot.api_key_required" {
		t.Fatalf("error=%v", err)
	}
	if err := adapter.(connector.ConfigValidator).ValidateConfig(connector.Connection{Config: map[string]any{"base_url": "http://remote.example", "business_unit_id": "unit"}}); err == nil {
		t.Fatal("remote HTTP accepted")
	}
}
