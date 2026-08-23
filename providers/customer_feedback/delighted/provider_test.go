package delighted

import (
	"context"
	"encoding/base64"
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
func TestResponsesUseBasicSecretHeaderAndTypedTimeRange(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`[{"id":"response-1"}]`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	input, _ := json.Marshal(ListResponsesInput{Since: "2026-07-01T00:00:00Z", Until: "1783814400", After: "2", PageSize: 25})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListResponses.Key, ContractSHA256: ListResponses.ContractSHA256, Mode: connector.ModeCall, Connection: connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080"}}, Secrets: map[string]string{"api_key": "project-key"}, Payload: input})
	if err != nil {
		t.Fatal(err)
	}
	request := transport.requests[0]
	expected := "Basic " + base64.StdEncoding.EncodeToString([]byte("project-key:"))
	if request.SecretHeaders["Authorization"][0] != expected || strings.Contains(request.URL, "project-key") || !strings.Contains(request.URL, "since=1782864000") || !strings.Contains(request.URL, "per_page=25") {
		t.Fatalf("request=%+v", request)
	}
	if !strings.Contains(string(result.Payload), "response-1") {
		t.Fatalf("result=%s", result.Payload)
	}
}
func TestProbeAndFailureClassification(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"nps":50}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	connection := connector.Connection{Config: map[string]any{"base_url": "http://127.0.0.1:8080"}}
	probe, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: connection, Secrets: map[string]string{"api_key": "key"}})
	if err != nil || !probe.Connected {
		t.Fatalf("probe=%+v error=%v", probe, err)
	}
	transport.response = connector.HTTPResponse{StatusCode: http.StatusTooManyRequests, Body: []byte(`{}`)}
	input, _ := json.Marshal(ListResponsesInput{})
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListResponses.Key, ContractSHA256: ListResponses.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Secrets: map[string]string{"api_key": "key"}, Payload: input})
	classification, ok := connector.ErrorClassificationOf(err)
	if !ok || classification != connector.ErrorRetryable {
		t.Fatalf("classification=%q/%v error=%v", classification, ok, err)
	}
}
func TestUnknownInputInvalidTimeSecretAndConfigFailClosed(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`[]`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	connection := connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080"}}
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListResponses.Key, ContractSHA256: ListResponses.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Secrets: map[string]string{"api_key": "key"}, Payload: []byte(`{"ignored":true}`)})
	if err == nil {
		t.Fatal("unknown input accepted")
	}
	input, _ := json.Marshal(ListResponsesInput{Since: "bad"})
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListResponses.Key, ContractSHA256: ListResponses.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Secrets: map[string]string{"api_key": "key"}, Payload: input})
	if code, _ := connector.ProviderErrorCodeOf(err); code != "delighted.time_invalid" {
		t.Fatalf("error=%v", err)
	}
	input, _ = json.Marshal(ListResponsesInput{})
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListResponses.Key, ContractSHA256: ListResponses.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Payload: input})
	if code, _ := connector.ProviderErrorCodeOf(err); code != "delighted.api_key_required" {
		t.Fatalf("error=%v", err)
	}
	if err := adapter.(connector.ConfigValidator).ValidateConfig(connector.Connection{Config: map[string]any{"base_url": "http://remote.example"}}); err == nil {
		t.Fatal("remote HTTP accepted")
	}
}
