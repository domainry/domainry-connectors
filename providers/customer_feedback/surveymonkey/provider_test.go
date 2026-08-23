package surveymonkey

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
func TestResponsesUseTypedPaginationAndRuntimeOnlyBearerToken(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"data":[]}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	input, _ := json.Marshal(ListResponsesInput{FormID: "survey-1", Since: "2026-07-01", After: "2", PageSize: 25})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListResponses.Key, ContractSHA256: ListResponses.ContractSHA256, Mode: connector.ModeCall, Connection: connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080"}}, Secrets: map[string]string{"access_token": "secret-token"}, Payload: input})
	if err != nil {
		t.Fatal(err)
	}
	request := transport.requests[0]
	if request.SecretHeaders["Authorization"][0] != "Bearer secret-token" || strings.Contains(request.URL, "secret-token") || !strings.Contains(request.URL, "/surveys/survey-1/responses/bulk?") || !strings.Contains(request.URL, "per_page=25") {
		t.Fatalf("request=%+v", request)
	}
	if len(result.Payload) == 0 {
		t.Fatal("empty result")
	}
}
func TestProbeFallbackIDAndFailures(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"id":"account-1"}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	connection := connector.Connection{Config: map[string]any{"base_url": "http://127.0.0.1:8080", "survey_id": "default"}}
	probe, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: connection, Secrets: map[string]string{"access_token": "token"}})
	if err != nil || !probe.Connected {
		t.Fatalf("probe=%+v error=%v", probe, err)
	}
	input, _ := json.Marshal(GetFormInput{})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: GetForm.Key, ContractSHA256: GetForm.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Secrets: map[string]string{"access_token": "token"}, Payload: input})
	if err != nil || !strings.Contains(string(result.Payload), "account-1") {
		t.Fatalf("result=%s error=%v", result.Payload, err)
	}
	transport.response = connector.HTTPResponse{StatusCode: http.StatusInternalServerError, Body: []byte(`{}`)}
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: GetForm.Key, ContractSHA256: GetForm.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Secrets: map[string]string{"access_token": "token"}, Payload: input})
	classification, ok := connector.ErrorClassificationOf(err)
	if !ok || classification != connector.ErrorRetryable {
		t.Fatalf("classification=%q/%v error=%v", classification, ok, err)
	}
}
func TestUnknownInputMissingIDSecretAndRemoteHTTPFailClosed(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	connection := connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080"}}
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: GetForm.Key, ContractSHA256: GetForm.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Secrets: map[string]string{"access_token": "token"}, Payload: []byte(`{"ignored":true}`)})
	if err == nil {
		t.Fatal("unknown field accepted")
	}
	input, _ := json.Marshal(GetFormInput{})
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: GetForm.Key, ContractSHA256: GetForm.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Secrets: map[string]string{"access_token": "token"}, Payload: input})
	if code, _ := connector.ProviderErrorCodeOf(err); code != "surveymonkey.survey_id_required" {
		t.Fatalf("error=%v", err)
	}
	if err := adapter.(connector.ConfigValidator).ValidateConfig(connector.Connection{Config: map[string]any{"base_url": "http://remote.example"}}); err == nil {
		t.Fatal("remote HTTP accepted")
	}
}
