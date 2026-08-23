package adobesign

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
	requests  []connector.HTTPRequest
	responses []connector.HTTPResponse
	err       error
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, request)
	if t.err != nil {
		return connector.HTTPResponse{}, t.err
	}
	response := connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"id":"agr-1"}`)}
	if len(t.responses) >= len(t.requests) {
		response = t.responses[len(t.requests)-1]
	}
	return response, nil
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestDescriptorRoutingAndSecretBoundary(t *testing.T) {
	transport := &recordingTransport{}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	operations := []struct {
		key, hash string
		input     any
	}{
		{CreateEnvelope.Key, CreateEnvelope.ContractSHA256, CreateEnvelopeInput{Input: map[string]any{"name": "Agreement"}}},
		{GetEnvelope.Key, GetEnvelope.ContractSHA256, EnvelopeInput{EnvelopeID: "agr-1"}},
		{SendEnvelope.Key, SendEnvelope.ContractSHA256, EnvelopeInput{EnvelopeID: "agr-1"}},
		{VoidEnvelope.Key, VoidEnvelope.ContractSHA256, VoidEnvelopeInput{EnvelopeID: "agr-1", Reason: "superseded"}},
		{CreateRecipientView.Key, CreateRecipientView.ContractSHA256, EnvelopeInput{EnvelopeID: "agr-1"}},
		{ListEnvelopeStatusChanges.Key, ListEnvelopeStatusChanges.ContractSHA256, ListEnvelopeStatusChangesInput{PageSize: 20, Cursor: "next", Query: "status"}},
		{TestConnection.Key, TestConnection.ContractSHA256, struct{}{}},
	}
	for _, operation := range operations {
		payload, _ := json.Marshal(operation.input)
		result, callErr := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: operation.key, ContractSHA256: operation.hash, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_token": "top-secret"}, Payload: payload})
		if callErr != nil || result.ResponseRef != "adobe_sign:agr-1" {
			t.Fatalf("operation=%s result=%+v err=%v", operation.key, result, callErr)
		}
	}
	for _, request := range transport.requests {
		if request.Headers["Authorization"] != nil || request.SecretHeaders["Authorization"][0] != "Bearer top-secret" {
			t.Fatalf("secret boundary=%+v", request)
		}
		encoded, _ := json.Marshal(request)
		if strings.Contains(string(encoded), "top-secret") {
			t.Fatal("access token leaked through JSON")
		}
	}
	if query := transport.requests[5].URL; !strings.Contains(query, "cursor=next") || !strings.Contains(query, "pageSize=20") || !strings.Contains(query, "query=status") {
		t.Fatalf("query=%s", query)
	}
}

func TestValidationAndWriteFailureClassification(t *testing.T) {
	transport := &recordingTransport{err: errors.New("timeout")}
	adapter, _ := New(transport)
	validator := adapter.(connector.ConfigValidator)
	for _, endpoint := range []string{"http://example.test/api/rest/v6", "https://example.test/api/rest/v5", "https://user@example.test/api/rest/v6?bad=1"} {
		if err := validator.ValidateConfig(connector.Connection{Config: map[string]any{"base_url": endpoint}}); err == nil {
			t.Fatalf("accepted endpoint=%s", endpoint)
		}
	}
	payload, _ := json.Marshal(CreateEnvelopeInput{Input: map[string]any{"name": "Agreement"}})
	_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CreateEnvelope.Key, ContractSHA256: CreateEnvelope.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_token": "token"}, Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "delivery_uncertain") {
		t.Fatalf("write error=%v", err)
	}
}

func validConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "https://example.test/api/rest/v6", "timeout_seconds": 30}}
}
