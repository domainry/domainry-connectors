package signnow

import (
	"context"
	"encoding/json"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
	"strings"
	"testing"
)

type recordingTransport struct {
	requests []connector.HTTPRequest
	err      error
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, r connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, r)
	if t.err != nil {
		return connector.HTTPResponse{}, t.err
	}
	return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"id":"doc-1"}`)}, nil
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func TestDescriptorRoutingAndSecretBoundary(t *testing.T) {
	tr := &recordingTransport{}
	adapter, err := New(tr)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	ops := []struct {
		key, hash string
		input     any
	}{{GetEnvelope.Key, GetEnvelope.ContractSHA256, EnvelopeInput{EnvelopeID: "doc-1"}}, {SendEnvelope.Key, SendEnvelope.ContractSHA256, SendEnvelopeInput{EnvelopeID: "doc-1", Input: map[string]any{"to": "signer@example.test"}}}, {VoidEnvelope.Key, VoidEnvelope.ContractSHA256, VoidEnvelopeInput{EnvelopeID: "doc-1", Reason: "superseded"}}, {CreateRecipientView.Key, CreateRecipientView.ContractSHA256, RecipientViewInput{EnvelopeID: "doc-1", Input: map[string]any{"field_invite_id": "invite-1", "auth_method": "none"}}}, {TestConnection.Key, TestConnection.ContractSHA256, struct{}{}}}
	for _, op := range ops {
		raw, _ := json.Marshal(op.input)
		result, callErr := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: op.key, ContractSHA256: op.hash, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_token": "top-secret"}, Payload: raw})
		if callErr != nil || result.ResponseRef != "signnow:doc-1" {
			t.Fatalf("operation=%s result=%+v err=%v", op.key, result, callErr)
		}
	}
	for _, request := range tr.requests {
		encoded, _ := json.Marshal(request)
		if strings.Contains(string(encoded), "top-secret") || request.SecretHeaders["Authorization"][0] != "Bearer top-secret" {
			t.Fatalf("secret boundary=%+v", request)
		}
	}
	if strings.Contains(string(tr.requests[3].Body), "field_invite_id") {
		t.Fatal("route-only invite id leaked into body")
	}
}
func TestValidationAndWriteUncertainty(t *testing.T) {
	tr := &recordingTransport{err: errors.New("timeout")}
	adapter, _ := New(tr)
	if err := adapter.(connector.ConfigValidator).ValidateConfig(connector.Connection{Config: map[string]any{"base_url": "http://api.example.test"}}); err == nil {
		t.Fatal("insecure endpoint accepted")
	}
	raw, _ := json.Marshal(SendEnvelopeInput{EnvelopeID: "doc-1", Input: map[string]any{"to": "signer@example.test"}})
	_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SendEnvelope.Key, ContractSHA256: SendEnvelope.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_token": "token"}, Payload: raw})
	if err == nil {
		t.Fatal("write transport failure accepted")
	}
}
func validConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "https://api.signnow.com", "timeout_seconds": 30}}
}
