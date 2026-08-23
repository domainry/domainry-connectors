package fadada

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
	business int
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, request)
	if strings.HasSuffix(request.URL, "/service/get-access-token") {
		return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"code":"100000","data":{"accessToken":"access-token"}}`)}, nil
	}
	t.business++
	return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"code":"100000","data":{"signTaskId":"task-1","actorSignTaskUrl":"https://sign.example/task-1"}}`)}, nil
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func TestDescriptorRoutingAndSignatureBoundary(t *testing.T) {
	transport := &recordingTransport{}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	operations := []struct {
		key, hash string
		input     any
	}{{CreateEnvelope.Key, CreateEnvelope.ContractSHA256, CreateEnvelopeInput{Input: map[string]any{"signTaskSubject": "Contract"}}}, {GetEnvelope.Key, GetEnvelope.ContractSHA256, EnvelopeInput{EnvelopeID: "task-1"}}, {SendEnvelope.Key, SendEnvelope.ContractSHA256, EnvelopeInput{EnvelopeID: "task-1"}}, {VoidEnvelope.Key, VoidEnvelope.ContractSHA256, VoidEnvelopeInput{EnvelopeID: "task-1", Reason: "superseded"}}, {CreateRecipientView.Key, CreateRecipientView.ContractSHA256, RecipientViewInput{EnvelopeID: "task-1", Input: map[string]any{"actorId": "actor-1"}}}, {TestConnection.Key, TestConnection.ContractSHA256, struct{}{}}}
	for _, op := range operations {
		payload, _ := json.Marshal(op.input)
		result, callErr := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: op.key, ContractSHA256: op.hash, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"app_secret": "top-secret"}, Payload: payload})
		if callErr != nil || !strings.HasPrefix(result.ResponseRef, "fadada:") {
			t.Fatalf("operation=%s result=%+v err=%v", op.key, result, callErr)
		}
	}
	if transport.business != 5 {
		t.Fatalf("business=%d", transport.business)
	}
	for _, request := range transport.requests {
		if request.Headers["X-FASC-Sign"] != nil || len(request.SecretHeaders["X-FASC-Sign"]) != 1 {
			t.Fatalf("signature boundary=%+v", request)
		}
		encoded, _ := json.Marshal(request)
		if strings.Contains(string(encoded), "top-secret") || strings.Contains(string(encoded), request.SecretHeaders["X-FASC-Sign"][0]) {
			t.Fatal("secret material leaked through JSON")
		}
	}
}
func TestValidationAndWriteUncertainty(t *testing.T) {
	adapter, _ := New(&failingTransport{})
	if err := adapter.(connector.ConfigValidator).ValidateConfig(connector.Connection{Config: map[string]any{"base_url": "http://api.example.test", "app_id": "app"}}); err == nil {
		t.Fatal("insecure remote endpoint accepted")
	}
	payload, _ := json.Marshal(CreateEnvelopeInput{Input: map[string]any{"name": "Contract"}})
	_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CreateEnvelope.Key, ContractSHA256: CreateEnvelope.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"app_secret": "secret"}, Payload: payload})
	if err == nil {
		t.Fatal("network failure accepted")
	}
}

type failingTransport struct{ calls int }

func (t *failingTransport) RoundTripHTTP(context.Context, connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.calls++
	if t.calls == 1 {
		return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"code":"100000","data":{"accessToken":"token"}}`)}, nil
	}
	return connector.HTTPResponse{}, errors.New("timeout")
}
func (*failingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func validConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "https://api.example.test", "app_id": "app-1", "timeout_seconds": 30}}
}
