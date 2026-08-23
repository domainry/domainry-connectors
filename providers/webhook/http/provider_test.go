package http

import (
	"context"
	"encoding/json"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
	stdhttp "net/http"
	"strings"
	"testing"
	"time"
)

type recordingTransport struct {
	requests []connector.HTTPRequest
	response connector.HTTPResponse
	err      error
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, r connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, r)
	if t.err != nil {
		return connector.HTTPResponse{}, t.err
	}
	if t.response.StatusCode != 0 {
		return t.response, nil
	}
	return connector.HTTPResponse{StatusCode: stdhttp.StatusOK, Body: []byte(`{"accepted":true}`)}, nil
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func TestDeliveryUsesRuntimeTransportAndSecretHeaders(t *testing.T) {
	transport := &recordingTransport{}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	adapter.(*provider).now = func() time.Time { return time.Unix(1700000000, 0).UTC() }
	payload, _ := json.Marshal(SendInput{"event": "created"})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: Send.Key, ContractSHA256: Send.ContractSHA256, Mode: connector.ModeEnqueue, Delivery: true, Connection: validConnection(), Secrets: map[string]string{"webhook_secret": "runtime-secret"}, RequestRef: "request-1", Headers: map[string]string{"X-Caller": "caller"}, Payload: payload})
	if err != nil || result.ResponseRef != "http:200" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	request := transport.requests[0]
	if request.Headers["X-Integration-Signature"] != nil || request.SecretHeaders["X-Integration-Signature"] == nil || request.Headers["X-Caller"][0] != "caller" {
		t.Fatalf("request=%+v", request)
	}
	raw, _ := json.Marshal(request)
	if strings.Contains(string(raw), "runtime-secret") {
		t.Fatalf("request leaks secret: %s", raw)
	}
}
func TestWriteFailuresAreUncertain(t *testing.T) {
	for _, fixture := range []recordingTransport{{err: errors.New("reset")}, {response: connector.HTTPResponse{StatusCode: 503}}} {
		adapter, _ := New(&fixture)
		payload, _ := json.Marshal(SendInput{"event": "created"})
		_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: Send.Key, ContractSHA256: Send.ContractSHA256, Mode: connector.ModeEnqueue, Delivery: true, Connection: validConnection(), Secrets: map[string]string{"webhook_secret": "secret"}, Payload: payload})
		classification, _ := connector.ErrorClassificationOf(err)
		if classification != connector.ErrorUncertain {
			t.Fatalf("classification=%q err=%v", classification, err)
		}
	}
}
func TestRejectsRemotePlaintextAndSupportsFeishuSignature(t *testing.T) {
	transport := &recordingTransport{}
	adapter, _ := New(transport)
	connection := validConnection()
	connection.Config["url"] = "http://remote.example/hook"
	if adapter.(connector.ConfigValidator).ValidateConfig(connection) == nil {
		t.Fatal("accepted remote plaintext endpoint")
	}
	connection = validConnection()
	connection.Config["signature_algorithm"] = "feishu_sha256_hex"
	payload, _ := json.Marshal(SendInput{})
	_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: Send.Key, ContractSHA256: Send.ContractSHA256, Mode: connector.ModeEnqueue, Delivery: true, Connection: connection, Secrets: map[string]string{"webhook_secret": "secret"}, Payload: payload})
	if err != nil || transport.requests[len(transport.requests)-1].SecretHeaders["X-Integration-Signature"] == nil {
		t.Fatalf("err=%v request=%+v", err, transport.requests)
	}
}
func validConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"url": "http://127.0.0.1:8080/hook", "method": "POST", "signature_algorithm": "hmac_sha256_hex", "signature_secret_ref_name": "webhook_secret"}, SecretRefs: map[string]string{"webhook_secret": "secret:webhook"}}
}
