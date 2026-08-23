package metacloudapi

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
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
	return connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"messages":[{"id":"wamid.sent"}]}`)}, nil
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func TestProviderUsesRuntimeTransportAndSecretHeaders(t *testing.T) {
	transport := &recordingTransport{}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	payload, _ := json.Marshal(SendMessageInput{Recipient: "15551234567", Message: "hello"})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SendMessage.Key, ContractSHA256: SendMessage.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_token": "runtime-token"}, Payload: payload})
	if err != nil || result.ResponseRef != "whatsapp:wamid.sent" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	request := transport.requests[0]
	if request.Headers["Authorization"] != nil || request.SecretHeaders["Authorization"][0] != "Bearer runtime-token" || !strings.HasSuffix(request.URL, "/phone/messages") {
		t.Fatalf("request=%+v", request)
	}
	raw, _ := json.Marshal(request)
	if strings.Contains(string(raw), "runtime-token") {
		t.Fatalf("serialized request leaks token: %s", raw)
	}
}
func TestWriteFailureIsUncertain(t *testing.T) {
	adapter, _ := New(&recordingTransport{err: errors.New("reset")})
	payload, _ := json.Marshal(SendMessageInput{Recipient: "1", Message: "hello"})
	_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SendMessage.Key, ContractSHA256: SendMessage.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_token": "token"}, Payload: payload})
	classification, ok := connector.ErrorClassificationOf(err)
	if !ok || classification != connector.ErrorUncertain {
		t.Fatalf("classification=%q err=%v", classification, err)
	}
}
func TestWebhookVerification(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	verifier := adapter.(connector.WebhookVerifier)
	body := []byte(`{"entry":[{"changes":[{"value":{"messages":[{"id":"wamid.received","from":"1555"}]}}]}]}`)
	mac := hmac.New(sha256.New, []byte("secret"))
	_, _ = mac.Write(body)
	event, err := verifier.VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Secrets: map[string]string{"app_secret": "secret"}, Body: body, Headers: map[string][]string{"X-Hub-Signature-256": {"sha256=" + hex.EncodeToString(mac.Sum(nil))}}})
	if err != nil || event.ExternalID != "wamid.received" || event.ExternalIdentity.Subject != "1555" {
		t.Fatalf("event=%+v err=%v", event, err)
	}
}
func validConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "http://127.0.0.1:8080", "phone_number_id": "phone", "business_account_id": "business"}, SecretRefs: map[string]string{"access_token": "secret:token"}}
}
