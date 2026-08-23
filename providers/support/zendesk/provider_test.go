package zendesk

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
	"net/http"
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
	return connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"ticket":{"id":42}}`)}, nil
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func TestProviderTransportSecretsAndReliability(t *testing.T) {
	transport := &recordingTransport{}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	payload, _ := json.Marshal(TicketInput{"subject": "help"})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CreateTicket.Key, ContractSHA256: CreateTicket.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"api_token": "runtime-token"}, RequestRef: "request-42", Payload: payload})
	if err != nil || result.ResponseRef != "zendesk:ticket:42" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	request := transport.requests[0]
	if request.Headers["Authorization"] != nil || request.SecretHeaders["Authorization"] == nil || request.Headers["Idempotency-Key"][0] != "request-42" {
		t.Fatalf("request=%+v", request)
	}
	raw, _ := json.Marshal(request)
	if strings.Contains(string(raw), "runtime-token") {
		t.Fatalf("request leaks token: %s", raw)
	}
	failed, _ := New(&recordingTransport{err: errors.New("reset")})
	_, err = failed.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CreateTicket.Key, ContractSHA256: CreateTicket.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"api_token": "token"}, Payload: payload})
	classification, _ := connector.ErrorClassificationOf(err)
	if classification != connector.ErrorUncertain {
		t.Fatalf("classification=%q err=%v", classification, err)
	}
}
func TestWebhook(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	body := []byte(`{"id":"event-1","type":"ticket.updated","ticket_id":"42"}`)
	timestamp := "1710000000"
	mac := hmac.New(sha256.New, []byte("secret"))
	_, _ = mac.Write(append([]byte(timestamp), body...))
	event, err := adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Secrets: map[string]string{"webhook_secret": "secret"}, Body: body, Headers: map[string][]string{"X-Zendesk-Webhook-Signature-Timestamp": {timestamp}, "X-Zendesk-Webhook-Signature": {base64.StdEncoding.EncodeToString(mac.Sum(nil))}}})
	if err != nil || event.ExternalID != "event-1" {
		t.Fatalf("event=%+v err=%v", event, err)
	}
}
func validConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "http://127.0.0.1:8080", "email": "agent@example.com"}, SecretRefs: map[string]string{"api_token": "secret:token"}}
}
