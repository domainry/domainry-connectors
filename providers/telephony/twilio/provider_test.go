package twilio

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
	"net/http"
	"net/url"
	"sort"
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
	return connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"sid":"SM_runtime"}`)}, nil
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func TestRuntimeTransportSecretsAndWriteReliability(t *testing.T) {
	transport := &recordingTransport{}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	payload, _ := json.Marshal(SendSMSInput{To: "+15550002", Body: "hello"})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SendSMS.Key, ContractSHA256: SendSMS.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"account_sid": "AC_runtime", "auth_token": "runtime-token"}, RequestRef: "message-1", Payload: payload})
	if err != nil || result.ResponseRef != "twilio:SM_runtime" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	request := transport.requests[0]
	if request.Headers["Authorization"] != nil || request.SecretHeaders["Authorization"] == nil || request.Headers["Idempotency-Key"][0] != "message-1" || string(request.Body) != "Body=hello&From=%2B15550001&To=%2B15550002" {
		t.Fatalf("request=%+v", request)
	}
	raw, _ := json.Marshal(request)
	if strings.Contains(string(raw), "runtime-token") {
		t.Fatalf("request leaks token: %s", raw)
	}
	failed, _ := New(&recordingTransport{err: errors.New("reset")})
	_, err = failed.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SendSMS.Key, ContractSHA256: SendSMS.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"account_sid": "AC", "auth_token": "token"}, Payload: payload})
	classification, _ := connector.ErrorClassificationOf(err)
	if classification != connector.ErrorUncertain {
		t.Fatalf("classification=%q err=%v", classification, err)
	}
}
func TestWebhook(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	form := url.Values{"MessageSid": {"SM_received"}, "MessageStatus": {"delivered"}, "From": {"+1555"}}
	canonical := "https://runtime.example/webhook"
	signature := testSignature("secret", canonical, form)
	event, err := adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Connection: connector.Connection{Config: map[string]any{"webhook_url": canonical}}, Secrets: map[string]string{"auth_token": "secret"}, Body: []byte(form.Encode()), Headers: map[string][]string{"X-Twilio-Signature": {signature}}})
	if err != nil || event.ExternalID != "SM_received" || event.ExternalIdentity.Subject != "+1555" {
		t.Fatalf("event=%+v err=%v", event, err)
	}
}
func testSignature(secret, canonical string, form url.Values) string {
	keys := make([]string, 0, len(form))
	for key := range form {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	value := canonical
	for _, key := range keys {
		values := append([]string(nil), form[key]...)
		sort.Strings(values)
		for _, item := range values {
			value += key + item
		}
	}
	mac := hmac.New(sha1.New, []byte(secret))
	_, _ = mac.Write([]byte(value))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
func validConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "http://127.0.0.1:8080", "from_number": "+15550001"}, SecretRefs: map[string]string{"account_sid": "secret:sid", "auth_token": "secret:token"}}
}

var _ = http.MethodPost
