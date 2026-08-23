package feishu

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
)

type recordingTransport struct {
	requests  []connector.HTTPRequest
	responses []connector.HTTPResponse
	errors    []error
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, request)
	index := len(t.requests) - 1
	var response connector.HTTPResponse
	if index < len(t.responses) {
		response = t.responses[index]
	}
	var err error
	if index < len(t.errors) {
		err = t.errors[index]
	}
	return response, err
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestDescriptorAndEndpointBoundary(t *testing.T) {
	adapter, err := New(&recordingTransport{})
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("adapter=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	descriptor := adapter.Descriptor()
	if descriptor.ConnectorKey != ConnectorKey || descriptor.ProviderKey != ProviderKey || len(descriptor.Operations) != 2 || len(descriptor.SecretFields) != 3 {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	modes := map[string]connector.OperationMode{}
	for _, operation := range descriptor.Operations {
		modes[operation.Key] = operation.Mode
	}
	if modes[SendMessage.Key] != connector.ModeEnqueue || modes[TestConnection.Key] != connector.ModeCall {
		t.Fatalf("modes=%v", modes)
	}
	validator := adapter.(connector.ConfigValidator)
	for _, endpoint := range []string{defaultAPIBase, "http://localhost:8080"} {
		if err = validator.ValidateConfig(connection(endpoint)); err != nil {
			t.Fatalf("valid endpoint=%s err=%v", endpoint, err)
		}
	}
	for _, endpoint := range []string{"https://open.feishu.cn.evil.test", "http://open.feishu.cn", "https://open.feishu.cn/custom", "https://open.larksuite.com"} {
		if err = validator.ValidateConfig(connection(endpoint)); err == nil {
			t.Fatalf("invalid endpoint accepted=%s", endpoint)
		}
	}
	invalid := connection(defaultAPIBase)
	invalid.Config["receive_id_type"] = "phone"
	if err = validator.ValidateConfig(invalid); err == nil {
		t.Fatal("invalid recipient type accepted")
	}
}

func TestTenantTokenAndMessageSecretsStayRuntimeOnly(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{
		{StatusCode: 200, Body: []byte(`{"code":0,"tenant_access_token":"temporary","expire":7200}`)},
		{StatusCode: 200, Body: []byte(`{"code":0,"tenant_access_token":"temporary","expire":7200}`)},
		{StatusCode: 200, Body: []byte(`{"code":0,"data":{"message_id":"message-1"}}`)},
	}}
	adapter, _ := New(transport)
	result, err := adapter.Call(t.Context(), call(TestConnection.Key, TestConnection.ContractSHA256, connector.ModeCall, false, struct{}{}, ""))
	if err != nil || !strings.Contains(string(result.Payload), `"connected":true`) {
		t.Fatalf("test=%+v err=%v", result, err)
	}
	delivery, err := adapter.Call(t.Context(), call(SendMessage.Key, SendMessage.ContractSHA256, connector.ModeEnqueue, true, SendMessageInput{Recipient: "ou-user", Message: "Approved"}, "request-1"))
	if err != nil || delivery.ResponseRef != "feishu:message-1" {
		t.Fatalf("delivery=%+v err=%v", delivery, err)
	}
	if len(transport.requests) != 3 {
		t.Fatalf("requests=%d", len(transport.requests))
	}
	for _, index := range []int{0, 1} {
		request := transport.requests[index]
		if request.SecretJSON["app_id"] != "cli-test" || request.SecretJSON["app_secret"] != "app-secret" || strings.Contains(request.URL+string(request.Body), "app-secret") {
			t.Fatalf("token secret boundary=%+v", request)
		}
	}
	message := transport.requests[2]
	if message.SecretHeaders["Authorization"][0] != "Bearer temporary" || strings.Contains(message.URL+string(message.Body), "temporary") {
		t.Fatalf("message token boundary=%+v", message)
	}
	for _, value := range []string{`"receive_id":"ou-user"`, `"content":"{\"text\":\"Approved\"}"`, `"uuid":"request-1"`} {
		if !strings.Contains(string(message.Body), value) {
			t.Fatalf("missing %s in %s", value, message.Body)
		}
	}
}

func TestMessageFailureClassificationAndProviderPayload(t *testing.T) {
	for _, test := range []struct {
		name         string
		message      connector.HTTPResponse
		transportErr error
		want         connector.ErrorClassification
	}{
		{"network", connector.HTTPResponse{}, errors.New("reset"), connector.ErrorUncertain},
		{"server", connector.HTTPResponse{StatusCode: 502}, nil, connector.ErrorUncertain},
		{"rate", connector.HTTPResponse{StatusCode: 429}, nil, connector.ErrorRetryable},
		{"reject", connector.HTTPResponse{StatusCode: 400}, nil, connector.ErrorPermanent},
		{"provider reject", connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"code":230002}`)}, nil, connector.ErrorPermanent},
		{"invalid", connector.HTTPResponse{StatusCode: 200, Body: []byte(`{`)}, nil, connector.ErrorUncertain},
		{"missing id", connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"code":0,"data":{}}`)}, nil, connector.ErrorUncertain},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: 200, Body: []byte(`{"code":0,"tenant_access_token":"token"}`)}, test.message}, errors: []error{nil, test.transportErr}}
			adapter, _ := New(transport)
			_, err := adapter.Call(t.Context(), call(SendMessage.Key, SendMessage.ContractSHA256, connector.ModeEnqueue, true, SendMessageInput{Recipient: "ou-user", Message: "fallback", ProviderPayload: map[string]any{"msg_type": "interactive", "card": map[string]any{"title": "custom"}}}, ""))
			classification, ok := connector.ErrorClassificationOf(err)
			if !ok || classification != test.want {
				t.Fatalf("err=%v classification=%q want=%q", err, classification, test.want)
			}
		})
	}
}

func TestWebhookChallengeEncryptionAndIdentity(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	verifier := adapter.(connector.WebhookVerifier)
	challenge, err := verifier.VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Secrets: map[string]string{"verification_token": "verify"}, Body: []byte(`{"type":"url_verification","token":"verify","challenge":"challenge-1"}`)})
	if err != nil || challenge.Challenge != "challenge-1" || challenge.Security == nil || !challenge.Security.SignatureVerified {
		t.Fatalf("challenge=%+v err=%v", challenge, err)
	}
	payload := []byte(`{"schema":"2.0","header":{"event_id":"event-1","event_type":"im.message.receive_v1","token":"verify"},"event":{"sender":{"sender_id":{"open_id":"ou-user"},"sender_type":"user"}}}`)
	key := "encrypt-key"
	body := encryptEnvelope(t, payload, key)
	now := time.Now().UTC()
	timestamp, nonce := fmt.Sprint(now.Unix()), "nonce-1"
	sum := sha256.Sum256(append([]byte(timestamp+nonce+key), body...))
	event, err := verifier.VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Secrets: map[string]string{"encrypt_key": key, "verification_token": "verify"}, Headers: map[string][]string{"X-Lark-Request-Timestamp": {timestamp}, "X-Lark-Request-Nonce": {nonce}, "X-Lark-Signature": {hex.EncodeToString(sum[:])}}, Body: body, ReceivedAt: now})
	if err != nil || event.ExternalID != "event-1" || event.EventType != "im.message.receive_v1" || event.ExternalIdentity == nil || event.ExternalIdentity.Subject != "ou-user" || event.Security == nil || !event.Security.SignatureVerified {
		t.Fatalf("event=%+v err=%v", event, err)
	}
	if _, err = verifier.VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Secrets: map[string]string{"verification_token": "wrong"}, Body: payload}); err == nil {
		t.Fatal("invalid verification token accepted")
	}
}

func encryptEnvelope(t *testing.T, payload []byte, key string) []byte {
	t.Helper()
	padding := aes.BlockSize - len(payload)%aes.BlockSize
	for range padding {
		payload = append(payload, byte(padding))
	}
	sum := sha256.Sum256([]byte(key))
	block, err := aes.NewCipher(sum[:])
	if err != nil {
		t.Fatal(err)
	}
	iv := []byte("0123456789abcdef")
	ciphertext := make([]byte, len(payload))
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(ciphertext, payload)
	body, _ := json.Marshal(map[string]string{"encrypt": base64.StdEncoding.EncodeToString(append(iv, ciphertext...))})
	return body
}

func connection(endpoint string) connector.Connection {
	return connector.Connection{Config: map[string]any{"app_id": "cli-test", "api_base_url": endpoint, "receive_id_type": "open_id", "timeout_seconds": 15}}
}
func call(key, hash string, mode connector.OperationMode, delivery bool, payload any, ref string) connector.CallRequest {
	raw, _ := json.Marshal(payload)
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: key, ContractSHA256: hash, Mode: mode, Delivery: delivery, RequestRef: ref, Connection: connection("http://localhost:8080"), Secrets: map[string]string{"app_secret": "app-secret"}, Payload: raw}
}
