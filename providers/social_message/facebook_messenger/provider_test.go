package facebookmessenger

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
	response connector.HTTPResponse
	err      error
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, request)
	if t.err != nil {
		return connector.HTTPResponse{}, t.err
	}
	if t.response.StatusCode != 0 {
		return t.response, nil
	}
	return connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"message_id":"mid.sent"}`)}, nil
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
	payload, _ := json.Marshal(SendMessageInput{RecipientID: "social-user", Message: "hello"})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SendMessage.Key, ContractSHA256: SendMessage.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_token": "runtime-token"}, Payload: payload})
	if err != nil || result.ResponseRef != "facebook:mid.sent" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	request := transport.requests[0]
	if request.Method != http.MethodPost || !strings.HasSuffix(request.URL, "/page-runtime/messages") || request.Headers["Authorization"] != nil || request.SecretHeaders["Authorization"][0] != "Bearer runtime-token" {
		t.Fatalf("request=%+v", request)
	}
	raw, _ := json.Marshal(request)
	if strings.Contains(string(raw), "runtime-token") {
		t.Fatalf("serialized request leaks token: %s", raw)
	}
}

func TestWriteFailuresAreUncertainAndReadsRetryable(t *testing.T) {
	for _, tc := range []struct {
		name, key, hash string
		input           any
		want            connector.ErrorClassification
	}{{"write", SendMessage.Key, SendMessage.ContractSHA256, SendMessageInput{RecipientID: "user", Message: "hello"}, connector.ErrorUncertain}, {"read", TestConnection.Key, TestConnection.ContractSHA256, struct{}{}, connector.ErrorRetryable}} {
		t.Run(tc.name, func(t *testing.T) {
			adapter, _ := New(&recordingTransport{err: errors.New("reset")})
			payload, _ := json.Marshal(tc.input)
			_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: tc.key, ContractSHA256: tc.hash, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_token": "token"}, Payload: payload})
			got, ok := connector.ErrorClassificationOf(err)
			if !ok || got != tc.want {
				t.Fatalf("classification=%q err=%v", got, err)
			}
		})
	}
}

func TestWebhookChallengeAndSignature(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	verifier := adapter.(connector.WebhookVerifier)
	challenge, err := verifier.VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Secrets: map[string]string{"webhook_secret": "verify"}, Query: map[string][]string{"hub.mode": {"subscribe"}, "hub.verify_token": {"verify"}, "hub.challenge": {"challenge"}}})
	if err != nil || challenge.Challenge != "challenge" {
		t.Fatalf("challenge=%+v err=%v", challenge, err)
	}
	body := []byte(`{"entry":[{"messaging":[{"sender":{"id":"social-user"},"message":{"mid":"mid.received"}}]}]}`)
	mac := hmac.New(sha256.New, []byte("app-secret"))
	_, _ = mac.Write(body)
	event, err := verifier.VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Secrets: map[string]string{"app_secret": "app-secret"}, Body: body, Headers: map[string][]string{"X-Hub-Signature-256": {"sha256=" + hex.EncodeToString(mac.Sum(nil))}}})
	if err != nil || event.ExternalID != "mid.received" || event.ExternalIdentity.Subject != "social-user" {
		t.Fatalf("event=%+v err=%v", event, err)
	}
}

func validConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "http://127.0.0.1:8080", "page_id": "page-runtime"}, SecretRefs: map[string]string{"access_token": "secret:token"}}
}
