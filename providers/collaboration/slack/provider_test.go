package slack

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
	"strconv"
	"strings"
	"testing"
	"time"
)

type recordingTransport struct {
	requests  []connector.HTTPRequest
	responses []connector.HTTPResponse
	errors    []error
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, request)
	i := len(t.requests) - 1
	var response connector.HTTPResponse
	if i < len(t.responses) {
		response = t.responses[i]
	}
	var err error
	if i < len(t.errors) {
		err = t.errors[i]
	}
	return response, err
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func TestDescriptorEndpointAndSecretBoundary(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: 200, Body: []byte(`{"ok":true,"team_id":"T1","user_id":"U1"}`)}, {StatusCode: 200, Body: []byte(`{"ok":true,"channel":"C1","ts":"123.456"}`)}}}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	validator := adapter.(connector.ConfigValidator)
	for _, endpoint := range []string{defaultAPIBase, "http://localhost:8080"} {
		if err = validator.ValidateConfig(connection(endpoint)); err != nil {
			t.Fatalf("valid=%s err=%v", endpoint, err)
		}
	}
	for _, endpoint := range []string{"https://slack.com", "https://slack.com/api/custom", "https://api.slack.com/api", "https://slack.com.evil.test/api", "http://slack.com/api"} {
		if err = validator.ValidateConfig(connection(endpoint)); err == nil {
			t.Fatalf("invalid accepted=%s", endpoint)
		}
	}
	_, err = adapter.Call(t.Context(), call(TestConnection.Key, TestConnection.ContractSHA256, connector.ModeCall, false, struct{}{}, ""))
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := adapter.Call(t.Context(), call(SendMessage.Key, SendMessage.ContractSHA256, connector.ModeEnqueue, true, SendMessageInput{Recipient: "C1", Message: "Approved"}, "request-1"))
	if err != nil || delivery.ResponseRef != "slack:123.456" {
		t.Fatalf("delivery=%+v err=%v", delivery, err)
	}
	for _, request := range transport.requests {
		if request.SecretHeaders["Authorization"][0] != "Bearer xoxb-secret" || strings.Contains(request.URL+string(request.Body), "xoxb-secret") {
			t.Fatalf("secret boundary=%+v", request)
		}
	}
	if !strings.Contains(string(transport.requests[1].Body), `"client_msg_id"`) {
		t.Fatalf("body=%s", transport.requests[1].Body)
	}
}
func TestSignedWebhookChallengeAndIdentity(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	now := time.Unix(1800000000, 0).UTC()
	verify := func(body string) (connector.VerifiedWebhook, error) {
		timestamp := strconv.FormatInt(now.Unix(), 10)
		return adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Secrets: map[string]string{"signing_secret": "signing-secret"}, Headers: map[string][]string{"X-Slack-Request-Timestamp": {timestamp}, "X-Slack-Signature": {signature("signing-secret", timestamp, []byte(body))}}, Body: []byte(body), ReceivedAt: now})
	}
	event, err := verify(`{"type":"event_callback","event_id":"Ev1","event":{"type":"message","user":"U1","username":"Alice","channel":"C1"}}`)
	if err != nil || event.ExternalID != "Ev1" || event.ExternalIdentity == nil || event.ExternalIdentity.Subject != "U1" || event.Security == nil || !event.Security.SignatureVerified {
		t.Fatalf("event=%+v err=%v", event, err)
	}
	challenge, err := verify(`{"type":"url_verification","challenge":"challenge-1"}`)
	if err != nil || challenge.Challenge != "challenge-1" {
		t.Fatalf("challenge=%+v err=%v", challenge, err)
	}
	timestamp := strconv.FormatInt(now.Add(-10*time.Minute).Unix(), 10)
	if _, err = adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Secrets: map[string]string{"signing_secret": "signing-secret"}, Headers: map[string][]string{"X-Slack-Request-Timestamp": {timestamp}, "X-Slack-Signature": {"v0=bad"}}, Body: []byte(`{}`), ReceivedAt: now}); err == nil {
		t.Fatal("stale/bad webhook accepted")
	}
}
func TestPayloadAndWriteFailureClassification(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: 200, Body: []byte(`{"ok":true,"ts":"1"}`)}}}
	adapter, _ := New(transport)
	_, err := adapter.Call(t.Context(), call(SendMessage.Key, SendMessage.ContractSHA256, connector.ModeEnqueue, true, SendMessageInput{Recipient: "C1", Message: "fallback", ProviderPayload: map[string]any{"blocks": []any{map[string]any{"type": "section"}}}}, ""))
	if err != nil || !strings.Contains(string(transport.requests[0].Body), `"blocks"`) {
		t.Fatalf("request=%+v err=%v", transport.requests, err)
	}
	for _, test := range []struct {
		name         string
		response     connector.HTTPResponse
		transportErr error
		want         connector.ErrorClassification
	}{{"network", connector.HTTPResponse{}, errors.New("reset"), connector.ErrorUncertain}, {"server", connector.HTTPResponse{StatusCode: 502}, nil, connector.ErrorUncertain}, {"rate", connector.HTTPResponse{StatusCode: 429}, nil, connector.ErrorRetryable}, {"api rate", connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"ok":false,"error":"ratelimited"}`)}, nil, connector.ErrorRetryable}, {"reject", connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"ok":false,"error":"channel_not_found"}`)}, nil, connector.ErrorPermanent}, {"invalid", connector.HTTPResponse{StatusCode: 200, Body: []byte(`{`)}, nil, connector.ErrorUncertain}, {"missing timestamp", connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"ok":true}`)}, nil, connector.ErrorUncertain}} {
		t.Run(test.name, func(t *testing.T) {
			current, _ := New(&recordingTransport{responses: []connector.HTTPResponse{test.response}, errors: []error{test.transportErr}})
			_, callErr := current.Call(t.Context(), call(SendMessage.Key, SendMessage.ContractSHA256, connector.ModeEnqueue, true, SendMessageInput{Recipient: "C1", Message: "hello"}, ""))
			classification, ok := connector.ErrorClassificationOf(callErr)
			if !ok || classification != test.want {
				t.Fatalf("err=%v class=%q want=%q", callErr, classification, test.want)
			}
		})
	}
}
func connection(endpoint string) connector.Connection {
	return connector.Connection{Config: map[string]any{"api_base_url": endpoint, "timeout_seconds": 15}}
}
func call(key, hash string, mode connector.OperationMode, delivery bool, payload any, ref string) connector.CallRequest {
	raw, _ := json.Marshal(payload)
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: key, ContractSHA256: hash, Mode: mode, Delivery: delivery, RequestRef: ref, Connection: connection("http://localhost:8080"), Secrets: map[string]string{"bot_token": "xoxb-secret"}, Payload: raw}
}
func signature(secret, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte("v0:" + timestamp + ":"))
	_, _ = mac.Write(body)
	return "v0=" + hex.EncodeToString(mac.Sum(nil))
}
