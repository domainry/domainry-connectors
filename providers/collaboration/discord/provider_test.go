package discord

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
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

func TestDescriptorModeCredentialAndEndpointBoundary(t *testing.T) {
	adapter, err := New(&recordingTransport{})
	if err != nil {
		t.Fatal(err)
	}
	if err = contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	d := adapter.Descriptor()
	if d.ConnectorKey != ConnectorKey || d.ProviderKey != ProviderKey || len(d.Operations) != 2 || len(d.SecretFields) != 1 || d.SecretFields[0].Key != "bot_token" {
		t.Fatalf("descriptor=%+v", d)
	}
	modes := map[string]connector.OperationMode{}
	for _, operation := range d.Operations {
		modes[operation.Key] = operation.Mode
	}
	if modes[SendMessage.Key] != connector.ModeEnqueue || modes[TestConnection.Key] != connector.ModeCall {
		t.Fatalf("modes=%v", modes)
	}
	validator := adapter.(connector.ConfigValidator)
	for _, endpoint := range []string{defaultAPIBase, "https://discord.com/api/v10/", "http://localhost:8080"} {
		if err = validator.ValidateConfig(connection(endpoint)); err != nil {
			t.Fatalf("valid=%s err=%v", endpoint, err)
		}
	}
	for _, endpoint := range []string{"https://discord.com", "https://discord.com/api/v9", "https://discord.com/api/v10/custom", "https://discord.com.evil.test/api/v10", "http://discord.com/api/v10"} {
		if err = validator.ValidateConfig(connection(endpoint)); err == nil {
			t.Fatalf("invalid accepted=%s", endpoint)
		}
	}
}

func TestBotRequestsUseRuntimeOnlyAuthorizationAndNonce(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: 200, Body: []byte(`{"id":"bot-1"}`)}, {StatusCode: 200, Body: []byte(`{"id":"message-1"}`)}}}
	adapter, _ := New(transport)
	testResult, err := adapter.Call(t.Context(), call(TestConnection.Key, TestConnection.ContractSHA256, connector.ModeCall, false, struct{}{}, ""))
	if err != nil || !strings.Contains(string(testResult.Payload), `"connected":true`) {
		t.Fatalf("test=%+v err=%v", testResult, err)
	}
	delivery, err := adapter.Call(t.Context(), call(SendMessage.Key, SendMessage.ContractSHA256, connector.ModeEnqueue, true, SendMessageInput{Recipient: "channel-1", Message: "Approved"}, "request-1"))
	if err != nil || delivery.ResponseRef != "discord:message-1" {
		t.Fatalf("delivery=%+v err=%v", delivery, err)
	}
	if transport.requests[0].URL != "http://localhost:8080/users/@me" || !strings.HasSuffix(transport.requests[1].URL, "/channels/channel-1/messages") {
		t.Fatalf("requests=%+v", transport.requests)
	}
	for _, request := range transport.requests {
		if request.SecretHeaders["Authorization"][0] != "Bot bot-secret" || strings.Contains(request.URL+string(request.Body), "bot-secret") {
			t.Fatalf("token leaked=%+v", request)
		}
	}
	body := string(transport.requests[1].Body)
	for _, want := range []string{`"content":"Approved"`, `"nonce":"request-1"`, `"enforce_nonce":true`} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %s in %s", want, body)
		}
	}
}

func TestWebhookVerifiesEd25519AndNormalizesIdentity(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	adapter, _ := New(&recordingTransport{})
	verifier := adapter.(connector.WebhookVerifier)
	verify := func(body string) (connector.VerifiedWebhook, error) {
		timestamp := "1780000000"
		signature := ed25519.Sign(privateKey, append([]byte(timestamp), []byte(body)...))
		return verifier.VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Connection: connector.Connection{Config: map[string]any{"application_public_key": hex.EncodeToString(publicKey)}}, Headers: map[string][]string{"X-Signature-Timestamp": {timestamp}, "X-Signature-Ed25519": {hex.EncodeToString(signature)}}, Body: []byte(body)})
	}
	event, err := verify(`{"id":"interaction-1","type":2,"member":{"user":{"id":"user-1","global_name":"Alice"}}}`)
	if err != nil || event.ExternalID != "discord:interaction-1" || event.ExternalIdentity == nil || event.ExternalIdentity.Subject != "user-1" || event.Security == nil || !event.Security.SignatureVerified {
		t.Fatalf("event=%+v err=%v", event, err)
	}
	ping, err := verify(`{"type":1}`)
	if err != nil || ping.EventType != "ping" || ping.Challenge != `{"type":1}` {
		t.Fatalf("ping=%+v err=%v", ping, err)
	}
	timestamp := "1780000000"
	bad := connector.VerifyWebhookRequest{Connection: connector.Connection{Config: map[string]any{"application_public_key": hex.EncodeToString(publicKey)}}, Headers: map[string][]string{"X-Signature-Timestamp": {timestamp}, "X-Signature-Ed25519": {hex.EncodeToString(make([]byte, ed25519.SignatureSize))}}, Body: []byte(`{"id":"x"}`)}
	if _, err = verifier.VerifyWebhook(t.Context(), bad); err == nil {
		t.Fatal("invalid signature accepted")
	}
}

func TestValidationProviderPayloadAndFailureClassification(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	for _, input := range []SendMessageInput{{Message: "hello"}, {Recipient: "channel"}} {
		if _, err := adapter.Call(t.Context(), call(SendMessage.Key, SendMessage.ContractSHA256, connector.ModeEnqueue, true, input, "")); err == nil {
			t.Fatalf("invalid accepted=%+v", input)
		}
	}
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: 200, Body: []byte(`{"id":"custom"}`)}}}
	adapter, _ = New(transport)
	_, err := adapter.Call(t.Context(), call(SendMessage.Key, SendMessage.ContractSHA256, connector.ModeEnqueue, true, SendMessageInput{Recipient: "channel", Message: "fallback", ProviderPayload: map[string]any{"content": "custom", "flags": 4}}, ""))
	if err != nil || !strings.Contains(string(transport.requests[0].Body), `"flags":4`) {
		t.Fatalf("request=%+v err=%v", transport.requests, err)
	}
	for _, tc := range []struct {
		name         string
		response     connector.HTTPResponse
		transportErr error
		want         connector.ErrorClassification
	}{{"network", connector.HTTPResponse{}, errors.New("reset"), connector.ErrorUncertain}, {"server", connector.HTTPResponse{StatusCode: 502}, nil, connector.ErrorUncertain}, {"rate", connector.HTTPResponse{StatusCode: 429}, nil, connector.ErrorRetryable}, {"reject", connector.HTTPResponse{StatusCode: 400}, nil, connector.ErrorPermanent}, {"invalid", connector.HTTPResponse{StatusCode: 200, Body: []byte(`{`)}, nil, connector.ErrorUncertain}} {
		t.Run(tc.name, func(t *testing.T) {
			current, _ := New(&recordingTransport{responses: []connector.HTTPResponse{tc.response}, errors: []error{tc.transportErr}})
			_, err := current.Call(t.Context(), call(SendMessage.Key, SendMessage.ContractSHA256, connector.ModeEnqueue, true, SendMessageInput{Recipient: "channel", Message: "hello"}, ""))
			class, ok := connector.ErrorClassificationOf(err)
			if !ok || class != tc.want {
				t.Fatalf("err=%v class=%q want=%q", err, class, tc.want)
			}
		})
	}
}
func connection(endpoint string) connector.Connection {
	return connector.Connection{Config: map[string]any{"api_base_url": endpoint, "channel_id": "default-channel", "timeout_seconds": 15}}
}
func call(key, hash string, mode connector.OperationMode, delivery bool, payload any, ref string) connector.CallRequest {
	raw, _ := json.Marshal(payload)
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: key, ContractSHA256: hash, Mode: mode, Delivery: delivery, RequestRef: ref, Connection: connection("http://localhost:8080"), Secrets: map[string]string{"bot_token": "bot-secret"}, Payload: raw}
}
