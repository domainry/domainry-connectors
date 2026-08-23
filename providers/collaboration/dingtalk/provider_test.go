package dingtalk

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
func TestDescriptorModesCredentialsAndEndpointBoundary(t *testing.T) {
	adapter, err := New(&recordingTransport{})
	if err != nil {
		t.Fatal(err)
	}
	if err = contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	d := adapter.Descriptor()
	if d.ConnectorKey != ConnectorKey || d.ProviderKey != ProviderKey || len(d.Operations) != 2 || len(d.SecretFields) != 4 {
		t.Fatalf("descriptor=%+v", d)
	}
	modes := map[string]connector.OperationMode{}
	for _, operation := range d.Operations {
		modes[operation.Key] = operation.Mode
	}
	if modes[SendMessage.Key] != connector.ModeEnqueue || modes[TestConnection.Key] != connector.ModeCall {
		t.Fatalf("modes=%v", modes)
	}
	if d.SecretFields[0].Key != "client_id" || d.SecretFields[1].Key != "client_secret" || d.SecretFields[2].Key != "bot_token" || d.SecretFields[3].Key != "webhook_secret" {
		t.Fatalf("secrets=%+v", d.SecretFields)
	}
	validator := adapter.(connector.ConfigValidator)
	for _, pair := range [][2]string{{defaultAPIBase, defaultBotBase}, {"http://localhost:8080", "http://127.0.0.1:8081"}} {
		if err = validator.ValidateConfig(connection(pair[0], pair[1])); err != nil {
			t.Fatalf("valid=%v err=%v", pair, err)
		}
	}
	for _, pair := range [][2]string{{"https://api.dingtalk.com.evil.test", defaultBotBase}, {"http://api.dingtalk.com", defaultBotBase}, {defaultAPIBase, "https://oapi.dingtalk.com/custom"}, {defaultAPIBase, "https://api.dingtalk.com"}} {
		if err = validator.ValidateConfig(connection(pair[0], pair[1])); err == nil {
			t.Fatalf("invalid accepted=%v", pair)
		}
	}
}
func TestOAuthAndSignedBotKeepAllSecretsRuntimeOnly(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: 200, Body: []byte(`{"accessToken":"app-token","expireIn":7200}`)}, {StatusCode: 200, Body: []byte(`{"errcode":0,"errmsg":"ok"}`)}}}
	adapter, _ := New(transport)
	result, err := adapter.Call(t.Context(), call(TestConnection.Key, TestConnection.ContractSHA256, connector.ModeCall, false, struct{}{}))
	if err != nil || !strings.Contains(string(result.Payload), `"connected":true`) {
		t.Fatalf("test result=%+v err=%v", result, err)
	}
	delivery, err := adapter.Call(t.Context(), call(SendMessage.Key, SendMessage.ContractSHA256, connector.ModeEnqueue, true, SendMessageInput{Recipient: "ops", Message: "Approved"}))
	if err != nil || delivery.ResponseRef != "dingtalk:bot:accepted" {
		t.Fatalf("delivery=%+v err=%v", delivery, err)
	}
	oauth := transport.requests[0]
	if oauth.SecretJSON["appKey"] != "client-id" || oauth.SecretJSON["appSecret"] != "client-secret" || len(oauth.Body) != 0 || strings.Contains(oauth.URL, "client-id") {
		t.Fatalf("oauth=%+v", oauth)
	}
	bot := transport.requests[1]
	if bot.SecretQuery["access_token"] != "bot-token" || strings.Contains(bot.URL+string(bot.Body), "bot-token") || !strings.Contains(bot.URL, "timestamp=") || !strings.Contains(bot.URL, "sign=") {
		t.Fatalf("bot=%+v", bot)
	}
	if !strings.Contains(string(bot.Body), `"content":"Approved"`) {
		t.Fatalf("body=%s", bot.Body)
	}
}
func TestProviderPayloadValidationAndWriteClassification(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: 200, Body: []byte(`{"errcode":0}`)}}}
	adapter, _ := New(transport)
	_, err := adapter.Call(t.Context(), call(SendMessage.Key, SendMessage.ContractSHA256, connector.ModeEnqueue, true, SendMessageInput{Recipient: "ops", Message: "fallback", ProviderPayload: map[string]any{"msgtype": "markdown", "markdown": map[string]any{"title": "Release", "text": "Done"}}}))
	if err != nil || !strings.Contains(string(transport.requests[0].Body), `"msgtype":"markdown"`) {
		t.Fatalf("request=%+v err=%v", transport.requests, err)
	}
	for _, payload := range []SendMessageInput{{Message: "hello"}, {Recipient: "ops"}} {
		if _, err = adapter.Call(t.Context(), call(SendMessage.Key, SendMessage.ContractSHA256, connector.ModeEnqueue, true, payload)); err == nil {
			t.Fatalf("invalid accepted=%+v", payload)
		}
	}
	for _, tc := range []struct {
		name         string
		response     connector.HTTPResponse
		transportErr error
		want         connector.ErrorClassification
	}{{"network", connector.HTTPResponse{}, errors.New("reset"), connector.ErrorUncertain}, {"server", connector.HTTPResponse{StatusCode: 502}, nil, connector.ErrorUncertain}, {"rate", connector.HTTPResponse{StatusCode: 429}, nil, connector.ErrorRetryable}, {"reject", connector.HTTPResponse{StatusCode: 400}, nil, connector.ErrorPermanent}, {"bot reject", connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"errcode":123}`)}, nil, connector.ErrorPermanent}, {"invalid", connector.HTTPResponse{StatusCode: 200, Body: []byte(`{`)}, nil, connector.ErrorUncertain}} {
		t.Run(tc.name, func(t *testing.T) {
			current, _ := New(&recordingTransport{responses: []connector.HTTPResponse{tc.response}, errors: []error{tc.transportErr}})
			_, err := current.Call(t.Context(), call(SendMessage.Key, SendMessage.ContractSHA256, connector.ModeEnqueue, true, SendMessageInput{Recipient: "ops", Message: "hello"}))
			class, ok := connector.ErrorClassificationOf(err)
			if !ok || class != tc.want {
				t.Fatalf("err=%v class=%q want=%q", err, class, tc.want)
			}
		})
	}
}
func connection(api, bot string) connector.Connection {
	return connector.Connection{Config: map[string]any{"api_base_url": api, "bot_base_url": bot, "timeout_seconds": 15}}
}
func call(key, hash string, mode connector.OperationMode, delivery bool, payload any) connector.CallRequest {
	raw, _ := json.Marshal(payload)
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: key, ContractSHA256: hash, Mode: mode, Delivery: delivery, Connection: connection("http://localhost:8080", "http://localhost:8080"), Secrets: map[string]string{"client_id": "client-id", "client_secret": "client-secret", "bot_token": "bot-token", "webhook_secret": "sign-secret"}, Payload: raw}
}
