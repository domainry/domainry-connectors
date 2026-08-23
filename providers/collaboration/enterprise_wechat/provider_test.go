package enterprisewechat

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
	if d.ConnectorKey != ConnectorKey || d.ProviderKey != ProviderKey || len(d.Operations) != 2 || len(d.SecretFields) != 2 || d.SecretFields[0].Key != "corp_id" || d.SecretFields[1].Key != "corp_secret" {
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
	for _, endpoint := range []string{defaultAPIBase, "http://localhost:8080"} {
		if err = validator.ValidateConfig(connection(endpoint)); err != nil {
			t.Fatalf("valid=%s err=%v", endpoint, err)
		}
	}
	for _, endpoint := range []string{"https://qyapi.weixin.qq.com.evil.test", "http://qyapi.weixin.qq.com", "https://qyapi.weixin.qq.com/custom", "https://api.weixin.qq.com"} {
		if err = validator.ValidateConfig(connection(endpoint)); err == nil {
			t.Fatalf("invalid accepted=%s", endpoint)
		}
	}
	invalid := connection("http://localhost")
	invalid.Config["agent_id"] = 0
	if err = validator.ValidateConfig(invalid); err == nil {
		t.Fatal("invalid agent accepted")
	}
}
func TestTokenAndMessageSecretsStayRuntimeOnly(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: 200, Body: []byte(`{"errcode":0,"access_token":"temporary","expires_in":7200}`)}, {StatusCode: 200, Body: []byte(`{"errcode":0,"access_token":"temporary"}`)}, {StatusCode: 200, Body: []byte(`{"errcode":0,"msgid":"message-1"}`)}}}
	adapter, _ := New(transport)
	result, err := adapter.Call(t.Context(), call(TestConnection.Key, TestConnection.ContractSHA256, connector.ModeCall, false, struct{}{}))
	if err != nil || !strings.Contains(string(result.Payload), `"connected":true`) {
		t.Fatalf("test=%+v err=%v", result, err)
	}
	delivery, err := adapter.Call(t.Context(), call(SendMessage.Key, SendMessage.ContractSHA256, connector.ModeEnqueue, true, SendMessageInput{Recipient: "user-1", Message: "Approved"}))
	if err != nil || delivery.ResponseRef != "enterprise_wechat:message-1" {
		t.Fatalf("delivery=%+v err=%v", delivery, err)
	}
	if len(transport.requests) != 3 {
		t.Fatalf("requests=%d", len(transport.requests))
	}
	for _, index := range []int{0, 1} {
		request := transport.requests[index]
		if request.SecretQuery["corpid"] != "corp-id" || request.SecretQuery["corpsecret"] != "corp-secret" || strings.Contains(request.URL+string(request.Body), "corp-secret") {
			t.Fatalf("token request=%+v", request)
		}
	}
	message := transport.requests[2]
	if message.SecretQuery["access_token"] != "temporary" || strings.Contains(message.URL+string(message.Body), "temporary") {
		t.Fatalf("message=%+v", message)
	}
	body := string(message.Body)
	for _, want := range []string{`"touser":"user-1"`, `"agentid":1001`, `"content":"Approved"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %s in %s", want, body)
		}
	}
}
func TestPayloadValidationTokenRejectionAndWriteFailures(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	for _, input := range []SendMessageInput{{Message: "hello"}, {Recipient: "user"}} {
		if _, err := adapter.Call(t.Context(), call(SendMessage.Key, SendMessage.ContractSHA256, connector.ModeEnqueue, true, input)); err == nil {
			t.Fatalf("invalid accepted=%+v", input)
		}
	}
	tokenRejected, _ := New(&recordingTransport{responses: []connector.HTTPResponse{{StatusCode: 200, Body: []byte(`{"errcode":40013}`)}}})
	if _, err := tokenRejected.Call(t.Context(), call(TestConnection.Key, TestConnection.ContractSHA256, connector.ModeCall, false, struct{}{})); err == nil {
		t.Fatal("token rejection accepted")
	}
	for _, tc := range []struct {
		name         string
		message      connector.HTTPResponse
		transportErr error
		want         connector.ErrorClassification
	}{{"network", connector.HTTPResponse{}, errors.New("reset"), connector.ErrorUncertain}, {"server", connector.HTTPResponse{StatusCode: 502}, nil, connector.ErrorUncertain}, {"rate", connector.HTTPResponse{StatusCode: 429}, nil, connector.ErrorRetryable}, {"reject", connector.HTTPResponse{StatusCode: 400}, nil, connector.ErrorPermanent}, {"api reject", connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"errcode":40001}`)}, nil, connector.ErrorPermanent}, {"invalid", connector.HTTPResponse{StatusCode: 200, Body: []byte(`{`)}, nil, connector.ErrorUncertain}} {
		t.Run(tc.name, func(t *testing.T) {
			current, _ := New(&recordingTransport{responses: []connector.HTTPResponse{{StatusCode: 200, Body: []byte(`{"errcode":0,"access_token":"token"}`)}, tc.message}, errors: []error{nil, tc.transportErr}})
			_, err := current.Call(t.Context(), call(SendMessage.Key, SendMessage.ContractSHA256, connector.ModeEnqueue, true, SendMessageInput{Recipient: "user", Message: "hello"}))
			class, ok := connector.ErrorClassificationOf(err)
			if !ok || class != tc.want {
				t.Fatalf("err=%v class=%q want=%q", err, class, tc.want)
			}
		})
	}
}
func connection(endpoint string) connector.Connection {
	return connector.Connection{Config: map[string]any{"api_base_url": endpoint, "agent_id": 1001, "timeout_seconds": 15}}
}
func call(key, hash string, mode connector.OperationMode, delivery bool, payload any) connector.CallRequest {
	raw, _ := json.Marshal(payload)
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: key, ContractSHA256: hash, Mode: mode, Delivery: delivery, Connection: connection("http://localhost:8080"), Secrets: map[string]string{"corp_id": "corp-id", "corp_secret": "corp-secret"}, Payload: raw}
}
