package line

import (
	"context"
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

func TestDescriptorAndEndpointBoundary(t *testing.T) {
	adapter, err := New(&recordingTransport{})
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	d := adapter.Descriptor()
	if d.ConnectorKey != ConnectorKey || d.ProviderKey != ProviderKey || len(d.Operations) != 2 || len(d.SecretFields) != 1 {
		t.Fatalf("descriptor=%+v", d)
	}
	validator := adapter.(connector.ConfigValidator)
	for _, endpoint := range []string{defaultAPIBase, "http://localhost:8080"} {
		if err = validator.ValidateConfig(connection(endpoint)); err != nil {
			t.Fatalf("valid=%s err=%v", endpoint, err)
		}
	}
	for _, endpoint := range []string{"https://api.line.me/custom", "https://api.line.me.evil.test", "http://api.line.me", "https://api-data.line.me"} {
		if err = validator.ValidateConfig(connection(endpoint)); err == nil {
			t.Fatalf("invalid accepted=%s", endpoint)
		}
	}
}

func TestRequestsKeepTokenRuntimeOnlyAndPreserveNativeReceipt(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: 200, Body: []byte(`{"displayName":"Bot"}`)}, {StatusCode: 200, Headers: map[string][]string{"x-line-request-id": {"request-42"}}, Body: []byte(`{}`)}}}
	adapter, _ := New(transport)
	result, err := adapter.Call(t.Context(), call(TestConnection.Key, TestConnection.ContractSHA256, connector.ModeCall, false, struct{}{}))
	if err != nil || !strings.Contains(string(result.Payload), `"connected":true`) {
		t.Fatalf("test=%+v err=%v", result, err)
	}
	delivery, err := adapter.Call(t.Context(), call(SendMessage.Key, SendMessage.ContractSHA256, connector.ModeEnqueue, true, SendMessageInput{Recipient: "U123", Message: "Approved"}))
	if err != nil || delivery.ResponseRef != "line:request-42" {
		t.Fatalf("delivery=%+v err=%v", delivery, err)
	}
	for _, request := range transport.requests {
		if request.SecretHeaders["Authorization"][0] != "Bearer line-token" || strings.Contains(request.URL+string(request.Body), "line-token") {
			t.Fatalf("token boundary=%+v", request)
		}
	}
	if transport.requests[0].URL != "http://localhost:8080/v2/bot/info" || transport.requests[1].URL != "http://localhost:8080/v2/bot/message/push" || !strings.Contains(string(transport.requests[1].Body), `"to":"U123"`) {
		t.Fatalf("requests=%+v", transport.requests)
	}
}

func TestProviderPayloadValidationAndWriteClassification(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: 200, Body: []byte(`{}`)}}}
	adapter, _ := New(transport)
	_, err := adapter.Call(t.Context(), call(SendMessage.Key, SendMessage.ContractSHA256, connector.ModeEnqueue, true, SendMessageInput{Recipient: "U1", Message: "fallback", ProviderPayload: map[string]any{"messages": []any{map[string]any{"type": "flex", "altText": "Approval"}}}}))
	if err != nil || !strings.Contains(string(transport.requests[0].Body), `"type":"flex"`) {
		t.Fatalf("request=%+v err=%v", transport.requests, err)
	}
	for _, input := range []SendMessageInput{{Message: "hello"}, {Recipient: "U1"}} {
		if _, err = adapter.Call(t.Context(), call(SendMessage.Key, SendMessage.ContractSHA256, connector.ModeEnqueue, true, input)); err == nil {
			t.Fatalf("invalid accepted=%+v", input)
		}
	}
	for _, test := range []struct {
		name         string
		response     connector.HTTPResponse
		transportErr error
		want         connector.ErrorClassification
	}{{"network", connector.HTTPResponse{}, errors.New("reset"), connector.ErrorUncertain}, {"server", connector.HTTPResponse{StatusCode: 502}, nil, connector.ErrorUncertain}, {"rate", connector.HTTPResponse{StatusCode: 429}, nil, connector.ErrorRetryable}, {"reject", connector.HTTPResponse{StatusCode: 400}, nil, connector.ErrorPermanent}, {"invalid", connector.HTTPResponse{StatusCode: 200, Body: []byte(`{`)}, nil, connector.ErrorUncertain}} {
		t.Run(test.name, func(t *testing.T) {
			current, _ := New(&recordingTransport{responses: []connector.HTTPResponse{test.response}, errors: []error{test.transportErr}})
			_, callErr := current.Call(t.Context(), call(SendMessage.Key, SendMessage.ContractSHA256, connector.ModeEnqueue, true, SendMessageInput{Recipient: "U1", Message: "hello"}))
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
func call(key, hash string, mode connector.OperationMode, delivery bool, payload any) connector.CallRequest {
	raw, _ := json.Marshal(payload)
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: key, ContractSHA256: hash, Mode: mode, Delivery: delivery, Connection: connection("http://localhost:8080"), Secrets: map[string]string{"channel_access_token": "line-token"}, Payload: raw}
}
