package googleworkspace

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

func TestDescriptorEndpointAndSpaceBoundaries(t *testing.T) {
	adapter, err := New(&recordingTransport{})
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	descriptor := adapter.Descriptor()
	if descriptor.ConnectorKey != ConnectorKey || descriptor.ProviderKey != ProviderKey || len(descriptor.Operations) != 2 || len(descriptor.SecretFields) != 1 || descriptor.SecretFields[0].RotationPolicy != connector.SecretRotationOAuthRefresh {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	validator := adapter.(connector.ConfigValidator)
	for _, endpoint := range []string{defaultAPIBase, "http://localhost:8080"} {
		if err = validator.ValidateConfig(connection(endpoint)); err != nil {
			t.Fatalf("valid=%s err=%v", endpoint, err)
		}
	}
	for _, endpoint := range []string{"https://chat.googleapis.com", "https://chat.googleapis.com/v2", "https://chat.googleapis.com/v1/custom", "https://chat.googleapis.com.evil.test/v1", "http://chat.googleapis.com/v1"} {
		if err = validator.ValidateConfig(connection(endpoint)); err == nil {
			t.Fatalf("invalid endpoint accepted=%s", endpoint)
		}
	}
	for _, space := range []string{"", "space-1", "spaces", "spaces/../messages", "rooms/one"} {
		current := connection(defaultAPIBase)
		current.Config["space_name"] = space
		if err = validator.ValidateConfig(current); err == nil {
			t.Fatalf("invalid space accepted=%q", space)
		}
	}
}

func TestCallsKeepOAuthTokenRuntimeOnly(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{
		{StatusCode: 200, Body: []byte(`{"name":"spaces/default"}`)},
		{StatusCode: 200, Body: []byte(`{"name":"spaces/override/messages/message-1"}`)},
	}}
	adapter, _ := New(transport)
	testResult, err := adapter.Call(t.Context(), call(TestConnection.Key, TestConnection.ContractSHA256, connector.ModeCall, false, struct{}{}))
	if err != nil || !strings.Contains(string(testResult.Payload), `"connected":true`) {
		t.Fatalf("test=%+v err=%v", testResult, err)
	}
	delivery, err := adapter.Call(t.Context(), call(SendMessage.Key, SendMessage.ContractSHA256, connector.ModeEnqueue, true, SendMessageInput{Recipient: "spaces/override", Message: "Approved"}))
	if err != nil || delivery.ResponseRef != "google_chat:spaces/override/messages/message-1" {
		t.Fatalf("delivery=%+v err=%v", delivery, err)
	}
	if transport.requests[0].URL != "http://localhost:8080/spaces/default" || transport.requests[1].URL != "http://localhost:8080/spaces/override/messages" {
		t.Fatalf("requests=%+v", transport.requests)
	}
	for _, request := range transport.requests {
		if request.SecretHeaders["Authorization"][0] != "Bearer oauth-token" || strings.Contains(request.URL+string(request.Body), "oauth-token") {
			t.Fatalf("token boundary=%+v", request)
		}
	}
	if !strings.Contains(string(transport.requests[1].Body), `"text":"Approved"`) {
		t.Fatalf("body=%s", transport.requests[1].Body)
	}
}

func TestProviderPayloadValidationAndWriteClassification(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: 200, Body: []byte(`{"name":"custom"}`)}}}
	adapter, _ := New(transport)
	_, err := adapter.Call(t.Context(), call(SendMessage.Key, SendMessage.ContractSHA256, connector.ModeEnqueue, true, SendMessageInput{Recipient: "spaces/one", Message: "fallback", ProviderPayload: map[string]any{"cardsV2": []any{map[string]any{"cardId": "card-1"}}}}))
	if err != nil || !strings.Contains(string(transport.requests[0].Body), `"cardsV2"`) {
		t.Fatalf("request=%+v err=%v", transport.requests, err)
	}
	for _, input := range []SendMessageInput{{Message: "hello"}, {Recipient: "spaces/one"}, {Recipient: "invalid", Message: "hello"}} {
		if _, err = adapter.Call(t.Context(), call(SendMessage.Key, SendMessage.ContractSHA256, connector.ModeEnqueue, true, input)); err == nil {
			t.Fatalf("invalid input accepted=%+v", input)
		}
	}
	for _, test := range []struct {
		name         string
		response     connector.HTTPResponse
		transportErr error
		want         connector.ErrorClassification
	}{
		{"network", connector.HTTPResponse{}, errors.New("reset"), connector.ErrorUncertain},
		{"server", connector.HTTPResponse{StatusCode: 502}, nil, connector.ErrorUncertain},
		{"rate", connector.HTTPResponse{StatusCode: 429}, nil, connector.ErrorRetryable},
		{"reject", connector.HTTPResponse{StatusCode: 400}, nil, connector.ErrorPermanent},
		{"invalid", connector.HTTPResponse{StatusCode: 200, Body: []byte(`{`)}, nil, connector.ErrorUncertain},
	} {
		t.Run(test.name, func(t *testing.T) {
			current, _ := New(&recordingTransport{responses: []connector.HTTPResponse{test.response}, errors: []error{test.transportErr}})
			_, callErr := current.Call(t.Context(), call(SendMessage.Key, SendMessage.ContractSHA256, connector.ModeEnqueue, true, SendMessageInput{Recipient: "spaces/one", Message: "hello"}))
			classification, ok := connector.ErrorClassificationOf(callErr)
			if !ok || classification != test.want {
				t.Fatalf("err=%v classification=%q want=%q", callErr, classification, test.want)
			}
		})
	}
}

func connection(endpoint string) connector.Connection {
	return connector.Connection{Config: map[string]any{"api_base_url": endpoint, "space_name": "spaces/default", "timeout_seconds": 15}}
}
func call(key, hash string, mode connector.OperationMode, delivery bool, payload any) connector.CallRequest {
	raw, _ := json.Marshal(payload)
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: key, ContractSHA256: hash, Mode: mode, Delivery: delivery, Connection: connection("http://localhost:8080"), Secrets: map[string]string{"access_token": "oauth-token"}, Payload: raw}
}
