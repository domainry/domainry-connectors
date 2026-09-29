package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/collaborationwrite"
	"github.com/domainry/domainry-connector-sdk/contracttest"
)

type directSendTransport struct {
	requests  []connector.HTTPRequest
	responses []connector.HTTPResponse
	err       error
}

func (t *directSendTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, request)
	if t.err != nil && len(t.requests) == 2 {
		return connector.HTTPResponse{}, t.err
	}
	index := len(t.requests) - 1
	if index >= len(t.responses) {
		return connector.HTTPResponse{}, nil
	}
	return t.responses[index], nil
}

func (*directSendTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestDirectSendIsOneCallOperationWithBoundReceipt(t *testing.T) {
	transport := &directSendTransport{responses: []connector.HTTPResponse{
		{StatusCode: 200, Body: []byte(`{"code":0,"tenant_access_token":"temporary","expire":7200}`)},
		{StatusCode: 200, Body: []byte(`{"code":0,"data":{"message_id":"message-1"}}`)},
	}}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("adapter=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	descriptor := adapter.Descriptor()
	if len(descriptor.Operations) != 3 {
		t.Fatalf("operations=%+v", descriptor.Operations)
	}
	found := false
	for _, operation := range descriptor.Operations {
		if operation.Key == collaborationwrite.SendOperationKey {
			found = operation.Mode == connector.ModeCall && operation.ContractSHA256 == collaborationwrite.OperationSHA256(operation.Key)
		}
	}
	if !found {
		t.Fatalf("direct operation missing: %+v", descriptor.Operations)
	}
	if scopes, declared := connector.ResolveOAuthOperationScopes(adapter, collaborationwrite.SendOperationKey); !declared || len(scopes) != 1 || len(scopes[0]) != 0 {
		t.Fatalf("scopes=%v declared=%t", scopes, declared)
	}
	payload, _ := json.Marshal(collaborationwrite.SendRequest{Recipient: "buyer@example.test", Text: "已确认，下周见。"})
	result, err := adapter.Call(t.Context(), connector.CallRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: collaborationwrite.SendOperationKey,
		ContractSHA256: collaborationwrite.OperationSHA256(collaborationwrite.SendOperationKey), Mode: connector.ModeCall,
		RequestRef: "account-write:1", Connection: connector.Connection{Config: map[string]any{
			"app_id": "cli-test", "api_base_url": "http://localhost:8080", "receive_id_type": "email", "timeout_seconds": 15,
		}}, Secrets: map[string]string{"app_secret": "app-secret"}, Payload: payload,
	})
	var receipt collaborationwrite.Result
	if err != nil || json.Unmarshal(result.Payload, &receipt) != nil || receipt.Validate("account-write:1") != nil || receipt.MessageID != "message-1" {
		t.Fatalf("result=%s receipt=%+v err=%v", result.Payload, receipt, err)
	}
	if len(transport.requests) != 2 || !strings.Contains(string(transport.requests[1].Body), `"receive_id":"buyer@example.test"`) || !strings.Contains(string(transport.requests[1].Body), `"uuid":"account-write:1"`) {
		t.Fatalf("requests=%+v", transport.requests)
	}
}

func TestDirectSendClassifiesLostProviderResultAsUncertain(t *testing.T) {
	transport := &directSendTransport{
		responses: []connector.HTTPResponse{{StatusCode: 200, Body: []byte(`{"code":0,"tenant_access_token":"temporary","expire":7200}`)}},
		err:       errors.New("connection reset after write"),
	}
	adapter, _ := New(transport)
	payload, _ := json.Marshal(collaborationwrite.SendRequest{Recipient: "buyer@example.test", Text: "已确认。"})
	_, err := adapter.Call(t.Context(), connector.CallRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: collaborationwrite.SendOperationKey,
		ContractSHA256: collaborationwrite.OperationSHA256(collaborationwrite.SendOperationKey), Mode: connector.ModeCall,
		RequestRef: "account-write:uncertain", Connection: connector.Connection{Config: map[string]any{
			"app_id": "cli-test", "api_base_url": "http://localhost:8080", "receive_id_type": "email", "timeout_seconds": 15,
		}}, Secrets: map[string]string{"app_secret": "app-secret"}, Payload: payload,
	})
	classification, ok := connector.ErrorClassificationOf(err)
	if !ok || classification != connector.ErrorUncertain || len(transport.requests) != 2 {
		t.Fatalf("classification=%q requests=%d err=%v", classification, len(transport.requests), err)
	}
}
