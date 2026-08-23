package monday

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

func (t *recordingTransport) RoundTripHTTP(_ context.Context, r connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, r)
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

func TestDescriptorEndpointRoutingAndSecretBoundary(t *testing.T) {
	responses := make([]connector.HTTPResponse, 6)
	for i := range responses {
		responses[i] = connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"data":{"create_item":{"id":"I-1"}}}`)}
	}
	transport := &recordingTransport{responses: responses}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	if len(adapter.Descriptor().Operations) != 6 {
		t.Fatalf("descriptor=%+v", adapter.Descriptor())
	}
	validator := adapter.(connector.ConfigValidator)
	for _, target := range []string{defaultEndpoint, "http://localhost:8080/v2"} {
		if err = validator.ValidateConfig(connection(target)); err != nil {
			t.Fatalf("valid=%s err=%v", target, err)
		}
	}
	for _, target := range []string{"https://api.monday.com", "https://api.monday.com/v3", "https://api.monday.com.evil.test/v2", "http://api.monday.com/v2", "https://user@api.monday.com/v2"} {
		if err = validator.ValidateConfig(connection(target)); err == nil {
			t.Fatalf("invalid accepted=%s", target)
		}
	}
	badVersion := connection(defaultEndpoint)
	badVersion.Config["api_version"] = "2026-13"
	if validator.ValidateConfig(badVersion) == nil {
		t.Fatal("bad version accepted")
	}
	calls := []connector.CallRequest{call(ListDeliveryProjects.Key, ListDeliveryProjects.ContractSHA256, ListProjectsInput{PageSize: 25, BoardIDs: "B-1, B-2"}), call(ListDeliveryItems.Key, ListDeliveryItems.ContractSHA256, ListItemsInput{Cursor: "next"}), call(CreateDeliveryItem.Key, CreateDeliveryItem.ContractSHA256, CreateItemInput{Fields: map[string]any{"item_name": "Task", "column_values": map[string]any{"status": map[string]any{"label": "Working"}}}}), call(UpdateDeliveryItem.Key, UpdateDeliveryItem.ContractSHA256, UpdateItemInput{ItemKey: "I-1", Fields: map[string]any{"column_values": map[string]any{"date": map[string]any{"date": "2026-08-24"}}}}), call(TransitionDeliveryItem.Key, TransitionDeliveryItem.ContractSHA256, TransitionItemInput{ItemKey: "I-1", TransitionID: "Done"}), call(TestConnection.Key, TestConnection.ContractSHA256, struct{}{})}
	for _, request := range calls {
		result, callErr := adapter.Call(t.Context(), request)
		if callErr != nil || result.ResponseRef != "monday:I-1" {
			t.Fatalf("operation=%s result=%+v err=%v", request.OperationKey, result, callErr)
		}
	}
	for i, request := range transport.requests {
		if request.URL != "http://localhost:8080/v2" || request.SecretHeaders["Authorization"][0] != "monday-token" || request.Headers["Authorization"] != nil || request.Headers["API-Version"][0] != defaultAPIVersion || strings.Contains(request.URL+string(request.Body), "monday-token") {
			t.Fatalf("request[%d]=%+v", i, request)
		}
	}
}

func TestValidationCloneJSONAndFailureClassifications(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	for _, request := range []connector.CallRequest{call(ListDeliveryItems.Key, ListDeliveryItems.ContractSHA256, ListItemsInput{BoardID: ""}), call(CreateDeliveryItem.Key, CreateDeliveryItem.ContractSHA256, CreateItemInput{Fields: map[string]any{}}), call(UpdateDeliveryItem.Key, UpdateDeliveryItem.ContractSHA256, UpdateItemInput{ItemKey: "I-1", Fields: map[string]any{"values": "{"}}), call(TransitionDeliveryItem.Key, TransitionDeliveryItem.ContractSHA256, TransitionItemInput{ItemKey: "I-1", TransitionID: "{"})} {
		request.Connection.Config = map[string]any{"endpoint": "http://localhost:8080/v2", "api_version": defaultAPIVersion}
		if _, err := adapter.Call(t.Context(), request); err == nil {
			t.Fatalf("invalid accepted=%s", request.OperationKey)
		}
	}
	original := map[string]any{"item_name": "one"}
	clone := providerInput(original, nil)
	clone["item_name"] = "two"
	if original["item_name"] != "one" {
		t.Fatal("input mutated")
	}
	if got, err := jsonValue(map[string]any{"x": 1}, nil); err != nil || got != `{"x":1}` {
		t.Fatalf("json=%s err=%v", got, err)
	}
	for _, test := range []struct {
		name         string
		response     connector.HTTPResponse
		transportErr error
		want         connector.ErrorClassification
	}{{"network", connector.HTTPResponse{}, errors.New("reset"), connector.ErrorUncertain}, {"server", connector.HTTPResponse{StatusCode: 502}, nil, connector.ErrorUncertain}, {"rate", connector.HTTPResponse{StatusCode: 429}, nil, connector.ErrorRetryable}, {"graphql rate", connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"errors":[{"extensions":{"code":"DAILY_LIMIT_EXCEEDED","retry_in_seconds":10}}]}`)}, nil, connector.ErrorRetryable}, {"graphql reject", connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"errors":[{"extensions":{"code":"INVALID_COLUMN"}}]}`)}, nil, connector.ErrorPermanent}, {"invalid", connector.HTTPResponse{StatusCode: 200, Body: []byte(`{`)}, nil, connector.ErrorUncertain}} {
		t.Run(test.name, func(t *testing.T) {
			current, _ := New(&recordingTransport{responses: []connector.HTTPResponse{test.response}, errors: []error{test.transportErr}})
			_, callErr := current.Call(t.Context(), call(CreateDeliveryItem.Key, CreateDeliveryItem.ContractSHA256, CreateItemInput{Fields: map[string]any{"item_name": "Task"}}))
			classification, ok := connector.ErrorClassificationOf(callErr)
			if !ok || classification != test.want {
				t.Fatalf("err=%v class=%q want=%q", callErr, classification, test.want)
			}
		})
	}
}

func connection(target string) connector.Connection {
	return connector.Connection{Config: map[string]any{"endpoint": target, "api_version": defaultAPIVersion, "board_id": "B-1", "status_column_id": "status", "timeout_seconds": 30}}
}
func call(key, hash string, payload any) connector.CallRequest {
	raw, _ := json.Marshal(payload)
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: key, ContractSHA256: hash, Mode: connector.ModeCall, Connection: connection("http://localhost:8080/v2"), Secrets: map[string]string{"api_token": "monday-token"}, Payload: raw}
}
