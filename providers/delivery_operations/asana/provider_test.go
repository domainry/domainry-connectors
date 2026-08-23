package asana

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
func TestDescriptorEndpointAndOperationRouting(t *testing.T) {
	responses := make([]connector.HTTPResponse, 6)
	for i := range responses {
		responses[i] = connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"data":{"gid":"T-1"}}`)}
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
	for _, endpoint := range []string{defaultAPIBase, "http://localhost:8080"} {
		if err = validator.ValidateConfig(connection(endpoint)); err != nil {
			t.Fatalf("valid=%s err=%v", endpoint, err)
		}
	}
	for _, endpoint := range []string{"https://app.asana.com", "https://app.asana.com/api/1.1", "https://app.asana.com/api/1.0/custom", "https://app.asana.com.evil.test/api/1.0", "http://app.asana.com/api/1.0"} {
		if err = validator.ValidateConfig(connection(endpoint)); err == nil {
			t.Fatalf("invalid accepted=%s", endpoint)
		}
	}
	calls := []connector.CallRequest{call(ListDeliveryProjects.Key, ListDeliveryProjects.ContractSHA256, ListProjectsInput{PageSize: 25, Cursor: "next"}), call(ListDeliveryItems.Key, ListDeliveryItems.ContractSHA256, ListItemsInput{}), call(CreateDeliveryItem.Key, CreateDeliveryItem.ContractSHA256, CreateItemInput{Fields: map[string]any{"name": "Task"}}), call(UpdateDeliveryItem.Key, UpdateDeliveryItem.ContractSHA256, UpdateItemInput{ItemKey: "T/1", Input: map[string]any{"notes": "Updated"}}), call(TransitionDeliveryItem.Key, TransitionDeliveryItem.ContractSHA256, TransitionItemInput{ItemKey: "T/1", TransitionID: "completed"}), call(TestConnection.Key, TestConnection.ContractSHA256, struct{}{})}
	for _, request := range calls {
		result, callErr := adapter.Call(t.Context(), request)
		if callErr != nil || result.ResponseRef != "asana:T-1" {
			t.Fatalf("operation=%s result=%+v err=%v", request.OperationKey, result, callErr)
		}
	}
	paths := []string{"/projects", "/projects/P-1/tasks", "/tasks", "/tasks/T%2F1", "/tasks/T%2F1", "/users/me"}
	for i, want := range paths {
		if !strings.Contains(transport.requests[i].URL, want) {
			t.Fatalf("request[%d]=%s want=%s", i, transport.requests[i].URL, want)
		}
		if transport.requests[i].SecretHeaders["Authorization"][0] != "Bearer asana-token" || strings.Contains(transport.requests[i].URL+string(transport.requests[i].Body), "asana-token") {
			t.Fatalf("secret boundary=%+v", transport.requests[i])
		}
	}
}
func TestInputValidationCloneAndWriteClassifications(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	for _, request := range []connector.CallRequest{call(ListDeliveryProjects.Key, ListDeliveryProjects.ContractSHA256, ListProjectsInput{}), call(CreateDeliveryItem.Key, CreateDeliveryItem.ContractSHA256, CreateItemInput{}), call(UpdateDeliveryItem.Key, UpdateDeliveryItem.ContractSHA256, UpdateItemInput{ItemKey: "T"}), call(TransitionDeliveryItem.Key, TransitionDeliveryItem.ContractSHA256, TransitionItemInput{ItemKey: "T", TransitionID: "review"})} {
		request.Connection.Config = map[string]any{"api_base_url": "http://localhost:8080"}
		if _, err := adapter.Call(t.Context(), request); err == nil {
			t.Fatalf("invalid accepted=%s", request.OperationKey)
		}
	}
	original := map[string]any{"name": "one"}
	clone := providerInput(original, nil)
	clone["name"] = "two"
	if original["name"] != "one" {
		t.Fatal("input mutated")
	}
	for _, test := range []struct {
		name         string
		response     connector.HTTPResponse
		transportErr error
		want         connector.ErrorClassification
	}{{"network", connector.HTTPResponse{}, errors.New("reset"), connector.ErrorUncertain}, {"server", connector.HTTPResponse{StatusCode: 502}, nil, connector.ErrorUncertain}, {"rate", connector.HTTPResponse{StatusCode: 429}, nil, connector.ErrorRetryable}, {"reject", connector.HTTPResponse{StatusCode: 400, Body: []byte(`{"errors":[{"phrase":"Bad Input!"}]}`)}, nil, connector.ErrorPermanent}, {"invalid", connector.HTTPResponse{StatusCode: 201, Body: []byte(`{`)}, nil, connector.ErrorUncertain}} {
		t.Run(test.name, func(t *testing.T) {
			current, _ := New(&recordingTransport{responses: []connector.HTTPResponse{test.response}, errors: []error{test.transportErr}})
			_, callErr := current.Call(t.Context(), call(CreateDeliveryItem.Key, CreateDeliveryItem.ContractSHA256, CreateItemInput{Fields: map[string]any{"name": "Task"}}))
			classification, ok := connector.ErrorClassificationOf(callErr)
			if !ok || classification != test.want {
				t.Fatalf("err=%v class=%q want=%q", callErr, classification, test.want)
			}
		})
	}
}
func connection(endpoint string) connector.Connection {
	return connector.Connection{Config: map[string]any{"api_base_url": endpoint, "workspace_gid": "W-1", "project_gid": "P-1", "timeout_seconds": 30}}
}
func call(key, hash string, payload any) connector.CallRequest {
	raw, _ := json.Marshal(payload)
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: key, ContractSHA256: hash, Mode: connector.ModeCall, Connection: connection("http://localhost:8080"), Secrets: map[string]string{"access_token": "asana-token"}, Payload: raw}
}
