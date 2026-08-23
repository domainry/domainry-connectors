package netsuite

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
		responses[i] = connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"id":"42"}`)}
	}
	transport := &recordingTransport{responses: responses}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	validator := adapter.(connector.ConfigValidator)
	for _, target := range []string{"https://123456.suitetalk.api.netsuite.com/services/rest/record/v1", "http://localhost:8080/services/rest/record/v1"} {
		if err = validator.ValidateConfig(connection(target)); err != nil {
			t.Fatalf("valid=%s err=%v", target, err)
		}
	}
	for _, target := range []string{"https://suitetalk.api.netsuite.com/services/rest/record/v1", "https://123456.suitetalk.api.netsuite.com/services/rest/record/v2", "https://123456.suitetalk.api.netsuite.com.evil.test/services/rest/record/v1", "http://123456.suitetalk.api.netsuite.com/services/rest/record/v1", "https://user@123456.suitetalk.api.netsuite.com/services/rest/record/v1"} {
		if validator.ValidateConfig(connection(target)) == nil {
			t.Fatalf("invalid accepted=%s", target)
		}
	}
	calls := []connector.CallRequest{call(ListDeliveryProjects.Key, ListDeliveryProjects.ContractSHA256, ListInput{PageSize: 25}), call(ListDeliveryItems.Key, ListDeliveryItems.ContractSHA256, ListInput{Query: "company ANY_OF 7"}), call(CreateDeliveryItem.Key, CreateDeliveryItem.ContractSHA256, CreateItemInput{Fields: map[string]any{"title": "Task"}}), call(UpdateDeliveryItem.Key, UpdateDeliveryItem.ContractSHA256, UpdateItemInput{ItemKey: "4/2", Fields: map[string]any{"message": "Updated"}}), call(TransitionDeliveryItem.Key, TransitionDeliveryItem.ContractSHA256, TransitionItemInput{ItemKey: "4/2", TransitionID: "2", Fields: map[string]any{"memo": "done"}}), call(TestConnection.Key, TestConnection.ContractSHA256, struct{}{})}
	for _, request := range calls {
		result, callErr := adapter.Call(t.Context(), request)
		if callErr != nil || result.ResponseRef != "netsuite:42" {
			t.Fatalf("operation=%s result=%+v err=%v", request.OperationKey, result, callErr)
		}
	}
	paths := []string{"/job?limit=25&offset=0", "/projecttask?limit=100&offset=0&q=company+ANY_OF+7", "/projecttask", "/projecttask/4%2F2", "/projecttask/4%2F2", "/job?limit=1"}
	for i, want := range paths {
		request := transport.requests[i]
		if !strings.Contains(request.URL, want) || request.SecretHeaders["Authorization"][0] != "Bearer netsuite-token" || request.Headers["Authorization"] != nil || strings.Contains(request.URL+string(request.Body), "netsuite-token") {
			t.Fatalf("request[%d]=%+v want=%s", i, request, want)
		}
	}
}
func TestValidationCloneAndFailureClassifications(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	for _, request := range []connector.CallRequest{call(CreateDeliveryItem.Key, CreateDeliveryItem.ContractSHA256, CreateItemInput{}), call(UpdateDeliveryItem.Key, UpdateDeliveryItem.ContractSHA256, UpdateItemInput{ItemKey: "42"}), call(TransitionDeliveryItem.Key, TransitionDeliveryItem.ContractSHA256, TransitionItemInput{ItemKey: "42"})} {
		if _, err := adapter.Call(t.Context(), request); err == nil {
			t.Fatalf("invalid accepted=%s", request.OperationKey)
		}
	}
	original := map[string]any{"title": "one"}
	clone := providerFields(original, nil)
	clone["title"] = "two"
	if original["title"] != "one" {
		t.Fatal("input mutated")
	}
	for _, test := range []struct {
		name         string
		response     connector.HTTPResponse
		transportErr error
		want         connector.ErrorClassification
	}{{"network", connector.HTTPResponse{}, errors.New("reset"), connector.ErrorUncertain}, {"server", connector.HTTPResponse{StatusCode: 502}, nil, connector.ErrorUncertain}, {"rate", connector.HTTPResponse{StatusCode: 429}, nil, connector.ErrorRetryable}, {"reject", connector.HTTPResponse{StatusCode: 400, Body: []byte(`{"o:errorDetails":[{"o:errorCode":"INVALID_FLD_VALUE"}]}`)}, nil, connector.ErrorPermanent}, {"invalid", connector.HTTPResponse{StatusCode: 201, Body: []byte(`{`)}, nil, connector.ErrorUncertain}} {
		t.Run(test.name, func(t *testing.T) {
			current, _ := New(&recordingTransport{responses: []connector.HTTPResponse{test.response}, errors: []error{test.transportErr}})
			_, callErr := current.Call(t.Context(), call(CreateDeliveryItem.Key, CreateDeliveryItem.ContractSHA256, CreateItemInput{Fields: map[string]any{"title": "Task"}}))
			classification, ok := connector.ErrorClassificationOf(callErr)
			if !ok || classification != test.want {
				t.Fatalf("err=%v class=%q want=%q", callErr, classification, test.want)
			}
		})
	}
}
func connection(target string) connector.Connection {
	return connector.Connection{Config: map[string]any{"record_base_url": target, "project_id": "7", "status_field": "status", "timeout_seconds": 30}}
}
func call(key, hash string, payload any) connector.CallRequest {
	raw, _ := json.Marshal(payload)
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: key, ContractSHA256: hash, Mode: connector.ModeCall, Connection: connection("http://localhost:8080/services/rest/record/v1"), Secrets: map[string]string{"access_token": "netsuite-token"}, Payload: raw}
}
