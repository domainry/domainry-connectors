package odoo

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
	responses := []connector.HTTPResponse{{StatusCode: 200, Body: []byte(`[]`)}, {StatusCode: 200, Body: []byte(`[]`)}, {StatusCode: 200, Body: []byte(`[42]`)}, {StatusCode: 200, Body: []byte(`true`)}, {StatusCode: 200, Body: []byte(`true`)}, {StatusCode: 200, Body: []byte(`2`)}}
	transport := &recordingTransport{responses: responses}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	validator := adapter.(connector.ConfigValidator)
	for _, target := range []string{"https://tenant.odoo.com/json/2", "https://erp.example.test/json/2", "http://localhost:8080/json/2"} {
		if err = validator.ValidateConfig(connection(target)); err != nil {
			t.Fatalf("valid=%s err=%v", target, err)
		}
	}
	for _, target := range []string{"https://tenant.odoo.com/jsonrpc", "https://tenant.odoo.com/json/2/extra", "http://tenant.odoo.com/json/2", "https://user@tenant.odoo.com/json/2", "https://tenant.odoo.com/json/2?x=1"} {
		if validator.ValidateConfig(connection(target)) == nil {
			t.Fatalf("invalid accepted=%s", target)
		}
	}
	calls := []connector.CallRequest{call(ListDeliveryProjects.Key, ListDeliveryProjects.ContractSHA256, ListInput{PageSize: 25}), call(ListDeliveryItems.Key, ListDeliveryItems.ContractSHA256, ListInput{ProjectID: "7"}), call(CreateDeliveryItem.Key, CreateDeliveryItem.ContractSHA256, CreateItemInput{Fields: map[string]any{"name": "Task"}}), call(UpdateDeliveryItem.Key, UpdateDeliveryItem.ContractSHA256, UpdateItemInput{ItemKey: "42", Fields: map[string]any{"description": "Updated"}}), call(TransitionDeliveryItem.Key, TransitionDeliveryItem.ContractSHA256, TransitionItemInput{ItemKey: "42", TransitionID: "8"}), call(TestConnection.Key, TestConnection.ContractSHA256, struct{}{})}
	for _, request := range calls {
		result, callErr := adapter.Call(t.Context(), request)
		if callErr != nil {
			t.Fatalf("operation=%s result=%+v err=%v", request.OperationKey, result, callErr)
		}
		if request.OperationKey == CreateDeliveryItem.Key && result.ResponseRef != "odoo:42" {
			t.Fatalf("create result=%+v", result)
		}
	}
	paths := []string{"/project.project/search_read", "/project.task/search_read", "/project.task/create", "/project.task/write", "/project.task/write", "/project.project/search_count"}
	for i, want := range paths {
		request := transport.requests[i]
		if !strings.HasSuffix(request.URL, want) || request.SecretHeaders["Authorization"][0] != "bearer odoo-key" || request.Headers["Authorization"] != nil || request.Headers["X-Odoo-Database"][0] != "runtime" || strings.Contains(request.URL+string(request.Body), "odoo-key") {
			t.Fatalf("request[%d]=%+v", i, request)
		}
	}
	if transport.requests[2].URL == "" {
		t.Fatal("missing create")
	}
}
func TestCreateReferenceValidationCloneAndFailureClassifications(t *testing.T) {
	current, _ := New(&recordingTransport{responses: []connector.HTTPResponse{{StatusCode: 200, Body: []byte(`[42]`)}}})
	result, err := current.Call(t.Context(), call(CreateDeliveryItem.Key, CreateDeliveryItem.ContractSHA256, CreateItemInput{Fields: map[string]any{"name": "Task"}}))
	if err != nil || result.ResponseRef != "odoo:42" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	adapter, _ := New(&recordingTransport{})
	for _, request := range []connector.CallRequest{call(CreateDeliveryItem.Key, CreateDeliveryItem.ContractSHA256, CreateItemInput{}), call(UpdateDeliveryItem.Key, UpdateDeliveryItem.ContractSHA256, UpdateItemInput{ItemKey: "42"}), call(TransitionDeliveryItem.Key, TransitionDeliveryItem.ContractSHA256, TransitionItemInput{ItemKey: "42"})} {
		if _, err := adapter.Call(t.Context(), request); err == nil {
			t.Fatalf("invalid accepted=%s", request.OperationKey)
		}
	}
	original := map[string]any{"name": "one"}
	clone := providerFields(original, nil)
	clone["name"] = "two"
	if original["name"] != "one" {
		t.Fatal("input mutated")
	}
	for _, test := range []struct {
		name         string
		response     connector.HTTPResponse
		transportErr error
		want         connector.ErrorClassification
	}{{"network", connector.HTTPResponse{}, errors.New("reset"), connector.ErrorUncertain}, {"server", connector.HTTPResponse{StatusCode: 502}, nil, connector.ErrorUncertain}, {"rate", connector.HTTPResponse{StatusCode: 429}, nil, connector.ErrorRetryable}, {"reject", connector.HTTPResponse{StatusCode: 422, Body: []byte(`{"name":"odoo.exceptions.ValidationError"}`)}, nil, connector.ErrorPermanent}, {"invalid", connector.HTTPResponse{StatusCode: 200, Body: []byte(`{`)}, nil, connector.ErrorUncertain}} {
		t.Run(test.name, func(t *testing.T) {
			provider, _ := New(&recordingTransport{responses: []connector.HTTPResponse{test.response}, errors: []error{test.transportErr}})
			_, callErr := provider.Call(t.Context(), call(CreateDeliveryItem.Key, CreateDeliveryItem.ContractSHA256, CreateItemInput{Fields: map[string]any{"name": "Task"}}))
			classification, ok := connector.ErrorClassificationOf(callErr)
			if !ok || classification != test.want {
				t.Fatalf("err=%v class=%q want=%q", callErr, classification, test.want)
			}
		})
	}
}
func connection(target string) connector.Connection {
	return connector.Connection{Config: map[string]any{"api_base_url": target, "database": "runtime", "project_id": "5", "context_language": "en_US", "timeout_seconds": 30}}
}
func call(key, hash string, payload any) connector.CallRequest {
	raw, _ := json.Marshal(payload)
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: key, ContractSHA256: hash, Mode: connector.ModeCall, Connection: connection("http://localhost:8080/json/2"), Secrets: map[string]string{"api_key": "odoo-key"}, Payload: raw}
}
