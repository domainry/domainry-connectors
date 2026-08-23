package sapbusinessone

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
		responses[i] = connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"AbsEntry":42}`)}
	}
	transport := &recordingTransport{responses: responses}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	if adapter.Descriptor().ProviderKey != ProviderKey || len(adapter.Descriptor().Operations) != 6 {
		t.Fatalf("descriptor=%+v", adapter.Descriptor())
	}
	validator := adapter.(connector.ConfigValidator)
	for _, target := range []string{"https://sap.example.test/b1s/v2", "http://localhost:8080/b1s/v2"} {
		if err = validator.ValidateConfig(connection(target)); err != nil {
			t.Fatalf("valid=%s err=%v", target, err)
		}
	}
	for _, target := range []string{"https://sap.example.test/b1s/v1", "https://sap.example.test/b1s/v2/extra", "http://sap.example.test/b1s/v2", "https://user@sap.example.test/b1s/v2", "https://sap.example.test/b1s/v2?x=1"} {
		if validator.ValidateConfig(connection(target)) == nil {
			t.Fatalf("invalid accepted=%s", target)
		}
	}
	missing := connection("https://sap.example.test/b1s/v2")
	missing.Config["company_id"] = ""
	if validator.ValidateConfig(missing) == nil {
		t.Fatal("missing company accepted")
	}
	calls := []connector.CallRequest{call(ListDeliveryProjects.Key, ListDeliveryProjects.ContractSHA256, ListInput{Select: "AbsEntry,ProjectName", PageSize: 25}), call(ListDeliveryItems.Key, ListDeliveryItems.ContractSHA256, ListInput{Filter: "ProjectStatus eq 'pst_Started'"}), call(CreateDeliveryItem.Key, CreateDeliveryItem.ContractSHA256, CreateItemInput{Fields: map[string]any{"ProjectName": "Delivery"}}), call(UpdateDeliveryItem.Key, UpdateDeliveryItem.ContractSHA256, UpdateItemInput{ItemKey: "42", Fields: map[string]any{"Reason": "Updated"}}), call(TransitionDeliveryItem.Key, TransitionDeliveryItem.ContractSHA256, TransitionItemInput{ItemKey: "42", TransitionID: "pst_Completed"}), call(TestConnection.Key, TestConnection.ContractSHA256, struct{}{})}
	for _, request := range calls {
		result, callErr := adapter.Call(t.Context(), request)
		if callErr != nil || result.ResponseRef != "sap_business_one:42" {
			t.Fatalf("operation=%s result=%+v err=%v", request.OperationKey, result, callErr)
		}
	}
	paths := []string{"/ProjectManagements?%24select=AbsEntry%2CProjectName&%24skip=0&%24top=25", "/ProjectManagements?%24filter=ProjectStatus+eq+%27pst_Started%27&%24skip=0&%24top=50", "/ProjectManagements", "/ProjectManagements(42)", "/ProjectManagements(42)", "/UsersService_GetCurrentUser"}
	for i, want := range paths {
		request := transport.requests[i]
		if !strings.Contains(request.URL, want) || request.SecretHeaders["Authorization"][0] != "Bearer sap-token" || request.Headers["Authorization"] != nil || request.Headers["X-b1-companyid"][0] != "SBODEMOUS" || strings.Contains(request.URL+string(request.Body), "sap-token") {
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
	original := map[string]any{"Reason": "one"}
	clone := providerFields(original, nil)
	clone["Reason"] = "two"
	if original["Reason"] != "one" {
		t.Fatal("input mutated")
	}
	for _, test := range []struct {
		name         string
		response     connector.HTTPResponse
		transportErr error
		want         connector.ErrorClassification
	}{{"network", connector.HTTPResponse{}, errors.New("reset"), connector.ErrorUncertain}, {"server", connector.HTTPResponse{StatusCode: 502}, nil, connector.ErrorUncertain}, {"rate", connector.HTTPResponse{StatusCode: 429}, nil, connector.ErrorRetryable}, {"reject", connector.HTTPResponse{StatusCode: 400, Body: []byte(`{"error":{"code":"Bad Code!"}}`)}, nil, connector.ErrorPermanent}, {"invalid", connector.HTTPResponse{StatusCode: 200, Body: []byte(`{`)}, nil, connector.ErrorUncertain}} {
		t.Run(test.name, func(t *testing.T) {
			current, _ := New(&recordingTransport{responses: []connector.HTTPResponse{test.response}, errors: []error{test.transportErr}})
			_, callErr := current.Call(t.Context(), call(CreateDeliveryItem.Key, CreateDeliveryItem.ContractSHA256, CreateItemInput{Fields: map[string]any{"ProjectName": "Delivery"}}))
			classification, ok := connector.ErrorClassificationOf(callErr)
			if !ok || classification != test.want {
				t.Fatalf("err=%v class=%q want=%q", callErr, classification, test.want)
			}
		})
	}
}
func connection(target string) connector.Connection {
	return connector.Connection{Config: map[string]any{"service_root": target, "company_id": "SBODEMOUS", "status_field": "ProjectStatus", "timeout_seconds": 30}}
}
func call(key, hash string, payload any) connector.CallRequest {
	raw, _ := json.Marshal(payload)
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: key, ContractSHA256: hash, Mode: connector.ModeCall, Connection: connection("http://localhost:8080/b1s/v2"), Secrets: map[string]string{"access_token": "sap-token"}, Payload: raw}
}
