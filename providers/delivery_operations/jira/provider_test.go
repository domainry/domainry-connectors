package jira

import (
	"context"
	"encoding/base64"
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

func TestDescriptorEndpointRoutingAndSecretBoundary(t *testing.T) {
	responses := make([]connector.HTTPResponse, 6)
	for i := range responses {
		responses[i] = connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"key":"OPS-1"}`)}
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
	for _, endpoint := range []string{"https://tenant.atlassian.net", "http://localhost:8080"} {
		if err = validator.ValidateConfig(connection(endpoint)); err != nil {
			t.Fatalf("valid=%s err=%v", endpoint, err)
		}
	}
	for _, endpoint := range []string{"http://tenant.atlassian.net", "https://user@tenant.atlassian.net", "https://tenant.atlassian.net/path", "https://tenant.atlassian.net?x=1", "/relative"} {
		if err = validator.ValidateConfig(connection(endpoint)); err == nil {
			t.Fatalf("invalid accepted=%s", endpoint)
		}
	}
	calls := []connector.CallRequest{
		call(ListDeliveryProjects.Key, ListDeliveryProjects.ContractSHA256, ListProjectsInput{StartAt: 1, MaxResults: 10, Query: "ops"}),
		call(ListDeliveryItems.Key, ListDeliveryItems.ContractSHA256, ListItemsInput{}),
		call(CreateDeliveryItem.Key, CreateDeliveryItem.ContractSHA256, CreateItemInput{Fields: map[string]any{"summary": "Task"}}),
		call(UpdateDeliveryItem.Key, UpdateDeliveryItem.ContractSHA256, UpdateItemInput{ItemKey: "OPS/1", Fields: map[string]any{"summary": "Updated"}}),
		call(TransitionDeliveryItem.Key, TransitionDeliveryItem.ContractSHA256, TransitionItemInput{ItemKey: "OPS/1", TransitionID: "31"}),
		call(TestConnection.Key, TestConnection.ContractSHA256, struct{}{}),
	}
	for _, request := range calls {
		result, callErr := adapter.Call(t.Context(), request)
		if callErr != nil || result.ResponseRef != "jira:OPS-1" {
			t.Fatalf("operation=%s result=%+v err=%v", request.OperationKey, result, callErr)
		}
	}
	paths := []string{"/rest/api/3/project/search", "/rest/api/3/search/jql", "/rest/api/3/issue", "/rest/api/3/issue/OPS%2F1?returnIssue=true", "/rest/api/3/issue/OPS%2F1/transitions", "/rest/api/3/myself"}
	wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("admin@example.test:jira-token"))
	for i, want := range paths {
		request := transport.requests[i]
		if !strings.Contains(request.URL, want) {
			t.Fatalf("request[%d]=%s want=%s", i, request.URL, want)
		}
		if request.SecretHeaders["Authorization"][0] != wantAuth || request.Headers["Authorization"] != nil || strings.Contains(request.URL+string(request.Body), "jira-token") {
			t.Fatalf("secret boundary=%+v", request)
		}
	}
	var search map[string]any
	if err := json.Unmarshal(transport.requests[1].Body, &search); err != nil || search["jql"] != "project = OPS ORDER BY updated DESC" || search["maxResults"] != float64(50) {
		t.Fatalf("search=%v err=%v", search, err)
	}
}

func TestBearerValidationCloneAndFailureClassifications(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: 200}}}
	adapter, _ := New(transport)
	request := call(TestConnection.Key, TestConnection.ContractSHA256, struct{}{})
	request.Connection.Config["account_email"] = ""
	if _, err := adapter.Call(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if transport.requests[0].SecretHeaders["Authorization"][0] != "Bearer jira-token" {
		t.Fatalf("request=%+v", transport.requests[0])
	}
	for _, invalid := range []connector.CallRequest{call(CreateDeliveryItem.Key, CreateDeliveryItem.ContractSHA256, CreateItemInput{}), call(UpdateDeliveryItem.Key, UpdateDeliveryItem.ContractSHA256, UpdateItemInput{}), call(TransitionDeliveryItem.Key, TransitionDeliveryItem.ContractSHA256, TransitionItemInput{ItemKey: "OPS-1"})} {
		if _, err := adapter.Call(t.Context(), invalid); err == nil {
			t.Fatalf("invalid accepted=%s", invalid.OperationKey)
		}
	}
	original := map[string]any{"summary": "one"}
	cloned := cloneMap(original)
	cloned["summary"] = "two"
	if original["summary"] != "one" {
		t.Fatal("input mutated")
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
		{"reject", connector.HTTPResponse{StatusCode: 400, Body: []byte(`{"errorMessages":["bad"]}`)}, nil, connector.ErrorPermanent},
		{"invalid", connector.HTTPResponse{StatusCode: 201, Body: []byte(`{`)}, nil, connector.ErrorUncertain},
	} {
		t.Run(test.name, func(t *testing.T) {
			current, _ := New(&recordingTransport{responses: []connector.HTTPResponse{test.response}, errors: []error{test.transportErr}})
			_, callErr := current.Call(t.Context(), call(CreateDeliveryItem.Key, CreateDeliveryItem.ContractSHA256, CreateItemInput{Fields: map[string]any{"summary": "Task"}}))
			classification, ok := connector.ErrorClassificationOf(callErr)
			if !ok || classification != test.want {
				t.Fatalf("err=%v class=%q want=%q", callErr, classification, test.want)
			}
		})
	}
}

func connection(endpoint string) connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": endpoint, "account_email": "admin@example.test", "project_key": "OPS", "timeout_seconds": 30}}
}
func call(key, hash string, payload any) connector.CallRequest {
	raw, _ := json.Marshal(payload)
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: key, ContractSHA256: hash, Mode: connector.ModeCall, Connection: connection("http://localhost:8080"), Secrets: map[string]string{"api_token": "jira-token"}, Payload: raw}
}
