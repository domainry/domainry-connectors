package talentlms

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"net/http"
	"strings"
	"testing"
)

type recordingTransport struct {
	requests []connector.HTTPRequest
	response connector.HTTPResponse
	err      error
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, request)
	return t.response, t.err
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestReadOperationsUseRuntimeOnlyBasicSecret(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`[{"id":"7"}]`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Call(t.Context(), callRequest(ListCourses, mustJSON(ListCoursesInput{})))
	if err != nil || !strings.Contains(string(result.Payload), "7") {
		t.Fatalf("result=%s error=%v", result.Payload, err)
	}
	request := transport.requests[0]
	expected := "Basic " + base64.StdEncoding.EncodeToString([]byte("api-key:"))
	if request.SecretHeaders["Authorization"][0] != expected || strings.Contains(request.URL, "api-key") || request.URL != "http://localhost:8080/api/v1/courses" {
		t.Fatalf("request=%+v", request)
	}
	transport.response.Body = []byte(`{"id":"42","users":[{"id":"11"}]}`)
	result, err = adapter.Call(t.Context(), callRequest(ListEnrolledUsers, mustJSON(ListEnrolledUsersInput{CourseID: "42/7"})))
	if err != nil || !strings.Contains(string(result.Payload), "items") {
		t.Fatalf("result=%s error=%v", result.Payload, err)
	}
	if !strings.Contains(transport.requests[1].URL, "/courses/id:42%2F7") {
		t.Fatalf("url=%s", transport.requests[1].URL)
	}
}

func TestSearchDoesNotSilentlyIgnoreCriteria(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`[{"id":"11"}]`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Call(t.Context(), callRequest(SearchUsers, mustJSON(SearchUsersInput{})))
	if err != nil || !strings.Contains(string(result.Payload), "users") {
		t.Fatalf("result=%s error=%v", result.Payload, err)
	}
	_, err = adapter.Call(t.Context(), callRequest(SearchUsers, mustJSON(SearchUsersInput{Criteria: []SearchCriterion{{Key: "email", Value: "person@example.test"}}})))
	if code, _ := connector.ProviderErrorCodeOf(err); code != "talentlms.criteria_unsupported" {
		t.Fatalf("error=%v", err)
	}
}

func TestValidationProbeAndFailures(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"domain":"tenant"}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: connection(), Secrets: map[string]string{"api_key": "api-key"}})
	if err != nil || !probe.Connected {
		t.Fatalf("probe=%+v error=%v", probe, err)
	}
	for _, request := range []connector.CallRequest{callRequest(ListCourses, []byte(`{"unknown":true}`)), callRequest(ListEnrolledUsers, mustJSON(ListEnrolledUsersInput{}))} {
		if _, err := adapter.Call(t.Context(), request); err == nil {
			t.Fatal("invalid input accepted")
		}
	}
	if err := adapter.(connector.ConfigValidator).ValidateConfig(connector.Connection{Config: map[string]any{"domain_url": "http://remote.example", "api_version": "v1"}}); err == nil {
		t.Fatal("remote HTTP accepted")
	}
	if err := adapter.(connector.ConfigValidator).ValidateConfig(connector.Connection{Config: map[string]any{"domain_url": "https://tenant.example", "api_version": "v2"}}); err == nil {
		t.Fatal("v2 accepted")
	}
	request := callRequest(ListCourses, mustJSON(ListCoursesInput{}))
	request.Secrets = nil
	if _, err := adapter.Call(t.Context(), request); err == nil {
		t.Fatal("missing API key accepted")
	}
	transport.response = connector.HTTPResponse{StatusCode: http.StatusTooManyRequests, Body: []byte(`{}`)}
	_, err = adapter.Call(t.Context(), callRequest(ListCourses, mustJSON(ListCoursesInput{})))
	if class, ok := connector.ErrorClassificationOf(err); !ok || class != connector.ErrorRetryable {
		t.Fatalf("error=%v class=%q", err, class)
	}
}

func connection() connector.Connection {
	return connector.Connection{Config: map[string]any{"domain_url": "http://localhost:8080", "api_version": "v1"}}
}
func callRequest[T any, O any](operation connector.CallOperation[T, O], payload []byte) connector.CallRequest {
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: operation.Key, ContractSHA256: operation.ContractSHA256, Mode: connector.ModeCall, Connection: connection(), Secrets: map[string]string{"api_key": "api-key"}, Payload: payload}
}
func mustJSON(value any) []byte {
	payload, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return payload
}
