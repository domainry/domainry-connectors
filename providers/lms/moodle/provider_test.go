package moodle

import (
	"context"
	"encoding/json"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"net/http"
	"net/url"
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

func TestMoodleUsesRuntimeOnlySecretFormAndTypedCriteria(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"users":[]}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Call(t.Context(), callRequest(SearchUsers, mustJSON(SearchUsersInput{Criteria: []SearchCriterion{{Key: "email", Value: "person@example.test"}}})))
	if err != nil || !strings.Contains(string(result.Payload), "users") {
		t.Fatalf("result=%s error=%v", result.Payload, err)
	}
	request := transport.requests[0]
	form, _ := url.ParseQuery(string(request.Body))
	if request.SecretForm["wstoken"] != "secret-token" || strings.Contains(string(request.Body), "secret-token") || form.Get("wsfunction") != "core_user_get_users" || form.Get("criteria[0][value]") != "person@example.test" {
		t.Fatalf("request=%+v form=%v", request, form)
	}
}

func TestDefaultSearchCourseAndProbe(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`[]`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.Call(t.Context(), callRequest(SearchUsers, mustJSON(SearchUsersInput{}))); err != nil {
		t.Fatal(err)
	}
	form, _ := url.ParseQuery(string(transport.requests[0].Body))
	if form.Get("criteria[0][key]") != "email" || form.Get("criteria[0][value]") != "%" {
		t.Fatalf("form=%v", form)
	}
	if _, err := adapter.Call(t.Context(), callRequest(ListEnrolledUsers, mustJSON(ListEnrolledUsersInput{CourseID: "42"}))); err != nil {
		t.Fatal(err)
	}
	form, _ = url.ParseQuery(string(transport.requests[1].Body))
	if form.Get("courseid") != "42" {
		t.Fatalf("form=%v", form)
	}
	transport.response.Body = []byte(`{"userid":7}`)
	probe, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: connection(), Secrets: map[string]string{"api_token": "secret-token"}})
	if err != nil || !probe.Connected {
		t.Fatalf("probe=%+v error=%v", probe, err)
	}
}

func TestInputConfigProviderAndNetworkFailures(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`[]`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []connector.CallRequest{callRequest(ListCourses, []byte(`{"unknown":true}`)), callRequest(ListEnrolledUsers, mustJSON(ListEnrolledUsersInput{})), callRequest(SearchUsers, mustJSON(SearchUsersInput{Criteria: []SearchCriterion{{Key: "email"}}}))} {
		if _, err := adapter.Call(t.Context(), request); err == nil {
			t.Fatal("invalid input accepted")
		}
	}
	if err := adapter.(connector.ConfigValidator).ValidateConfig(connector.Connection{Config: map[string]any{"base_url": "http://remote.example"}}); err == nil {
		t.Fatal("remote plaintext HTTP accepted")
	}
	request := callRequest(ListCourses, mustJSON(ListCoursesInput{}))
	request.Secrets = nil
	if _, err := adapter.Call(t.Context(), request); err == nil {
		t.Fatal("missing token accepted")
	}
	transport.response = connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"errorcode":"invalid-token/value"}`)}
	_, err = adapter.Call(t.Context(), callRequest(ListCourses, mustJSON(ListCoursesInput{})))
	if code, _ := connector.ProviderErrorCodeOf(err); code != "moodle.provider_invalid_token_value" {
		t.Fatalf("error=%v", err)
	}
	transport.response = connector.HTTPResponse{StatusCode: http.StatusServiceUnavailable, Body: []byte(`{}`)}
	_, err = adapter.Call(t.Context(), callRequest(ListCourses, mustJSON(ListCoursesInput{})))
	if class, ok := connector.ErrorClassificationOf(err); !ok || class != connector.ErrorRetryable {
		t.Fatalf("error=%v class=%q", err, class)
	}
}

func connection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080"}}
}
func callRequest[T any, O any](operation connector.CallOperation[T, O], payload []byte) connector.CallRequest {
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: operation.Key, ContractSHA256: operation.ContractSHA256, Mode: connector.ModeCall, Connection: connection(), Secrets: map[string]string{"api_token": "secret-token"}, Payload: payload}
}
func mustJSON(value any) []byte {
	payload, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return payload
}
