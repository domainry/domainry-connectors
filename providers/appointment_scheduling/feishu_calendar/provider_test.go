package feishucalendar

import (
	"context"
	"errors"
	"net/http"
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

func TestDescriptorModesIdentityAndEndpointBoundary(t *testing.T) {
	adapter, err := New(&recordingTransport{})
	if err != nil {
		t.Fatal(err)
	}
	if err = contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	descriptor := adapter.Descriptor()
	if descriptor.ConnectorKey != ConnectorKey || descriptor.ProviderKey != ProviderKey || len(descriptor.Operations) != 3 || len(descriptor.SecretFields) != 2 {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	modes := map[string]connector.OperationMode{}
	for _, operation := range descriptor.Operations {
		modes[operation.Key] = operation.Mode
	}
	if modes["create_booking"] != connector.ModeCall || modes["enqueue_booking"] != connector.ModeEnqueue {
		t.Fatalf("modes=%v", modes)
	}
	validator := adapter.(connector.ConfigValidator)
	for _, endpoint := range []string{"https://open.feishu.cn", "http://localhost:8080"} {
		if err = validator.ValidateConfig(connection(endpoint, true)); err != nil {
			t.Fatalf("valid %s: %v", endpoint, err)
		}
	}
	for _, endpoint := range []string{"https://open.larksuite.com", "http://open.feishu.cn", "https://open.feishu.cn.evil.test"} {
		if err = validator.ValidateConfig(connection(endpoint, true)); err == nil {
			t.Fatalf("invalid accepted=%s", endpoint)
		}
	}
}

func TestCreateMeetingUsesTenantTokenAndProviderIdempotency(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: 200, Body: []byte(`{"code":0,"tenant_access_token":"tenant-token","expire":7200}`)}, {StatusCode: 200, Body: []byte(`{"code":0,"data":{"event":{"event_id":"event-42"}}}`)}, {StatusCode: 200, Body: []byte(`{"code":0,"data":{"attendees":[]}}`)}, {StatusCode: 200, Body: []byte(`{"code":0,"data":{"event":{"event_id":"event-42","vchat":{"meeting_url":"https://vc.feishu.cn/j/42"}}}}`)}}}
	adapter, _ := New(transport)
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CreateBooking.Key, ContractSHA256: CreateBooking.ContractSHA256, Mode: connector.ModeCall, Connection: connection("http://localhost:8080", true), Secrets: map[string]string{"app_secret": "app-secret"}, RequestRef: "candidate-process-round1-20260809", Payload: []byte(`{"start":"2026-08-09T03:00:00Z","end":"2026-08-09T04:00:00Z","timeZone":"Asia/Shanghai","summary":"Interview","attendees":[{"email":"guest@example.test"},{"user_id":"ou_42"}]}`)})
	if err != nil || result.ResponseRef != "feishu_calendar:event-42" || !strings.Contains(string(result.Payload), "https://vc.feishu.cn/j/42") {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	tokenRequest := transport.requests[0]
	if tokenRequest.SecretJSON["app_id"] != "cli-test" || tokenRequest.SecretJSON["app_secret"] != "app-secret" || strings.Contains(string(tokenRequest.Body), "app-secret") {
		t.Fatalf("token request=%+v", tokenRequest)
	}
	create := transport.requests[1]
	if create.SecretHeaders["Authorization"][0] != "Bearer tenant-token" || !strings.Contains(create.URL, "idempotency_key=candidate-process-round1-20260809") {
		t.Fatalf("create=%+v", create)
	}
	if len(transport.requests) != 4 {
		t.Fatalf("requests=%d", len(transport.requests))
	}
}

func TestAttendeeFailureCompensatesAndCompensationFailureIsUncertain(t *testing.T) {
	base := []connector.HTTPResponse{{StatusCode: 200, Body: []byte(`{"code":0,"data":{"event":{"event_id":"event-42"}}}`)}, {StatusCode: http.StatusForbidden, Body: []byte(`{"code":194002}`)}}
	success := &recordingTransport{responses: append(append([]connector.HTTPResponse{}, base...), connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"code":0}`)})}
	adapter, _ := New(success)
	err := createWithDirectToken(t, adapter)
	classification, ok := connector.ErrorClassificationOf(err)
	if !ok || classification != connector.ErrorPermanent || success.requests[2].Method != http.MethodDelete {
		t.Fatalf("err=%v class=%q requests=%+v", err, classification, success.requests)
	}
	failed := &recordingTransport{responses: append(append([]connector.HTTPResponse{}, base...), connector.HTTPResponse{}), errors: []error{nil, nil, errors.New("delete reset")}}
	adapter, _ = New(failed)
	err = createWithDirectToken(t, adapter)
	classification, ok = connector.ErrorClassificationOf(err)
	if !ok || classification != connector.ErrorUncertain {
		t.Fatalf("err=%v class=%q", err, classification)
	}
}

func TestInputValidationAndCreateAmbiguity(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	for _, test := range []struct{ ref, payload string }{{"", `{"start":"2026-08-09T03:00:00Z","end":"2026-08-09T04:00:00Z","attendees":[{"email":"x@example.test"}]}`}, {"valid-request-reference-1234567890", `{"start":"bad","end":"2026-08-09T04:00:00Z","attendees":[{"email":"x@example.test"}]}`}, {"valid-request-reference-1234567890", `{"start":"2026-08-09T03:00:00Z","end":"2026-08-09T04:00:00Z","attendees":[]}`}, {"valid-request-reference-1234567890", `{"start":"2026-08-09T03:00:00Z","end":"2026-08-09T04:00:00Z","attendees":[{}]}`}} {
		_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CreateBooking.Key, ContractSHA256: CreateBooking.ContractSHA256, Mode: connector.ModeCall, Connection: connection("http://localhost", true), Secrets: map[string]string{"access_token": "token"}, RequestRef: test.ref, Payload: []byte(test.payload)})
		if err == nil {
			t.Fatalf("invalid accepted ref=%q payload=%s", test.ref, test.payload)
		}
	}
	for _, test := range []struct {
		name      string
		transport *recordingTransport
		want      connector.ErrorClassification
	}{
		{"network", &recordingTransport{errors: []error{errors.New("reset")}}, connector.ErrorUncertain},
		{"server", &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: http.StatusBadGateway}}}, connector.ErrorUncertain},
		{"missing identity", &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: 200, Body: []byte(`{"code":0,"data":{"event":{}}}`)}}}, connector.ErrorUncertain},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter, _ := New(test.transport)
			err := createWithDirectToken(t, adapter)
			class, ok := connector.ErrorClassificationOf(err)
			if !ok || class != test.want {
				t.Fatalf("err=%v class=%q want=%q", err, class, test.want)
			}
		})
	}
}

func createWithDirectToken(t *testing.T, adapter connector.Adapter) error {
	t.Helper()
	_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CreateBooking.Key, ContractSHA256: CreateBooking.ContractSHA256, Mode: connector.ModeCall, Connection: connection("http://localhost", true), Secrets: map[string]string{"access_token": "token"}, RequestRef: "valid-request-reference-1234567890", Payload: []byte(`{"start":"2026-08-09T03:00:00Z","end":"2026-08-09T04:00:00Z","attendees":[{"email":"x@example.test"}]}`)})
	return err
}
func connection(endpoint string, withCalendar bool) connector.Connection {
	config := map[string]any{"base_url": endpoint, "app_id": "cli-test", "default_timezone": "Asia/Shanghai"}
	if withCalendar {
		config["calendar_id"] = "calendar-primary"
	}
	return connector.Connection{Config: config}
}
