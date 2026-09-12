package microsoftbooking

import (
	"context"
	"encoding/json"
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

type operationRef struct{ Key, ContractSHA256 string }

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

func TestDescriptorIdentityReliabilityAndEndpointBoundary(t *testing.T) {
	adapter, err := New(&recordingTransport{})
	if err != nil {
		t.Fatal(err)
	}
	if err = contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	d := adapter.Descriptor()
	if d.ConnectorKey != ConnectorKey || d.ProviderKey != ProviderKey || len(d.Operations) != 5 || len(d.SecretFields) != 4 {
		t.Fatalf("descriptor=%+v", d)
	}
	wantSecrets := []string{"access_token", "refresh_token", "client_id", "client_secret"}
	for i, want := range wantSecrets {
		if d.SecretFields[i].Key != want {
			t.Fatalf("secret fields=%+v", d.SecretFields)
		}
	}
	for _, op := range d.Operations {
		if op.Reliability.Effect == connector.EffectWrite && (op.Reliability.Idempotency.Strategy != connector.IdempotencyNone || op.Reliability.Reconciliation != connector.ReconciliationNone || op.Reliability.Compensation.Mode != connector.CompensationNone) {
			t.Fatalf("write overclaims reliability: %+v", op)
		}
	}
	validator := adapter.(connector.ConfigValidator)
	for _, pair := range [][2]string{{"https://graph.microsoft.com/v1.0", "https://login.microsoftonline.com/organizations/oauth2/v2.0/token"}, {"http://localhost:8080", "http://127.0.0.1:8081/token"}} {
		if err = validator.ValidateConfig(connection(pair[0], pair[1])); err != nil {
			t.Fatalf("valid endpoints=%v err=%v", pair, err)
		}
	}
	for _, pair := range [][2]string{{"https://graph.microsoft.com.evil.test", defaultTokenURL}, {"http://graph.microsoft.com", defaultTokenURL}, {defaultBaseURL, "https://login.microsoftonline.com.evil.test/token"}, {defaultBaseURL, "https://login.live.com/token"}} {
		if err = validator.ValidateConfig(connection(pair[0], pair[1])); err == nil {
			t.Fatalf("invalid endpoints accepted=%v", pair)
		}
	}
}

func TestAllOperationsUseGraphPathsBodiesAndRuntimeOnlyToken(t *testing.T) {
	responses := make([]connector.HTTPResponse, 5)
	for i := range responses {
		responses[i] = connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"id":"appointment-42"}`)}
	}
	transport := &recordingTransport{responses: responses}
	adapter, _ := New(transport)
	count := false
	cases := []struct {
		op      operationRef
		payload any
	}{
		{ref(ListScheduledEvents.Key, ListScheduledEvents.ContractSHA256), ListAppointmentsInput{Count: &count, Expand: "customers", Top: 20, Skip: 2}},
		{ref(CreateBooking.Key, CreateBooking.ContractSHA256), CreateBookingInput{Start: "2026-08-24T09:00:00", End: "2026-08-24T10:00:00", ServiceID: "service-42", IsLocationOnline: boolPtr(false), StaffMemberIDs: []string{"staff-1"}}},
		{ref(RescheduleScheduledEvent.Key, RescheduleScheduledEvent.ContractSHA256), RescheduleAppointmentInput{BookingUID: "appointment-42", Start: "2026-08-25T09:00:00", End: "2026-08-25T10:00:00", TimeZone: "Asia/Shanghai"}},
		{ref(CancelScheduledEvent.Key, CancelScheduledEvent.ContractSHA256), CancelAppointmentInput{EventUUID: "appointment-42", Reason: "conflict"}},
		{ref(TestConnection.Key, TestConnection.ContractSHA256), struct{}{}},
	}
	for _, tc := range cases {
		raw, _ := json.Marshal(tc.payload)
		_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: tc.op.Key, ContractSHA256: tc.op.ContractSHA256, Mode: connector.ModeCall, Connection: connection("http://localhost:8080", "http://localhost:8081/token"), Secrets: secrets(), Payload: raw})
		if err != nil {
			t.Fatalf("operation=%s err=%v", tc.op.Key, err)
		}
	}
	if len(transport.requests) != 5 {
		t.Fatalf("requests=%d", len(transport.requests))
	}
	for _, request := range transport.requests {
		if request.SecretHeaders["Authorization"][0] != "Bearer graph-token" || strings.Contains(request.URL+string(request.Body), "graph-token") {
			t.Fatalf("secret leaked request=%+v", request)
		}
	}
	if got := transport.requests[0].URL; !strings.Contains(got, "/solutions/bookingBusinesses/business-42/appointments") || !strings.Contains(got, "%24count=false") || !strings.Contains(got, "%24top=20") {
		t.Fatalf("list URL=%s", got)
	}
	if body := string(transport.requests[1].Body); !strings.Contains(body, `"serviceId":"service-42"`) || !strings.Contains(body, `"isLocationOnline":false`) || !strings.Contains(body, `"timeZone":"UTC"`) {
		t.Fatalf("create body=%s", body)
	}
	if transport.requests[2].Method != http.MethodPatch || !strings.HasSuffix(strings.Split(transport.requests[2].URL, "?")[0], "/appointments/appointment-42") {
		t.Fatalf("reschedule=%+v", transport.requests[2])
	}
	if transport.requests[3].Method != http.MethodPost || !strings.HasSuffix(transport.requests[3].URL, "/appointments/appointment-42/cancel") || !strings.Contains(string(transport.requests[3].Body), `"cancellationMessage":"conflict"`) {
		t.Fatalf("cancel=%+v", transport.requests[3])
	}
	if !strings.HasSuffix(transport.requests[4].URL, "/solutions/bookingBusinesses/business-42") {
		t.Fatalf("test=%+v", transport.requests[4])
	}
}

func TestUnauthorizedRefreshUsesSecretFormScopeAndReturnsRotations(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: http.StatusUnauthorized}, {StatusCode: http.StatusOK, Body: []byte(`{"access_token":"new-access","refresh_token":"new-refresh"}`)}, {StatusCode: http.StatusOK, Body: []byte(`{"id":"business-42"}`)}}}
	adapter, _ := New(transport)
	result, err := adapter.Call(t.Context(), callRequest(ref(TestConnection.Key, TestConnection.ContractSHA256), struct{}{}, secrets()))
	if err != nil || result.SecretUpdates["access_token"] != "new-access" || result.SecretUpdates["refresh_token"] != "new-refresh" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	refresh := transport.requests[1]
	if refresh.SecretForm["refresh_token"] != "refresh-token" || refresh.SecretForm["client_id"] != "client-id" || refresh.SecretForm["client_secret"] != "client-secret" || strings.Contains(string(refresh.Body), "refresh-token") || !strings.Contains(string(refresh.Body), "scope=https%3A%2F%2Fgraph.microsoft.com%2F.default+offline_access") {
		t.Fatalf("refresh=%+v", refresh)
	}
	if transport.requests[2].SecretHeaders["Authorization"][0] != "Bearer new-access" {
		t.Fatalf("retry=%+v", transport.requests[2])
	}
}

func TestConnectionRetainsRotationWhenFollowupFails(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: http.StatusUnauthorized}, {StatusCode: http.StatusOK, Body: []byte(`{"access_token":"new-access","refresh_token":"new-refresh"}`)}, {StatusCode: http.StatusServiceUnavailable}}}
	adapter, _ := New(transport)
	result, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: connection("http://localhost:8080", "http://localhost:8081/token"), Secrets: secrets()})
	classification, ok := connector.ErrorClassificationOf(err)
	if !ok || classification != connector.ErrorRetryable || result.Connected || result.SecretUpdates["access_token"] != "new-access" || result.SecretUpdates["refresh_token"] != "new-refresh" || len(transport.requests) != 3 {
		t.Fatalf("result=%+v requests=%d classification=%q err=%v", result, len(transport.requests), classification, err)
	}
}

func TestFailureClassificationAndInputValidation(t *testing.T) {
	for _, tc := range []struct {
		name         string
		op           operationRef
		payload      any
		response     connector.HTTPResponse
		transportErr error
		want         connector.ErrorClassification
	}{
		{"write network", ref(CreateBooking.Key, CreateBooking.ContractSHA256), CreateBookingInput{Start: "start", End: "end"}, connector.HTTPResponse{}, errors.New("reset"), connector.ErrorUncertain},
		{"write server", ref(CreateBooking.Key, CreateBooking.ContractSHA256), CreateBookingInput{Start: "start", End: "end"}, connector.HTTPResponse{StatusCode: http.StatusBadGateway}, nil, connector.ErrorUncertain},
		{"write rate limit", ref(CreateBooking.Key, CreateBooking.ContractSHA256), CreateBookingInput{Start: "start", End: "end"}, connector.HTTPResponse{StatusCode: http.StatusTooManyRequests}, nil, connector.ErrorRetryable},
		{"write rejection", ref(CreateBooking.Key, CreateBooking.ContractSHA256), CreateBookingInput{Start: "start", End: "end"}, connector.HTTPResponse{StatusCode: http.StatusBadRequest}, nil, connector.ErrorPermanent},
		{"read server", ref(ListScheduledEvents.Key, ListScheduledEvents.ContractSHA256), ListAppointmentsInput{}, connector.HTTPResponse{StatusCode: http.StatusServiceUnavailable}, nil, connector.ErrorRetryable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			adapter, _ := New(&recordingTransport{responses: []connector.HTTPResponse{tc.response}, errors: []error{tc.transportErr}})
			_, err := adapter.Call(t.Context(), callRequest(tc.op, tc.payload, map[string]string{"access_token": "token"}))
			class, ok := connector.ErrorClassificationOf(err)
			if !ok || class != tc.want {
				t.Fatalf("err=%v class=%q want=%q", err, class, tc.want)
			}
		})
	}
	adapter, _ := New(&recordingTransport{})
	for _, tc := range []struct {
		op      operationRef
		payload any
	}{{ref(ListScheduledEvents.Key, ListScheduledEvents.ContractSHA256), ListAppointmentsInput{Top: -1}}, {ref(CreateBooking.Key, CreateBooking.ContractSHA256), CreateBookingInput{}}, {ref(CancelScheduledEvent.Key, CancelScheduledEvent.ContractSHA256), CancelAppointmentInput{}}, {ref(RescheduleScheduledEvent.Key, RescheduleScheduledEvent.ContractSHA256), RescheduleAppointmentInput{EventUUID: "id"}}} {
		if _, err := adapter.Call(t.Context(), callRequest(tc.op, tc.payload, map[string]string{"access_token": "token"})); err == nil {
			t.Fatalf("invalid accepted op=%s payload=%+v", tc.op.Key, tc.payload)
		}
	}
}

func connection(base, token string) connector.Connection {
	return connector.Connection{Config: map[string]any{"business_id": "business-42", "base_url": base, "token_url": token, "default_timezone": "UTC"}}
}
func secrets() map[string]string {
	return map[string]string{"access_token": "graph-token", "refresh_token": "refresh-token", "client_id": "client-id", "client_secret": "client-secret"}
}
func callRequest(op operationRef, payload any, resolved map[string]string) connector.CallRequest {
	raw, _ := json.Marshal(payload)
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: op.Key, ContractSHA256: op.ContractSHA256, Mode: connector.ModeCall, Connection: connection("http://localhost:8080", "http://localhost:8081/token"), Secrets: resolved, Payload: raw}
}
func ref(key, hash string) operationRef { return operationRef{Key: key, ContractSHA256: hash} }
func boolPtr(value bool) *bool          { return &value }
