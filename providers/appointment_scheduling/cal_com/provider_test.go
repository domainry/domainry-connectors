package calcom

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
)

type recordingTransport struct {
	request  connector.HTTPRequest
	response connector.HTTPResponse
	err      error
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.request = request
	return t.response, t.err
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestDescriptorUsesStableIdentityAndAccurateSecrets(t *testing.T) {
	adapter, err := New(&recordingTransport{})
	if err != nil {
		t.Fatal(err)
	}
	if err = contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	descriptor := adapter.Descriptor()
	if descriptor.ConnectorKey != ConnectorKey || descriptor.ProviderKey != "cal_com" || len(descriptor.Operations) != 6 || descriptor.SecretFields[0].Key != "access_token" || descriptor.SecretFields[1].Key != "webhook_signing_key" {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	for _, operation := range descriptor.Operations {
		if operation.Reliability.Effect == connector.EffectWrite && operation.Reliability.Idempotency.Strategy != connector.IdempotencyNone {
			t.Fatalf("unsupported idempotency=%+v", operation)
		}
	}
	validator := adapter.(connector.ConfigValidator)
	for _, endpoint := range []string{"https://api.cal.com", "http://localhost:8080"} {
		if err = validator.ValidateConfig(connection(endpoint)); err != nil {
			t.Fatalf("valid %s: %v", endpoint, err)
		}
	}
	for _, endpoint := range []string{"https://cal.com", "http://api.cal.com", "https://api.cal.com.evil.test"} {
		if err = validator.ValidateConfig(connection(endpoint)); err == nil {
			t.Fatalf("invalid endpoint accepted=%s", endpoint)
		}
	}
}

func TestOperationsPinVersionsAndKeepTokenRuntimeOnly(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"status":"success","data":{"uid":"booking-42"}}`)}}
	adapter, _ := New(transport)
	tests := []struct {
		operation             connector.OperationDescriptor
		payload               []byte
		method, path, version string
	}{
		{ListEventTypes.Descriptor(), []byte(`{"username":"alice","eventSlug":"intro","sortCreatedAt":"asc"}`), http.MethodGet, "/v2/event-types", versionEvents},
		{ListScheduledEvents.Descriptor(), []byte(`{"status":"upcoming","cursor":"next"}`), http.MethodGet, "/v2/bookings", versionList},
		{CreateBooking.Descriptor(), []byte(`{"start":"2026-08-13T09:00:00Z","attendee":{"name":"Guest","email":"guest@example.test","timeZone":"UTC"},"eventTypeId":123,"allowConflicts":false}`), http.MethodPost, "/v2/bookings", versionWrites},
		{CancelScheduledEvent.Descriptor(), []byte(`{"booking_uid":"booking-42","reason":"conflict"}`), http.MethodPost, "/v2/bookings/booking-42/cancel", versionWrites},
		{RescheduleScheduledEvent.Descriptor(), []byte(`{"booking_uid":"booking-42","start":"2026-08-14T09:00:00Z","reschedulingReason":"later"}`), http.MethodPost, "/v2/bookings/booking-42/reschedule", versionWrites},
	}
	for _, test := range tests {
		_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: test.operation.Key, ContractSHA256: test.operation.ContractSHA256, Mode: connector.ModeCall, Connection: connection("http://127.0.0.1:8080"), Secrets: secrets(), Payload: test.payload})
		if err != nil {
			t.Fatalf("%s: %v", test.operation.Key, err)
		}
		request := transport.request
		if request.Method != test.method || !strings.Contains(request.URL, test.path) || request.Headers["cal-api-version"][0] != test.version || request.SecretHeaders["Authorization"][0] != "Bearer cal-token" || strings.Contains(request.URL, "cal-token") || strings.Contains(string(request.Body), "cal-token") {
			t.Fatalf("%s request=%+v", test.operation.Key, request)
		}
	}
}

func TestInputAndFailureClassification(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	invalid := []struct {
		operation connector.OperationDescriptor
		payload   string
	}{{ListEventTypes.Descriptor(), `{"eventSlug":"intro"}`}, {ListEventTypes.Descriptor(), `{"sortCreatedAt":"newest"}`}, {ListScheduledEvents.Descriptor(), `{"status":"active"}`}, {CreateBooking.Descriptor(), `{"start":"bad","attendee":{},"eventTypeId":1}`}, {CreateBooking.Descriptor(), `{"start":"2026-08-13T09:00:00Z","attendee":{"name":"Guest","email":"guest@example.test","timeZone":"UTC"}}`}, {CancelScheduledEvent.Descriptor(), `{}`}, {RescheduleScheduledEvent.Descriptor(), `{"booking_uid":"one","start":"bad"}`}}
	for _, test := range invalid {
		_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: test.operation.Key, ContractSHA256: test.operation.ContractSHA256, Mode: connector.ModeCall, Connection: connection("http://localhost"), Secrets: secrets(), Payload: []byte(test.payload)})
		if err == nil {
			t.Fatalf("invalid accepted %s: %s", test.operation.Key, test.payload)
		}
	}
	call := func(transport *recordingTransport) error {
		current, _ := New(transport)
		_, err := current.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CreateBooking.Key, ContractSHA256: CreateBooking.ContractSHA256, Mode: connector.ModeCall, Connection: connection("http://localhost"), Secrets: secrets(), Payload: []byte(`{"start":"2026-08-13T09:00:00Z","attendee":{"name":"Guest","email":"guest@example.test","timeZone":"UTC"},"eventTypeId":1}`)})
		return err
	}
	for _, test := range []struct {
		name      string
		transport *recordingTransport
		want      connector.ErrorClassification
	}{{"network", &recordingTransport{err: errors.New("reset")}, connector.ErrorUncertain}, {"server", &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusBadGateway}}, connector.ErrorUncertain}, {"invalid success", &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusCreated, Body: []byte(`{`)}}, connector.ErrorUncertain}, {"rate limit", &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusTooManyRequests}}, connector.ErrorRetryable}, {"bad request", &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusBadRequest}}, connector.ErrorPermanent}} {
		t.Run(test.name, func(t *testing.T) {
			class, ok := connector.ErrorClassificationOf(call(test.transport))
			if !ok || class != test.want {
				t.Fatalf("class=%q want=%q", class, test.want)
			}
		})
	}
}

func TestWebhookSignatureAndIdentity(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	body := []byte(`{"triggerEvent":"BOOKING_CREATED","payload":{"uid":"booking-42","attendees":[{"email":"guest@example.test","name":"Guest"}]}}`)
	mac := hmac.New(sha256.New, []byte("webhook-key"))
	_, _ = mac.Write(body)
	signature := hex.EncodeToString(mac.Sum(nil))
	request := connector.VerifyWebhookRequest{Secrets: secrets(), Headers: map[string][]string{"X-Cal-Signature-256": {"sha256=" + signature}}, Body: body}
	verified, err := adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), request)
	if err != nil || verified.EventType != "booking_created" || verified.ExternalID != "booking-42:BOOKING_CREATED" || verified.ExternalIdentity == nil || verified.Security == nil || !verified.Security.SignatureVerified {
		t.Fatalf("verified=%+v err=%v", verified, err)
	}
	request.Headers["X-Cal-Signature-256"] = []string{"bad"}
	if _, err = adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), request); err == nil {
		t.Fatal("bad signature accepted")
	}
}

func connection(endpoint string) connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": endpoint}}
}
func secrets() map[string]string {
	return map[string]string{"access_token": "cal-token", "webhook_signing_key": "webhook-key"}
}
func mustJSON(value any) []byte {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return raw
}
