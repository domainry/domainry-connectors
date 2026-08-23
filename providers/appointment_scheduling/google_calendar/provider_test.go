package googlecalendar

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

func TestDescriptorIdentityModesAndEndpointBoundary(t *testing.T) {
	adapter, err := New(&recordingTransport{})
	if err != nil {
		t.Fatal(err)
	}
	if err = contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	descriptor := adapter.Descriptor()
	if descriptor.ConnectorKey != ConnectorKey || descriptor.ProviderKey != ProviderKey || len(descriptor.Operations) != 7 || len(descriptor.SecretFields) != 5 {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	modes := map[string]connector.OperationMode{}
	for _, operation := range descriptor.Operations {
		modes[operation.Key] = operation.Mode
		if operation.Reliability.Effect == connector.EffectWrite && operation.Reliability.Idempotency.Strategy != connector.IdempotencyNone {
			t.Fatalf("unsupported idempotency=%+v", operation)
		}
	}
	if modes["enqueue_booking"] != connector.ModeEnqueue {
		t.Fatalf("modes=%v", modes)
	}
	validator := adapter.(connector.ConfigValidator)
	for _, connection := range []connector.Connection{testConnection("https://www.googleapis.com", "https://oauth2.googleapis.com/token"), testConnection("http://localhost:8080", "http://127.0.0.1:8080/token")} {
		if err = validator.ValidateConfig(connection); err != nil {
			t.Fatalf("valid connection: %v", err)
		}
	}
	for _, connection := range []connector.Connection{testConnection("https://google.example.com", "https://oauth2.googleapis.com/token"), testConnection("https://www.googleapis.com", "https://oauth2.googleapis.com.evil.test/token"), testConnection("http://www.googleapis.com", "https://oauth2.googleapis.com/token")} {
		if err = validator.ValidateConfig(connection); err == nil {
			t.Fatalf("invalid connection accepted=%v", connection.Config)
		}
	}
}

func TestCreateAndWatchKeepSecretsRuntimeOnly(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: http.StatusOK, Body: []byte(`{"id":"event-42"}`)}, {StatusCode: http.StatusOK, Body: []byte(`{"id":"channel-42"}`)}}}
	adapter, _ := New(transport)
	create := connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CreateBooking.Key, ContractSHA256: CreateBooking.ContractSHA256, Mode: connector.ModeCall, Connection: testConnection("http://localhost:8080", "http://localhost:8080/token"), Secrets: secrets(), RequestRef: "workflow-request", Payload: []byte(`{"start":"2026-08-23T09:00:00Z","end":"2026-08-23T10:00:00Z","attendee":{"email":"guest@example.test"},"createOnlineMeeting":true,"sendUpdates":"none"}`)}
	result, err := adapter.Call(t.Context(), create)
	if err != nil || result.ResponseRef != "google_calendar:event-42" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	first := transport.requests[0]
	if first.SecretHeaders["Authorization"][0] != "Bearer access-secret" || strings.Contains(first.URL, "access-secret") || !strings.Contains(first.URL, "conferenceDataVersion=1") || !strings.Contains(first.URL, "sendUpdates=none") {
		t.Fatalf("request=%+v", first)
	}
	watch := connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: WatchScheduledEvents.Key, ContractSHA256: WatchScheduledEvents.ContractSHA256, Mode: connector.ModeCall, Connection: create.Connection, Secrets: secrets(), Payload: []byte(`{"channel_id":"channel-42","address":"https://runtime.example.test/webhooks","expiration":2000000000000}`)}
	if _, err = adapter.Call(t.Context(), watch); err != nil {
		t.Fatal(err)
	}
	second := transport.requests[1]
	if second.SecretJSON["token"] != "channel-secret" || strings.Contains(string(second.Body), "channel-secret") || strings.Contains(second.URL, "channel-secret") {
		t.Fatalf("watch request=%+v", second)
	}
}

func TestRefreshUsesPrivateKernelAndReturnsUpdates(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: http.StatusUnauthorized, Body: []byte(`{"error":"expired"}`)}, {StatusCode: http.StatusOK, Body: []byte(`{"access_token":"fresh-access","refresh_token":"fresh-refresh"}`)}, {StatusCode: http.StatusOK, Body: []byte(`{"id":"primary"}`)}}}
	adapter, _ := New(transport)
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: TestConnection.Key, ContractSHA256: TestConnection.ContractSHA256, Mode: connector.ModeCall, Connection: testConnection("http://localhost:8080", "http://localhost:8080/token"), Secrets: secrets(), Payload: []byte(`{}`)})
	if err != nil || result.SecretUpdates["access_token"] != "fresh-access" || result.SecretUpdates["refresh_token"] != "fresh-refresh" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	refresh := transport.requests[1]
	serialized := refresh.URL + string(refresh.Body)
	for _, secret := range []string{"refresh-secret", "client-id", "client-secret"} {
		if strings.Contains(serialized, secret) {
			t.Fatalf("refresh leaked %s: %+v", secret, refresh)
		}
	}
	if refresh.SecretForm["refresh_token"] != "refresh-secret" || refresh.SecretForm["client_id"] != "client-id" || refresh.SecretForm["client_secret"] != "client-secret" {
		t.Fatalf("refresh=%+v", refresh)
	}
	if transport.requests[2].SecretHeaders["Authorization"][0] != "Bearer fresh-access" {
		t.Fatalf("retry=%+v", transport.requests[2])
	}
}

func TestValidationAndWriteFailureClassification(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	invalid := []struct {
		descriptor connector.OperationDescriptor
		payload    string
	}{{ListScheduledEvents.Descriptor(), `{"maxResults":2501}`}, {ListScheduledEvents.Descriptor(), `{"syncToken":"sync","timeMin":"2026-01-01T00:00:00Z"}`}, {CreateBooking.Descriptor(), `{"start":"bad","end":"bad"}`}, {CreateBooking.Descriptor(), `{"start":"2026-08-23T10:00:00Z","end":"2026-08-23T09:00:00Z"}`}, {CreateBooking.Descriptor(), `{"start":"2026-08-23T09:00:00Z","end":"2026-08-23T10:00:00Z","sendUpdates":"sometimes"}`}, {CancelScheduledEvent.Descriptor(), `{}`}, {WatchScheduledEvents.Descriptor(), `{"channel_id":"one","address":"http://remote.example.test"}`}}
	for _, test := range invalid {
		_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: test.descriptor.Key, ContractSHA256: test.descriptor.ContractSHA256, Mode: test.descriptor.Mode, Connection: testConnection("http://localhost", "http://localhost/token"), Secrets: secrets(), Payload: []byte(test.payload)})
		if err == nil {
			t.Fatalf("invalid accepted %s: %s", test.descriptor.Key, test.payload)
		}
	}
	call := func(transport *recordingTransport) error {
		current, _ := New(transport)
		_, err := current.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CreateBooking.Key, ContractSHA256: CreateBooking.ContractSHA256, Mode: connector.ModeCall, Connection: testConnection("http://localhost", "http://localhost/token"), Secrets: map[string]string{"access_token": "access"}, Payload: []byte(`{"start":"2026-08-23T09:00:00Z","end":"2026-08-23T10:00:00Z"}`)})
		return err
	}
	for _, test := range []struct {
		name      string
		transport *recordingTransport
		want      connector.ErrorClassification
	}{{"network", &recordingTransport{errors: []error{errors.New("reset")}}, connector.ErrorUncertain}, {"server", &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: http.StatusBadGateway}}}, connector.ErrorUncertain}, {"invalid success", &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: http.StatusOK, Body: []byte(`{`)}}}, connector.ErrorUncertain}, {"rate limit", &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: http.StatusTooManyRequests}}}, connector.ErrorRetryable}, {"bad request", &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: http.StatusBadRequest}}}, connector.ErrorPermanent}} {
		t.Run(test.name, func(t *testing.T) {
			class, ok := connector.ErrorClassificationOf(call(test.transport))
			if !ok || class != test.want {
				t.Fatalf("class=%q want=%q", class, test.want)
			}
		})
	}
}

func TestPushHeadersAreAuthenticatedAndNormalized(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	request := connector.VerifyWebhookRequest{Secrets: secrets(), Headers: map[string][]string{"X-Goog-Channel-Token": {"channel-secret"}, "X-Goog-Channel-ID": {"channel-42"}, "X-Goog-Message-Number": {"7"}, "X-Goog-Resource-ID": {"resource-42"}, "X-Goog-Resource-State": {"exists"}, "X-Goog-Resource-URI": {"https://www.googleapis.com/calendar/v3/calendars/primary/events"}}}
	verified, err := adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), request)
	if err != nil || verified.EventType != "calendar_events.exists" || verified.ExternalID != "channel-42:7" || verified.Security == nil || !verified.Security.SignatureVerified || verified.Security.DeviceIdentity != "resource-42" {
		t.Fatalf("verified=%+v err=%v", verified, err)
	}
	request.Headers["X-Goog-Channel-Token"] = []string{"wrong"}
	if _, err = adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), request); err == nil {
		t.Fatal("wrong token accepted")
	}
}

func testConnection(baseURL, tokenURL string) connector.Connection {
	return connector.Connection{Config: map[string]any{"calendar_id": "primary", "base_url": baseURL, "token_url": tokenURL, "default_timezone": "UTC"}}
}
func secrets() map[string]string {
	return map[string]string{"access_token": "access-secret", "refresh_token": "refresh-secret", "client_id": "client-id", "client_secret": "client-secret", "channel_token": "channel-secret"}
}
