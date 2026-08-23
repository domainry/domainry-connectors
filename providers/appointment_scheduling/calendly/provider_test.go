package calendly

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

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

func TestDescriptorAndEndpointBoundary(t *testing.T) {
	adapter, err := New(&recordingTransport{})
	if err != nil {
		t.Fatal(err)
	}
	if err = contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	descriptor := adapter.Descriptor()
	if descriptor.ConnectorKey != ConnectorKey || descriptor.ProviderKey != ProviderKey || len(descriptor.Operations) != 5 || descriptor.SecretFields[0].Key != "access_token" || descriptor.SecretFields[1].Key != "webhook_signing_key" {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	for _, operation := range descriptor.Operations {
		if operation.Reliability.Effect == connector.EffectWrite && operation.Reliability.Idempotency.Strategy != connector.IdempotencyNone {
			t.Fatalf("unsupported idempotency advertised: %+v", operation)
		}
	}
	validator := adapter.(connector.ConfigValidator)
	for _, endpoint := range []string{"https://api.calendly.com", "https://api.calendly.com/", "http://127.0.0.1:8080"} {
		if err = validator.ValidateConfig(connection(endpoint)); err != nil {
			t.Fatalf("valid endpoint %s: %v", endpoint, err)
		}
	}
	for _, endpoint := range []string{"https://calendly.example.com", "http://api.calendly.com", "https://api.calendly.com.evil.test"} {
		if err = validator.ValidateConfig(connection(endpoint)); err == nil {
			t.Fatalf("invalid endpoint accepted: %s", endpoint)
		}
	}
}

func TestRequestsKeepAccessTokenRuntimeOnlyAndValidateInputs(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"collection":[]}`)}}
	adapter, _ := New(transport)
	active := false
	request := connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListEventTypes.Key, ContractSHA256: ListEventTypes.ContractSHA256, Mode: connector.ModeCall, Connection: connection("http://localhost:8080"), Secrets: secrets(), Payload: mustJSON(ListEventTypesInput{Organization: "https://api.calendly.com/organizations/42", Active: &active, Count: 100, PageToken: "next"})}
	if _, err := adapter.Call(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if got := transport.request.SecretHeaders["Authorization"]; len(got) != 1 || got[0] != "Bearer calendly-token" || strings.Contains(transport.request.URL, "calendly-token") || strings.Contains(string(transport.request.Body), "calendly-token") {
		t.Fatalf("request=%+v", transport.request)
	}
	for _, expected := range []string{"active=false", "count=100", "page_token=next", "organization="} {
		if !strings.Contains(transport.request.URL, expected) {
			t.Fatalf("missing %s in %s", expected, transport.request.URL)
		}
	}
	invalid := []connector.CallRequest{
		{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListEventTypes.Key, ContractSHA256: ListEventTypes.ContractSHA256, Mode: connector.ModeCall, Connection: connection("http://localhost"), Secrets: secrets(), Payload: []byte(`{"count":101}`)},
		{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListScheduledEvents.Key, ContractSHA256: ListScheduledEvents.ContractSHA256, Mode: connector.ModeCall, Connection: connection("http://localhost"), Secrets: secrets(), Payload: []byte(`{"organization":"org","status":"pending"}`)},
		{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListScheduledEvents.Key, ContractSHA256: ListScheduledEvents.ContractSHA256, Mode: connector.ModeCall, Connection: connection("http://localhost"), Secrets: secrets(), Payload: []byte(`{"organization":"org","min_start_time":"bad"}`)},
		{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CreateSchedulingLink.Key, ContractSHA256: CreateSchedulingLink.ContractSHA256, Mode: connector.ModeCall, Connection: connection("http://localhost"), Secrets: secrets(), Payload: []byte(`{"owner":"event","owner_type":"User"}`)},
		{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CancelScheduledEvent.Key, ContractSHA256: CancelScheduledEvent.ContractSHA256, Mode: connector.ModeCall, Connection: connection("http://localhost"), Secrets: secrets(), Payload: []byte(`{}`)},
	}
	for _, current := range invalid {
		if _, err := adapter.Call(t.Context(), current); err == nil {
			t.Fatalf("invalid input accepted: %s", current.Payload)
		}
	}
}

func TestWriteFailuresAreUncertainButExplicitRejectionsArePermanent(t *testing.T) {
	call := func(transport *recordingTransport) error {
		adapter, _ := New(transport)
		_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CreateSchedulingLink.Key, ContractSHA256: CreateSchedulingLink.ContractSHA256, Mode: connector.ModeCall, Connection: connection("http://localhost"), Secrets: secrets(), Payload: []byte(`{"owner":"event","owner_type":"EventType"}`)})
		return err
	}
	for _, test := range []struct {
		name      string
		transport *recordingTransport
		want      connector.ErrorClassification
	}{{"network", &recordingTransport{err: errors.New("reset")}, connector.ErrorUncertain}, {"server", &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusBadGateway}}, connector.ErrorUncertain}, {"invalid success", &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{`)}}, connector.ErrorUncertain}, {"rate limit", &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusTooManyRequests}}, connector.ErrorRetryable}, {"bad request", &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusBadRequest}}, connector.ErrorPermanent}} {
		t.Run(test.name, func(t *testing.T) {
			class, ok := connector.ErrorClassificationOf(call(test.transport))
			if !ok || class != test.want {
				t.Fatalf("class=%q want=%q", class, test.want)
			}
		})
	}
}

func TestWebhookSignatureRotationTimestampAndIdentity(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	verifier := adapter.(connector.WebhookVerifier)
	receivedAt := time.Unix(2_000_000_000, 0).UTC()
	body := []byte(`{"event":"invitee.created","payload":{"uri":"https://api.calendly.com/invitees/42","email":"person@example.test","name":"Person"}}`)
	mac := hmac.New(sha256.New, []byte("signing-key"))
	_, _ = fmt.Fprintf(mac, "%d.%s", receivedAt.Unix(), body)
	signature := hex.EncodeToString(mac.Sum(nil))
	request := connector.VerifyWebhookRequest{Connection: connection("https://api.calendly.com"), Secrets: secrets(), Headers: map[string][]string{"Calendly-Webhook-Signature": {fmt.Sprintf("t=%d,v1=old,v1=%s", receivedAt.Unix(), signature)}}, Body: body, ReceivedAt: receivedAt}
	verified, err := verifier.VerifyWebhook(t.Context(), request)
	if err != nil || verified.EventType != "invitee.created" || verified.ExternalID != "https://api.calendly.com/invitees/42:invitee.created" || verified.ExternalIdentity == nil || verified.Security == nil || !verified.Security.SignatureVerified {
		t.Fatalf("verified=%+v err=%v", verified, err)
	}
	request.ReceivedAt = receivedAt.Add(6 * time.Minute)
	if _, err = verifier.VerifyWebhook(t.Context(), request); err == nil {
		t.Fatal("stale signature accepted")
	}
}

func connection(endpoint string) connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": endpoint, "webhook_tolerance_seconds": 300}}
}
func secrets() map[string]string {
	return map[string]string{"access_token": "calendly-token", "webhook_signing_key": "signing-key"}
}
func mustJSON(value any) []byte {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return raw
}
