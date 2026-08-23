package stripe

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

type transportStub struct {
	request  connector.HTTPRequest
	response connector.HTTPResponse
	err      error
}

func (s *transportStub) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	s.request = request
	return s.response, s.err
}

func (*transportStub) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("SQL is unavailable")
}

func TestCompleteDescriptorAndContract(t *testing.T) {
	adapter, err := New(&transportStub{})
	if err != nil {
		t.Fatal(err)
	}
	if err := contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	descriptor := adapter.Descriptor()
	if descriptor.ConnectorKey != ConnectorKey || descriptor.ProviderKey != ProviderKey || len(descriptor.Operations) != 30 || len(descriptor.SecretFields) != 2 {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	raw, err := json.Marshal(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	got := fmt.Sprintf("%x", sha256.Sum256(raw))
	const want = "bab53ffb01431a95bbaf14a0f9f06ddf645a1fb0c89b56591c0a6b5b024dc2cf"
	if got != want {
		t.Fatalf("descriptor SHA-256=%s", got)
	}
}

func TestWriteUsesResolvedSecretAndRequestIdentity(t *testing.T) {
	stub := &transportStub{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"id":"pi_123","status":"requires_confirmation"}`)}}
	adapter, err := New(stub)
	if err != nil {
		t.Fatal(err)
	}
	operation := operationByKey(t, adapter.Descriptor(), "create_payment_intent")
	payload, _ := json.Marshal(map[string]any{"amount": 1200, "currency": "USD"})
	result, err := adapter.Call(t.Context(), connector.CallRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: operation.Key,
		ContractSHA256: operation.ContractSHA256, Mode: operation.Mode,
		Connection: connector.Connection{Config: map[string]any{"base_url": "http://localhost"}},
		Secrets:    map[string]string{"api_key": "sk_test_resolved"}, RequestRef: "request-123", Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stub.request.Headers["Authorization"][0] != "Bearer sk_test_resolved" || stub.request.Headers["Idempotency-Key"][0] != "request-123" {
		t.Fatalf("headers=%v", stub.request.Headers)
	}
	if string(stub.request.Body) != "amount=1200&currency=usd" || result.ResponseRef != "stripe:pi_123" {
		t.Fatalf("request body=%q result=%+v", stub.request.Body, result)
	}
}

func TestWriteNetworkFailureIsUncertain(t *testing.T) {
	stub := &transportStub{err: errors.New("connection reset")}
	adapter, err := New(stub)
	if err != nil {
		t.Fatal(err)
	}
	operation := operationByKey(t, adapter.Descriptor(), "create_payment_intent")
	payload, _ := json.Marshal(map[string]any{"amount": 1200, "currency": "USD"})
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: operation.Key, ContractSHA256: operation.ContractSHA256, Mode: operation.Mode, Connection: connector.Connection{Config: map[string]any{"base_url": "http://localhost"}}, Secrets: map[string]string{"api_key": "sk_test"}, Payload: payload})
	if classification, ok := connector.ErrorClassificationOf(err); !ok || classification != connector.ErrorUncertain {
		t.Fatalf("classification=%q ok=%v error=%v", classification, ok, err)
	}
}

func TestWebhookVerificationKeepsIngressOwnershipOutsideProvider(t *testing.T) {
	stub := &transportStub{}
	adapter, err := New(stub)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"id":"evt_123","type":"payment_intent.succeeded","data":{"object":{"customer":"cus_123"}}}`)
	receivedAt := time.Unix(1_800_000_000, 0).UTC()
	mac := hmac.New(sha256.New, []byte("whsec_resolved"))
	_, _ = fmt.Fprintf(mac, "%d.%s", receivedAt.Unix(), body)
	signature := hex.EncodeToString(mac.Sum(nil))
	verified, err := adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{
		Connection: connector.Connection{Config: map[string]any{"webhook_tolerance_seconds": 300}},
		Secrets:    map[string]string{"webhook_secret": "whsec_resolved"},
		Headers:    map[string][]string{"stripe-signature": {fmt.Sprintf("t=%d,v1=%s", receivedAt.Unix(), signature)}},
		Body:       body, ReceivedAt: receivedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if verified.ExternalID != "evt_123" || verified.EventType != "payment_intent.succeeded" || verified.Security == nil || !verified.Security.SignatureVerified || verified.ExternalIdentity == nil || verified.ExternalIdentity.Subject != "cus_123" {
		t.Fatalf("verified=%+v", verified)
	}
	if _, err := adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Secrets: map[string]string{"webhook_secret": "wrong"}, Headers: map[string][]string{"Stripe-Signature": {fmt.Sprintf("t=%d,v1=%s", receivedAt.Unix(), signature)}}, Body: body, ReceivedAt: receivedAt}); err == nil {
		t.Fatal("invalid webhook secret was accepted")
	}
}

func operationByKey(t *testing.T, descriptor connector.ProviderDescriptor, key string) connector.OperationDescriptor {
	t.Helper()
	for _, operation := range descriptor.Operations {
		if operation.Key == key {
			return operation
		}
	}
	t.Fatalf("operation %s is missing", key)
	return connector.OperationDescriptor{}
}

func TestRequestContractFailsClosed(t *testing.T) {
	adapter, err := New(&transportStub{})
	if err != nil {
		t.Fatal(err)
	}
	operation := operationByKey(t, adapter.Descriptor(), "retrieve_payment_intent")
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: operation.Key, ContractSHA256: strings.Repeat("0", 64), Mode: operation.Mode})
	if err == nil {
		t.Fatal("stale operation contract was accepted")
	}
}
