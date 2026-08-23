package persona

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
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

func TestDescriptorEndpointAndPinnedVersion(t *testing.T) {
	adapter, err := New(&recordingTransport{})
	if err != nil {
		t.Fatal(err)
	}
	if err = contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	if descriptor := adapter.Descriptor(); descriptor.ConnectorKey != ConnectorKey || descriptor.ProviderKey != ProviderKey || len(descriptor.Operations) != 3 || len(descriptor.SecretFields) != 2 {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	validator := adapter.(connector.ConfigValidator)
	for _, endpoint := range []string{"https://withpersona.com", "https://api.withpersona.com.evil.test", "http://api.withpersona.com"} {
		if err = validator.ValidateConfig(connection(endpoint)); err == nil {
			t.Fatalf("invalid endpoint accepted: %s", endpoint)
		}
	}
	if err = validator.ValidateConfig(connection("http://localhost:8080")); err != nil {
		t.Fatal(err)
	}
}

func TestCreateInquiryUsesRuntimeSecretAndDefensiveIdempotencyHeader(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusCreated, Body: []byte(`{"data":{"id":"inq_1","type":"inquiry"}}`)}}
	adapter, _ := New(transport)
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CreateInquiry.Key, ContractSHA256: CreateInquiry.ContractSHA256, Mode: connector.ModeCall, Connection: connection("http://localhost:8080"), Secrets: map[string]string{"api_key": "runtime-secret"}, RequestRef: "request-1", Payload: []byte(`{"reference-id":"customer-1"}`)})
	if err != nil || result.ResponseRef != "persona:inquiry:inq_1" {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	request := transport.request
	if request.SecretHeaders["Authorization"][0] != "Bearer runtime-secret" || request.Headers["Persona-Version"][0] != personaVersion || request.Headers["Idempotency-Key"][0] != "request-1" || strings.Contains(request.URL+string(request.Body), "runtime-secret") {
		t.Fatalf("request=%+v", request)
	}
	if CreateInquiry.Reliability.Idempotency.Strategy != connector.IdempotencyNone {
		t.Fatalf("undocumented retention guarantee published: %+v", CreateInquiry.Reliability)
	}
}

func TestCreateInquiryFailureClassification(t *testing.T) {
	for _, test := range []struct {
		name     string
		response connector.HTTPResponse
		err      error
		want     connector.ErrorClassification
	}{{"network", connector.HTTPResponse{}, errors.New("reset"), connector.ErrorUncertain}, {"rate_limit", connector.HTTPResponse{StatusCode: http.StatusTooManyRequests}, nil, connector.ErrorRetryable}, {"server", connector.HTTPResponse{StatusCode: http.StatusBadGateway}, nil, connector.ErrorUncertain}, {"bad_request", connector.HTTPResponse{StatusCode: http.StatusBadRequest}, nil, connector.ErrorPermanent}, {"malformed_success", connector.HTTPResponse{StatusCode: http.StatusCreated, Body: []byte(`{`)}, nil, connector.ErrorUncertain}, {"missing_identity", connector.HTTPResponse{StatusCode: http.StatusCreated, Body: []byte(`{"data":{}}`)}, nil, connector.ErrorUncertain}} {
		t.Run(test.name, func(t *testing.T) {
			transport := &recordingTransport{response: test.response, err: test.err}
			adapter, _ := New(transport)
			_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CreateInquiry.Key, ContractSHA256: CreateInquiry.ContractSHA256, Mode: connector.ModeCall, Connection: connection("http://localhost:8080"), Secrets: map[string]string{"api_key": "secret"}, Payload: []byte(`{}`)})
			class, ok := connector.ErrorClassificationOf(err)
			if !ok || class != test.want {
				t.Fatalf("error=%v class=%q want=%q", err, class, test.want)
			}
		})
	}
}

func TestWebhookSupportsRotatingSignaturePairs(t *testing.T) {
	body := []byte(`{"data":{"id":"evt_1","type":"inquiry.completed","attributes":{"payload":{"data":{"id":"inq_1"}}}}}`)
	timestamp := "1710000000"
	mac := hmac.New(sha256.New, []byte("webhook-secret"))
	_, _ = mac.Write([]byte(timestamp + "." + string(body)))
	valid := hex.EncodeToString(mac.Sum(nil))
	header := "t=" + timestamp + ",v1=invalid t=" + timestamp + ",v1=" + valid
	adapter, _ := New(&recordingTransport{})
	verified, err := adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Secrets: map[string]string{"webhook_secret": "webhook-secret"}, Headers: map[string][]string{"Persona-Signature": {header}}, Body: body})
	if err != nil || verified.ExternalID != "evt_1" || verified.EventType != "inquiry.completed" || verified.ExternalIdentity == nil || verified.ExternalIdentity.Subject != "inq_1" || verified.Security == nil || !verified.Security.SignatureVerified {
		t.Fatalf("verified=%+v error=%v", verified, err)
	}
	if _, err = adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Secrets: map[string]string{"webhook_secret": "webhook-secret"}, Headers: map[string][]string{"Persona-Signature": {"t=1,v1=bad"}}, Body: body}); err == nil {
		t.Fatal("invalid signature accepted")
	}
}

func connection(endpoint string) connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": endpoint, "inquiry_template_id": "itmpl_1"}}
}
