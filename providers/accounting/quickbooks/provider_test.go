package quickbooks

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
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
	err       error
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, request)
	if t.err != nil {
		return connector.HTTPResponse{}, t.err
	}
	response := t.responses[0]
	t.responses = t.responses[1:]
	return response, nil
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestDescriptorAndOfficialEndpointBoundary(t *testing.T) {
	adapter, err := New(&recordingTransport{})
	if err != nil {
		t.Fatal(err)
	}
	if err = contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	descriptor := adapter.Descriptor()
	if descriptor.ConnectorKey != ConnectorKey || descriptor.ProviderKey != ProviderKey || len(descriptor.Operations) != 13 || len(descriptor.ConfigFields) != 5 || len(descriptor.SecretFields) != 5 {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	validator := adapter.(connector.ConfigValidator)
	valid := connector.Connection{Config: map[string]any{"company_id": "realm", "base_url": "https://sandbox-quickbooks.api.intuit.com", "token_url": defaultTokenURL}}
	if err = validator.ValidateConfig(valid); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []connector.Connection{{}, {Config: map[string]any{"company_id": "realm", "base_url": "https://example.com", "token_url": defaultTokenURL}}, {Config: map[string]any{"company_id": "realm", "base_url": defaultBaseURL, "token_url": "https://example.com/token"}}} {
		if err = validator.ValidateConfig(invalid); err == nil {
			t.Fatalf("invalid config accepted: %+v", invalid)
		}
	}
}

func TestWriteUsesRuntimeSecretAndRequestID(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: http.StatusOK, Body: []byte(`{"Customer":{"Id":"customer-1"}}`)}}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(JSONObject{"DisplayName": "Buyer"})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CreateCustomer.Key, ContractSHA256: CreateCustomer.ContractSHA256, Mode: connector.ModeCall, Connection: testConnection(), Secrets: map[string]string{"access_token": "runtime-secret"}, RequestRef: "request-1", Payload: payload})
	if err != nil || result.ResponseRef != "quickbooks:customer:customer-1" {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	request := transport.requests[0]
	if request.SecretHeaders["Authorization"][0] != "Bearer runtime-secret" || !strings.Contains(request.URL, "requestid=request-1") || strings.Contains(request.URL+string(request.Body), "runtime-secret") {
		t.Fatalf("request=%+v", request)
	}
	if CreateCustomer.Reliability.Idempotency.Strategy != connector.IdempotencyNone {
		t.Fatalf("requestid was incorrectly published as Provider idempotency: %+v", CreateCustomer.Reliability)
	}
}

func TestRefreshRotatesRuntimeSecrets(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: http.StatusUnauthorized, Body: []byte(`{"Fault":{}}`)}, {StatusCode: http.StatusOK, Body: []byte(`{"access_token":"fresh-access","refresh_token":"fresh-refresh"}`)}, {StatusCode: http.StatusOK, Body: []byte(`{"CompanyInfo":{"Id":"realm"}}`)}}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: TestConnection.Key, ContractSHA256: TestConnection.ContractSHA256, Mode: connector.ModeCall, Connection: testConnection(), Secrets: map[string]string{"access_token": "expired", "refresh_token": "refresh-secret", "client_id": "client-id", "client_secret": "client-secret"}, Payload: []byte(`{}`)})
	if err != nil || result.SecretUpdates["access_token"] != "fresh-access" || result.SecretUpdates["refresh_token"] != "fresh-refresh" {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	refresh := transport.requests[1]
	if refresh.SecretForm["refresh_token"] != "refresh-secret" || refresh.SecretHeaders["Authorization"][0] == "" || strings.Contains(string(refresh.Body), "refresh-secret") {
		t.Fatalf("refresh=%+v", refresh)
	}
}

func TestWriteFailuresAreClassifiedWithoutUnsafeRetry(t *testing.T) {
	for _, test := range []struct {
		name     string
		response connector.HTTPResponse
		err      error
		want     connector.ErrorClassification
	}{{"network", connector.HTTPResponse{}, errors.New("reset"), connector.ErrorUncertain}, {"rate_limit", connector.HTTPResponse{StatusCode: http.StatusTooManyRequests}, nil, connector.ErrorRetryable}, {"bad_request", connector.HTTPResponse{StatusCode: http.StatusBadRequest}, nil, connector.ErrorPermanent}, {"server", connector.HTTPResponse{StatusCode: http.StatusBadGateway}, nil, connector.ErrorUncertain}, {"malformed_success", connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{`)}, nil, connector.ErrorUncertain}} {
		t.Run(test.name, func(t *testing.T) {
			transport := &recordingTransport{responses: []connector.HTTPResponse{test.response}, err: test.err}
			adapter, _ := New(transport)
			payload, _ := json.Marshal(JSONObject{"DisplayName": "Buyer"})
			_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CreateCustomer.Key, ContractSHA256: CreateCustomer.ContractSHA256, Mode: connector.ModeCall, Connection: testConnection(), Secrets: map[string]string{"access_token": "token"}, Payload: payload})
			classification, ok := connector.ErrorClassificationOf(err)
			if !ok || classification != test.want {
				t.Fatalf("error=%v classification=%q want=%q", err, classification, test.want)
			}
		})
	}
}

func TestWebhookUsesIntuitBase64HMACAndAllEntityNotifications(t *testing.T) {
	body := []byte(`{"eventNotifications":[{"realmId":"realm","dataChangeEvent":{"entities":[{"name":"Customer","id":"customer-1","operation":"Update","lastUpdated":"2026-08-23T00:00:00Z"}]}}]}`)
	mac := hmac.New(sha256.New, []byte("verifier"))
	_, _ = mac.Write(body)
	signature := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	adapter, _ := New(&recordingTransport{})
	verified, err := adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Connection: testConnection(), Secrets: map[string]string{"webhook_verifier_token": "verifier"}, Headers: map[string][]string{"intuit-signature": {signature}}, Body: body})
	if err != nil || verified.ExternalID != "realm:Customer:customer-1:2026-08-23T00:00:00Z" || verified.EventType != "data_change.customer.update" || verified.Security == nil || !verified.Security.SignatureVerified {
		t.Fatalf("verified=%+v error=%v", verified, err)
	}
	if _, err = adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Connection: testConnection(), Secrets: map[string]string{"webhook_verifier_token": "verifier"}, Headers: map[string][]string{"intuit-signature": {"bad"}}, Body: body}); err == nil {
		t.Fatal("invalid signature accepted")
	}
}

func testConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"company_id": "realm", "base_url": "http://localhost:8080", "token_url": "http://localhost:8081/token", "minor_version": 75}}
}
