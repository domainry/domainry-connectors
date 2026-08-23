package typeform

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
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

func TestTypedCallsUseRuntimeOnlyBearerHeader(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"items":[{"token":"response-1"}]}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	completed := false
	input, _ := json.Marshal(ListResponsesInput{FormID: "form/one", PageSize: 100, Since: "2026-01-01T00:00:00Z", After: "cursor", Completed: &completed})
	result, err := adapter.Call(t.Context(), callRequest(ListResponses, input))
	if err != nil {
		t.Fatal(err)
	}
	request := transport.requests[0]
	if request.SecretHeaders["Authorization"][0] != "Bearer token" || strings.Contains(request.URL, "token") || !strings.Contains(request.URL, "/forms/form%2Fone/responses") || !strings.Contains(request.URL, "completed=false") || !strings.Contains(request.URL, "page_size=100") {
		t.Fatalf("request=%+v", request)
	}
	if !strings.Contains(string(result.Payload), "response-1") {
		t.Fatalf("result=%s", result.Payload)
	}
}

func TestConnectionWebhookAndIdentity(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"alias":"domainry"}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: connection(), Secrets: map[string]string{"api_token": "token"}})
	if err != nil || !probe.Connected || !strings.Contains(string(probe.Details), "domainry") {
		t.Fatalf("probe=%+v error=%v", probe, err)
	}
	body := []byte(`{"event_id":"event-1","event_type":"form_response","form_response":{"token":"response-1","hidden":{"email":"person@example.test"}}}`)
	mac := hmac.New(sha256.New, []byte("webhook-secret"))
	_, _ = mac.Write(body)
	verified, err := adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Secrets: map[string]string{"webhook_secret": "webhook-secret"}, Body: body, Headers: map[string][]string{"typeform-signature": {"sha256=" + base64.StdEncoding.EncodeToString(mac.Sum(nil))}}})
	if err != nil || verified.ExternalID != "event-1" || verified.ExternalIdentity == nil || verified.ExternalIdentity.Subject != "person@example.test" || verified.Security == nil || !verified.Security.SignatureVerified {
		t.Fatalf("verified=%+v error=%v", verified, err)
	}
}

func TestInputsConfigFailuresAndRetryClassification(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	for name, payload := range map[string][]byte{
		"unknown":         []byte(`{"unknown":true}`),
		"page size":       mustJSON(ListResponsesInput{PageSize: 1001}),
		"cursor conflict": mustJSON(ListResponsesInput{After: "a", Before: "b"}),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := adapter.Call(t.Context(), callRequest(ListResponses, payload)); err == nil {
				t.Fatal("invalid input accepted")
			}
		})
	}
	missingForm := callRequest(GetForm, mustJSON(GetFormInput{}))
	missingForm.Connection.Config = map[string]any{"base_url": "http://localhost:8080"}
	if _, err := adapter.Call(t.Context(), missingForm); err == nil {
		t.Fatal("missing form ID accepted")
	}
	if err := adapter.(connector.ConfigValidator).ValidateConfig(connector.Connection{Config: map[string]any{"base_url": "http://remote.example"}}); err == nil {
		t.Fatal("remote HTTP accepted")
	}
	transport.response = connector.HTTPResponse{StatusCode: http.StatusTooManyRequests, Body: []byte(`{}`)}
	_, err = adapter.Call(t.Context(), callRequest(GetForm, mustJSON(GetFormInput{})))
	classification, ok := connector.ErrorClassificationOf(err)
	if !ok || classification != connector.ErrorRetryable {
		t.Fatalf("classification=%q/%v error=%v", classification, ok, err)
	}
	if _, err := adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Secrets: map[string]string{"webhook_secret": "secret"}, Body: []byte(`{}`)}); err == nil {
		t.Fatal("unsigned webhook accepted")
	}
}

func connection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080", "survey_id": "form-default"}}
}
func callRequest[T any, O any](operation connector.CallOperation[T, O], payload []byte) connector.CallRequest {
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: operation.Key, ContractSHA256: operation.ContractSHA256, Mode: connector.ModeCall, Connection: connection(), Secrets: map[string]string{"api_token": "token"}, Payload: payload}
}
func mustJSON(value any) []byte {
	payload, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return payload
}
