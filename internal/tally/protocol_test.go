package tally

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
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

func TestConnectionPinsVersionAndKeepsTokenRuntimeOnly(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"id":"form/one","name":"Lead"}`)}}
	protocol := Protocol{Transport: transport}
	payload, ref, err := protocol.TestConnection(t.Context(), connection(), map[string]string{"api_token": "token"})
	if err != nil || payload["name"] != "Lead" || ref != "tally:form:form/one" {
		t.Fatalf("payload=%v ref=%q error=%v", payload, ref, err)
	}
	request := transport.requests[0]
	if !strings.HasSuffix(request.URL, "/forms/form%2Fone") || request.Headers["tally-version"][0] != DefaultAPIVersion || request.SecretHeaders["Authorization"][0] != "Bearer token" || request.Headers["Authorization"] != nil || strings.Contains(request.URL, "token") {
		t.Fatalf("request=%+v", request)
	}
	transport.response = connector.HTTPResponse{StatusCode: http.StatusTooManyRequests, Body: []byte(`{"error":"rate"}`)}
	_, _, err = protocol.TestConnection(t.Context(), connection(), map[string]string{"api_token": "token"})
	if class, ok := connector.ErrorClassificationOf(err); !ok || class != connector.ErrorRetryable {
		t.Fatalf("429=%v class=%q", err, class)
	}
	transport.response = connector.HTTPResponse{StatusCode: http.StatusBadRequest, Body: []byte(`{}`)}
	_, _, err = protocol.TestConnection(t.Context(), connection(), map[string]string{"api_token": "token"})
	if class, ok := connector.ErrorClassificationOf(err); !ok || class != connector.ErrorPermanent {
		t.Fatalf("400=%v class=%q", err, class)
	}
}

func TestWebhookUsesDocumentedBase64HMACAndIdentity(t *testing.T) {
	body := []byte(`{"eventId":"event-1","eventType":"FORM_RESPONSE","data":{"responseId":"response-1","fields":[{"type":"INPUT_EMAIL","value":"lead@example.test"}]}}`)
	mac := hmac.New(sha256.New, []byte("secret"))
	_, _ = mac.Write(body)
	signature := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	request := connector.VerifyWebhookRequest{Secrets: map[string]string{"webhook_secret": "secret"}, Headers: map[string][]string{"tally-signature": {signature}}, Body: body}
	verified, err := VerifyWebhook(t.Context(), request)
	if err != nil || verified.ExternalID != "event-1" || verified.EventType != "form_response" || verified.ExternalIdentity == nil || verified.ExternalIdentity.Subject != "lead@example.test" || verified.Security == nil || !verified.Security.SignatureVerified {
		t.Fatalf("verified=%+v error=%v", verified, err)
	}
	request.Headers["tally-signature"] = []string{hex.EncodeToString(mac.Sum(nil))}
	if _, err = VerifyWebhook(t.Context(), request); err == nil {
		t.Fatal("legacy hex signature accepted")
	}
	request.Headers = nil
	if _, err = VerifyWebhook(t.Context(), request); err == nil {
		t.Fatal("missing signature accepted")
	}
}

func TestValidationAndWebhookEdges(t *testing.T) {
	protocol := Protocol{Transport: &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{`)}}}
	for _, invalid := range []connector.Connection{{}, {Config: map[string]any{"form_id": "f", "base_url": "http://remote.example"}}, {Config: map[string]any{"form_id": "f", "base_url": "https://user@example.com"}}, {Config: map[string]any{"form_id": "f", "base_url": "https://api.tally.so", "api_version": "latest"}}} {
		if err := protocol.Validate(invalid); err == nil {
			t.Fatalf("invalid connection accepted: %+v", invalid)
		}
	}
	if _, _, err := protocol.TestConnection(t.Context(), connection(), nil); err == nil {
		t.Fatal("missing API token accepted")
	}
	if _, _, err := protocol.TestConnection(t.Context(), connection(), map[string]string{"api_token": "token"}); err == nil {
		t.Fatal("invalid provider JSON accepted")
	}
	for _, body := range [][]byte{[]byte(`{`), []byte(`{"eventType":"FORM_RESPONSE","data":{}}`)} {
		mac := hmac.New(sha256.New, []byte("secret"))
		_, _ = mac.Write(body)
		_, err := VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Secrets: map[string]string{"webhook_secret": "secret"}, Headers: map[string][]string{"Tally-Signature": {base64.StdEncoding.EncodeToString(mac.Sum(nil))}}, Body: body})
		if err == nil {
			t.Fatalf("invalid webhook accepted: %s", body)
		}
	}
}

func connection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080", "form_id": "form/one"}}
}
