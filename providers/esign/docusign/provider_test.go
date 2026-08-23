package docusign

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
	if len(t.responses) >= len(t.requests) {
		return t.responses[len(t.requests)-1], nil
	}
	return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"envelopeId":"env-1"}`)}, nil
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestDescriptorRoutingAndRuntimeOnlyToken(t *testing.T) {
	transport := &recordingTransport{}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	operations := []struct {
		key, hash string
		input     any
	}{{CreateEnvelope.Key, CreateEnvelope.ContractSHA256, CreateEnvelopeInput{Input: map[string]any{"emailSubject": "Sign"}}}, {GetEnvelope.Key, GetEnvelope.ContractSHA256, EnvelopeInput{EnvelopeID: "env-1"}}, {SendEnvelope.Key, SendEnvelope.ContractSHA256, EnvelopeInput{EnvelopeID: "env-1"}}, {VoidEnvelope.Key, VoidEnvelope.ContractSHA256, VoidEnvelopeInput{EnvelopeID: "env-1", Reason: "superseded"}}, {CreateRecipientView.Key, CreateRecipientView.ContractSHA256, RecipientViewInput{EnvelopeID: "env-1", Input: map[string]any{"returnUrl": "https://app.test"}}}, {ListEnvelopeStatusChanges.Key, ListEnvelopeStatusChanges.ContractSHA256, ListEnvelopeStatusChangesInput{FromDate: "2026-07-01", Cursor: "42"}}, {TestConnection.Key, TestConnection.ContractSHA256, struct{}{}}}
	for _, op := range operations {
		payload, _ := json.Marshal(op.input)
		result, callErr := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: op.key, ContractSHA256: op.hash, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_token": "top-secret"}, Payload: payload})
		if callErr != nil || result.ResponseRef != "docusign:env-1" {
			t.Fatalf("operation=%s result=%+v err=%v", op.key, result, callErr)
		}
	}
	for _, request := range transport.requests {
		encoded, _ := json.Marshal(request)
		if strings.Contains(string(encoded), "top-secret") || request.SecretHeaders["Authorization"][0] != "Bearer top-secret" {
			t.Fatalf("secret boundary=%+v", request)
		}
	}
}

func TestOAuthRefreshAndWebhook(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: http.StatusUnauthorized, Body: []byte(`{"errorCode":"AUTHORIZATION_INVALID_TOKEN"}`)}, {StatusCode: 200, Body: []byte(`{"access_token":"fresh","refresh_token":"rotated"}`)}, {StatusCode: 200, Body: []byte(`{"envelopeId":"env-1"}`)}}}
	adapter, _ := New(transport)
	result, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: validConnection(), Secrets: map[string]string{"access_token": "stale", "refresh_token": "refresh", "client_id": "client", "client_secret": "secret"}})
	if err != nil || result.SecretUpdates["access_token"] != "fresh" || result.SecretUpdates["refresh_token"] != "rotated" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	refreshRequest := transport.requests[1]
	encoded, _ := json.Marshal(refreshRequest)
	if strings.Contains(string(encoded), "refresh") || len(refreshRequest.SecretHeaders["Authorization"]) != 1 || refreshRequest.SecretForm["refresh_token"] != "refresh" {
		t.Fatalf("refresh request=%+v", refreshRequest)
	}
	body := []byte(`{"event":"envelope-completed","data":{"envelopeId":"env-1"}}`)
	mac := hmac.New(sha256.New, []byte("webhook-secret"))
	_, _ = mac.Write(body)
	verified, err := adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Secrets: map[string]string{"webhook_secret": "webhook-secret"}, Headers: map[string][]string{"X-DocuSign-Signature-1": {base64.StdEncoding.EncodeToString(mac.Sum(nil))}}, Body: body})
	if err != nil || verified.ExternalID != "env-1:envelope-completed" || verified.Security == nil || !verified.Security.SignatureVerified {
		t.Fatalf("verified=%+v err=%v", verified, err)
	}
}

func validConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "https://api.example.test", "account_id": "account/1", "token_url": "https://account.example.test/oauth/token", "timeout_seconds": 30}}
}
