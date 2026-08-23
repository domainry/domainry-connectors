package zoomwebinar

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
	"strconv"
	"strings"
	"testing"
	"time"
)

type recordingTransport struct{ requests []connector.HTTPRequest }

func (t *recordingTransport) RoundTripHTTP(_ context.Context, r connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, r)
	return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"id":"9001","webinars":[]}`)}, nil
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func TestRoutingSecretBoundaryAndWebhook(t *testing.T) {
	tr := &recordingTransport{}
	adapter, err := New(tr)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	ops := []struct {
		key, hash string
		input     any
	}{{ListWebinars.Key, ListWebinars.ContractSHA256, ListWebinarsInput{UserID: "me", PageSize: 20, NextPageToken: "next", Type: "upcoming", Status: "active"}}, {GetWebinar.Key, GetWebinar.ContractSHA256, WebinarInput{WebinarID: "9001"}}, {ListRegistrants.Key, ListRegistrants.ContractSHA256, ListRegistrantsInput{WebinarID: "9001", PageSize: 30, Status: "approved"}}, {TestConnection.Key, TestConnection.ContractSHA256, struct{}{}}}
	for _, op := range ops {
		raw, _ := json.Marshal(op.input)
		result, callErr := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: op.key, ContractSHA256: op.hash, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"api_token": "top-secret"}, Payload: raw})
		if callErr != nil || result.ResponseRef != "zoom_webinar:9001" {
			t.Fatalf("operation=%s result=%+v err=%v", op.key, result, callErr)
		}
	}
	for _, request := range tr.requests {
		encoded, _ := json.Marshal(request)
		if strings.Contains(string(encoded), "top-secret") || request.SecretHeaders["Authorization"][0] != "Bearer top-secret" {
			t.Fatalf("secret boundary=%+v", request)
		}
	}
	if query := tr.requests[0].URL; !strings.Contains(query, "page_size=20") || !strings.Contains(query, "next_page_token=next") {
		t.Fatalf("query=%s", query)
	}
	now := time.Now().UTC().Truncate(time.Second)
	body := []byte(`{"event":"webinar.ended","payload":{"object":{"uuid":"uuid-1"}}}`)
	timestamp := strconv.FormatInt(now.Unix(), 10)
	mac := hmac.New(sha256.New, []byte("webhook-secret"))
	_, _ = mac.Write([]byte("v0:" + timestamp + ":"))
	_, _ = mac.Write(body)
	verified, err := adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Secrets: map[string]string{"webhook_secret": "webhook-secret"}, Headers: map[string][]string{"X-Zm-Request-Timestamp": {timestamp}, "X-Zm-Signature": {"v0=" + hex.EncodeToString(mac.Sum(nil))}}, Body: body, ReceivedAt: now})
	if err != nil || verified.ExternalID != "webinar.ended:uuid-1" || verified.Security == nil || !verified.Security.SignatureVerified {
		t.Fatalf("verified=%+v err=%v", verified, err)
	}
}
func TestRejectsReplayAndInsecureEndpoint(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	if err := adapter.(connector.ConfigValidator).ValidateConfig(connector.Connection{Config: map[string]any{"base_url": "http://api.zoom.us/v2"}}); err == nil {
		t.Fatal("insecure endpoint accepted")
	}
	_, err := adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Secrets: map[string]string{"webhook_secret": "secret"}, Headers: map[string][]string{"X-Zm-Request-Timestamp": {"1"}, "X-Zm-Signature": {"bad"}}, Body: []byte(`{}`), ReceivedAt: time.Now().UTC()})
	if err == nil {
		t.Fatal("replayed webhook accepted")
	}
}
func validConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "https://api.zoom.us/v2", "timeout_seconds": 30}}
}
