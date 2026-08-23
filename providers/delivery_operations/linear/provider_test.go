package linear

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

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
	i := len(t.requests) - 1
	var response connector.HTTPResponse
	if i < len(t.responses) {
		response = t.responses[i]
	}
	var err error
	if i < len(t.errors) {
		err = t.errors[i]
	}
	return response, err
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestDescriptorEndpointRoutingAndSecretBoundary(t *testing.T) {
	responses := make([]connector.HTTPResponse, 6)
	for i := range responses {
		responses[i] = connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"data":{"issueCreate":{"issue":{"identifier":"LIN-1"}}}}`)}
	}
	transport := &recordingTransport{responses: responses}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	if len(adapter.Descriptor().Operations) != 6 {
		t.Fatalf("descriptor=%+v", adapter.Descriptor())
	}
	validator := adapter.(connector.ConfigValidator)
	for _, target := range []string{defaultEndpoint, "http://localhost:8080/graphql"} {
		if err = validator.ValidateConfig(connection(target)); err != nil {
			t.Fatalf("valid=%s err=%v", target, err)
		}
	}
	for _, target := range []string{"https://api.linear.app", "https://api.linear.app/graphql/extra", "https://api.linear.app.evil.test/graphql", "http://api.linear.app/graphql", "https://user@api.linear.app/graphql"} {
		if err = validator.ValidateConfig(connection(target)); err == nil {
			t.Fatalf("invalid accepted=%s", target)
		}
	}
	calls := []connector.CallRequest{call(ListDeliveryProjects.Key, ListDeliveryProjects.ContractSHA256, ListProjectsInput{PageSize: 10, Cursor: "next"}), call(ListDeliveryItems.Key, ListDeliveryItems.ContractSHA256, ListItemsInput{ProviderFilter: map[string]any{"team": map[string]any{"id": map[string]any{"eq": "team-1"}}}}), call(CreateDeliveryItem.Key, CreateDeliveryItem.ContractSHA256, CreateItemInput{Fields: map[string]any{"title": "Task"}}), call(UpdateDeliveryItem.Key, UpdateDeliveryItem.ContractSHA256, UpdateItemInput{ItemKey: "LIN-1", Fields: map[string]any{"title": "Updated"}}), call(TransitionDeliveryItem.Key, TransitionDeliveryItem.ContractSHA256, TransitionItemInput{ItemKey: "LIN-1", TransitionID: "state-2"}), call(TestConnection.Key, TestConnection.ContractSHA256, struct{}{})}
	for _, request := range calls {
		result, callErr := adapter.Call(t.Context(), request)
		if callErr != nil || result.ResponseRef != "linear:LIN-1" {
			t.Fatalf("operation=%s result=%+v err=%v", request.OperationKey, result, callErr)
		}
	}
	for i, request := range transport.requests {
		if request.URL != "http://localhost:8080/graphql" || request.SecretHeaders["Authorization"][0] != "lin_api_runtime" || request.Headers["Authorization"] != nil || strings.Contains(request.URL+string(request.Body), "lin_api_runtime") {
			t.Fatalf("request[%d]=%+v", i, request)
		}
	}
	var create map[string]any
	_ = json.Unmarshal(transport.requests[2].Body, &create)
	variables := create["variables"].(map[string]any)
	input := variables["input"].(map[string]any)
	if input["teamId"] != "team-default" {
		t.Fatalf("input=%v", input)
	}
}

func TestOAuthWebhookAndFailureClassifications(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: 200, Body: []byte(`{"data":{}}`)}}}
	adapter, _ := New(transport)
	request := call(TestConnection.Key, TestConnection.ContractSHA256, struct{}{})
	request.Secrets["api_token"] = "oauth-token"
	if _, err := adapter.Call(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if transport.requests[0].SecretHeaders["Authorization"][0] != "Bearer oauth-token" {
		t.Fatalf("request=%+v", transport.requests[0])
	}
	for _, invalid := range []connector.CallRequest{call(CreateDeliveryItem.Key, CreateDeliveryItem.ContractSHA256, CreateItemInput{}), call(UpdateDeliveryItem.Key, UpdateDeliveryItem.ContractSHA256, UpdateItemInput{ItemKey: "LIN-1"}), call(TransitionDeliveryItem.Key, TransitionDeliveryItem.ContractSHA256, TransitionItemInput{ItemKey: "LIN-1"})} {
		if _, err := adapter.Call(t.Context(), invalid); err == nil {
			t.Fatalf("invalid accepted=%s", invalid.OperationKey)
		}
	}
	now := time.Now().UTC()
	body, _ := json.Marshal(map[string]any{"action": "update", "type": "Issue", "data": map[string]any{"id": "id-1"}, "webhookTimestamp": now.UnixMilli()})
	mac := hmac.New(sha256.New, []byte("webhook-secret"))
	_, _ = mac.Write(body)
	verified, err := adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Secrets: map[string]string{"webhook_secret": "webhook-secret"}, Headers: map[string][]string{"Linear-Signature": {strings.ToUpper(hex.EncodeToString(mac.Sum(nil)))}, "Linear-Delivery": {"delivery-1"}}, Body: body, ReceivedAt: now})
	if err != nil || verified.EventType != "issue.update" || verified.ExternalID != "delivery-1" || verified.ExternalIdentity == nil || !verified.Security.SignatureVerified {
		t.Fatalf("verified=%+v err=%v", verified, err)
	}
	badBody, _ := json.Marshal(map[string]any{"action": "update", "type": "Issue", "data": map[string]any{"id": "id-1"}, "webhookTimestamp": now.Add(-2 * time.Minute).UnixMilli()})
	badMac := hmac.New(sha256.New, []byte("webhook-secret"))
	_, _ = badMac.Write(badBody)
	if _, err = adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Secrets: map[string]string{"webhook_secret": "webhook-secret"}, Headers: map[string][]string{"Linear-Signature": {hex.EncodeToString(badMac.Sum(nil))}, "Linear-Delivery": {"delivery-1"}}, Body: badBody, ReceivedAt: now}); err == nil {
		t.Fatal("old webhook accepted")
	}
	for _, test := range []struct {
		name         string
		response     connector.HTTPResponse
		transportErr error
		want         connector.ErrorClassification
	}{{"network", connector.HTTPResponse{}, errors.New("reset"), connector.ErrorUncertain}, {"server", connector.HTTPResponse{StatusCode: 502}, nil, connector.ErrorUncertain}, {"rate", connector.HTTPResponse{StatusCode: 429}, nil, connector.ErrorRetryable}, {"graphql rate", connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"errors":[{"extensions":{"code":"RATELIMITED"}}]}`)}, nil, connector.ErrorRetryable}, {"graphql reject", connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"errors":[{"extensions":{"code":"BAD_USER_INPUT"}}]}`)}, nil, connector.ErrorPermanent}, {"invalid", connector.HTTPResponse{StatusCode: 200, Body: []byte(`{`)}, nil, connector.ErrorUncertain}} {
		t.Run(test.name, func(t *testing.T) {
			current, _ := New(&recordingTransport{responses: []connector.HTTPResponse{test.response}, errors: []error{test.transportErr}})
			_, callErr := current.Call(t.Context(), call(CreateDeliveryItem.Key, CreateDeliveryItem.ContractSHA256, CreateItemInput{Fields: map[string]any{"teamId": "team-1", "title": "Task"}}))
			classification, ok := connector.ErrorClassificationOf(callErr)
			if !ok || classification != test.want {
				t.Fatalf("err=%v class=%q want=%q", callErr, classification, test.want)
			}
		})
	}
}

func connection(target string) connector.Connection {
	return connector.Connection{Config: map[string]any{"endpoint": target, "team_id": "team-default", "timeout_seconds": 30}}
}
func call(key, hash string, payload any) connector.CallRequest {
	raw, _ := json.Marshal(payload)
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: key, ContractSHA256: hash, Mode: connector.ModeCall, Connection: connection("http://localhost:8080/graphql"), Secrets: map[string]string{"api_token": "lin_api_runtime"}, Payload: raw}
}
