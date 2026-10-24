package fcm

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
)

type scriptedTransport struct {
	requests  []connector.HTTPRequest
	responses []connector.HTTPResponse
	errors    []error
}

func (transport *scriptedTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	transport.requests = append(transport.requests, request)
	index := len(transport.requests) - 1
	if index < len(transport.errors) && transport.errors[index] != nil {
		return connector.HTTPResponse{}, transport.errors[index]
	}
	if index >= len(transport.responses) {
		return connector.HTTPResponse{}, errors.New("unexpected HTTP request")
	}
	return transport.responses[index], nil
}

func (*scriptedTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestSendUsesRuntimeTransportAndKeepsCredentialsSecret(t *testing.T) {
	serviceAccountJSON := testServiceAccount(t)
	transport := &scriptedTransport{responses: []connector.HTTPResponse{
		{StatusCode: http.StatusOK, Body: []byte(`{"access_token":"access-token","token_type":"Bearer","expires_in":3600}`)},
		{StatusCode: http.StatusOK, Body: []byte(`{"name":"projects/facility-prod/messages/message-1"}`)},
	}}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	payload, _ := json.Marshal(SendInput{DeviceToken: "device-token", Title: "Emergency", Body: "Please acknowledge", Data: map[string]string{"event_id": "event-1"}, TTLSeconds: 120, CollapseKey: "emergency-event-1", Priority: "high"})
	result, err := adapter.Call(t.Context(), connector.CallRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SendNative.Key,
		ContractSHA256: SendNative.ContractSHA256, Mode: connector.ModeEnqueue,
		Connection: validConnection(), Secrets: map[string]string{"service_account_json": serviceAccountJSON},
		Payload: payload, RequestRef: "delivery-1", Delivery: true,
	})
	if err != nil || result.ResponseRef != "projects/facility-prod/messages/message-1" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if len(transport.requests) != 2 {
		t.Fatalf("requests=%d", len(transport.requests))
	}
	oauth := transport.requests[0]
	if oauth.URL != googleTokenURL || oauth.SecretForm["assertion"] == "" || strings.Contains(string(oauth.Body), "assertion") {
		t.Fatalf("oauth request did not isolate the signed assertion: %+v", oauth)
	}
	assertJWTClaims(t, oauth.SecretForm["assertion"])
	send := transport.requests[1]
	if send.URL != "https://fcm.googleapis.com/v1/projects/facility-prod/messages:send" || len(send.SecretHeaders["Authorization"]) != 1 || send.Headers["Authorization"] != nil {
		t.Fatalf("send request=%+v", send)
	}
	var envelope struct {
		Message struct {
			Token        string            `json:"token"`
			Notification map[string]string `json:"notification"`
			Data         map[string]string `json:"data"`
			Android      struct {
				Priority    string `json:"priority"`
				TTL         string `json:"ttl"`
				CollapseKey string `json:"collapse_key"`
			} `json:"android"`
		} `json:"message"`
	}
	if err := json.Unmarshal(send.Body, &envelope); err != nil || envelope.Message.Token != "device-token" || envelope.Message.Notification["title"] != "Emergency" || envelope.Message.Data["event_id"] != "event-1" || envelope.Message.Android.Priority != "HIGH" || envelope.Message.Android.TTL != "120s" || envelope.Message.Android.CollapseKey != "emergency-event-1" {
		t.Fatalf("translated payload=%s err=%v", send.Body, err)
	}
	serialized, _ := json.Marshal(transport.requests)
	if strings.Contains(string(serialized), "PRIVATE KEY") || strings.Contains(string(serialized), "access-token") || strings.Contains(string(serialized), "assertion") {
		t.Fatalf("serialized transport requests leaked credentials: %s", serialized)
	}
}

func TestTestConnectionAuthenticatesServiceAccount(t *testing.T) {
	transport := &scriptedTransport{responses: []connector.HTTPResponse{{StatusCode: http.StatusOK, Body: []byte(`{"access_token":"access-token","token_type":"Bearer"}`)}}}
	adapter, _ := New(transport)
	result, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: validConnection(), Secrets: map[string]string{"service_account_json": testServiceAccount(t)}})
	if err != nil || !result.Connected || len(transport.requests) != 1 {
		t.Fatalf("result=%+v requests=%d err=%v", result, len(transport.requests), err)
	}
}

func TestFailuresAreClassified(t *testing.T) {
	serviceAccountJSON := testServiceAccount(t)
	payload, _ := json.Marshal(SendInput{DeviceToken: "device-token", Title: "Emergency", Body: "Please acknowledge"})
	tests := []struct {
		name           string
		responses      []connector.HTTPResponse
		errors         []error
		classification connector.ErrorClassification
	}{
		{name: "oauth unavailable", responses: []connector.HTTPResponse{{StatusCode: http.StatusServiceUnavailable}}, classification: connector.ErrorRetryable},
		{name: "delivery network unknown", responses: []connector.HTTPResponse{{StatusCode: http.StatusOK, Body: []byte(`{"access_token":"access-token","token_type":"Bearer"}`)}, {}}, errors: []error{nil, errors.New("connection reset")}, classification: connector.ErrorUncertain},
		{name: "rate limited", responses: []connector.HTTPResponse{{StatusCode: http.StatusOK, Body: []byte(`{"access_token":"access-token","token_type":"Bearer"}`)}, {StatusCode: http.StatusTooManyRequests}}, classification: connector.ErrorRetryable},
		{name: "provider unavailable", responses: []connector.HTTPResponse{{StatusCode: http.StatusOK, Body: []byte(`{"access_token":"access-token","token_type":"Bearer"}`)}, {StatusCode: http.StatusServiceUnavailable}}, classification: connector.ErrorRetryable},
		{name: "request rejected", responses: []connector.HTTPResponse{{StatusCode: http.StatusOK, Body: []byte(`{"access_token":"access-token","token_type":"Bearer"}`)}, {StatusCode: http.StatusBadRequest}}, classification: connector.ErrorPermanent},
		{name: "invalid receipt", responses: []connector.HTTPResponse{{StatusCode: http.StatusOK, Body: []byte(`{"access_token":"access-token","token_type":"Bearer"}`)}, {StatusCode: http.StatusOK, Body: []byte(`{}`)}}, classification: connector.ErrorUncertain},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transport := &scriptedTransport{responses: test.responses, errors: test.errors}
			adapter, _ := New(transport)
			_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SendNative.Key, ContractSHA256: SendNative.ContractSHA256, Mode: connector.ModeEnqueue, Connection: validConnection(), Secrets: map[string]string{"service_account_json": serviceAccountJSON}, Payload: payload, Delivery: true})
			if classification, ok := connector.ErrorClassificationOf(err); !ok || classification != test.classification {
				t.Fatalf("classification=%q ok=%v err=%v", classification, ok, err)
			}
		})
	}
}

func TestRejectsInvalidConfigurationMessageAndServiceAccount(t *testing.T) {
	adapter, _ := New(&scriptedTransport{})
	if adapter.(connector.ConfigValidator).ValidateConfig(connector.Connection{}) == nil {
		t.Fatal("accepted empty configuration")
	}
	invalidPayload, _ := json.Marshal(SendInput{DeviceToken: "token with spaces", Title: "Emergency", Body: "Please acknowledge"})
	_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SendNative.Key, ContractSHA256: SendNative.ContractSHA256, Mode: connector.ModeEnqueue, Connection: validConnection(), Secrets: map[string]string{"service_account_json": `{}`}, Payload: invalidPayload, Delivery: true})
	if classification, ok := connector.ErrorClassificationOf(err); !ok || classification != connector.ErrorPermanent {
		t.Fatalf("classification=%q ok=%v err=%v", classification, ok, err)
	}
}

func validConnection() connector.Connection {
	return connector.Connection{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Config: map[string]any{"project_id": "facility-prod", "timeout_seconds": 15, "ttl_seconds": 3600}, SecretRefs: map[string]string{"service_account_json": "secret:fcm"}}
}

func testServiceAccount(t *testing.T) string {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	keyBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	value, err := json.Marshal(serviceAccount{Type: "service_account", ProjectID: "facility-prod", PrivateKeyID: "key-1", PrivateKey: string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes})), ClientEmail: "firebase-adminsdk@example.iam.gserviceaccount.com", TokenURI: googleTokenURL})
	if err != nil {
		t.Fatal(err)
	}
	return string(value)
}

func assertJWTClaims(t *testing.T, token string) {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("JWT parts=%d", len(parts))
	}
	decode := func(part string) map[string]any {
		raw, err := base64.RawURLEncoding.DecodeString(part)
		if err != nil {
			t.Fatal(err)
		}
		var value map[string]any
		if err := json.Unmarshal(raw, &value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	header, claims := decode(parts[0]), decode(parts[1])
	if header["alg"] != "RS256" || header["kid"] != "key-1" || claims["iss"] != "firebase-adminsdk@example.iam.gserviceaccount.com" || claims["aud"] != googleTokenURL || claims["scope"] != fcmScope {
		t.Fatalf("header=%v claims=%v", header, claims)
	}
}
