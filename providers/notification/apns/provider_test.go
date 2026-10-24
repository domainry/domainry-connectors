package apns

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
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

func (transport *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
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

func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestSendUsesRuntimeTransportAndReusesAuthorizationToken(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{
		{StatusCode: http.StatusOK, Headers: map[string][]string{"apns-id": {"accepted-1"}}},
		{StatusCode: http.StatusOK, Headers: map[string][]string{"Apns-Id": {"accepted-2"}}},
	}}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	current := adapter.(*provider)
	current.now = func() time.Time { return time.Date(2026, time.October, 25, 8, 30, 0, 0, time.UTC) }
	privateKey := testPrivateKey(t)
	variableLengthToken := strings.Repeat("ab", 40)
	payload, _ := json.Marshal(SendInput{DeviceToken: variableLengthToken, Title: "Emergency", Body: "Please acknowledge", Data: map[string]string{"event_id": "event-1"}, TTLSeconds: 120, CollapseKey: "emergency-event-1", Priority: "high"})
	request := connector.CallRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SendNative.Key,
		ContractSHA256: SendNative.ContractSHA256, Mode: connector.ModeEnqueue,
		Connection: validConnection(), Secrets: map[string]string{"private_key": privateKey},
		Payload: payload, RequestRef: "delivery-1", Delivery: true,
	}
	first, firstErr := adapter.Call(t.Context(), request)
	request.RequestRef = "delivery-2"
	second, secondErr := adapter.Call(t.Context(), request)
	if firstErr != nil || secondErr != nil || first.ResponseRef != "accepted-1" || second.ResponseRef != "accepted-2" {
		t.Fatalf("first=%+v/%v second=%+v/%v", first, firstErr, second, secondErr)
	}
	if len(transport.requests) != 2 {
		t.Fatalf("requests=%d", len(transport.requests))
	}
	firstRequest, secondRequest := transport.requests[0], transport.requests[1]
	wantURL := sandboxEndpoint + "/3/device/" + variableLengthToken
	if firstRequest.URL != wantURL || firstRequest.Headers["apns-topic"][0] != "com.domainry.facility" || firstRequest.Headers["apns-push-type"][0] != "alert" || firstRequest.Headers["apns-priority"][0] != "10" || firstRequest.Headers["apns-expiration"][0] != "1792917120" || firstRequest.Headers["apns-collapse-id"][0] != "emergency-event-1" {
		t.Fatalf("request=%+v", firstRequest)
	}
	if len(firstRequest.SecretHeaders["Authorization"]) != 1 || firstRequest.Headers["Authorization"] != nil || firstRequest.SecretHeaders["Authorization"][0] != secondRequest.SecretHeaders["Authorization"][0] {
		t.Fatalf("authorization was public or was not reused: first=%+v second=%+v", firstRequest, secondRequest)
	}
	assertJWTClaims(t, strings.TrimPrefix(firstRequest.SecretHeaders["Authorization"][0], "bearer "))
	var body struct {
		APS struct {
			Alert map[string]string `json:"alert"`
			Sound string            `json:"sound"`
		} `json:"aps"`
		Data map[string]string `json:"data"`
	}
	if err := json.Unmarshal(firstRequest.Body, &body); err != nil || body.APS.Alert["title"] != "Emergency" || body.APS.Alert["body"] != "Please acknowledge" || body.APS.Sound != "default" || body.Data["event_id"] != "event-1" {
		t.Fatalf("body=%s err=%v", firstRequest.Body, err)
	}
	serialized, _ := json.Marshal(transport.requests)
	if strings.Contains(string(serialized), "PRIVATE KEY") || strings.Contains(string(serialized), "Authorization") {
		t.Fatalf("serialized transport request leaked credentials: %s", serialized)
	}
}

func TestTestConnectionValidatesKeyWithoutNetworkClaim(t *testing.T) {
	transport := &recordingTransport{}
	adapter, _ := New(transport)
	result, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: validConnection(), Secrets: map[string]string{"private_key": testPrivateKey(t)}})
	if err != nil || !result.Connected || len(transport.requests) != 0 {
		t.Fatalf("result=%+v requests=%d err=%v", result, len(transport.requests), err)
	}
}

func TestFailuresAreClassified(t *testing.T) {
	privateKey := testPrivateKey(t)
	payload, _ := json.Marshal(SendInput{DeviceToken: strings.Repeat("ab", 32), Title: "Emergency", Body: "Please acknowledge"})
	tests := []struct {
		name           string
		response       connector.HTTPResponse
		err            error
		classification connector.ErrorClassification
	}{
		{name: "delivery network unknown", err: errors.New("connection reset"), classification: connector.ErrorUncertain},
		{name: "rate limited", response: connector.HTTPResponse{StatusCode: http.StatusTooManyRequests}, classification: connector.ErrorRetryable},
		{name: "provider unavailable", response: connector.HTTPResponse{StatusCode: http.StatusServiceUnavailable}, classification: connector.ErrorRetryable},
		{name: "request rejected", response: connector.HTTPResponse{StatusCode: http.StatusBadRequest}, classification: connector.ErrorPermanent},
		{name: "unexpected success", response: connector.HTTPResponse{StatusCode: http.StatusAccepted}, classification: connector.ErrorUncertain},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transport := &recordingTransport{responses: []connector.HTTPResponse{test.response}, errors: []error{test.err}}
			adapter, _ := New(transport)
			_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SendNative.Key, ContractSHA256: SendNative.ContractSHA256, Mode: connector.ModeEnqueue, Connection: validConnection(), Secrets: map[string]string{"private_key": privateKey}, Payload: payload, Delivery: true})
			if classification, ok := connector.ErrorClassificationOf(err); !ok || classification != test.classification {
				t.Fatalf("classification=%q ok=%v err=%v", classification, ok, err)
			}
		})
	}
}

func TestRejectsInvalidConfigurationMessageAndKey(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	if adapter.(connector.ConfigValidator).ValidateConfig(connector.Connection{}) == nil {
		t.Fatal("accepted empty configuration")
	}
	payload, _ := json.Marshal(SendInput{DeviceToken: "not-a-device-token", Title: "Emergency", Body: "Please acknowledge"})
	_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SendNative.Key, ContractSHA256: SendNative.ContractSHA256, Mode: connector.ModeEnqueue, Connection: validConnection(), Secrets: map[string]string{"private_key": "invalid"}, Payload: payload, Delivery: true})
	if classification, ok := connector.ErrorClassificationOf(err); !ok || classification != connector.ErrorPermanent {
		t.Fatalf("classification=%q ok=%v err=%v", classification, ok, err)
	}
}

func validConnection() connector.Connection {
	return connector.Connection{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Config: map[string]any{"team_id": "TEAMID1234", "key_id": "KEYID12345", "bundle_id": "com.domainry.facility", "environment": "sandbox", "timeout_seconds": 15, "ttl_seconds": 3600}, SecretRefs: map[string]string{"private_key": "secret:apns"}}
}

func testPrivateKey(t *testing.T) string {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	keyBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes}))
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
	if header["alg"] != "ES256" || header["kid"] != "KEYID12345" || claims["iss"] != "TEAMID1234" || claims["iat"] != float64(1792917000) {
		t.Fatalf("header=%v claims=%v", header, claims)
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(signature) != 64 {
		t.Fatalf("signature length=%d err=%v", len(signature), err)
	}
}
