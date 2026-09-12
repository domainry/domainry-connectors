package ups

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
)

type recordingTransport struct {
	requests []connector.HTTPRequest
	respond  func(connector.HTTPRequest) (connector.HTTPResponse, error)
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, request)
	if t.respond != nil {
		return t.respond(request)
	}
	return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"RateResponse":{}}`)}, nil
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestOperationsUseRuntimeHTTPAndTypedContracts(t *testing.T) {
	transport := &recordingTransport{respond: func(request connector.HTTPRequest) (connector.HTTPResponse, error) {
		if strings.Contains(request.URL, "/shipments/") {
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"ShipmentResponse":{"ShipmentResults":{"ShipmentIdentificationNumber":"1Z001"}}}`)}, nil
		}
		return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"response":{}}`)}, nil
	}}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	tests := []struct {
		operation, hash, path string
		input                 any
	}{
		{TestConnection.Key, TestConnection.ContractSHA256, "/api/rating/v2409/Rate", struct{}{}},
		{QuoteRates.Key, QuoteRates.ContractSHA256, "/api/rating/v2409/Rate", ProviderInput{Input: map[string]any{"RateRequest": map[string]any{}}}},
		{CreateShipment.Key, CreateShipment.ContractSHA256, "/api/shipments/v2409/ship", ProviderInput{Input: map[string]any{"ShipmentRequest": map[string]any{}}}},
		{TrackShipment.Key, TrackShipment.ContractSHA256, "/api/track/v1/details/1Z%2F001", TrackShipmentInput{TrackingNumber: "1Z/001", Locale: "en_US", ReturnSignature: "true"}},
	}
	for _, test := range tests {
		payload, _ := json.Marshal(test.input)
		result, callErr := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: test.operation, ContractSHA256: test.hash, Mode: connector.ModeCall, RequestRef: "tx-1", Connection: validConnection(), Secrets: map[string]string{"access_token": "runtime-access"}, Payload: payload})
		if callErr != nil {
			t.Fatalf("operation=%s result=%+v err=%v", test.operation, result, callErr)
		}
		request := transport.requests[len(transport.requests)-1]
		if !strings.Contains(request.URL, test.path) || request.SecretHeaders["Authorization"][0] != "Bearer runtime-access" || request.Headers["transId"][0] != "tx-1" || request.MaxResponseBytes != responseLimit {
			t.Fatalf("request=%+v", request)
		}
		encoded, _ := json.Marshal(request)
		if strings.Contains(string(encoded), "runtime-access") {
			t.Fatalf("serialized request leaks token: %s", encoded)
		}
	}
}

func TestUnauthorizedRefreshUsesBasicAuthAndReturnsSecretUpdate(t *testing.T) {
	apiCalls := 0
	transport := &recordingTransport{respond: func(request connector.HTTPRequest) (connector.HTTPResponse, error) {
		if strings.HasSuffix(request.URL, "/oauth/token") {
			if !strings.HasPrefix(request.SecretHeaders["Authorization"][0], "Basic ") || request.SecretForm != nil {
				t.Fatalf("token request=%+v", request)
			}
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"access_token":"fresh"}`)}, nil
		}
		apiCalls++
		if request.SecretHeaders["Authorization"][0] == "Bearer stale" {
			return connector.HTTPResponse{StatusCode: 401, Body: []byte(`{"response":{"errors":[{"code":"250002"}]}}`)}, nil
		}
		return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"RateResponse":{}}`)}, nil
	}}
	adapter, _ := New(transport)
	payload, _ := json.Marshal(ProviderInput{Input: map[string]any{"RateRequest": map[string]any{}}})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: QuoteRates.Key, ContractSHA256: QuoteRates.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_token": "stale", "client_id": "client", "client_secret": "secret"}, Payload: payload})
	if err != nil || apiCalls != 2 || result.SecretUpdates["access_token"] != "fresh" {
		t.Fatalf("result=%+v calls=%d err=%v", result, apiCalls, err)
	}
}

func TestConnectionRetainsRotationWhenFollowupFails(t *testing.T) {
	transport := &recordingTransport{respond: func(request connector.HTTPRequest) (connector.HTTPResponse, error) {
		if strings.HasSuffix(request.URL, "/oauth/token") {
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"access_token":"fresh"}`)}, nil
		}
		if request.SecretHeaders["Authorization"][0] == "Bearer stale" {
			return connector.HTTPResponse{StatusCode: 401}, nil
		}
		return connector.HTTPResponse{StatusCode: 503}, nil
	}}
	adapter, _ := New(transport)
	result, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: validConnection(), Secrets: map[string]string{"access_token": "stale", "client_id": "client", "client_secret": "secret"}})
	classification, ok := connector.ErrorClassificationOf(err)
	if !ok || classification != connector.ErrorRetryable || result.Connected || result.SecretUpdates["access_token"] != "fresh" || len(transport.requests) != 3 {
		t.Fatalf("result=%+v requests=%d classification=%q err=%v", result, len(transport.requests), classification, err)
	}
}

func TestValidationInputsAndFailureClassification(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	validator := adapter.(connector.ConfigValidator)
	for _, config := range []map[string]any{{"base_url": "http://remote.example", "token_url": defaultTokenURL}, {"base_url": defaultBaseURL, "token_url": "ftp://token.example"}, {"base_url": defaultBaseURL, "token_url": defaultTokenURL, "rating_version": "../../v1"}, {"base_url": defaultBaseURL, "token_url": defaultTokenURL, "transaction_source": "bad\nheader"}, {"base_url": defaultBaseURL, "token_url": defaultTokenURL, "timeout_seconds": 121}} {
		if validator.ValidateConfig(connector.Connection{Config: config}) == nil {
			t.Fatalf("accepted config=%v", config)
		}
	}
	tests := []struct {
		name, operation, hash string
		input                 any
		response              connector.HTTPResponse
		transportErr          error
		class                 connector.ErrorClassification
	}{
		{"write network", CreateShipment.Key, CreateShipment.ContractSHA256, ProviderInput{Input: map[string]any{"shipment": true}}, connector.HTTPResponse{}, errors.New("reset"), connector.ErrorUncertain},
		{"write server", CreateShipment.Key, CreateShipment.ContractSHA256, ProviderInput{Input: map[string]any{"shipment": true}}, connector.HTTPResponse{StatusCode: 503, Body: []byte(`{}`)}, nil, connector.ErrorUncertain},
		{"read network", TrackShipment.Key, TrackShipment.ContractSHA256, TrackShipmentInput{TrackingNumber: "1Z1"}, connector.HTTPResponse{}, errors.New("reset"), connector.ErrorRetryable},
		{"rate limited", CreateShipment.Key, CreateShipment.ContractSHA256, ProviderInput{Input: map[string]any{"shipment": true}}, connector.HTTPResponse{StatusCode: 429, Body: []byte(`{}`)}, nil, connector.ErrorRetryable},
		{"rejected", QuoteRates.Key, QuoteRates.ContractSHA256, ProviderInput{Input: map[string]any{"rate": true}}, connector.HTTPResponse{StatusCode: 400, Body: []byte(`{"response":{"errors":[{"code":"111210"}]}}`)}, nil, connector.ErrorPermanent},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transport := &recordingTransport{respond: func(connector.HTTPRequest) (connector.HTTPResponse, error) { return test.response, test.transportErr }}
			adapter, _ := New(transport)
			payload, _ := json.Marshal(test.input)
			_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: test.operation, ContractSHA256: test.hash, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_token": "token"}, Payload: payload})
			classification, ok := connector.ErrorClassificationOf(err)
			if !ok || classification != test.class {
				t.Fatalf("classification=%q err=%v", classification, err)
			}
		})
	}
}

func validConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "http://127.0.0.1:8080", "token_url": "http://127.0.0.1:8080/oauth/token", "rating_version": "v2409", "shipping_version": "v2409", "transaction_source": "domainry-runtime", "test_rate_request": map[string]any{"RateRequest": map[string]any{}}, "timeout_seconds": 30}, SecretRefs: map[string]string{"access_token": "secret:access", "client_id": "secret:client", "client_secret": "secret:secret"}}
}
