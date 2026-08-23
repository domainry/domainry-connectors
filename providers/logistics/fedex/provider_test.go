package fedex

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
)

type recordingTransport struct {
	requests []connector.HTTPRequest
	respond  func(connector.HTTPRequest) (connector.HTTPResponse, error)
}

func (transport *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	transport.requests = append(transport.requests, request)
	if transport.respond != nil {
		return transport.respond(request)
	}
	return connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"output":{"transactionShipments":[{"masterTrackingNumber":"FDX001"}],"pickupConfirmationCode":"PU001"}}`)}, nil
}

func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestOperationsUseRuntimeHTTPAndTypedContracts(t *testing.T) {
	transport := &recordingTransport{}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	operations := []struct {
		key, hash, path string
		input           any
	}{{TestConnection.Key, TestConnection.ContractSHA256, "/rate/v1/rates/quotes", struct{}{}}, {QuoteRates.Key, QuoteRates.ContractSHA256, "/rate/v1/rates/quotes", ProviderInput{Input: map[string]any{"accountNumber": map[string]any{}}}}, {CreateShipment.Key, CreateShipment.ContractSHA256, "/ship/v1/shipments", ProviderInput{Input: map[string]any{"requestedShipment": map[string]any{}}}}, {TrackShipment.Key, TrackShipment.ContractSHA256, "/track/v1/trackingnumbers", TrackShipmentInput{TrackingNumber: "FDX001", IncludeDetailedScans: "false"}}, {CreatePickup.Key, CreatePickup.ContractSHA256, "/pickup/v1/pickups", ProviderInput{Input: map[string]any{"associatedAccountNumber": map[string]any{}}}}, {CancelPickup.Key, CancelPickup.ContractSHA256, "/pickup/v1/pickups/cancel", CancelPickupInput{PickupID: "PU001"}}}
	for _, operation := range operations {
		payload, _ := json.Marshal(operation.input)
		result, callErr := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: operation.key, ContractSHA256: operation.hash, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_token": "runtime-access", "client_id": "runtime-client", "client_secret": "runtime-secret"}, Payload: payload})
		if callErr != nil || result.ResponseRef != "fedex:FDX001" {
			t.Fatalf("operation=%s result=%+v err=%v", operation.key, result, callErr)
		}
		request := transport.requests[len(transport.requests)-1]
		if !strings.HasSuffix(request.URL, operation.path) || request.Headers["Authorization"] != nil || request.SecretHeaders["Authorization"][0] != "Bearer runtime-access" || request.MaxResponseBytes != responseLimit {
			t.Fatalf("request=%+v", request)
		}
		encoded, _ := json.Marshal(request)
		if strings.Contains(string(encoded), "runtime-access") || strings.Contains(string(encoded), "runtime-secret") {
			t.Fatalf("serialized request leaks secrets: %s", encoded)
		}
	}
	var tracking map[string]any
	_ = json.Unmarshal(transport.requests[3].Body, &tracking)
	if tracking["includeDetailedScans"] != false {
		t.Fatalf("tracking body=%v", tracking)
	}
}

func TestUnauthorizedRefreshesWithClientCredentialsAndReturnsSecretUpdate(t *testing.T) {
	apiCalls := 0
	transport := &recordingTransport{respond: func(request connector.HTTPRequest) (connector.HTTPResponse, error) {
		if strings.HasSuffix(request.URL, "/oauth/token") {
			if request.SecretForm["client_id"] != "client" || request.SecretForm["client_secret"] != "secret" || strings.Contains(string(request.Body), "client_id=") || strings.Contains(string(request.Body), "client_secret=") {
				t.Fatalf("token request=%+v", request)
			}
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"access_token":"fresh"}`)}, nil
		}
		apiCalls++
		if request.SecretHeaders["Authorization"][0] == "Bearer stale" {
			return connector.HTTPResponse{StatusCode: 401, Body: []byte(`{"errors":[{"code":"NOT.AUTHORIZED.ERROR"}]}`)}, nil
		}
		return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"output":{"transactionShipments":[{"masterTrackingNumber":"FDX002"}]}}`)}, nil
	}}
	adapter, _ := New(transport)
	payload, _ := json.Marshal(ProviderInput{Input: map[string]any{"accountNumber": map[string]any{}}})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: QuoteRates.Key, ContractSHA256: QuoteRates.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_token": "stale", "client_id": "client", "client_secret": "secret"}, Payload: payload})
	if err != nil || apiCalls != 2 || result.SecretUpdates["access_token"] != "fresh" || result.ResponseRef != "fedex:FDX002" {
		t.Fatalf("result=%+v calls=%d err=%v", result, apiCalls, err)
	}
}

func TestValidationAndRequiredInputsFailClosed(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	validator := adapter.(connector.ConfigValidator)
	for _, config := range []map[string]any{{"base_url": "http://remote.example", "token_url": defaultTokenURL}, {"base_url": defaultBaseURL, "token_url": "ftp://token.example"}, {"base_url": "https://user:password@api.example", "token_url": defaultTokenURL}, {"base_url": defaultBaseURL, "token_url": defaultTokenURL, "timeout_seconds": 121}} {
		if err := validator.ValidateConfig(connector.Connection{Config: config}); err == nil {
			t.Fatalf("accepted config=%v", config)
		}
	}
	badInputs := []struct {
		operation, hash string
		input           any
	}{{QuoteRates.Key, QuoteRates.ContractSHA256, ProviderInput{}}, {TrackShipment.Key, TrackShipment.ContractSHA256, TrackShipmentInput{}}, {CancelPickup.Key, CancelPickup.ContractSHA256, CancelPickupInput{}}}
	for _, test := range badInputs {
		payload, _ := json.Marshal(test.input)
		_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: test.operation, ContractSHA256: test.hash, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_token": "token", "client_id": "client", "client_secret": "secret"}, Payload: payload})
		if err == nil {
			t.Fatalf("accepted input for %s", test.operation)
		}
	}
}

func TestWriteFailuresAreUncertainAndReadsRetryable(t *testing.T) {
	tests := []struct {
		name, operation, hash string
		input                 any
		response              connector.HTTPResponse
		transportErr          error
		classification        connector.ErrorClassification
	}{{"write network", CreateShipment.Key, CreateShipment.ContractSHA256, ProviderInput{Input: map[string]any{"shipment": true}}, connector.HTTPResponse{}, errors.New("reset"), connector.ErrorUncertain}, {"write server", CancelPickup.Key, CancelPickup.ContractSHA256, CancelPickupInput{PickupID: "PU1"}, connector.HTTPResponse{StatusCode: 503}, nil, connector.ErrorUncertain}, {"read network", TrackShipment.Key, TrackShipment.ContractSHA256, TrackShipmentInput{TrackingNumber: "FDX1"}, connector.HTTPResponse{}, errors.New("reset"), connector.ErrorRetryable}, {"rate limited", CreatePickup.Key, CreatePickup.ContractSHA256, ProviderInput{Input: map[string]any{"pickup": true}}, connector.HTTPResponse{StatusCode: 429}, nil, connector.ErrorRetryable}, {"rejected", QuoteRates.Key, QuoteRates.ContractSHA256, ProviderInput{Input: map[string]any{"rate": true}}, connector.HTTPResponse{StatusCode: 400, Body: []byte(`{"errors":[{"code":"SHIPMENT.VALIDATION.ERROR"}]}`)}, nil, connector.ErrorPermanent}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transport := &recordingTransport{respond: func(connector.HTTPRequest) (connector.HTTPResponse, error) { return test.response, test.transportErr }}
			adapter, _ := New(transport)
			payload, _ := json.Marshal(test.input)
			_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: test.operation, ContractSHA256: test.hash, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_token": "token", "client_id": "client", "client_secret": "secret"}, Payload: payload})
			classification, ok := connector.ErrorClassificationOf(err)
			if !ok || classification != test.classification {
				t.Fatalf("classification=%q err=%v", classification, err)
			}
		})
	}
}

func validConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "http://127.0.0.1:8080", "token_url": "http://127.0.0.1:8080/oauth/token", "test_rate_request": map[string]any{"accountNumber": map[string]any{}}, "timeout_seconds": 30}, SecretRefs: map[string]string{"access_token": "secret:access", "client_id": "secret:client", "client_secret": "secret:secret"}}
}
