package dhl

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

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
	return connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"shipmentTrackingNumber":"JD001","products":[]}`)}, nil
}

func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestOperationsUseRuntimeHTTPAndSecretHeaders(t *testing.T) {
	transport := &recordingTransport{}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	adapter.(*provider).now = func() time.Time { return time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC) }
	operations := []struct {
		key, hash, method, path string
		input                   any
	}{{TestConnection.Key, TestConnection.ContractSHA256, http.MethodGet, "/rates", struct{}{}}, {QuoteRates.Key, QuoteRates.ContractSHA256, http.MethodPost, "/rates", ProviderInput{Input: map[string]any{"customerDetails": map[string]any{}}}}, {CreateShipment.Key, CreateShipment.ContractSHA256, http.MethodPost, "/shipments", ProviderInput{Input: map[string]any{"plannedShippingDateAndTime": "2026-08-25"}}}, {TrackShipment.Key, TrackShipment.ContractSHA256, http.MethodGet, "/shipments/JD%2F1/tracking", TrackShipmentInput{TrackingNumber: "JD/1", TrackingView: "all", LevelOfDetail: "all"}}, {CreatePickup.Key, CreatePickup.ContractSHA256, http.MethodPost, "/pickups", ProviderInput{Input: map[string]any{"plannedPickupDateAndTime": "2026-08-25"}}}, {UpdatePickup.Key, UpdatePickup.ContractSHA256, http.MethodPatch, "/pickups/PU%2F1", UpdatePickupInput{PickupID: "PU/1", Input: map[string]any{"remark": "updated"}}}, {CancelPickup.Key, CancelPickup.ContractSHA256, http.MethodDelete, "/pickups/PU%2F1", CancelPickupInput{PickupID: "PU/1", RequestorName: "Runtime"}}}
	for _, operation := range operations {
		payload, _ := json.Marshal(operation.input)
		result, callErr := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: operation.key, ContractSHA256: operation.hash, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"api_key": "runtime-key", "api_secret": "runtime-secret"}, Payload: payload})
		if callErr != nil || result.ResponseRef != "dhl:JD001" {
			t.Fatalf("operation=%s result=%+v err=%v", operation.key, result, callErr)
		}
		request := transport.requests[len(transport.requests)-1]
		if request.Method != operation.method || !strings.Contains(request.URL, operation.path) || request.Headers["Authorization"] != nil || len(request.SecretHeaders["Authorization"]) != 1 {
			t.Fatalf("request=%+v", request)
		}
		encoded, _ := json.Marshal(request)
		if strings.Contains(string(encoded), "runtime-key") || strings.Contains(string(encoded), "runtime-secret") {
			t.Fatalf("serialized request leaks credentials: %s", encoded)
		}
	}
	if !strings.Contains(transport.requests[0].URL, "plannedShippingDate=2026-08-25") || !strings.Contains(transport.requests[3].URL, "trackingView=all") || !strings.Contains(transport.requests[6].URL, "requestorName=Runtime") {
		t.Fatalf("queries=%s %s %s", transport.requests[0].URL, transport.requests[3].URL, transport.requests[6].URL)
	}
}

func TestValidationAndRequiredInputsFailClosed(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	validator := adapter.(connector.ConfigValidator)
	for _, baseURL := range []string{"http://remote.example", "ftp://api.example", "https://user:password@api.example", "https://api.example?token=x"} {
		connection := validConnection()
		connection.Config["base_url"] = baseURL
		if err := validator.ValidateConfig(connection); err == nil {
			t.Fatalf("accepted base URL=%q", baseURL)
		}
	}
	connection := validConnection()
	connection.Config["timeout_seconds"] = 121
	if err := validator.ValidateConfig(connection); err == nil {
		t.Fatal("accepted oversized timeout")
	}
	badInputs := []struct {
		operation, hash string
		input           any
	}{{QuoteRates.Key, QuoteRates.ContractSHA256, ProviderInput{}}, {TrackShipment.Key, TrackShipment.ContractSHA256, TrackShipmentInput{}}, {UpdatePickup.Key, UpdatePickup.ContractSHA256, UpdatePickupInput{PickupID: "PU1"}}, {CancelPickup.Key, CancelPickup.ContractSHA256, CancelPickupInput{}}}
	for _, test := range badInputs {
		payload, _ := json.Marshal(test.input)
		_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: test.operation, ContractSHA256: test.hash, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"api_key": "key", "api_secret": "secret"}, Payload: payload})
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
	}{{"write network", CreateShipment.Key, CreateShipment.ContractSHA256, ProviderInput{Input: map[string]any{"shipment": true}}, connector.HTTPResponse{}, errors.New("reset"), connector.ErrorUncertain}, {"write server", CancelPickup.Key, CancelPickup.ContractSHA256, CancelPickupInput{PickupID: "PU1"}, connector.HTTPResponse{StatusCode: 503, Body: []byte(`{"message":"down"}`)}, nil, connector.ErrorUncertain}, {"read network", TrackShipment.Key, TrackShipment.ContractSHA256, TrackShipmentInput{TrackingNumber: "JD1"}, connector.HTTPResponse{}, errors.New("reset"), connector.ErrorRetryable}, {"rate limited", CreatePickup.Key, CreatePickup.ContractSHA256, ProviderInput{Input: map[string]any{"pickup": true}}, connector.HTTPResponse{StatusCode: 429}, nil, connector.ErrorRetryable}, {"rejected", QuoteRates.Key, QuoteRates.ContractSHA256, ProviderInput{Input: map[string]any{"route": true}}, connector.HTTPResponse{StatusCode: 400, Body: []byte(`{"title":"Bad Request"}`)}, nil, connector.ErrorPermanent}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transport := &recordingTransport{respond: func(connector.HTTPRequest) (connector.HTTPResponse, error) { return test.response, test.transportErr }}
			adapter, _ := New(transport)
			payload, _ := json.Marshal(test.input)
			_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: test.operation, ContractSHA256: test.hash, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"api_key": "key", "api_secret": "secret"}, Payload: payload})
			classification, ok := connector.ErrorClassificationOf(err)
			if !ok || classification != test.classification {
				t.Fatalf("classification=%q err=%v", classification, err)
			}
		})
	}
}

func validConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "http://127.0.0.1:8080", "origin_country_code": "CN", "origin_postal_code": "200000", "origin_city_name": "Shanghai", "destination_country_code": "US", "destination_postal_code": "10001", "destination_city_name": "New York", "timeout_seconds": 30}, SecretRefs: map[string]string{"api_key": "secret:key", "api_secret": "secret:secret"}}
}
