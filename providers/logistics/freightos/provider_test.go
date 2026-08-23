package freightos

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

type recordingTransport struct {
	requests []connector.HTTPRequest
	response connector.HTTPResponse
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, request)
	return t.response, nil
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestProviderKeepsAPIKeyOutOfURL(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Headers: map[string][]string{"Date": {"Sun, 23 Aug 2026 00:00:00 GMT"}}, Body: []byte(`{"response":{"quotes":[{"id":"Q-1"}]}}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	adapter.(*provider).now = func() time.Time { return time.Date(2026, 8, 23, 2, 3, 4, 5, time.UTC) }
	connection := connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080", "authorization_ref": "contract:2026"}}
	payload, _ := json.Marshal(QuoteRatesInput{Input: RouteInput{Origin: "CNSHA", Destination: "BRSSZ", ContainerType: "40HQ", ContainerCount: 1, WeightKG: "15000", Currency: "brl"}})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: QuoteRates.Key, ContractSHA256: QuoteRates.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Secrets: map[string]string{"api_key": "secret-key"}, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	if len(transport.requests) != 1 || strings.Contains(transport.requests[0].URL, "secret-key") || transport.requests[0].SecretQuery["apiKey"] != "secret-key" {
		t.Fatalf("request=%+v", transport.requests)
	}
	if !strings.Contains(transport.requests[0].URL, "loadtype=container40HC") || !strings.Contains(transport.requests[0].URL, "weight=15000kg") {
		t.Fatalf("url=%s", transport.requests[0].URL)
	}
	var output QuoteRatesOutput
	if err := json.Unmarshal(result.Payload, &output); err != nil {
		t.Fatal(err)
	}
	if output.Products["trust_mode"] != "production_authorized" || output.Products["authorization_ref"] != "contract:2026" {
		t.Fatalf("products=%+v", output.Products)
	}
}

func TestProviderRejectsPublicSiteInvalidInputAndMissingSecret(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"ok":true}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	validator := adapter.(connector.ConfigValidator)
	if err := validator.ValidateConfig(connector.Connection{Config: map[string]any{"base_url": "https://ship.freightos.com", "authorization_ref": "contract"}}); err == nil {
		t.Fatal("public estimator was accepted")
	}
	connection := connector.Connection{Config: map[string]any{"base_url": "http://127.0.0.1:8080", "authorization_ref": "contract"}}
	payload, _ := json.Marshal(QuoteRatesInput{Input: RouteInput{Origin: "CNSHA", Destination: "BRSSZ", ContainerType: "pallet", ContainerCount: 1, Currency: "BRL"}})
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: QuoteRates.Key, ContractSHA256: QuoteRates.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Secrets: map[string]string{"api_key": "key"}, Payload: payload})
	if code, _ := connector.ProviderErrorCodeOf(err); code != "freightos.container_type_invalid" {
		t.Fatalf("error=%v", err)
	}
	payload, _ = json.Marshal(QuoteRatesInput{Input: RouteInput{Origin: "CNSHA", Destination: "BRSSZ", ContainerType: "20gp", ContainerCount: 1, Currency: "USD"}})
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: QuoteRates.Key, ContractSHA256: QuoteRates.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Payload: payload})
	if code, _ := connector.ProviderErrorCodeOf(err); code != "freightos.api_key_required" {
		t.Fatalf("error=%v", err)
	}
}

func TestConnectionUsesConfiguredProbe(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"ok":true}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	connection := connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080", "authorization_ref": "contract", "test_origin": "CNSHA", "test_destination": "BRSSZ", "test_container_type": "20gp", "test_weight_kg": "12000", "test_currency": "USD"}}
	result, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: connection, Secrets: map[string]string{"api_key": "key"}})
	if err != nil || !result.Connected {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	if got := adapter.Descriptor(); len(got.Operations) != 1 || got.Operations[0].Key != "quote_rates" || len(got.SecretFields) != 1 {
		t.Fatalf("descriptor=%+v", got)
	}
}
