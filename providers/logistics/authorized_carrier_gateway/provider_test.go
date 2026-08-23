package authorizedcarriergateway

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

func TestProviderUsesResolvedSecretAndReturnsProviderOwnedEvidence(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"response":{"queryId":"q-1","quotes":[{"carrier":"MSC","total":"100"}]}}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	adapter.(*provider).now = func() time.Time { return time.Date(2026, 8, 23, 2, 3, 4, 5, time.UTC) }
	connection := connector.Connection{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Config: map[string]any{
		"base_url": "http://localhost:8080", "quote_path": defaultQuotePath, "provider_identity": "customer_carrier_gateway", "authorization_ref": "contract:customer:2026",
	}}
	payload, _ := json.Marshal(QuoteRatesInput{Input: RouteInput{Origin: "CNSHA", Destination: "BRSSZ", ContainerType: "40HQ", ContainerCount: 1, Currency: "brl"}})
	result, err := adapter.Call(t.Context(), connector.CallRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: QuoteRates.Key, ContractSHA256: QuoteRates.ContractSHA256,
		Mode: connector.ModeCall, Connection: connection, Secrets: map[string]string{"api_token": "secret-token"}, Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	var output QuoteRatesOutput
	if err := json.Unmarshal(result.Payload, &output); err != nil {
		t.Fatal(err)
	}
	if output.Products["provider"] != "customer_carrier_gateway" || output.Products["authorization_ref"] != "contract:customer:2026" || output.Products["trust_mode"] != "production_authorized" || output.Products["queried_at"] != "2026-08-23T02:03:04.000000005Z" {
		t.Fatalf("evidence=%+v", output.Products)
	}
	if len(transport.requests) != 1 || transport.requests[0].Headers["Authorization"][0] != "Bearer secret-token" || transport.requests[0].URL != "http://localhost:8080"+defaultQuotePath {
		t.Fatalf("requests=%+v", transport.requests)
	}
	if strings.Contains(string(transport.requests[0].Body), "secret-token") || !strings.Contains(string(transport.requests[0].Body), `"container_type":"40hq"`) {
		t.Fatalf("body=%s", transport.requests[0].Body)
	}
	probeConnection := connection
	probeConnection.Config["test_origin"], probeConnection.Config["test_destination"] = "CNSHA", "BRSSZ"
	probeConnection.Config["test_container_type"], probeConnection.Config["test_currency"] = "40hq", "BRL"
	probe, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: probeConnection, Secrets: map[string]string{"api_token": "secret-token"}})
	if err != nil || !probe.Connected {
		t.Fatalf("probe=%+v error=%v", probe, err)
	}
	if got := adapter.Descriptor(); len(got.Operations) != 1 || len(got.SecretFields) != 1 || got.SecretFields[0].Key != "api_token" {
		t.Fatalf("descriptor=%+v", got)
	}
}

func TestProviderRejectsInvalidConfigInputSecretAndRemoteContract(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"unexpected":true}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	validator := adapter.(connector.ConfigValidator)
	for _, config := range []map[string]any{
		{"base_url": "http://remote.example", "provider_identity": "carrier", "authorization_ref": "contract"},
		{"base_url": "https://gateway.example", "authorization_ref": "contract"},
		{"base_url": "https://gateway.example", "provider_identity": "carrier"},
		{"base_url": "https://gateway.example", "provider_identity": "carrier", "authorization_ref": "contract", "quote_path": "/v1/../quotes"},
	} {
		if err := validator.ValidateConfig(connector.Connection{Config: config}); err == nil {
			t.Fatalf("invalid config accepted: %+v", config)
		}
	}
	connection := connector.Connection{Config: map[string]any{"base_url": "http://127.0.0.1:8080", "provider_identity": "carrier", "authorization_ref": "contract"}}
	badPayload, _ := json.Marshal(QuoteRatesInput{Input: RouteInput{Origin: "CNSHA", ContainerType: "40hq", ContainerCount: 1, Currency: "BRL"}})
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: QuoteRates.Key, ContractSHA256: QuoteRates.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Secrets: map[string]string{"api_token": "token"}, Payload: badPayload})
	if code, _ := connector.ProviderErrorCodeOf(err); code != "authorized_carrier_gateway.route_required" {
		t.Fatalf("route error=%v", err)
	}
	validPayload, _ := json.Marshal(QuoteRatesInput{Input: RouteInput{Origin: "CNSHA", Destination: "BRSSZ", ContainerType: "40hq", ContainerCount: 1, Currency: "BRL"}})
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: QuoteRates.Key, ContractSHA256: QuoteRates.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Payload: validPayload})
	if code, _ := connector.ProviderErrorCodeOf(err); code != "authorized_carrier_gateway.api_token_required" {
		t.Fatalf("secret error=%v", err)
	}
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: QuoteRates.Key, ContractSHA256: QuoteRates.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Secrets: map[string]string{"api_token": "token"}, Payload: validPayload})
	if code, _ := connector.ProviderErrorCodeOf(err); code != "authorized_carrier_gateway.response_contract_invalid" {
		t.Fatalf("contract error=%v", err)
	}
	transport.response = connector.HTTPResponse{StatusCode: http.StatusTooManyRequests}
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: QuoteRates.Key, ContractSHA256: QuoteRates.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Secrets: map[string]string{"api_token": "token"}, Payload: validPayload})
	classification, ok := connector.ErrorClassificationOf(err)
	if !ok || classification != connector.ErrorRetryable {
		t.Fatalf("rate limit classification=%q/%v error=%v", classification, ok, err)
	}
}
