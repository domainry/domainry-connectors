package nominatim

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
)

type recordingTransport struct {
	requests []connector.HTTPRequest
	handle   func(connector.HTTPRequest) (connector.HTTPResponse, error)
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, request)
	return t.handle(request)
}

func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestProviderUsesRuntimeTransportForTypedOperationsAndProbe(t *testing.T) {
	transport := &recordingTransport{handle: func(request connector.HTTPRequest) (connector.HTTPResponse, error) {
		switch {
		case strings.HasSuffix(request.URL, "/status?format=json"):
			return connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"status":0}`)}, nil
		case strings.Contains(request.URL, "/search?"):
			return connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`[{"place_id":1,"display_name":"Paris"}]`)}, nil
		case strings.Contains(request.URL, "/reverse?"):
			return connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"place_id":1,"display_name":"Paris"}`)}, nil
		default:
			t.Fatalf("unexpected request URL %q", request.URL)
			return connector.HTTPResponse{}, nil
		}
	}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	connection := connector.Connection{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Config: map[string]any{
		"base_url": "http://127.0.0.1:8080/root", "user_agent": "domainry-customer/1.0", "locale": "fr-FR",
	}}
	if validator := adapter.(connector.ConfigValidator); validator.ValidateConfig(connection) != nil {
		t.Fatal(validator.ValidateConfig(connection))
	}
	probe, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Connection: connection})
	if err != nil || !probe.Connected {
		t.Fatalf("probe=%+v error=%v", probe, err)
	}
	searchPayload, _ := json.Marshal(SearchAddressInput{Query: " Paris ", Limit: 3, CountryCodes: " FR,DE "})
	search, err := adapter.Call(t.Context(), connector.CallRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SearchAddress.Key,
		ContractSHA256: SearchAddress.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Payload: searchPayload,
	})
	if err != nil || !strings.Contains(string(search.Payload), `"display_name":"Paris"`) {
		t.Fatalf("search=%s error=%v", search.Payload, err)
	}
	reversePayload, _ := json.Marshal(ReverseGeocodeInput{Latitude: "48.8", Longitude: "2.3", Zoom: 12})
	reverse, err := adapter.Call(t.Context(), connector.CallRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ReverseGeocode.Key,
		ContractSHA256: ReverseGeocode.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Payload: reversePayload,
	})
	if err != nil || !strings.Contains(string(reverse.Payload), `"display_name":"Paris"`) {
		t.Fatalf("reverse=%s error=%v", reverse.Payload, err)
	}
	if len(transport.requests) != 3 || transport.requests[1].Headers["User-Agent"][0] != "domainry-customer/1.0" || transport.requests[1].Headers["Accept-Language"][0] != "fr-FR" {
		t.Fatalf("requests=%+v", transport.requests)
	}
	if got := adapter.Descriptor(); got.ConnectorKey != ConnectorKey || got.ProviderKey != ProviderKey || len(got.Operations) != 2 {
		t.Fatalf("descriptor=%+v", got)
	}
}

func TestProviderRejectsUnsafeConfigurationAndClassifiesHTTPFailures(t *testing.T) {
	transport := &recordingTransport{handle: func(connector.HTTPRequest) (connector.HTTPResponse, error) {
		return connector.HTTPResponse{StatusCode: http.StatusTooManyRequests, Body: []byte(`{}`)}, nil
	}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	validator := adapter.(connector.ConfigValidator)
	for _, config := range []map[string]any{
		{"base_url": "http://maps.example.test", "user_agent": "custom"},
		{"base_url": "https://nominatim.openstreetmap.org", "user_agent": "custom"},
		{"base_url": "https://maps.example.test", "user_agent": "Go-http-client/1.1"},
	} {
		if err := validator.ValidateConfig(connector.Connection{Config: config}); err == nil {
			t.Fatalf("unsafe config accepted: %+v", config)
		}
	}
	payload, _ := json.Marshal(SearchAddressInput{Query: "Paris"})
	_, err = adapter.Call(t.Context(), connector.CallRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SearchAddress.Key,
		ContractSHA256: SearchAddress.ContractSHA256, Mode: connector.ModeCall,
		Connection: connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080", "user_agent": "test"}}, Payload: payload,
	})
	classification, ok := connector.ErrorClassificationOf(err)
	code, coded := connector.ProviderErrorCodeOf(err)
	if !ok || classification != connector.ErrorRetryable || !coded || code != "nominatim.http_429" {
		t.Fatalf("classification=%q/%v code=%q/%v error=%v", classification, ok, code, coded, err)
	}
}
