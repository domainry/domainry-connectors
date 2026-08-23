package holidaysjp

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
	calls    int
	response connector.HTTPResponse
	err      error
}

func (t *recordingTransport) RoundTripHTTP(context.Context, connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.calls++
	return t.response, t.err
}

func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestProviderPreservesTypedFactorContractAndWorkspaceCache(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string][]string{"Last-Modified": {"Wed, 11 Feb 2026 00:00:00 GMT"}},
		Body:       []byte(`{"2026-02-11":"建国記念の日","2026-02-23":"天皇誕生日"}`),
	}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	connection := connector.Connection{Key: "calendar-primary", ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Config: map[string]any{
		"base_url": "http://127.0.0.1:8080/holidays.json", "cache_ttl_seconds": 3600,
	}}
	payload, _ := json.Marshal(ListFactorsInput{Locale: "ja-JP", Region: "JP", StartDate: "2026-02-01", EndDate: "2026-02-28"})
	request := connector.CallRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListFactors.Key,
		ContractSHA256: ListFactors.ContractSHA256, Mode: connector.ModeCall, Connection: connection,
		Payload: payload, Principal: connector.Principal{WorkspaceID: "workspace-a"},
	}
	first, err := adapter.Call(t.Context(), request)
	if err != nil || !strings.Contains(string(first.Payload), `"provider_status":"live"`) || !strings.Contains(string(first.Payload), `"name":"建国記念の日"`) {
		t.Fatalf("first=%s error=%v", first.Payload, err)
	}
	second, err := adapter.Call(t.Context(), request)
	if err != nil || !strings.Contains(string(second.Payload), `"provider_status":"cache_hit"`) || !strings.HasPrefix(second.ResponseRef, "cache:sha256:") {
		t.Fatalf("second=%s ref=%q error=%v", second.Payload, second.ResponseRef, err)
	}
	request.Principal.WorkspaceID = "workspace-b"
	if _, err := adapter.Call(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if transport.calls != 2 {
		t.Fatalf("transport calls=%d, want one per workspace", transport.calls)
	}
	probe, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: connection, Principal: connector.Principal{WorkspaceID: "workspace-a"}})
	if err != nil || !probe.Connected {
		t.Fatalf("probe=%+v error=%v", probe, err)
	}
	if got := adapter.Descriptor(); got.StartupActivation != connector.StartupActivationDefaultSafe || len(got.Operations) != 1 {
		t.Fatalf("descriptor=%+v", got)
	}
}

func TestProviderClassifiesValidationAndRemoteFailures(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusTooManyRequests}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.(connector.ConfigValidator).ValidateConfig(connector.Connection{Config: map[string]any{"base_url": "http://calendar.example.test"}}); err == nil {
		t.Fatal("public HTTP endpoint accepted")
	}
	for _, input := range []ListFactorsInput{
		{Locale: "", Region: "JP", StartDate: "2026-01-01", EndDate: "2026-01-02"},
		{Locale: "ja-JP", Region: "US", StartDate: "2026-01-01", EndDate: "2026-01-02"},
		{Locale: "ja-JP", Region: "JP", StartDate: "2026-02-01", EndDate: "2026-01-01"},
	} {
		payload, _ := json.Marshal(input)
		_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListFactors.Key, ContractSHA256: ListFactors.ContractSHA256, Mode: connector.ModeCall, Payload: payload})
		if code, _ := connector.ProviderErrorCodeOf(err); code != "calendar.request_invalid" {
			t.Fatalf("input=%+v error=%v", input, err)
		}
	}
	payload, _ := json.Marshal(ListFactorsInput{Locale: "ja-JP", Region: "JP", StartDate: "2026-01-01", EndDate: "2026-01-02"})
	_, err = adapter.Call(t.Context(), connector.CallRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListFactors.Key, ContractSHA256: ListFactors.ContractSHA256, Mode: connector.ModeCall,
		Connection: connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080", "cache_ttl_seconds": 0}}, Payload: payload,
	})
	classification, ok := connector.ErrorClassificationOf(err)
	code, coded := connector.ProviderErrorCodeOf(err)
	if !ok || classification != connector.ErrorRetryable || !coded || code != "calendar.rate_limited" {
		t.Fatalf("classification=%q/%v code=%q/%v error=%v", classification, ok, code, coded, err)
	}

	transport.response = connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{`)}
	_, err = adapter.Call(t.Context(), connector.CallRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListFactors.Key, ContractSHA256: ListFactors.ContractSHA256, Mode: connector.ModeCall,
		Connection: connector.Connection{Key: time.Now().String(), Config: map[string]any{"base_url": "http://localhost:8080", "cache_ttl_seconds": 0}}, Payload: payload,
	})
	if code, _ := connector.ProviderErrorCodeOf(err); code != "calendar.response_invalid" {
		t.Fatalf("invalid response error=%v", err)
	}
}
