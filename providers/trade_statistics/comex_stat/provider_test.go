package comexstat

import (
	"context"
	"encoding/base64"
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
	handle   func(connector.HTTPRequest) (connector.HTTPResponse, error)
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, request)
	return t.handle(request)
}

func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestProviderComposesOfficialQueryAndPublicationEvidence(t *testing.T) {
	queryRaw := []byte(`{"data":{"list":[{"coNcm":"84818099","metricFOB":"6448507"}]},"success":true}`)
	publicationRaw := []byte(`{"data":{"updated":"2026-07-03","year":"2026","monthNumber":"06"},"success":true}`)
	transport := &recordingTransport{handle: func(request connector.HTTPRequest) (connector.HTTPResponse, error) {
		switch {
		case strings.Contains(request.URL, "/general?"):
			return connector.HTTPResponse{StatusCode: http.StatusOK, Body: queryRaw}, nil
		case strings.Contains(request.URL, "/general/dates/updated?"):
			return connector.HTTPResponse{StatusCode: http.StatusOK, Body: publicationRaw}, nil
		default:
			t.Fatalf("unexpected request %s %s", request.Method, request.URL)
			return connector.HTTPResponse{}, nil
		}
	}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	implementation := adapter.(*provider)
	implementation.now = func() time.Time { return time.Date(2026, 8, 23, 1, 2, 3, 4, time.UTC) }
	connection := connector.Connection{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Config: map[string]any{"base_url": "http://127.0.0.1:8080", "language": "pt"}}
	payload, _ := json.Marshal(QueryImportStatisticsInput{NCM: "84818099", PeriodFrom: "2026-01", PeriodTo: "2026-06", UFCode: "41", URFCode: "0817800", ViaCode: "01"})
	result, err := adapter.Call(t.Context(), connector.CallRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: QueryImportStatistics.Key,
		ContractSHA256: QueryImportStatistics.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	var output QueryImportStatisticsOutput
	if err := json.Unmarshal(result.Payload, &output); err != nil {
		t.Fatal(err)
	}
	decoded, err := base64.StdEncoding.DecodeString(output.Result.RawResponseBase64)
	if err != nil || string(decoded) != string(queryRaw) {
		t.Fatalf("decoded=%q error=%v", decoded, err)
	}
	if output.Result.Provider != "comex_stat" || output.Result.SourceType != "official_api" || output.Result.QueriedAt != "2026-08-23T01:02:03.000000004Z" || output.Result.PublicationResponseRef != "http:200" {
		t.Fatalf("evidence=%+v", output.Result)
	}
	if len(transport.requests) != 2 || transport.requests[0].Method != http.MethodPost || transport.requests[1].Method != http.MethodGet {
		t.Fatalf("requests=%+v", transport.requests)
	}
	var posted map[string]any
	if err := json.Unmarshal(transport.requests[0].Body, &posted); err != nil || posted["flow"] != "import" || posted["monthDetail"] != true {
		t.Fatalf("posted=%+v error=%v", posted, err)
	}
	probe, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: connection})
	if err != nil || !probe.Connected {
		t.Fatalf("probe=%+v error=%v", probe, err)
	}
	if got := adapter.Descriptor(); got.StartupActivation != connector.StartupActivationDefaultSafe || len(got.Operations) != 1 {
		t.Fatalf("descriptor=%+v", got)
	}
}

func TestProviderRejectsNonOfficialEndpointInvalidFiltersAndRateLimit(t *testing.T) {
	transport := &recordingTransport{handle: func(connector.HTTPRequest) (connector.HTTPResponse, error) {
		return connector.HTTPResponse{StatusCode: http.StatusTooManyRequests, Body: []byte(`{}`)}, nil
	}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	validator := adapter.(connector.ConfigValidator)
	if err := validator.ValidateConfig(connector.Connection{Config: map[string]any{"base_url": "https://example.com", "language": "pt"}}); err == nil {
		t.Fatal("non-official HTTPS endpoint accepted")
	}
	if err := validator.ValidateConfig(connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080", "language": "es"}}); err != nil {
		t.Fatal(err)
	}
	invalid, _ := json.Marshal(QueryImportStatisticsInput{NCM: "8481", PeriodFrom: "2026-07", PeriodTo: "2026-06", UFCode: "SP", URFCode: "0817800", ViaCode: "01"})
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: QueryImportStatistics.Key, ContractSHA256: QueryImportStatistics.ContractSHA256, Mode: connector.ModeCall, Payload: invalid})
	if code, _ := connector.ProviderErrorCodeOf(err); code != "comex_stat.ncm_invalid" {
		t.Fatalf("invalid input error=%v", err)
	}
	valid, _ := json.Marshal(QueryImportStatisticsInput{NCM: "84818099", PeriodFrom: "2026-01", PeriodTo: "2026-06", UFCode: "41", URFCode: "0817800", ViaCode: "01"})
	_, err = adapter.Call(t.Context(), connector.CallRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: QueryImportStatistics.Key, ContractSHA256: QueryImportStatistics.ContractSHA256, Mode: connector.ModeCall,
		Connection: connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080"}}, Payload: valid,
	})
	classification, ok := connector.ErrorClassificationOf(err)
	code, coded := connector.ProviderErrorCodeOf(err)
	if !ok || classification != connector.ErrorRetryable || !coded || code != "comex_stat.rate_limited" {
		t.Fatalf("classification=%q/%v code=%q/%v error=%v", classification, ok, code, coded, err)
	}
}
