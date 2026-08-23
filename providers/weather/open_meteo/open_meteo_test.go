package openmeteo

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
)

type transportStub struct {
	calls    int
	response connector.HTTPResponse
	err      error
}

func (s *transportStub) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	s.calls++
	if request.Method != http.MethodGet || !strings.Contains(request.URL, "/v1/forecast?") || request.MaxResponseBytes != responseLimit {
		return connector.HTTPResponse{}, errors.New("unexpected Open-Meteo request")
	}
	return s.response, s.err
}

func (*transportStub) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("SQL is unavailable")
}

func TestProviderContractAndWorkspaceCache(t *testing.T) {
	stub := &transportStub{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{
		"latitude":35,"longitude":139,"timezone":"Asia/Tokyo",
		"daily":{"time":["2026-08-12","2026-08-13"],"weather_code":[1,2],"temperature_2m_max":[30,31],"temperature_2m_min":[22,23],"precipitation_sum":[0,1],"rain_sum":[0,1],"snowfall_sum":[0,0]}
	}`)}}
	adapter, err := New(stub)
	if err != nil {
		t.Fatal(err)
	}
	if err := contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	implementation := adapter.(*provider)
	implementation.now = func() time.Time { return time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC) }
	input := GetDailyInput{Latitude: float64Pointer(35), Longitude: float64Pointer(139), StartDate: "2026-08-12", EndDate: "2026-08-13", Timezone: "Asia/Tokyo"}
	payload, _ := json.Marshal(input)
	for _, workspace := range []string{"store-a", "store-a", "store-b"} {
		result, callErr := adapter.Call(t.Context(), connector.CallRequest{
			ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: GetDaily.Key,
			ContractSHA256: GetDaily.ContractSHA256, Mode: connector.ModeCall,
			Connection: connector.Connection{Key: "weather", Config: map[string]any{"base_url": "http://127.0.0.1", "cache_ttl_seconds": 300}},
			Payload:    payload, Principal: connector.Principal{WorkspaceID: workspace},
		})
		if callErr != nil {
			t.Fatal(callErr)
		}
		var output GetDailyOutput
		if err := json.Unmarshal(result.Payload, &output); err != nil || len(output.Days) != 2 {
			t.Fatalf("output=%+v error=%v", output, err)
		}
	}
	if stub.calls != 2 {
		t.Fatalf("transport calls=%d, want 2", stub.calls)
	}
}

func TestProviderFailuresAreClassified(t *testing.T) {
	for _, test := range []struct {
		name           string
		response       connector.HTTPResponse
		transportError error
		classification connector.ErrorClassification
		code           string
	}{
		{name: "rate limited", response: connector.HTTPResponse{StatusCode: http.StatusTooManyRequests}, classification: connector.ErrorRetryable, code: "weather.rate_limited"},
		{name: "unavailable", transportError: errors.New("dial failed"), classification: connector.ErrorRetryable, code: "weather.unavailable"},
		{name: "rejected", response: connector.HTTPResponse{StatusCode: http.StatusBadRequest}, classification: connector.ErrorPermanent, code: "weather.provider_rejected"},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter, err := New(&transportStub{response: test.response, err: test.transportError})
			if err != nil {
				t.Fatal(err)
			}
			payload, _ := json.Marshal(GetDailyInput{Latitude: float64Pointer(35), Longitude: float64Pointer(139), StartDate: "2026-08-12", EndDate: "2026-08-12"})
			_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: GetDaily.Key, ContractSHA256: GetDaily.ContractSHA256, Mode: connector.ModeCall, Connection: connector.Connection{Config: map[string]any{"base_url": "http://localhost"}}, Payload: payload})
			if classification, ok := connector.ErrorClassificationOf(err); !ok || classification != test.classification {
				t.Fatalf("classification=%q ok=%v error=%v", classification, ok, err)
			}
			if code, ok := connector.ProviderErrorCodeOf(err); !ok || code != test.code {
				t.Fatalf("code=%q ok=%v error=%v", code, ok, err)
			}
		})
	}
}

func TestNewRequiresRuntimeTransport(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("nil Runtime transport was accepted")
	}
}

func TestDescriptorIdentity(t *testing.T) {
	adapter, err := New(&transportStub{})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(adapter.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	got := fmt.Sprintf("%x", sha256.Sum256(raw))
	const want = "696747ed6927ca7fd43609a6ce264cd3addcd04702eadb4dcbe3edb6bb3a7e42"
	if got != want {
		t.Fatalf("descriptor SHA-256=%s", got)
	}
}
