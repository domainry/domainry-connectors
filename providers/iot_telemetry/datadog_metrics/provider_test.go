package datadogmetrics

import (
	"context"
	"encoding/json"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"net/http"
	"strings"
	"testing"
	"time"
)

type recordingTransport struct {
	requests []connector.HTTPRequest
	response connector.HTTPResponse
	err      error
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, request)
	return t.response, t.err
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestPublishUsesTypedStableSeriesAndRuntimeOnlyHeaders(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusAccepted, Body: []byte(`{"errors":[]}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	p := adapter.(*provider)
	p.now = func() time.Time { return time.Unix(100, 0) }
	result, err := adapter.Call(t.Context(), callRequest(mustJSON(PublishTelemetryInput{DeviceID: "device", EventID: "event", Metrics: map[string]float64{"temperature": 21.5, "battery": 80}})))
	if err != nil || !strings.Contains(string(result.Payload), `"accepted":true`) {
		t.Fatalf("result=%s error=%v", result.Payload, err)
	}
	request := transport.requests[0]
	if request.SecretHeaders["Dd-Api-Key"][0] != "api-key" || request.SecretHeaders["Dd-Application-Key"][0] != "app-key" || strings.Contains(string(request.Body), "api-key") {
		t.Fatalf("request=%+v", request)
	}
	var body map[string]any
	if err := json.Unmarshal(request.Body, &body); err != nil {
		t.Fatal(err)
	}
	series := body["series"].([]any)
	if series[0].(map[string]any)["metric"] != "battery" || series[1].(map[string]any)["metric"] != "temperature" {
		t.Fatalf("series=%v", series)
	}
}

func TestTimestampMetricAndInputValidation(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusAccepted, Body: []byte(`{}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	for _, payload := range [][]byte{[]byte(`{"unknown":true}`), mustJSON(PublishTelemetryInput{}), mustJSON(PublishTelemetryInput{DeviceID: "d", EventID: "e", Metrics: map[string]float64{"m": 1}, Timestamp: "invalid"}), mustJSON(PublishTelemetryInput{DeviceID: "d", EventID: "e", Metrics: map[string]float64{"": 1}})} {
		if _, err := adapter.Call(t.Context(), callRequest(payload)); err == nil {
			t.Fatal("invalid telemetry accepted")
		}
	}
}

func TestProbeFailuresAndWriteAmbiguity(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"valid":true}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: connection(), Secrets: map[string]string{"api_key": "api-key"}})
	if err != nil || !probe.Connected {
		t.Fatalf("probe=%+v error=%v", probe, err)
	}
	transport.err = errors.New("connection reset")
	_, err = adapter.Call(t.Context(), callRequest(mustJSON(PublishTelemetryInput{DeviceID: "d", EventID: "e", Metrics: map[string]float64{"m": 1}})))
	if class, ok := connector.ErrorClassificationOf(err); !ok || class != connector.ErrorUncertain {
		t.Fatalf("error=%v class=%q", err, class)
	}
	if err := adapter.(connector.ConfigValidator).ValidateConfig(connector.Connection{Config: map[string]any{"api_base_url": "http://remote.example"}}); err == nil {
		t.Fatal("remote HTTP accepted")
	}
	request := callRequest(mustJSON(PublishTelemetryInput{DeviceID: "d", EventID: "e", Metrics: map[string]float64{"m": 1}}))
	request.Secrets = nil
	if _, err := adapter.Call(t.Context(), request); err == nil {
		t.Fatal("missing API key accepted")
	}
}

func connection() connector.Connection {
	return connector.Connection{Config: map[string]any{"api_base_url": "http://localhost:8080"}}
}
func callRequest(payload []byte) connector.CallRequest {
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: PublishTelemetry.Key, ContractSHA256: PublishTelemetry.ContractSHA256, Mode: connector.ModeCall, Connection: connection(), Secrets: map[string]string{"api_key": "api-key", "application_key": "app-key"}, Payload: payload}
}
func mustJSON(value any) []byte {
	payload, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return payload
}
