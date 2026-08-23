package posthog

import (
	"context"
	"encoding/json"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"net/http"
	"strings"
	"testing"
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

func TestCaptureUsesRuntimeOnlyJSONSecretAndCopiesProperties(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"status":1}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	properties := map[string]any{"source": "runtime"}
	result, err := adapter.Call(t.Context(), callRequest(CaptureEvent, mustJSON(CaptureEventInput{Event: "activated", DistinctID: "user-1", Properties: properties, Timestamp: "2026-08-23T00:00:00Z"}), "request-1"))
	if err != nil || !strings.Contains(string(result.Payload), `"status":1`) {
		t.Fatalf("result=%s error=%v", result.Payload, err)
	}
	request := transport.requests[0]
	if request.SecretJSON["api_key"] != "project-key" || strings.Contains(string(request.Body), "project-key") {
		t.Fatalf("request=%+v", request)
	}
	var body map[string]any
	if err := json.Unmarshal(request.Body, &body); err != nil {
		t.Fatal(err)
	}
	props := body["properties"].(map[string]any)
	if props["distinct_id"] != "user-1" || props["$insert_id"] != "request-1" || properties["distinct_id"] != nil {
		t.Fatalf("body=%v original=%v", body, properties)
	}
}

func TestListUsesBearerHeaderAndBoundedTypedInput(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"results":[]}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Call(t.Context(), callRequest(ListEvents, mustJSON(ListEventsInput{Limit: 25, Offset: 5, After: "2026-01-01T00:00:00Z", Event: "activated", DistinctID: "user"}), ""))
	if err != nil {
		t.Fatal(err)
	}
	request := transport.requests[0]
	if request.SecretHeaders["Authorization"][0] != "Bearer personal-key" || !strings.Contains(request.URL, "limit=25") || !strings.Contains(request.URL, "distinct_id=user") {
		t.Fatalf("request=%+v", request)
	}
	for _, payload := range [][]byte{[]byte(`{"unknown":true}`), mustJSON(ListEventsInput{Limit: 1001}), mustJSON(ListEventsInput{Offset: -1})} {
		if _, err := adapter.Call(t.Context(), callRequest(ListEvents, payload, "")); err == nil {
			t.Fatal("invalid list input accepted")
		}
	}
}

func TestWriteAmbiguityProbeAndConfigFailures(t *testing.T) {
	transport := &recordingTransport{err: errors.New("connection reset")}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Call(t.Context(), callRequest(CaptureEvent, mustJSON(CaptureEventInput{Event: "activated", DistinctID: "user"}), "request"))
	if class, ok := connector.ErrorClassificationOf(err); !ok || class != connector.ErrorUncertain {
		t.Fatalf("error=%v class=%q", err, class)
	}
	transport.err = nil
	transport.response = connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"id":42}`)}
	probe, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: connection(), Secrets: map[string]string{"api_token": "personal-key"}})
	if err != nil || !probe.Connected {
		t.Fatalf("probe=%+v error=%v", probe, err)
	}
	if err := adapter.(connector.ConfigValidator).ValidateConfig(connector.Connection{Config: map[string]any{"project_id": "42", "base_url": "http://remote.example"}}); err == nil {
		t.Fatal("remote HTTP accepted")
	}
	request := callRequest(CaptureEvent, mustJSON(CaptureEventInput{Event: "activated", DistinctID: "user"}), "")
	delete(request.Secrets, "project_secret")
	if _, err := adapter.Call(t.Context(), request); err == nil {
		t.Fatal("missing project key accepted")
	}
}

func connection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080", "project_id": "42"}}
}
func callRequest[T any, O any](operation connector.CallOperation[T, O], payload []byte, ref string) connector.CallRequest {
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: operation.Key, ContractSHA256: operation.ContractSHA256, Mode: connector.ModeCall, Connection: connection(), Secrets: map[string]string{"api_token": "personal-key", "project_secret": "project-key"}, Payload: payload, RequestRef: ref}
}
func mustJSON(value any) []byte {
	payload, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return payload
}
