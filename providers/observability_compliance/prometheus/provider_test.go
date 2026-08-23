package prometheus

import (
	"context"
	"encoding/base64"
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

func TestTypedQueriesAndRuntimeOnlyAuthentication(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"status":"success","data":{}}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Call(t.Context(), callRequest(QueryRange, mustJSON(QueryRangeInput{Query: "rate(http_requests_total[5m])", Start: "100", End: "200", Step: "15", Limit: 25}), map[string]string{"basic_auth_password": "password"}))
	if err != nil || !strings.Contains(string(result.Payload), "success") {
		t.Fatalf("result=%s error=%v", result.Payload, err)
	}
	request := transport.requests[0]
	expected := "Basic " + base64.StdEncoding.EncodeToString([]byte("reader:password"))
	if request.SecretHeaders["Authorization"][0] != expected || strings.Contains(request.URL, "password") || !strings.Contains(request.URL, "/api/v1/query_range") || !strings.Contains(request.URL, "limit=25") {
		t.Fatalf("request=%+v", request)
	}
	_, err = adapter.Call(t.Context(), callRequest(Query, mustJSON(QueryInput{Query: "up"}), map[string]string{"bearer_token": "bearer"}))
	if err != nil || transport.requests[1].SecretHeaders["Authorization"][0] != "Bearer bearer" {
		t.Fatalf("request=%+v error=%v", transport.requests[1], err)
	}
}

func TestAuthenticationInputAndEndpointFailClosed(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"status":"success"}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	for _, payload := range [][]byte{[]byte(`{"query":"up","unknown":true}`), mustJSON(QueryInput{}), mustJSON(QueryInput{Query: "up", Limit: -1})} {
		if _, err := adapter.Call(t.Context(), callRequest(Query, payload, nil)); err == nil {
			t.Fatal("invalid query accepted")
		}
	}
	if _, err := adapter.Call(t.Context(), callRequest(Query, mustJSON(QueryInput{Query: "up"}), map[string]string{"bearer_token": "one", "basic_auth_password": "two"})); err == nil {
		t.Fatal("conflicting credentials accepted")
	}
	request := callRequest(Query, mustJSON(QueryInput{Query: "up"}), map[string]string{"basic_auth_password": "two"})
	request.Connection.Config = map[string]any{"base_url": "http://localhost:9090"}
	if _, err := adapter.Call(t.Context(), request); err == nil {
		t.Fatal("Basic auth without username accepted")
	}
	if err := adapter.(connector.ConfigValidator).ValidateConfig(connector.Connection{Config: map[string]any{"base_url": "http://remote.example"}}); err == nil {
		t.Fatal("remote plaintext HTTP accepted")
	}
}

func TestProbeAndFailureClassification(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"status":"success"}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: connection()})
	if err != nil || !probe.Connected {
		t.Fatalf("probe=%+v error=%v", probe, err)
	}
	transport.response = connector.HTTPResponse{StatusCode: http.StatusServiceUnavailable, Body: []byte(`{"status":"error"}`)}
	_, err = adapter.Call(t.Context(), callRequest(Query, mustJSON(QueryInput{Query: "up"}), nil))
	if class, ok := connector.ErrorClassificationOf(err); !ok || class != connector.ErrorRetryable {
		t.Fatalf("error=%v class=%q", err, class)
	}
	transport.response = connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"status":"error"}`)}
	_, err = adapter.Call(t.Context(), callRequest(Query, mustJSON(QueryInput{Query: "up"}), nil))
	if code, _ := connector.ProviderErrorCodeOf(err); code != "prometheus.query_failed" {
		t.Fatalf("error=%v", err)
	}
}

func connection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "http://localhost:9090", "username": "reader"}}
}
func callRequest[T any, O any](operation connector.CallOperation[T, O], payload []byte, secrets map[string]string) connector.CallRequest {
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: operation.Key, ContractSHA256: operation.ContractSHA256, Mode: connector.ModeCall, Connection: connection(), Secrets: secrets, Payload: payload}
}
func mustJSON(value any) []byte {
	payload, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return payload
}
