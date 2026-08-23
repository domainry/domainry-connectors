package openai

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
)

type recordingTransport struct {
	request  connector.HTTPRequest
	response connector.HTTPResponse
	err      error
}

func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.request = request
	return t.response, t.err
}

func TestDescriptorAndOfficialEndpointBoundary(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"id":"gpt-5"}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	if err = contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	descriptor := adapter.Descriptor()
	if descriptor.ConnectorKey != ConnectorKey || descriptor.ProviderKey != ProviderKey || len(descriptor.Operations) != 3 || len(descriptor.SecretFields) != 1 {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	validator := adapter.(connector.ConfigValidator)
	for _, endpoint := range []string{"https://example.com/v1", "http://api.openai.com/v1", "https://api.openai.com.evil.test/v1"} {
		if err = validator.ValidateConfig(connection(endpoint)); err == nil {
			t.Fatalf("accepted non-official endpoint %q", endpoint)
		}
	}
	if err = validator.ValidateConfig(connection("http://127.0.0.1:8080/v1")); err != nil {
		t.Fatalf("loopback test endpoint rejected: %v", err)
	}
}

func TestGenerateResponseUsesRuntimeSecretAndParsesOutput(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"id":"resp_1","status":"completed","output":[{"content":[{"type":"output_text","text":"hello"}]}]}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Call(t.Context(), connector.CallRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: GenerateResponse.Key,
		ContractSHA256: GenerateResponse.ContractSHA256, Mode: connector.ModeCall,
		Connection: connection("http://localhost:8080/v1"), Secrets: map[string]string{"api_key": "runtime-secret"},
		Payload: []byte(`{"input":"hello","instructions":"be concise","max_output_tokens":32}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if transport.request.Method != http.MethodPost || transport.request.URL != "http://localhost:8080/v1/responses" {
		t.Fatalf("request=%+v", transport.request)
	}
	if got := transport.request.SecretHeaders["Authorization"]; len(got) != 1 || got[0] != "Bearer runtime-secret" {
		t.Fatalf("secret headers=%v", transport.request.SecretHeaders)
	}
	if strings.Contains(string(transport.request.Body), "runtime-secret") || strings.Contains(string(result.Payload), "runtime-secret") {
		t.Fatal("runtime secret leaked into request or result payload")
	}
	if !strings.Contains(string(result.Payload), `"output_text":"hello"`) || result.ResponseRef != "openai:response:resp_1" {
		t.Fatalf("result=%+v", result)
	}
}

func TestWriteFailureClassification(t *testing.T) {
	for _, test := range []struct {
		name     string
		response connector.HTTPResponse
		err      error
		want     connector.ErrorClassification
	}{
		{name: "network", err: errors.New("connection reset"), want: connector.ErrorUncertain},
		{name: "rate limit", response: connector.HTTPResponse{StatusCode: http.StatusTooManyRequests}, want: connector.ErrorRetryable},
		{name: "bad request", response: connector.HTTPResponse{StatusCode: http.StatusBadRequest}, want: connector.ErrorPermanent},
		{name: "server", response: connector.HTTPResponse{StatusCode: http.StatusBadGateway}, want: connector.ErrorUncertain},
		{name: "malformed success", response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte("not-json")}, want: connector.ErrorUncertain},
		{name: "missing identity", response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"status":"completed"}`)}, want: connector.ErrorUncertain},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := &recordingTransport{response: test.response, err: test.err}
			adapter, err := New(transport)
			if err != nil {
				t.Fatal(err)
			}
			_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: GenerateResponse.Key, ContractSHA256: GenerateResponse.ContractSHA256, Mode: connector.ModeCall, Connection: connection("http://localhost:8080/v1"), Secrets: map[string]string{"api_key": "secret"}, Payload: []byte(`{"input":"hello"}`)})
			if classification, ok := connector.ErrorClassificationOf(err); !ok || classification != test.want {
				t.Fatalf("error=%v classification=%q want=%q", err, classification, test.want)
			}
		})
	}
}

func connection(baseURL string) connector.Connection {
	return connector.Connection{Config: map[string]any{"model": "gpt-5", "base_url": baseURL, "store": false}}
}
