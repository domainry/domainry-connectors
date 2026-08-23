package plaid

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

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.request = request
	return t.response, t.err
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestDescriptorAndEnvironmentBoundary(t *testing.T) {
	adapter, err := New(&recordingTransport{})
	if err != nil {
		t.Fatal(err)
	}
	if err = contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	if descriptor := adapter.Descriptor(); descriptor.ConnectorKey != ConnectorKey || descriptor.ProviderKey != ProviderKey || len(descriptor.Operations) != 3 || len(descriptor.SecretFields) != 3 {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	validator := adapter.(connector.ConfigValidator)
	for _, valid := range []string{"https://production.plaid.com", "https://sandbox.plaid.com", "http://127.0.0.1:8080"} {
		if err = validator.ValidateConfig(connection(valid)); err != nil {
			t.Fatalf("valid endpoint %q: %v", valid, err)
		}
	}
	for _, invalid := range []string{"https://development.plaid.com", "https://plaid.example.com", "http://production.plaid.com", "https://production.plaid.com.evil.test"} {
		if err = validator.ValidateConfig(connection(invalid)); err == nil {
			t.Fatalf("invalid endpoint accepted: %s", invalid)
		}
	}
}

func TestSecretsStayInRuntimeOnlyJSONFields(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"request_id":"request-1","next_cursor":"cursor-2","added":[],"modified":[],"removed":[]}`)}}
	adapter, _ := New(transport)
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SyncTransactions.Key, ContractSHA256: SyncTransactions.ContractSHA256, Mode: connector.ModeCall, Connection: connection("http://localhost:8080"), Secrets: secrets(), Payload: []byte(`{"cursor":"cursor-1","count":100}`)})
	if err != nil || result.ResponseRef != "plaid:request:request-1" {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	request := transport.request
	if request.SecretJSON["client_id"] != "client-runtime" || request.SecretJSON["secret"] != "secret-runtime" || request.SecretJSON["access_token"] != "access-runtime" {
		t.Fatalf("secret JSON=%v", request.SecretJSON)
	}
	if strings.Contains(string(request.Body), "runtime") || string(request.Body) != `{"count":100,"cursor":"cursor-1"}` {
		t.Fatalf("public body=%s", request.Body)
	}
}

func TestReadFailuresAreRetryableOrPermanent(t *testing.T) {
	for _, test := range []struct {
		name     string
		response connector.HTTPResponse
		err      error
		want     connector.ErrorClassification
	}{{"network", connector.HTTPResponse{}, errors.New("reset"), connector.ErrorRetryable}, {"rate_limit", connector.HTTPResponse{StatusCode: http.StatusTooManyRequests}, nil, connector.ErrorRetryable}, {"server", connector.HTTPResponse{StatusCode: http.StatusBadGateway}, nil, connector.ErrorRetryable}, {"bad_request", connector.HTTPResponse{StatusCode: http.StatusBadRequest, Body: []byte(`{"error_code":"INVALID_INPUT"}`)}, nil, connector.ErrorPermanent}} {
		t.Run(test.name, func(t *testing.T) {
			transport := &recordingTransport{response: test.response, err: test.err}
			adapter, _ := New(transport)
			_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: GetAccounts.Key, ContractSHA256: GetAccounts.ContractSHA256, Mode: connector.ModeCall, Connection: connection("http://localhost:8080"), Secrets: secrets(), Payload: []byte(`{}`)})
			class, ok := connector.ErrorClassificationOf(err)
			if !ok || class != test.want {
				t.Fatalf("error=%v class=%q want=%q", err, class, test.want)
			}
		})
	}
}

func connection(endpoint string) connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": endpoint}}
}
func secrets() map[string]string {
	return map[string]string{"client_id": "client-runtime", "client_secret": "secret-runtime", "access_token": "access-runtime"}
}
