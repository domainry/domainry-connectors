package etsy

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

func TestDescriptorUsesAccurateCredentialNamesAndOfficialHosts(t *testing.T) {
	adapter, err := New(&recordingTransport{})
	if err != nil {
		t.Fatal(err)
	}
	if err = contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	descriptor := adapter.Descriptor()
	if descriptor.ConnectorKey != ConnectorKey || descriptor.ProviderKey != ProviderKey || len(descriptor.Operations) != 3 || descriptor.SecretFields[0].Key != "api_key" || descriptor.SecretFields[1].Key != "access_token" {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	validator := adapter.(connector.ConfigValidator)
	for _, endpoint := range []string{"https://api.etsy.com/v3", "https://openapi.etsy.com/v3", "http://127.0.0.1:8080"} {
		if err = validator.ValidateConfig(connection(endpoint)); err != nil {
			t.Fatalf("valid endpoint %s: %v", endpoint, err)
		}
	}
	for _, endpoint := range []string{"https://etsy.example.com/v3", "http://api.etsy.com/v3", "https://api.etsy.com.evil.test/v3"} {
		if err = validator.ValidateConfig(connection(endpoint)); err == nil {
			t.Fatalf("invalid endpoint accepted: %s", endpoint)
		}
	}
}

func TestListOrdersUsesRuntimeOnlyHeadersAndValidatedQuery(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"count":1,"results":[]}`)}}
	adapter, _ := New(transport)
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListOrders.Key, ContractSHA256: ListOrders.ContractSHA256, Mode: connector.ModeCall, Connection: connection("http://localhost:8080/v3"), Secrets: secrets(), Payload: []byte(`{"limit":100,"offset":5,"min_created":946684800,"max_created":946684900}`)})
	if err != nil || result.ResponseRef != "etsy:page:count:1" {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	request := transport.request
	if request.SecretHeaders["x-api-key"][0] != "keystring:shared-secret" || request.SecretHeaders["Authorization"][0] != "Bearer user.oauth-token" || strings.Contains(request.URL, "shared-secret") {
		t.Fatalf("request=%+v", request)
	}
	for _, expected := range []string{"limit=100", "offset=5", "min_created=946684800", "max_created=946684900"} {
		if !strings.Contains(request.URL, expected) {
			t.Fatalf("missing %s in %s", expected, request.URL)
		}
	}
}

func TestPaginationAndFailureClassification(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	for _, payload := range []string{`{"limit":101}`, `{"offset":-1}`, `{"min_created":1}`, `{"min_created":946684900,"max_created":946684800}`} {
		_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListOrders.Key, ContractSHA256: ListOrders.ContractSHA256, Mode: connector.ModeCall, Connection: connection("http://localhost:8080"), Secrets: secrets(), Payload: []byte(payload)})
		if err == nil {
			t.Fatalf("invalid payload accepted: %s", payload)
		}
	}
	for _, test := range []struct {
		name     string
		response connector.HTTPResponse
		err      error
		want     connector.ErrorClassification
	}{{"network", connector.HTTPResponse{}, errors.New("reset"), connector.ErrorRetryable}, {"rate_limit", connector.HTTPResponse{StatusCode: http.StatusTooManyRequests}, nil, connector.ErrorRetryable}, {"server", connector.HTTPResponse{StatusCode: http.StatusBadGateway}, nil, connector.ErrorRetryable}, {"bad_request", connector.HTTPResponse{StatusCode: http.StatusBadRequest}, nil, connector.ErrorPermanent}} {
		t.Run(test.name, func(t *testing.T) {
			transport := &recordingTransport{response: test.response, err: test.err}
			current, _ := New(transport)
			_, err := current.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListListings.Key, ContractSHA256: ListListings.ContractSHA256, Mode: connector.ModeCall, Connection: connection("http://localhost:8080"), Secrets: secrets(), Payload: []byte(`{}`)})
			class, ok := connector.ErrorClassificationOf(err)
			if !ok || class != test.want {
				t.Fatalf("error=%v class=%q want=%q", err, class, test.want)
			}
		})
	}
}

func connection(endpoint string) connector.Connection {
	return connector.Connection{Config: map[string]any{"shop_id": "42", "base_url": endpoint}}
}
func secrets() map[string]string {
	return map[string]string{"api_key": "keystring:shared-secret", "access_token": "user.oauth-token"}
}
