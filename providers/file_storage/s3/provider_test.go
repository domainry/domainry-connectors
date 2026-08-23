package s3

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
)

type recordingTransport struct {
	requests []connector.HTTPRequest
	respond  func(connector.HTTPRequest) (connector.HTTPResponse, error)
}

func (transport *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	transport.requests = append(transport.requests, request)
	if transport.respond != nil {
		return transport.respond(request)
	}
	return connector.HTTPResponse{StatusCode: http.StatusOK, Headers: map[string][]string{"ETag": {`"etag-1"`}, "Content-Type": {"text/plain"}}, Body: []byte("stored")}, nil
}

func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestOperationsUseRuntimeHTTPAndKeepCredentialsNonserializable(t *testing.T) {
	transport := &recordingTransport{}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	adapter.(*provider).now = func() time.Time { return time.Date(2026, 8, 24, 1, 2, 3, 0, time.UTC) }
	operations := []struct {
		key, hash, method string
		input             any
	}{{TestConnection.Key, TestConnection.ContractSHA256, http.MethodHead, struct{}{}}, {PutText.Key, PutText.ContractSHA256, http.MethodPut, PutTextInput{Path: "proof/a.txt", Content: "stored"}}, {GetText.Key, GetText.ContractSHA256, http.MethodGet, PathInput{Path: "proof/a.txt"}}, {Delete.Key, Delete.ContractSHA256, http.MethodDelete, PathInput{Path: "proof/a.txt"}}}
	for _, operation := range operations {
		payload, _ := json.Marshal(operation.input)
		result, callErr := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: operation.key, ContractSHA256: operation.hash, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_key_id": "runtime-access", "secret_access_key": "runtime-secret", "session_token": "runtime-session"}, Payload: payload})
		if callErr != nil || result.ResponseRef == "" {
			t.Fatalf("operation=%s result=%+v err=%v", operation.key, result, callErr)
		}
		request := transport.requests[len(transport.requests)-1]
		encoded, _ := json.Marshal(request)
		if request.Method != operation.method || request.Headers["Authorization"] != nil || len(request.SecretHeaders["Authorization"]) != 1 || len(request.SecretHeaders["X-Amz-Security-Token"]) != 1 {
			t.Fatalf("request=%+v", request)
		}
		serialized := string(encoded)
		for _, secret := range []string{"runtime-access", "runtime-secret", "runtime-session", "Authorization", "X-Amz-Security-Token"} {
			if strings.Contains(serialized, secret) {
				t.Fatalf("serialized request leaks %q: %s", secret, serialized)
			}
		}
	}
}

func TestConfigAndObjectNamespaceFailClosed(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	provider := adapter.(*provider)
	for _, connection := range []connector.Connection{
		{Config: map[string]any{"region": "us-east-1"}},
		{Config: map[string]any{"bucket": "bucket", "region": "us-east-1", "endpoint": "http://example.test"}},
		{Config: map[string]any{"bucket": "bucket/name", "region": "us-east-1"}},
		{Config: map[string]any{"bucket": "bucket", "region": "us-east-1", "prefix": "../escape"}},
		{Config: map[string]any{"bucket": "bucket", "region": "us-east-1", "max_file_bytes": 0}},
	} {
		if err := provider.ValidateConfig(connection); err == nil {
			t.Fatalf("accepted config=%v", connection.Config)
		}
	}
	for _, value := range []string{"", "/absolute", ".", "..", "../escape"} {
		if _, err := objectKey(validConnection(), value); err == nil {
			t.Fatalf("accepted path=%q", value)
		}
	}
	if key, err := objectKey(validConnection(), "nested/../file.txt"); err != nil || key != "tenant/root/file.txt" {
		t.Fatalf("key=%q err=%v", key, err)
	}
}

func TestWriteFailuresAreUncertainAndReadsRetryable(t *testing.T) {
	for _, test := range []struct {
		name, operation, hash string
		input                 any
		response              connector.HTTPResponse
		transportErr          error
		category              connector.ErrorClassification
	}{
		{"put network", PutText.Key, PutText.ContractSHA256, PutTextInput{Path: "a.txt", Content: "x"}, connector.HTTPResponse{}, errors.New("reset"), connector.ErrorUncertain},
		{"delete server", Delete.Key, Delete.ContractSHA256, PathInput{Path: "a.txt"}, connector.HTTPResponse{StatusCode: 503}, nil, connector.ErrorUncertain},
		{"get network", GetText.Key, GetText.ContractSHA256, PathInput{Path: "a.txt"}, connector.HTTPResponse{}, errors.New("reset"), connector.ErrorRetryable},
		{"get rate", GetText.Key, GetText.ContractSHA256, PathInput{Path: "a.txt"}, connector.HTTPResponse{StatusCode: 429}, nil, connector.ErrorRetryable},
		{"get reject", GetText.Key, GetText.ContractSHA256, PathInput{Path: "a.txt"}, connector.HTTPResponse{StatusCode: 404}, nil, connector.ErrorPermanent},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := &recordingTransport{respond: func(connector.HTTPRequest) (connector.HTTPResponse, error) { return test.response, test.transportErr }}
			adapter, _ := New(transport)
			payload, _ := json.Marshal(test.input)
			_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: test.operation, ContractSHA256: test.hash, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_key_id": "access", "secret_access_key": "secret"}, Payload: payload})
			if category, ok := connector.ErrorClassificationOf(err); !ok || category != test.category {
				t.Fatalf("category=%q err=%v", category, err)
			}
		})
	}
}

func validConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"endpoint": "http://127.0.0.1:9000", "region": "us-east-1", "bucket": "bucket", "prefix": "tenant/root", "max_file_bytes": 1024, "allow_delete": true}, SecretRefs: map[string]string{"access_key_id": "secret:access", "secret_access_key": "secret:key", "session_token": "secret:session"}}
}
