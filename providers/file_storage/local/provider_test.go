package local

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
)

type filesystemTransport struct {
	requests []connector.FilesystemRequest
	files    map[string][]byte
	err      error
}

func (*filesystemTransport) RoundTripHTTP(context.Context, connector.HTTPRequest) (connector.HTTPResponse, error) {
	return connector.HTTPResponse{}, errors.New("unexpected HTTP")
}
func (*filesystemTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func (transport *filesystemTransport) ExecuteFilesystem(_ context.Context, request connector.FilesystemRequest) (connector.FilesystemResult, error) {
	transport.requests = append(transport.requests, request)
	if transport.err != nil {
		return connector.FilesystemResult{}, transport.err
	}
	if transport.files == nil {
		transport.files = map[string][]byte{}
	}
	switch request.Operation {
	case connector.FilesystemOperationProbe:
		return connector.FilesystemResult{Exists: true}, nil
	case connector.FilesystemOperationWrite:
		transport.files[request.Path] = append([]byte(nil), request.Content...)
		return connector.FilesystemResult{Path: request.Path, Size: int64(len(request.Content)), Exists: true}, nil
	case connector.FilesystemOperationRead:
		content, ok := transport.files[request.Path]
		if !ok {
			return connector.FilesystemResult{}, errors.New("missing")
		}
		return connector.FilesystemResult{Path: request.Path, Content: append([]byte(nil), content...), Size: int64(len(content)), Exists: true}, nil
	case connector.FilesystemOperationDelete:
		delete(transport.files, request.Path)
		return connector.FilesystemResult{Path: request.Path}, nil
	default:
		return connector.FilesystemResult{}, errors.New("unexpected filesystem operation")
	}
}

func TestOperationsUseOnlyRuntimeFilesystemCapability(t *testing.T) {
	transport := &filesystemTransport{}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	connection := connector.Connection{Config: map[string]any{"storage_root": "/runtime/storage", "max_file_bytes": 8, "allow_delete": true}}
	operations := []struct {
		key   string
		hash  string
		input any
	}{
		{key: TestConnection.Key, hash: TestConnection.ContractSHA256, input: struct{}{}},
		{key: PutText.Key, hash: PutText.ContractSHA256, input: PutTextInput{Path: "proof/a.txt", Content: "approved"}},
		{key: GetText.Key, hash: GetText.ContractSHA256, input: PathInput{Path: "proof/a.txt"}},
		{key: Delete.Key, hash: Delete.ContractSHA256, input: PathInput{Path: "proof/a.txt"}},
	}
	for _, operation := range operations {
		payload, _ := json.Marshal(operation.input)
		result, callErr := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: operation.key, ContractSHA256: operation.hash, Mode: connector.ModeCall, Connection: connection, Payload: payload})
		if callErr != nil || result.ResponseRef == "" {
			t.Fatalf("operation=%s result=%+v err=%v", operation.key, result, callErr)
		}
	}
	if got := len(transport.requests); got != 4 {
		t.Fatalf("filesystem requests=%d", got)
	}
}

func TestCapabilityConfigPathAndFailureBoundaries(t *testing.T) {
	if _, err := New(baseTransport{}); err == nil {
		t.Fatal("accepted transport without FilesystemTransport")
	}
	adapter, _ := New(&filesystemTransport{})
	validator := adapter.(connector.ConfigValidator)
	for _, connection := range []connector.Connection{{}, {Config: map[string]any{"storage_root": "relative"}}, {Config: map[string]any{"storage_root": "/root", "max_file_bytes": 0}}} {
		if validator.ValidateConfig(connection) == nil {
			t.Fatalf("accepted config=%v", connection.Config)
		}
	}
	for _, value := range []string{"", "/absolute", `\\absolute`, ".", "..", "../escape"} {
		if _, err := relativePath(value); err == nil {
			t.Fatalf("accepted path=%q", value)
		}
	}
	connection := connector.Connection{Config: map[string]any{"storage_root": "/root", "max_file_bytes": 2}}
	payload, _ := json.Marshal(PutTextInput{Path: "a.txt", Content: "large"})
	if _, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: PutText.Key, ContractSHA256: PutText.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Payload: payload}); err == nil {
		t.Fatal("accepted oversized content")
	}
}

type baseTransport struct{}

func (baseTransport) RoundTripHTTP(context.Context, connector.HTTPRequest) (connector.HTTPResponse, error) {
	return connector.HTTPResponse{}, nil
}
func (baseTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, nil
}
