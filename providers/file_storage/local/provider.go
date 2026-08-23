// Package local implements the official Runtime-governed local filesystem
// Provider. All filesystem access is delegated to connector.FilesystemTransport.
package local

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	ConnectorKey    = "file_storage"
	ProviderKey     = "local"
	defaultMaxBytes = 1 << 20
)

type PathInput struct {
	Path string `json:"path"`
}

type PutTextInput struct {
	Path        string `json:"path"`
	Content     string `json:"content"`
	ContentType string `json:"content_type,omitempty"`
}

var (
	Delete         = operation[PathInput]("delete", "6a170bdbf602291c72d9f37fd2dfbf3591f0c32cf8ce3b751062e3fcd20dbbb8", connector.EffectWrite)
	GetText        = operation[PathInput]("get_text", "37a7acbfb6ba0e7382a9e39668290cc2d62e0d74567e5029a240712b799210dc", connector.EffectRead)
	PutText        = operation[PutTextInput]("put_text", "98623acc286f328d5bb9208fd741e85c0b147fa24eb685e602982d83760750ec", connector.EffectWrite)
	TestConnection = operation[struct{}]("test_connection", "e1d1443180eb710663214176b0c6987a8086f9260203b88b57bdcaf3fbcb6964", connector.EffectRead)
)

func operation[I any](key, hash string, effect connector.OperationEffect) connector.CallOperation[I, map[string]any] {
	return connector.CallOperation[I, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: key, ContractSHA256: hash, Reliability: connector.ReliabilityContract{Effect: effect, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}}
}

type provider struct {
	connector.Adapter
	filesystem connector.FilesystemTransport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("local filesystem transport is required")
	}
	filesystem, ok := transport.(connector.FilesystemTransport)
	if !ok || filesystem == nil {
		return nil, errors.New("local Provider requires connector.FilesystemTransport")
	}
	p := &provider{filesystem: filesystem}
	deleteOperation, err := connector.BindCall(Delete, p.delete)
	if err != nil {
		return nil, err
	}
	getOperation, err := connector.BindCall(GetText, p.get)
	if err != nil {
		return nil, err
	}
	putOperation, err := connector.BindCall(PutText, p.put)
	if err != nil {
		return nil, err
	}
	testOperation, err := connector.BindCall(TestConnection, p.test)
	if err != nil {
		return nil, err
	}
	adapter, err := connector.NewProvider(schema(), deleteOperation, getOperation, putOperation, testOperation)
	if err != nil {
		return nil, err
	}
	p.Adapter = adapter
	return p, nil
}

func schema() connector.ProviderSchema {
	minimum := float64(1)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{
		{Key: "storage_root", Name: "Storage Root", Type: connector.ConfigFieldText, Required: true, I18n: i18n("Storage Root", "存储根目录")},
		{Key: "max_file_bytes", Name: "Maximum File Bytes", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`1048576`), Validation: connector.ConfigValidation{Min: &minimum}, I18n: i18n("Maximum File Bytes", "单文件最大字节数")},
		{Key: "allow_delete", Name: "Allow Delete", Type: connector.ConfigFieldBoolean, Default: json.RawMessage(`false`), I18n: i18n("Allow Delete", "允许删除")},
	}}
}

func i18n(en, zh string) map[string]connector.FieldLocalization {
	return map[string]connector.FieldLocalization{"en-US": {Name: en, Description: en}, "zh-CN": {Name: zh, Description: zh}}
}

func (*provider) ValidateConfig(connection connector.Connection) error {
	root := configString(connection.Config["storage_root"])
	if root == "" {
		return permanent("root_required", "storage_root is required")
	}
	if !filepath.IsAbs(root) {
		return permanent("root_must_be_absolute", "storage_root must be absolute")
	}
	if integer(connection.Config["max_file_bytes"], defaultMaxBytes) < 1 {
		return permanent("max_file_bytes_invalid", "max_file_bytes must be positive")
	}
	return nil
}

func (p *provider) test(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[map[string]any], error) {
	if err := p.ValidateConfig(request.Connection); err != nil {
		return empty(), err
	}
	_, err := p.filesystem.ExecuteFilesystem(ctx, connector.FilesystemRequest{Root: configString(request.Connection.Config["storage_root"]), Operation: connector.FilesystemOperationProbe})
	if err != nil {
		return empty(), connector.RetryableError("local.root_unavailable", err)
	}
	return connector.TypedResult[map[string]any]{Output: map[string]any{"connected": true}, ResponseRef: "file:root-ready"}, nil
}

func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.test(ctx, connector.TypedRequest[struct{}]{Connection: request.Connection})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) put(ctx context.Context, request connector.TypedRequest[PutTextInput]) (connector.TypedResult[map[string]any], error) {
	if err := p.ValidateConfig(request.Connection); err != nil {
		return empty(), err
	}
	cleaned, err := relativePath(request.Input.Path)
	if err != nil {
		return empty(), err
	}
	content := []byte(request.Input.Content)
	if len(content) > integer(request.Connection.Config["max_file_bytes"], defaultMaxBytes) {
		return empty(), permanent("file_too_large", "content exceeds max_file_bytes")
	}
	contentType, err := fileContentType(cleaned, content, request.Input.ContentType)
	if err != nil {
		return empty(), err
	}
	_, err = p.filesystem.ExecuteFilesystem(ctx, connector.FilesystemRequest{Root: configString(request.Connection.Config["storage_root"]), Path: cleaned, Operation: connector.FilesystemOperationWrite, Content: content})
	if err != nil {
		return empty(), connector.UncertainError("local.write_failed", err)
	}
	return connector.TypedResult[map[string]any]{Output: map[string]any{"path": cleaned, "bytes": len(content), "content_type": contentType}, ResponseRef: "file:" + cleaned}, nil
}

func (p *provider) get(ctx context.Context, request connector.TypedRequest[PathInput]) (connector.TypedResult[map[string]any], error) {
	if err := p.ValidateConfig(request.Connection); err != nil {
		return empty(), err
	}
	cleaned, err := relativePath(request.Input.Path)
	if err != nil {
		return empty(), err
	}
	result, err := p.filesystem.ExecuteFilesystem(ctx, connector.FilesystemRequest{Root: configString(request.Connection.Config["storage_root"]), Path: cleaned, Operation: connector.FilesystemOperationRead, MaxReadBytes: int64(integer(request.Connection.Config["max_file_bytes"], defaultMaxBytes))})
	if err != nil {
		return empty(), connector.RetryableError("local.read_failed", err)
	}
	contentType, _ := fileContentType(cleaned, result.Content, "")
	return connector.TypedResult[map[string]any]{Output: map[string]any{"path": cleaned, "content": string(result.Content), "bytes": len(result.Content), "content_type": contentType}, ResponseRef: "file:" + cleaned}, nil
}

func (p *provider) delete(ctx context.Context, request connector.TypedRequest[PathInput]) (connector.TypedResult[map[string]any], error) {
	if !boolean(request.Connection.Config["allow_delete"], false) {
		return empty(), permanent("delete_forbidden", "delete is disabled")
	}
	if err := p.ValidateConfig(request.Connection); err != nil {
		return empty(), err
	}
	cleaned, err := relativePath(request.Input.Path)
	if err != nil {
		return empty(), err
	}
	_, err = p.filesystem.ExecuteFilesystem(ctx, connector.FilesystemRequest{Root: configString(request.Connection.Config["storage_root"]), Path: cleaned, Operation: connector.FilesystemOperationDelete})
	if err != nil {
		return empty(), connector.UncertainError("local.delete_failed", err)
	}
	return connector.TypedResult[map[string]any]{Output: map[string]any{"path": cleaned, "deleted": true}, ResponseRef: "file:" + cleaned}, nil
}

func relativePath(value string) (string, error) {
	value = strings.TrimSpace(strings.ReplaceAll(value, `\`, "/"))
	if value == "" || strings.HasPrefix(value, "/") {
		return "", permanent("path_invalid", "path must be relative")
	}
	cleaned := path.Clean(value)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", permanent("path_invalid", "path escapes the configured root")
	}
	return cleaned, nil
}

func fileContentType(filename string, content []byte, declared string) (string, error) {
	if declared = strings.TrimSpace(declared); declared != "" {
		mediaType, _, err := mime.ParseMediaType(declared)
		if err != nil {
			return "", permanent("content_type_invalid", "content_type is invalid")
		}
		return mediaType, nil
	}
	if inferred := mime.TypeByExtension(path.Ext(filename)); inferred != "" {
		mediaType, _, _ := mime.ParseMediaType(inferred)
		return mediaType, nil
	}
	return http.DetectContentType(content), nil
}

func configString(value any) string {
	result := strings.TrimSpace(fmt.Sprint(value))
	if result == "<nil>" {
		return ""
	}
	return result
}

func integer(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		parsed, err := typed.Int64()
		if err == nil {
			return int(parsed)
		}
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(typed))
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func boolean(value any, fallback bool) bool {
	if typed, ok := value.(bool); ok {
		return typed
	}
	parsed, err := strconv.ParseBool(configString(value))
	if err != nil {
		return fallback
	}
	return parsed
}

func empty() connector.TypedResult[map[string]any] { return connector.TypedResult[map[string]any]{} }
func permanent(suffix, message string) error {
	return connector.PermanentError("local."+suffix, errors.New(message))
}
