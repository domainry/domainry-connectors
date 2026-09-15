// Package mcp implements the Model Context Protocol Provider using
// Runtime-governed HTTP and subprocess transports.
package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/mcptool"
)

const (
	ConnectorKey           = mcptool.ConnectorKey
	ProviderKey            = mcptool.ProviderKey
	defaultProtocolVersion = "2025-06-18"
	responseLimit          = int64(4 << 20)
)

type CallToolInput = mcptool.CallToolRequest

var (
	TestConnection = readOperation[struct{}](mcptool.TestConnectionOperationKey, mcptool.TestConnectionOperationSHA256)
	ListTools      = readOperation[mcptool.ListToolsRequest](mcptool.ListToolsOperationKey, mcptool.ListToolsOperationSHA256)
	CallTool       = connector.CallOperation[CallToolInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: mcptool.CallToolOperationKey, ContractSHA256: mcptool.CallToolOperationSHA256, Reliability: reliability(connector.EffectWrite, connector.IdempotencyNone)}
)

func readOperation[I any](key, hash string) connector.CallOperation[I, map[string]any] {
	return connector.CallOperation[I, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: key, ContractSHA256: hash, Reliability: reliability(connector.EffectRead, connector.IdempotencyNatural)}
}
func reliability(effect connector.OperationEffect, idempotency connector.IdempotencyStrategy) connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: effect, Idempotency: connector.IdempotencyContract{Strategy: idempotency}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

// MCP credentials are explicit connection fields/secrets rather than an OAuth
// grant managed by Integration. A declared empty alternative lets the account
// owner distinguish this from an operation that forgot to declare its scope.
func (*provider) OAuthOperationScopes(key string) ([][]string, bool) {
	switch key {
	case mcptool.ListToolsOperationKey, mcptool.CallToolOperationKey:
		return [][]string{{}}, true
	default:
		return nil, false
	}
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("MCP transport is required")
	}
	p := &provider{transport: transport}
	test, err := connector.BindCall(TestConnection, p.test)
	if err != nil {
		return nil, err
	}
	list, err := connector.BindCall(ListTools, p.list)
	if err != nil {
		return nil, err
	}
	call, err := connector.BindCall(CallTool, p.call)
	if err != nil {
		return nil, err
	}
	adapter, err := connector.NewProvider(schema(), test, list, call)
	if err != nil {
		return nil, err
	}
	p.Adapter = adapter
	return p, nil
}

func schema() connector.ProviderSchema {
	minimum, maximum := float64(1), float64(300)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{
		{Key: "transport", Name: "Transport", Type: connector.ConfigFieldSelect, Required: true, Default: json.RawMessage(`"http"`), Validation: connector.ConfigValidation{Options: []string{"http", "stdio"}}},
		{Key: "url", Name: "MCP Endpoint URL", Type: connector.ConfigFieldText},
		{Key: "command", Name: "Command", Type: connector.ConfigFieldText},
		{Key: "args", Name: "Arguments", Type: connector.ConfigFieldJSON},
		{Key: "allow_stdio", Name: "Allow Local Process", Type: connector.ConfigFieldBoolean, Default: json.RawMessage(`false`)},
		{Key: "working_directory", Name: "Working Directory", Type: connector.ConfigFieldText},
		{Key: "protocol_version", Name: "Protocol Version", Type: connector.ConfigFieldText, Default: json.RawMessage(`"2025-06-18"`)},
		{Key: "headers", Name: "Custom Headers", Type: connector.ConfigFieldJSON},
		{Key: "allowed_tools", Name: "Allowed Tools", Type: connector.ConfigFieldJSON, Required: true},
		{Key: "requires_approval", Name: "Require Approval", Type: connector.ConfigFieldBoolean, Default: json.RawMessage(`true`)},
		{Key: "timeout_seconds", Name: "Timeout Seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}},
	}, SecretFields: []connector.SecretField{secret("api_key", "API Key"), secret("authorization", "Authorization Header"), secret("header_authorization", "Legacy Authorization Header")}}
}
func secret(key, name string) connector.SecretField {
	return connector.SecretField{Key: key, Name: name, CredentialKind: connector.SecretCredentialAPIKey, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	if len(stringList(connection.Config["allowed_tools"])) == 0 {
		return permanent("allowed_tools_required", "allowed_tools is required")
	}
	timeout := integer(connection.Config["timeout_seconds"], 30)
	if timeout < 1 || timeout > 300 {
		return permanent("timeout_invalid", "timeout_seconds is outside its supported range")
	}
	switch config(connection, "transport") {
	case "http":
		_, err := endpoint(connection)
		return err
	case "stdio":
		if !boolean(connection.Config["allow_stdio"], false) {
			return permanent("stdio_not_explicitly_allowed", "allow_stdio must be true")
		}
		command := config(connection, "command")
		if command == "" || !filepath.IsAbs(command) || strings.ContainsAny(command, "\r\n\x00") {
			return permanent("absolute_command_required", "command must be an absolute path")
		}
		if directory := config(connection, "working_directory"); directory != "" && !filepath.IsAbs(directory) {
			return permanent("working_directory_must_be_absolute", "working_directory must be absolute")
		}
		if _, ok := p.transport.(connector.ProcessTransport); !ok {
			return permanent("process_transport_required", "Runtime process capability is unavailable")
		}
		return nil
	default:
		return permanent("transport_unsupported", "transport must be http or stdio")
	}
}

func (p *provider) test(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[map[string]any], error) {
	client, initialized, err := p.open(ctx, request.Connection, request.Secrets)
	if err != nil {
		return empty(), err
	}
	defer client.close(ctx)
	result, err := client.request(ctx, "tools/list", map[string]any{})
	if err != nil {
		return empty(), connector.RetryableError("mcp.tools_list_failed", err)
	}
	tools := filterAllowedTools(result["tools"], stringList(request.Connection.Config["allowed_tools"]))
	return connector.TypedResult[map[string]any]{Output: map[string]any{"connected": true, "protocol_version": initialized.protocolVersion, "server_info": initialized.serverInfo, "tool_count": len(tools)}, ResponseRef: "mcp:tools/list"}, nil
}

func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.test(ctx, connector.TypedRequest[struct{}]{Connection: request.Connection, Secrets: request.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) list(ctx context.Context, request connector.TypedRequest[mcptool.ListToolsRequest]) (connector.TypedResult[map[string]any], error) {
	client, _, err := p.open(ctx, request.Connection, request.Secrets)
	if err != nil {
		return empty(), err
	}
	defer client.close(ctx)
	allowed, seen, all := stringList(request.Connection.Config["allowed_tools"]), map[string]bool{}, []any{}
	cursor, complete := "", true
	for page := 0; page < 8; page++ {
		params := map[string]any{}
		if cursor != "" {
			params["cursor"] = cursor
		}
		result, err := client.request(ctx, "tools/list", params)
		if err != nil {
			return empty(), connector.RetryableError("mcp.tools_list_failed", err)
		}
		for _, item := range filterAllowedTools(result["tools"], allowed) {
			tool, _ := item.(map[string]any)
			name := clean(tool["name"])
			if name == "" || seen[name] {
				continue
			}
			seen[name] = true
			all = append(all, item)
			if len(all) == 64 {
				complete = false
				break
			}
		}
		if !complete {
			break
		}
		next := clean(result["nextCursor"])
		if next == "" {
			break
		}
		if next == cursor || page == 7 {
			complete = false
			break
		}
		cursor = next
	}
	return connector.TypedResult[map[string]any]{Output: map[string]any{"tools": all, "complete": complete}, ResponseRef: "mcp:tools/list"}, nil
}

func (p *provider) call(ctx context.Context, request connector.TypedRequest[CallToolInput]) (connector.TypedResult[map[string]any], error) {
	name := strings.TrimSpace(request.Input.ToolName)
	allowed := stringList(request.Connection.Config["allowed_tools"])
	if name == "" || !toolAllowed(name, allowed) {
		return empty(), permanent("tool_not_allowed", "tool is not allowlisted")
	}
	if boolean(request.Connection.Config["requires_approval"], true) && !request.Input.Approved {
		return empty(), permanent("approval_required", "tool call requires approval")
	}
	client, _, err := p.open(ctx, request.Connection, request.Secrets)
	if err != nil {
		return empty(), err
	}
	defer client.close(ctx)
	result, err := client.request(ctx, "tools/call", map[string]any{"name": name, "arguments": request.Input.Arguments})
	ref := "mcp:tools/call:" + name
	if err != nil {
		return connector.TypedResult[map[string]any]{ResponseRef: ref}, connector.UncertainError("mcp.tool_outcome_unknown", err)
	}
	result["tool_name"] = name
	if boolean(result["isError"], false) {
		return connector.TypedResult[map[string]any]{Output: result, ResponseRef: ref}, permanent("tool_failed", "MCP tool returned isError")
	}
	return connector.TypedResult[map[string]any]{Output: result, ResponseRef: ref}, nil
}

type initializeResult struct {
	protocolVersion string
	serverInfo      map[string]any
}
type rpcClient interface {
	request(context.Context, string, map[string]any) (map[string]any, error)
	close(context.Context) error
}

func (p *provider) open(ctx context.Context, connection connector.Connection, secrets map[string]string) (rpcClient, initializeResult, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, initializeResult{}, err
	}
	var client rpcClient
	var err error
	switch config(connection, "transport") {
	case "http":
		client, err = newHTTPClient(p.transport, connection, secrets)
	case "stdio":
		client, err = newStdioClient(ctx, p.transport.(connector.ProcessTransport), connection)
	}
	if err != nil {
		return nil, initializeResult{}, err
	}
	version := config(connection, "protocol_version")
	if version == "" {
		version = defaultProtocolVersion
	}
	result, err := client.request(ctx, "initialize", map[string]any{"protocolVersion": version, "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "domainry-runtime", "version": "1.0.0"}})
	if err != nil {
		_ = client.close(ctx)
		return nil, initializeResult{}, connector.RetryableError("mcp.initialize_failed", err)
	}
	negotiated := clean(result["protocolVersion"])
	if negotiated == "" {
		_ = client.close(ctx)
		return nil, initializeResult{}, permanent("initialize_response_invalid", "protocolVersion is missing")
	}
	if err := notify(ctx, client, "notifications/initialized", map[string]any{}); err != nil {
		_ = client.close(ctx)
		return nil, initializeResult{}, connector.RetryableError("mcp.initialize_notification_failed", err)
	}
	serverInfo, _ := result["serverInfo"].(map[string]any)
	return client, initializeResult{protocolVersion: negotiated, serverInfo: serverInfo}, nil
}

func notify(ctx context.Context, client rpcClient, method string, params map[string]any) error {
	if notifying, ok := client.(interface {
		notify(context.Context, string, map[string]any) error
	}); ok {
		return notifying.notify(ctx, method, params)
	}
	return errors.New("MCP client cannot send notifications")
}

func endpoint(connection connector.Connection) (string, error) {
	parsed, err := url.Parse(config(connection, "url"))
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && (parsed.Hostname() == "localhost" || parsed.Hostname() == "127.0.0.1" || parsed.Hostname() == "::1"))) {
		return "", permanent("endpoint_invalid", "MCP endpoint must use HTTPS unless it targets loopback")
	}
	return parsed.String(), nil
}
func filterAllowedTools(value any, allowed []string) []any {
	items, _ := value.([]any)
	out := []any{}
	for _, item := range items {
		tool, ok := item.(map[string]any)
		if ok && toolAllowed(clean(tool["name"]), allowed) {
			out = append(out, tool)
		}
	}
	return out
}
func toolAllowed(name string, allowed []string) bool {
	for _, candidate := range allowed {
		if candidate == "*" || candidate == name {
			return true
		}
	}
	return false
}
func config(connection connector.Connection, key string) string { return clean(connection.Config[key]) }
func clean(value any) string {
	result := strings.TrimSpace(fmt.Sprint(value))
	if result == "<nil>" {
		return ""
	}
	return result
}
func stringList(value any) []string {
	switch typed := value.(type) {
	case []string:
		return append([]string(nil), typed...)
	case []any:
		out := []string{}
		for _, item := range typed {
			if text := clean(item); text != "" {
				out = append(out, text)
			}
		}
		return out
	case string:
		parts := strings.Split(typed, ",")
		out := []string{}
		for _, part := range parts {
			if text := strings.TrimSpace(part); text != "" {
				out = append(out, text)
			}
		}
		return out
	}
	return nil
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
		if parsed, err := typed.Int64(); err == nil {
			return int(parsed)
		}
	case string:
		if parsed, err := strconv.Atoi(strings.TrimSpace(typed)); err == nil {
			return parsed
		}
	}
	return fallback
}
func boolean(value any, fallback bool) bool {
	if typed, ok := value.(bool); ok {
		return typed
	}
	if parsed, err := strconv.ParseBool(clean(value)); err == nil {
		return parsed
	}
	return fallback
}
func timeout(connection connector.Connection) time.Duration {
	return time.Duration(integer(connection.Config["timeout_seconds"], 30)) * time.Second
}
func empty() connector.TypedResult[map[string]any] { return connector.TypedResult[map[string]any]{} }
func permanent(suffix, message string) error {
	return connector.PermanentError("mcp."+suffix, errors.New(message))
}
