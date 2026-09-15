package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
	"github.com/domainry/domainry-connector-sdk/mcptool"
)

type mcpTransport struct {
	requests []connector.HTTPRequest
	session  *mcpSession
	paginate bool
}

func (transport *mcpTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	transport.requests = append(transport.requests, request)
	var payload map[string]any
	_ = json.Unmarshal(request.Body, &payload)
	method, _ := payload["method"].(string)
	if method == "notifications/initialized" {
		return connector.HTTPResponse{StatusCode: http.StatusAccepted}, nil
	}
	id := payload["id"]
	result := map[string]any{}
	switch method {
	case "initialize":
		result = map[string]any{"protocolVersion": defaultProtocolVersion, "serverInfo": map[string]any{"name": "fixture"}}
	case "tools/list":
		params, _ := payload["params"].(map[string]any)
		if transport.paginate && clean(params["cursor"]) == "" {
			result = map[string]any{"tools": []any{map[string]any{"name": "allowed"}}, "nextCursor": "page-2"}
		} else if transport.paginate {
			result = map[string]any{"tools": []any{map[string]any{"name": "second"}}}
		} else {
			result = map[string]any{"tools": []any{map[string]any{"name": "allowed"}, map[string]any{"name": "blocked"}}}
		}
	case "tools/call":
		result = map[string]any{"content": []any{map[string]any{"type": "text", "text": "done"}}}
	}
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
	return connector.HTTPResponse{StatusCode: http.StatusOK, Headers: map[string][]string{"Content-Type": {"application/json"}}, Body: body}, nil
}
func (*mcpTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func (transport *mcpTransport) StartProcess(context.Context, connector.ProcessRequest) (connector.ProcessSession, error) {
	transport.session = &mcpSession{}
	return transport.session, nil
}

type mcpSession struct {
	requests  [][]byte
	responses [][]byte
}

func (session *mcpSession) SendLine(_ context.Context, message []byte) error {
	session.requests = append(session.requests, append([]byte(nil), message...))
	var payload map[string]any
	_ = json.Unmarshal(message, &payload)
	if payload["id"] == nil {
		return nil
	}
	result := map[string]any{}
	switch payload["method"] {
	case "initialize":
		result = map[string]any{"protocolVersion": defaultProtocolVersion}
	case "tools/list":
		result = map[string]any{"tools": []any{map[string]any{"name": "allowed"}}}
	}
	response, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": payload["id"], "result": result})
	session.responses = append(session.responses, response)
	return nil
}
func (session *mcpSession) ReceiveLine(context.Context) ([]byte, error) {
	if len(session.responses) == 0 {
		return nil, errors.New("no response")
	}
	result := session.responses[0]
	session.responses = session.responses[1:]
	return result, nil
}
func (*mcpSession) Close(context.Context) error { return nil }

func TestHTTPMCPUsesRuntimeTransportFiltersToolsAndProtectsAuthorization(t *testing.T) {
	transport := &mcpTransport{}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	result, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: httpConnection(), Secrets: map[string]string{"api_key": "runtime-secret"}})
	if err != nil || !result.Connected {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if len(transport.requests) != 3 {
		t.Fatalf("requests=%d", len(transport.requests))
	}
	for _, request := range transport.requests {
		if len(request.SecretHeaders["Authorization"]) != 1 || request.Headers["Authorization"] != nil {
			t.Fatalf("authorization boundary=%+v", request)
		}
		raw, _ := json.Marshal(request)
		if string(raw) == "" || strings.Contains(string(raw), "runtime-secret") || strings.Contains(string(raw), "Authorization") {
			t.Fatalf("serialized request leaked authorization: %s", raw)
		}
	}
}

func TestMCPAccountOperationsExplicitlyRequireNoOAuthScope(t *testing.T) {
	adapter, err := New(&mcpTransport{})
	if err != nil {
		t.Fatal(err)
	}
	provider := adapter.(connector.OAuthOperationScopeProvider)
	for _, key := range []string{mcptool.ListToolsOperationKey, mcptool.CallToolOperationKey} {
		alternatives, declared := provider.OAuthOperationScopes(key)
		if !declared || len(alternatives) != 1 || len(alternatives[0]) != 0 {
			t.Fatalf("%s scopes=%v declared=%v", key, alternatives, declared)
		}
	}
}

func TestMCPListToolsCollectsEveryAllowedPage(t *testing.T) {
	transport := &mcpTransport{paginate: true}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	connection := httpConnection()
	connection.Config["allowed_tools"] = []any{"allowed", "second"}
	payload, _ := json.Marshal(mcptool.ListToolsRequest{})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListTools.Key, ContractSHA256: ListTools.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	var output struct {
		Tools    []map[string]any `json:"tools"`
		Complete bool             `json:"complete"`
	}
	if json.Unmarshal(result.Payload, &output) != nil || !output.Complete || len(output.Tools) != 2 || output.Tools[0]["name"] != "allowed" || output.Tools[1]["name"] != "second" {
		t.Fatalf("paginated catalog=%s", result.Payload)
	}
	if len(transport.requests) != 4 {
		t.Fatalf("requests=%d", len(transport.requests))
	}
}

func TestStdioMCPRequiresRuntimeProcessCapabilityAndUsesSession(t *testing.T) {
	base := baseTransport{}
	adapter, _ := New(base)
	if err := adapter.(connector.ConfigValidator).ValidateConfig(stdioConnection()); err == nil {
		t.Fatal("accepted stdio without ProcessTransport")
	}
	transport := &mcpTransport{}
	adapter, _ = New(transport)
	payload, _ := json.Marshal(struct{}{})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ListTools.Key, ContractSHA256: ListTools.ContractSHA256, Mode: connector.ModeCall, Connection: stdioConnection(), Payload: payload})
	if err != nil || result.ResponseRef != "mcp:tools/list" || transport.session == nil || len(transport.session.requests) != 3 {
		t.Fatalf("result=%+v session=%+v err=%v", result, transport.session, err)
	}
}

func httpConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"transport": "http", "url": "https://mcp.example.test/mcp", "allowed_tools": []any{"allowed"}, "requires_approval": true, "headers": map[string]any{"X-Project": "one"}}, SecretRefs: map[string]string{"api_key": "secret:key"}}
}
func stdioConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"transport": "stdio", "command": "/opt/domainry/mcp-server", "allow_stdio": true, "allowed_tools": []any{"allowed"}}}
}

type baseTransport struct{}

func (baseTransport) RoundTripHTTP(context.Context, connector.HTTPRequest) (connector.HTTPResponse, error) {
	return connector.HTTPResponse{}, nil
}
func (baseTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, nil
}
