package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
)

type httpClient struct {
	transport                            connector.Transport
	connection                           connector.Connection
	secrets                              map[string]string
	endpoint, sessionID, protocolVersion string
	nextID                               int
}

func newHTTPClient(transport connector.Transport, connection connector.Connection, secrets map[string]string) (*httpClient, error) {
	endpoint, err := endpoint(connection)
	if err != nil {
		return nil, err
	}
	version := config(connection, "protocol_version")
	if version == "" {
		version = defaultProtocolVersion
	}
	return &httpClient{transport: transport, connection: connection, secrets: secrets, endpoint: endpoint, protocolVersion: version}, nil
}
func (*httpClient) close(context.Context) error { return nil }

func (client *httpClient) request(ctx context.Context, method string, params map[string]any) (map[string]any, error) {
	client.nextID++
	response, _, err := client.post(ctx, map[string]any{"jsonrpc": "2.0", "id": client.nextID, "method": method, "params": params}, method, clean(params["name"]))
	if err != nil {
		return nil, err
	}
	if rpcError, ok := response["error"].(map[string]any); ok {
		return nil, fmt.Errorf("MCP RPC error %s", clean(rpcError["code"]))
	}
	result, ok := response["result"].(map[string]any)
	if !ok {
		return nil, errors.New("MCP result is invalid")
	}
	return result, nil
}

func (client *httpClient) notify(ctx context.Context, method string, params map[string]any) error {
	_, status, err := client.post(ctx, map[string]any{"jsonrpc": "2.0", "method": method, "params": params}, method, "")
	if err != nil {
		return err
	}
	if status != http.StatusAccepted && status != http.StatusNoContent && status != http.StatusOK {
		return fmt.Errorf("MCP returned HTTP %d", status)
	}
	return nil
}

func (client *httpClient) post(ctx context.Context, payload map[string]any, method, name string) (map[string]any, int, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, 0, err
	}
	headers := map[string][]string{"Content-Type": {"application/json"}, "Accept": {"application/json, text/event-stream"}, "Mcp-Protocol-Version": {client.protocolVersion}, "Mcp-Method": {method}}
	if name != "" {
		headers["Mcp-Name"] = []string{name}
	}
	if client.sessionID != "" {
		headers["Mcp-Session-Id"] = []string{client.sessionID}
	}
	for key, value := range customHeaders(client.connection.Config["headers"]) {
		headers[key] = []string{value}
	}
	secretHeaders := map[string][]string{}
	if authorization := resolvedAuthorization(client.connection, client.secrets); authorization != "" {
		secretHeaders["Authorization"] = []string{authorization}
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout(client.connection))
	defer cancel()
	response, err := client.transport.RoundTripHTTP(requestCtx, connector.HTTPRequest{
		Method: http.MethodPost, URL: client.endpoint, Headers: headers,
		SecretHeaders: secretHeaders, Body: raw, MaxResponseBytes: responseLimit,
	})
	if err != nil {
		return nil, 0, err
	}
	if values := response.Headers["Mcp-Session-Id"]; len(values) > 0 {
		client.sessionID = strings.TrimSpace(values[0])
	}
	if response.StatusCode == http.StatusAccepted || response.StatusCode == http.StatusNoContent {
		return map[string]any{}, response.StatusCode, nil
	}
	if response.StatusCode/100 != 2 {
		return nil, response.StatusCode, fmt.Errorf("MCP returned HTTP %d", response.StatusCode)
	}
	decoded, err := decodeRPCResponse(response.Body, firstHeader(response.Headers, "Content-Type"))
	return decoded, response.StatusCode, err
}

func decodeRPCResponse(body []byte, contentType string) (map[string]any, error) {
	if strings.Contains(strings.ToLower(contentType), "text/event-stream") {
		for _, line := range bytes.Split(body, []byte("\n")) {
			line = bytes.TrimSpace(line)
			if bytes.HasPrefix(line, []byte("data:")) {
				body = bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
				break
			}
		}
	}
	response := map[string]any{}
	if json.Unmarshal(body, &response) != nil {
		return nil, errors.New("MCP response is invalid")
	}
	return response, nil
}

func customHeaders(value any) map[string]string {
	input, _ := value.(map[string]any)
	out := map[string]string{}
	for key, current := range input {
		canonical := http.CanonicalHeaderKey(strings.TrimSpace(key))
		text := clean(current)
		if canonical != "" && safeCustomHeader(canonical) && !strings.ContainsAny(canonical+text, "\r\n") {
			out[canonical] = text
		}
	}
	return out
}

func safeCustomHeader(key string) bool {
	switch strings.ToLower(key) {
	case "authorization", "content-type", "accept", "mcp-protocol-version", "mcp-session-id", "mcp-method", "mcp-name", "host", "origin":
		return false
	}
	return true
}

func resolvedAuthorization(connection connector.Connection, secrets map[string]string) string {
	for _, key := range []string{"authorization", "header_authorization"} {
		if value := strings.TrimSpace(secrets[key]); value != "" {
			return value
		}
	}
	if value := strings.TrimSpace(secrets["api_key"]); value != "" {
		return "Bearer " + value
	}
	return ""
}

func firstHeader(headers map[string][]string, key string) string {
	for name, values := range headers {
		if strings.EqualFold(name, key) && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}
