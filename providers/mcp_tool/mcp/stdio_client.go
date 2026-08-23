package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

type stdioClient struct {
	session connector.ProcessSession
	nextID  int
}

func newStdioClient(ctx context.Context, transport connector.ProcessTransport, connection connector.Connection) (*stdioClient, error) {
	session, err := transport.StartProcess(ctx, connector.ProcessRequest{
		Executable: config(connection, "command"), Arguments: stringList(connection.Config["args"]),
		WorkingDirectory: config(connection, "working_directory"), MaxMessageBytes: responseLimit,
		ShutdownGrace: 500 * time.Millisecond,
	})
	if err != nil {
		return nil, err
	}
	return &stdioClient{session: session}, nil
}
func (client *stdioClient) close(ctx context.Context) error { return client.session.Close(ctx) }

func (client *stdioClient) request(ctx context.Context, method string, params map[string]any) (map[string]any, error) {
	client.nextID++
	id := client.nextID
	if err := client.write(ctx, map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}); err != nil {
		return nil, err
	}
	for {
		raw, err := client.session.ReceiveLine(ctx)
		if err != nil {
			return nil, err
		}
		response := map[string]any{}
		if json.Unmarshal(raw, &response) != nil || fmt.Sprint(response["id"]) != fmt.Sprint(id) {
			continue
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
}

func (client *stdioClient) notify(ctx context.Context, method string, params map[string]any) error {
	return client.write(ctx, map[string]any{"jsonrpc": "2.0", "method": method, "params": params})
}

func (client *stdioClient) write(ctx context.Context, payload map[string]any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return client.session.SendLine(ctx, raw)
}
