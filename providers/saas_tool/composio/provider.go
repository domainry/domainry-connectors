// Package composio executes explicitly mapped SaaS tools through Composio.
// Runtime owns connection authorization, secrets, transport and durable delivery.
package composio

import (
	"context"
	"encoding/json"
	"errors"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	ConnectorKey = "saas_tool"
	ProviderKey  = "composio"
)

// ToolInput contains a connection-owned alias, never an account, URL or version.
type ToolInput struct {
	ToolKey   string                     `json:"tool_key"`
	Arguments map[string]json.RawMessage `json:"arguments,omitempty"`
}

type ToolOutput struct {
	Data  json.RawMessage `json:"data"`
	LogID string          `json:"log_id"`
}

type ConnectionOutput struct {
	Connected          bool   `json:"connected"`
	ConnectedAccountID string `json:"connected_account_id"`
	UserID             string `json:"user_id"`
	Toolkit            string `json:"toolkit"`
}

var (
	TestConnection = connector.CallOperation[struct{}, ConnectionOutput]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection", ContractSHA256: testConnectionHash, Reliability: reliability(connector.EffectRead)}
	QueryTool      = connector.CallOperation[ToolInput, ToolOutput]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "query_tool", ContractSHA256: queryToolHash, Reliability: reliability(connector.EffectRead)}
	StartTool      = connector.StartOperation[ToolInput]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "start_tool", ContractSHA256: startToolHash, Reliability: reliability(connector.EffectWrite)}
	EnqueueTool    = connector.EnqueueOperation[ToolInput]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "enqueue_tool", ContractSHA256: enqueueToolHash, Reliability: reliability(connector.EffectWrite)}
)

func reliability(effect connector.OperationEffect) connector.ReliabilityContract {
	idempotency := connector.IdempotencyNone
	if effect == connector.EffectRead {
		idempotency = connector.IdempotencyNatural
	}
	return connector.ReliabilityContract{Effect: effect, Idempotency: connector.IdempotencyContract{Strategy: idempotency}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Composio transport is required")
	}
	p := &provider{transport: transport}
	test, err := connector.BindCall(TestConnection, p.testConnection)
	if err != nil {
		return nil, err
	}
	query, err := connector.BindCall(QueryTool, p.queryTool)
	if err != nil {
		return nil, err
	}
	start, err := connector.BindStartOperationDelivery(StartTool, p.startTool)
	if err != nil {
		return nil, err
	}
	enqueue, err := connector.BindEnqueueDelivery(EnqueueTool, p.enqueueTool)
	if err != nil {
		return nil, err
	}
	p.Adapter, err = connector.NewProvider(schema(), test, query, start, enqueue)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	_, err := readSettings(connection)
	return err
}

func (p *provider) testConnection(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[ConnectionOutput], error) {
	s, err := readSettings(r.Connection)
	if err != nil {
		return connector.TypedResult[ConnectionOutput]{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	output, err := p.account(ctx, s, r.Secrets)
	if err != nil {
		return connector.TypedResult[ConnectionOutput]{}, err
	}
	return connector.TypedResult[ConnectionOutput]{Output: output, ResponseRef: "composio:account:" + s.AccountID}, nil
}

func (p *provider) TestConnection(ctx context.Context, r connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.testConnection(ctx, connector.TypedRequest[struct{}]{Connection: r.Connection, Secrets: r.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, err := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: result.Output.Connected, Details: details}, err
}

func (p *provider) queryTool(ctx context.Context, r connector.TypedRequest[ToolInput]) (connector.TypedResult[ToolOutput], error) {
	return p.execute(ctx, r, "read")
}

func (p *provider) startTool(ctx context.Context, r connector.TypedRequest[ToolInput]) (connector.DeliveryResult, error) {
	return p.enqueueTool(ctx, r)
}

func (p *provider) enqueueTool(ctx context.Context, r connector.TypedRequest[ToolInput]) (connector.DeliveryResult, error) {
	if !r.Delivery {
		return connector.DeliveryResult{}, permanent("delivery_required", "write tools must run through Runtime durable delivery")
	}
	result, err := p.execute(ctx, r, "write")
	return connector.DeliveryResult{ResponseRef: result.ResponseRef}, err
}

func (p *provider) execute(ctx context.Context, r connector.TypedRequest[ToolInput], effect string) (connector.TypedResult[ToolOutput], error) {
	s, err := readSettings(r.Connection)
	if err != nil {
		return connector.TypedResult[ToolOutput]{}, err
	}
	tool, ok := s.Tools[r.Input.ToolKey]
	if !ok {
		return connector.TypedResult[ToolOutput]{}, permanent("tool_not_allowed", "tool_key is not configured for this connection")
	}
	if tool.Effect != effect {
		return connector.TypedResult[ToolOutput]{}, permanent("effect_mismatch", "tool mapping does not allow this operation's effect")
	}
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	// Recheck ownership and health on every call; no global account cache.
	if _, err := p.account(ctx, s, r.Secrets); err != nil {
		return connector.TypedResult[ToolOutput]{}, err
	}
	return p.executeTool(ctx, s, tool, r.Secrets, r.Input.Arguments)
}

func permanent(code, message string) error {
	return connector.PermanentError("composio."+code, errors.New(message))
}
