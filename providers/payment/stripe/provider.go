package stripe

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	ConnectorKey = "payment"
	ProviderKey  = "stripe"
)

//go:embed descriptor.json
var descriptorJSON []byte

// New constructs the complete Stripe Provider without performing I/O.
func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Stripe transport is required")
	}
	var descriptor connector.ProviderDescriptor
	if err := json.Unmarshal(descriptorJSON, &descriptor); err != nil {
		return nil, err
	}
	if err := descriptor.Validate(); err != nil {
		return nil, err
	}
	return &provider{descriptor: descriptor, transport: transport}, nil
}

func (a *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := a.callTestConnection(ctx, connector.CallRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: "test_connection",
		Connection: request.Connection, Secrets: request.Secrets, Principal: request.Principal,
	})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: result.Payload}, nil
}

var _ connector.ConnectionTester = (*provider)(nil)
