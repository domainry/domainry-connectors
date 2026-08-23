// Package tally implements the official Tally lead-source Provider.
package tally

import (
	"context"
	"encoding/json"
	"errors"

	connector "github.com/domainry/domainry-connector-sdk"
	tallyprotocol "github.com/domainry/domainry-connectors/internal/tally"
)

const (
	ConnectorKey = "lead_source"
	ProviderKey  = "tally"
)

var TestConnection = connector.CallOperation[struct{}, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection", ContractSHA256: "6f96577a2f01dc10aac94035a8d3c511677014e6443cd099c176017da1edac5f", Reliability: connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}}

type provider struct {
	connector.Adapter
	protocol tallyprotocol.Protocol
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Tally lead-source transport is required")
	}
	p := &provider{protocol: tallyprotocol.Protocol{Transport: transport}}
	operation, err := connector.BindCall(TestConnection, p.callTestConnection)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), operation)
	if err != nil {
		return nil, err
	}
	p.Adapter = bound
	return p, nil
}

func schema() connector.ProviderSchema {
	minimum, maximum := float64(1), float64(120)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{
		{Key: "account_id", Name: "Account ID", Type: connector.ConfigFieldText},
		{Key: "audience_id", Name: "Audience ID", Type: connector.ConfigFieldText},
		{Key: "campaign_field", Name: "Campaign field", Type: connector.ConfigFieldText},
		{Key: "default_country", Name: "Default country", Type: connector.ConfigFieldText},
		{Key: "default_language", Name: "Default language", Type: connector.ConfigFieldText},
		{Key: "default_owner_queue", Name: "Default owner queue", Type: connector.ConfigFieldText},
		{Key: "form_id", Name: "Form ID", Type: connector.ConfigFieldText, Required: true},
		{Key: "lead_score_field", Name: "Lead score field", Type: connector.ConfigFieldText},
		{Key: "base_url", Name: "Tally API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://api.tally.so"`)},
		{Key: "api_version", Name: "Tally API version", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"2026-02-05"`)},
		{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}},
	}, SecretFields: []connector.SecretField{
		{Key: "api_token", Name: "Tally API token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound},
		{Key: "webhook_secret", Name: "Webhook signing secret", Required: true, CredentialKind: connector.SecretCredentialSigningSecret, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound},
	}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	return p.protocol.Validate(connection)
}
func (p *provider) callTestConnection(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[map[string]any], error) {
	payload, ref, err := p.protocol.TestConnection(ctx, request.Connection, request.Secrets)
	return connector.TypedResult[map[string]any]{Output: map[string]any{"connected": err == nil, "form": payload}, ResponseRef: ref}, err
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	payload, ref, err := p.protocol.TestConnection(ctx, request.Connection, request.Secrets)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, err := json.Marshal(map[string]any{"form": payload, "response_ref": ref})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}
func (*provider) VerifyWebhook(ctx context.Context, request connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	return tallyprotocol.VerifyWebhook(ctx, request)
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
var _ connector.WebhookVerifier = (*provider)(nil)
