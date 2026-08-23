// Package plaid implements the official Plaid bank-reconciliation Provider.
package plaid

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	ConnectorKey         = "bank_reconciliation"
	ProviderKey          = "plaid"
	defaultBaseURL       = "https://production.plaid.com"
	responseLimit  int64 = 4 << 20
)

type SyncTransactionsInput struct {
	Cursor string `json:"cursor,omitempty"`
	Count  int    `json:"count,omitempty"`
}
type PlaidResponse map[string]any

var (
	GetAccounts      = connector.CallOperation[struct{}, PlaidResponse]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "get_accounts", ContractSHA256: "2b9c30e77e81764fbf1fa539b1082ba8e9e8c8aa7d2abb210aac68abe34ac827", Reliability: readReliability()}
	SyncTransactions = connector.CallOperation[SyncTransactionsInput, PlaidResponse]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "sync_transactions", ContractSHA256: "e0d805dd5a1f5191a59b108b640a6958b6be2b69d646d5db8506a4a45486a6e6", Reliability: readReliability()}
	TestConnection   = connector.CallOperation[struct{}, PlaidResponse]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection", ContractSHA256: "ab98da9d50679388e48506edf0c31abcc58a9c55ddc278ec13c220eb9d7d2e07", Reliability: readReliability()}
)

func readReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Plaid transport is required")
	}
	p := &provider{transport: transport}
	accounts, err := connector.BindCall(GetAccounts, p.getAccounts)
	if err != nil {
		return nil, err
	}
	syncTransactions, err := connector.BindCall(SyncTransactions, p.syncTransactions)
	if err != nil {
		return nil, err
	}
	test, err := connector.BindCall(TestConnection, p.callTestConnection)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), accounts, syncTransactions, test)
	if err != nil {
		return nil, err
	}
	p.Adapter = bound
	return p, nil
}

func schema() connector.ProviderSchema {
	minimum, maximum := float64(1), float64(120)
	secret := func(key, name string, kind connector.SecretCredentialKind) connector.SecretField {
		return connector.SecretField{Key: key, Name: name, Required: true, CredentialKind: kind, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}
	}
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{
		{Key: "base_url", Name: "Plaid API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://production.plaid.com"`)},
		{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}},
	}, SecretFields: []connector.SecretField{secret("client_id", "Client ID", connector.SecretCredentialIdentifier), secret("client_secret", "Client secret", connector.SecretCredentialOAuthClientSecret), secret("access_token", "Item access token", connector.SecretCredentialBearerToken)}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(baseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return permanent("endpoint_invalid", "valid Plaid endpoint is required")
	}
	if parsed.Scheme == "http" && isLoopback(parsed.Hostname()) {
		return nil
	}
	if parsed.Scheme != "https" {
		return permanent("endpoint_invalid", "Plaid endpoint must use HTTPS")
	}
	for _, host := range []string{"production.plaid.com", "sandbox.plaid.com"} {
		if strings.EqualFold(parsed.Hostname(), host) {
			return nil
		}
	}
	return permanent("endpoint_invalid", "official Plaid environment or loopback HTTP is required")
}

func (p *provider) getAccounts(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[PlaidResponse], error) {
	return p.execute(ctx, request.Connection, request.Secrets, "/accounts/get", nil)
}
func (p *provider) syncTransactions(ctx context.Context, request connector.TypedRequest[SyncTransactionsInput]) (connector.TypedResult[PlaidResponse], error) {
	if request.Input.Count < 0 || request.Input.Count > 500 {
		return connector.TypedResult[PlaidResponse]{}, permanent("count_invalid", "count must be between 1 and 500 when set")
	}
	payload := map[string]any{}
	if cursor := strings.TrimSpace(request.Input.Cursor); cursor != "" {
		payload["cursor"] = cursor
	}
	if request.Input.Count > 0 {
		payload["count"] = request.Input.Count
	}
	return p.execute(ctx, request.Connection, request.Secrets, "/transactions/sync", payload)
}
func (p *provider) callTestConnection(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[PlaidResponse], error) {
	return p.execute(ctx, request.Connection, request.Secrets, "/accounts/get", nil)
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.execute(ctx, request.Connection, request.Secrets, "/accounts/get", nil)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, marshalErr := json.Marshal(map[string]any{"accounts": result.Output["accounts"], "item": result.Output["item"], "response_ref": result.ResponseRef})
	if marshalErr != nil {
		return connector.TestConnectionResult{}, marshalErr
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, path string, input map[string]any) (connector.TypedResult[PlaidResponse], error) {
	if err := p.ValidateConfig(connection); err != nil {
		return connector.TypedResult[PlaidResponse]{}, err
	}
	secretJSON := map[string]string{}
	for source, target := range map[string]string{"client_id": "client_id", "client_secret": "secret", "access_token": "access_token"} {
		value := strings.TrimSpace(secrets[source])
		if value == "" {
			return connector.TypedResult[PlaidResponse]{}, permanent(source+"_required", "resolved "+source+" is required")
		}
		secretJSON[target] = value
	}
	body, err := json.Marshal(input)
	if err != nil {
		return connector.TypedResult[PlaidResponse]{}, connector.PermanentError("plaid.request_invalid", err)
	}
	if input == nil {
		body = []byte(`{}`)
	}
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodPost, URL: baseURL(connection) + path, Headers: map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/json"}}, SecretJSON: secretJSON, Body: body, MaxResponseBytes: responseLimit})
	if err != nil {
		return connector.TypedResult[PlaidResponse]{}, connector.RetryableError("plaid.network_error", err)
	}
	ref, payload := "http:"+strconv.Itoa(response.StatusCode), PlaidResponse{}
	validJSON := json.Unmarshal(response.Body, &payload) == nil
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code, cause := errorCode(payload, response.StatusCode), fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			return connector.TypedResult[PlaidResponse]{Output: payload, ResponseRef: ref}, connector.RetryableError(code, cause)
		}
		return connector.TypedResult[PlaidResponse]{Output: payload, ResponseRef: ref}, connector.PermanentError(code, cause)
	}
	if !validJSON {
		return connector.TypedResult[PlaidResponse]{ResponseRef: ref}, permanent("response_invalid", "provider response is invalid JSON")
	}
	if id := mapString(payload, "request_id"); id != "" {
		ref = "plaid:request:" + id
	}
	return connector.TypedResult[PlaidResponse]{Output: payload, ResponseRef: ref}, nil
}

func baseURL(connection connector.Connection) string {
	if value := strings.TrimRight(mapString(connection.Config, "base_url"), "/"); value != "" {
		return value
	}
	return defaultBaseURL
}
func mapString(values map[string]any, key string) string {
	if values[key] == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(values[key]))
}
func errorCode(payload PlaidResponse, status int) string {
	if code := mapString(payload, "error_code"); code != "" {
		return "plaid.provider_" + strings.ToLower(code)
	}
	return "plaid.http_" + strconv.Itoa(status)
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
func permanent(code, message string) error {
	return connector.PermanentError("plaid."+code, errors.New(message))
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
