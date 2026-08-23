// Package adobesign implements the official Adobe Acrobat Sign Provider.
package adobesign

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	ConnectorKey = "esign"
	ProviderKey  = "adobe_sign"
)

type CreateEnvelopeInput struct {
	Input map[string]any `json:"input"`
}
type EnvelopeInput struct {
	EnvelopeID string `json:"envelope_id"`
}
type VoidEnvelopeInput struct {
	EnvelopeID string `json:"envelope_id"`
	Reason     string `json:"reason"`
}
type ListEnvelopeStatusChangesInput struct {
	PageSize int    `json:"page_size,omitempty"`
	Cursor   string `json:"cursor,omitempty"`
	Query    string `json:"query,omitempty"`
}

var (
	CreateEnvelope            = callWrite[CreateEnvelopeInput]("create_envelope", "cffe2007f4352b8542e199f6578f5476b0a0aa7a7bc3ccf3875916e90da8b663")
	CreateRecipientView       = callWrite[EnvelopeInput]("create_recipient_view", "0defebfe26cb6649fb00973fd883fc7a9bf9a20ef911570d206975a49190b045")
	GetEnvelope               = callRead[EnvelopeInput]("get_envelope", "d7dbda5c4ae2787e050cdf5935aed6a8d429a2ebddc93643dcd991aca972d267")
	ListEnvelopeStatusChanges = callRead[ListEnvelopeStatusChangesInput]("list_envelope_status_changes", "f5fd79d5a3011418986efdf1216f47099c8bd45956cab5201b5a62f4c3cbbd6e")
	SendEnvelope              = callWrite[EnvelopeInput]("send_envelope", "4936ba0cd6632815d0548c127cfcb36411edbe98990641559161dfec8b4221df")
	TestConnection            = callRead[struct{}]("test_connection", "973cfc9b2671622a791ab84975fb9aa853730640a998307ae0f66f38847ce6fa")
	VoidEnvelope              = callWrite[VoidEnvelopeInput]("void_envelope", "49b989e409f53805a2135308785ebe4c8170fc16044805aafde6c32f875dcd9f")
)

func callRead[I any](key, hash string) connector.CallOperation[I, map[string]any] {
	return connector.CallOperation[I, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: key, ContractSHA256: hash, Reliability: reliability(connector.EffectRead)}
}
func callWrite[I any](key, hash string) connector.CallOperation[I, map[string]any] {
	return connector.CallOperation[I, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: key, ContractSHA256: hash, Reliability: reliability(connector.EffectWrite)}
}
func reliability(effect connector.OperationEffect) connector.ReliabilityContract {
	strategy := connector.IdempotencyNone
	if effect == connector.EffectRead {
		strategy = connector.IdempotencyNatural
	}
	return connector.ReliabilityContract{Effect: effect, Idempotency: connector.IdempotencyContract{Strategy: strategy}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Adobe Sign transport is required")
	}
	p := &provider{transport: transport}
	operations := make([]connector.BoundOperation, 0, 7)
	bind := func(operation connector.BoundOperation, err error) error {
		operations = append(operations, operation)
		return err
	}
	if err := bind(connector.BindCall(CreateEnvelope, p.createEnvelope)); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(CreateRecipientView, p.createRecipientView)); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(GetEnvelope, p.getEnvelope)); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(ListEnvelopeStatusChanges, p.listEnvelopeStatusChanges)); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(SendEnvelope, p.sendEnvelope)); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(TestConnection, p.testConnection)); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(VoidEnvelope, p.voidEnvelope)); err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), operations...)
	if err != nil {
		return nil, err
	}
	p.Adapter = bound
	return p, nil
}

func schema() connector.ProviderSchema {
	minimum, maximum := float64(1), float64(120)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{
		{Key: "base_url", Name: "Acrobat Sign shard API base URL", Type: connector.ConfigFieldText, Required: true},
		{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}},
	}, SecretFields: []connector.SecretField{{Key: "access_token", Name: "Acrobat Sign OAuth access token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(config(connection, "base_url"))
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || !strings.HasSuffix(strings.TrimRight(parsed.Path, "/"), "/api/rest/v6") || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return permanent("endpoint_invalid", "valid Acrobat Sign v6 shard API base URL is required")
	}
	timeout := intValue(connection.Config["timeout_seconds"], 30)
	if timeout < 1 || timeout > 120 {
		return permanent("timeout_invalid", "timeout_seconds must be between 1 and 120")
	}
	return nil
}

func (p *provider) createEnvelope(ctx context.Context, r connector.TypedRequest[CreateEnvelopeInput]) (connector.TypedResult[map[string]any], error) {
	if len(r.Input.Input) == 0 {
		return empty(), permanent("input_required", "envelope input is required")
	}
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodPost, "/agreements", nil, r.Input.Input, true)
}
func (p *provider) getEnvelope(ctx context.Context, r connector.TypedRequest[EnvelopeInput]) (connector.TypedResult[map[string]any], error) {
	id, err := envelopeID(r.Input.EnvelopeID)
	if err != nil {
		return empty(), err
	}
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodGet, "/agreements/"+url.PathEscape(id), nil, nil, false)
}
func (p *provider) sendEnvelope(ctx context.Context, r connector.TypedRequest[EnvelopeInput]) (connector.TypedResult[map[string]any], error) {
	id, err := envelopeID(r.Input.EnvelopeID)
	if err != nil {
		return empty(), err
	}
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodPut, "/agreements/"+url.PathEscape(id)+"/state", nil, map[string]any{"state": "IN_PROCESS"}, true)
}
func (p *provider) voidEnvelope(ctx context.Context, r connector.TypedRequest[VoidEnvelopeInput]) (connector.TypedResult[map[string]any], error) {
	id, err := envelopeID(r.Input.EnvelopeID)
	if err != nil || strings.TrimSpace(r.Input.Reason) == "" {
		return empty(), permanent("void_fields_required", "envelope_id and reason are required")
	}
	body := map[string]any{"state": "CANCELLED", "agreementCancellationInfo": map[string]any{"comment": strings.TrimSpace(r.Input.Reason), "notifyOthers": true}}
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodPut, "/agreements/"+url.PathEscape(id)+"/state", nil, body, true)
}
func (p *provider) createRecipientView(ctx context.Context, r connector.TypedRequest[EnvelopeInput]) (connector.TypedResult[map[string]any], error) {
	id, err := envelopeID(r.Input.EnvelopeID)
	if err != nil {
		return empty(), err
	}
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodGet, "/agreements/"+url.PathEscape(id)+"/signingUrls", nil, nil, true)
}
func (p *provider) listEnvelopeStatusChanges(ctx context.Context, r connector.TypedRequest[ListEnvelopeStatusChangesInput]) (connector.TypedResult[map[string]any], error) {
	pageSize := r.Input.PageSize
	if pageSize == 0 {
		pageSize = 50
	}
	if pageSize < 1 {
		return empty(), permanent("page_size_invalid", "page_size must be positive")
	}
	query := url.Values{"pageSize": {strconv.Itoa(pageSize)}}
	if value := strings.TrimSpace(r.Input.Cursor); value != "" {
		query.Set("cursor", value)
	}
	if value := strings.TrimSpace(r.Input.Query); value != "" {
		query.Set("query", value)
	}
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodGet, "/agreements", query, nil, false)
}
func (p *provider) testConnection(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[map[string]any], error) {
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodGet, "/baseUris", nil, nil, false)
}
func (p *provider) TestConnection(ctx context.Context, r connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.testConnection(ctx, connector.TypedRequest[struct{}]{Connection: r.Connection, Secrets: r.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, method, path string, query url.Values, body map[string]any, write bool) (connector.TypedResult[map[string]any], error) {
	if err := p.ValidateConfig(connection); err != nil {
		return empty(), err
	}
	token := strings.TrimSpace(secrets["access_token"])
	if token == "" {
		return empty(), permanent("access_token_required", "access token is required")
	}
	var raw []byte
	var err error
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return empty(), permanent("request_invalid", "request body is invalid")
		}
	}
	endpoint := strings.TrimRight(config(connection, "base_url"), "/") + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint, Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, Body: raw, MaxResponseBytes: 4 << 20})
	if err != nil {
		if write {
			return empty(), connector.UncertainError("adobe_sign.delivery_uncertain", err)
		}
		return empty(), connector.RetryableError("adobe_sign.network_error", err)
	}
	payload := map[string]any{}
	if len(response.Body) > 0 && json.Unmarshal(response.Body, &payload) != nil {
		if write {
			return empty(), connector.UncertainError("adobe_sign.response_uncertain", errors.New("Adobe Sign response is invalid"))
		}
		return empty(), connector.RetryableError("adobe_sign.response_invalid", errors.New("Adobe Sign response is invalid"))
	}
	ref := fmt.Sprintf("http:%d", response.StatusCode)
	if id, _ := payload["id"].(string); strings.TrimSpace(id) != "" {
		ref = "adobe_sign:" + strings.TrimSpace(id)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code, _ := payload["code"].(string)
		if code == "" {
			code = fmt.Sprintf("http_%d", response.StatusCode)
		}
		err := errors.New("Adobe Sign request failed")
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			return connector.TypedResult[map[string]any]{ResponseRef: ref}, connector.RetryableError("adobe_sign."+strings.ToLower(code), err)
		}
		return connector.TypedResult[map[string]any]{ResponseRef: ref}, connector.PermanentError("adobe_sign."+strings.ToLower(code), err)
	}
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, nil
}

func envelopeID(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", permanent("agreement_id_required", "envelope_id is required")
	}
	return value, nil
}
func empty() connector.TypedResult[map[string]any] { return connector.TypedResult[map[string]any]{} }
func config(connection connector.Connection, key string) string {
	value, _ := connection.Config[key].(string)
	return strings.TrimSpace(value)
}
func intValue(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		return typed
	case float64:
		return int(typed)
	case json.Number:
		parsed, err := strconv.Atoi(typed.String())
		if err == nil {
			return parsed
		}
	}
	return fallback
}
func isLoopback(host string) bool {
	return strings.EqualFold(host, "localhost") || (net.ParseIP(host) != nil && net.ParseIP(host).IsLoopback())
}
func permanent(code, message string) error {
	return connector.PermanentError("adobe_sign."+code, errors.New(message))
}
