// Package signnow implements the official SignNow Provider.
package signnow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	connector "github.com/domainry/domainry-connector-sdk"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const (
	ConnectorKey   = "esign"
	ProviderKey    = "signnow"
	defaultBaseURL = "https://api.signnow.com"
)

type EnvelopeInput struct {
	EnvelopeID string `json:"envelope_id"`
}
type SendEnvelopeInput struct {
	EnvelopeID string         `json:"envelope_id"`
	Input      map[string]any `json:"input"`
}
type VoidEnvelopeInput struct {
	EnvelopeID string `json:"envelope_id"`
	Reason     string `json:"reason"`
}
type RecipientViewInput struct {
	EnvelopeID string         `json:"envelope_id"`
	Input      map[string]any `json:"input"`
}

var (
	CreateRecipientView = writeOp[RecipientViewInput]("create_recipient_view", "0defebfe26cb6649fb00973fd883fc7a9bf9a20ef911570d206975a49190b045")
	GetEnvelope         = readOp[EnvelopeInput]("get_envelope", "d7dbda5c4ae2787e050cdf5935aed6a8d429a2ebddc93643dcd991aca972d267")
	SendEnvelope        = writeOp[SendEnvelopeInput]("send_envelope", "4936ba0cd6632815d0548c127cfcb36411edbe98990641559161dfec8b4221df")
	TestConnection      = readOp[struct{}]("test_connection", "973cfc9b2671622a791ab84975fb9aa853730640a998307ae0f66f38847ce6fa")
	VoidEnvelope        = writeOp[VoidEnvelopeInput]("void_envelope", "49b989e409f53805a2135308785ebe4c8170fc16044805aafde6c32f875dcd9f")
)

func readOp[I any](key, hash string) connector.CallOperation[I, map[string]any] {
	return op[I](key, hash, connector.EffectRead)
}
func writeOp[I any](key, hash string) connector.CallOperation[I, map[string]any] {
	return op[I](key, hash, connector.EffectWrite)
}
func op[I any](key, hash string, effect connector.OperationEffect) connector.CallOperation[I, map[string]any] {
	strategy := connector.IdempotencyNone
	if effect == connector.EffectRead {
		strategy = connector.IdempotencyNatural
	}
	return connector.CallOperation[I, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: key, ContractSHA256: hash, Reliability: connector.ReliabilityContract{Effect: effect, Idempotency: connector.IdempotencyContract{Strategy: strategy}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("SignNow transport is required")
	}
	p := &provider{transport: transport}
	ops := make([]connector.BoundOperation, 0, 5)
	bind := func(bound connector.BoundOperation, err error) error { ops = append(ops, bound); return err }
	if err := bind(connector.BindCall(CreateRecipientView, p.view)); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(GetEnvelope, p.get)); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(SendEnvelope, p.send)); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(TestConnection, p.test)); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(VoidEnvelope, p.void)); err != nil {
		return nil, err
	}
	adapter, err := connector.NewProvider(schema(), ops...)
	if err != nil {
		return nil, err
	}
	p.Adapter = adapter
	return p, nil
}
func schema() connector.ProviderSchema {
	min, max := float64(1), float64(120)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "base_url", Name: "SignNow API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://api.signnow.com"`)}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &min, Max: &max}}}, SecretFields: []connector.SecretField{{Key: "access_token", Name: "OAuth access token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(config(connection, "base_url", defaultBaseURL))
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return permanent("endpoint_invalid", "valid SignNow API base URL is required")
	}
	timeout := intValue(connection.Config["timeout_seconds"], 30)
	if timeout < 1 || timeout > 120 {
		return permanent("timeout_invalid", "timeout_seconds must be between 1 and 120")
	}
	return nil
}
func (p *provider) get(ctx context.Context, r connector.TypedRequest[EnvelopeInput]) (connector.TypedResult[map[string]any], error) {
	id, err := documentID(r.Input.EnvelopeID)
	if err != nil {
		return empty(), err
	}
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodGet, "/document/"+url.PathEscape(id), nil, false)
}
func (p *provider) send(ctx context.Context, r connector.TypedRequest[SendEnvelopeInput]) (connector.TypedResult[map[string]any], error) {
	id, err := documentID(r.Input.EnvelopeID)
	if err != nil {
		return empty(), err
	}
	if len(r.Input.Input) == 0 {
		return empty(), permanent("invite_input_required", "invite input is required")
	}
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodPost, "/document/"+url.PathEscape(id)+"/invite", r.Input.Input, true)
}
func (p *provider) void(ctx context.Context, r connector.TypedRequest[VoidEnvelopeInput]) (connector.TypedResult[map[string]any], error) {
	id, err := documentID(r.Input.EnvelopeID)
	if err != nil {
		return empty(), err
	}
	reason := strings.TrimSpace(r.Input.Reason)
	if reason == "" {
		return empty(), permanent("reason_required", "reason is required")
	}
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodPut, "/document/"+url.PathEscape(id)+"/fieldinvitecancel", map[string]any{"document_id": id, "reason": reason}, true)
}
func (p *provider) view(ctx context.Context, r connector.TypedRequest[RecipientViewInput]) (connector.TypedResult[map[string]any], error) {
	id, err := documentID(r.Input.EnvelopeID)
	if err != nil {
		return empty(), err
	}
	body := clone(r.Input.Input)
	inviteID, _ := body["field_invite_id"].(string)
	inviteID = strings.TrimSpace(inviteID)
	if inviteID == "" {
		return empty(), permanent("field_invite_id_required", "field_invite_id is required")
	}
	delete(body, "field_invite_id")
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodPost, "/v2/documents/"+url.PathEscape(id)+"/embedded-invites/"+url.PathEscape(inviteID)+"/link", body, true)
}
func (p *provider) test(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[map[string]any], error) {
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodGet, "/user", nil, false)
}
func (p *provider) TestConnection(ctx context.Context, r connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.test(ctx, connector.TypedRequest[struct{}]{Connection: r.Connection, Secrets: r.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}
func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, method, path string, body map[string]any, write bool) (connector.TypedResult[map[string]any], error) {
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
	headers := map[string][]string{"Accept": {"application/json"}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: strings.TrimRight(config(connection, "base_url", defaultBaseURL), "/") + path, Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, Body: raw, MaxResponseBytes: 4 << 20})
	if err != nil {
		if write {
			return empty(), connector.UncertainError("signnow.delivery_uncertain", err)
		}
		return empty(), connector.RetryableError("signnow.network_error", err)
	}
	payload := map[string]any{}
	if len(response.Body) > 0 && json.Unmarshal(response.Body, &payload) != nil {
		if write {
			return empty(), connector.UncertainError("signnow.response_uncertain", errors.New("SignNow response is invalid"))
		}
		return empty(), connector.RetryableError("signnow.response_invalid", errors.New("SignNow response is invalid"))
	}
	ref := fmt.Sprintf("http:%d", response.StatusCode)
	for _, key := range []string{"id", "document_id", "field_invite_id"} {
		if id, _ := payload[key].(string); strings.TrimSpace(id) != "" {
			ref = "signnow:" + strings.TrimSpace(id)
			break
		}
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, nil
	}
	code, _ := payload["error"].(string)
	if code == "" {
		code = fmt.Sprintf("http_%d", response.StatusCode)
	}
	cause := errors.New("SignNow request failed")
	if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
		return connector.TypedResult[map[string]any]{ResponseRef: ref}, connector.RetryableError("signnow."+strings.ToLower(code), cause)
	}
	return connector.TypedResult[map[string]any]{ResponseRef: ref}, connector.PermanentError("signnow."+strings.ToLower(code), cause)
}
func documentID(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", permanent("document_id_required", "envelope_id is required")
	}
	return value, nil
}
func clone(input map[string]any) map[string]any {
	result := make(map[string]any, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}
func empty() connector.TypedResult[map[string]any] { return connector.TypedResult[map[string]any]{} }
func config(connection connector.Connection, key, fallback string) string {
	value, _ := connection.Config[key].(string)
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return fallback
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
	return connector.PermanentError("signnow."+code, errors.New(message))
}
