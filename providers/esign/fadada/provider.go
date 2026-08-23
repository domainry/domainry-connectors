// Package fadada implements the official Fadada OpenAPI 5.1 Provider.
package fadada

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	ConnectorKey = "esign"
	ProviderKey  = "fadada"
)

type CreateEnvelopeInput struct {
	Input map[string]any `json:"input"`
}
type EnvelopeInput struct {
	EnvelopeID string `json:"envelope_id"`
}
type RecipientViewInput struct {
	EnvelopeID string         `json:"envelope_id"`
	Input      map[string]any `json:"input,omitempty"`
}
type VoidEnvelopeInput struct {
	EnvelopeID string `json:"envelope_id"`
	Reason     string `json:"reason,omitempty"`
}

var (
	CreateEnvelope      = writeOp[CreateEnvelopeInput]("create_envelope", "cffe2007f4352b8542e199f6578f5476b0a0aa7a7bc3ccf3875916e90da8b663")
	CreateRecipientView = writeOp[RecipientViewInput]("create_recipient_view", "0defebfe26cb6649fb00973fd883fc7a9bf9a20ef911570d206975a49190b045")
	GetEnvelope         = readOp[EnvelopeInput]("get_envelope", "d7dbda5c4ae2787e050cdf5935aed6a8d429a2ebddc93643dcd991aca972d267")
	SendEnvelope        = writeOp[EnvelopeInput]("send_envelope", "4936ba0cd6632815d0548c127cfcb36411edbe98990641559161dfec8b4221df")
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
		return nil, errors.New("Fadada transport is required")
	}
	p := &provider{transport: transport}
	ops := make([]connector.BoundOperation, 0, 6)
	bind := func(bound connector.BoundOperation, err error) error { ops = append(ops, bound); return err }
	if err := bind(connector.BindCall(CreateEnvelope, p.create)); err != nil {
		return nil, err
	}
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
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "base_url", Name: "Fadada OpenAPI base URL", Type: connector.ConfigFieldText, Required: true}, {Key: "app_id", Name: "Application ID", Type: connector.ConfigFieldText, Required: true}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &min, Max: &max}}}, SecretFields: []connector.SecretField{{Key: "app_secret", Name: "Application secret", Required: true, CredentialKind: connector.SecretCredentialAPIKey, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(config(connection, "base_url"))
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return permanent("endpoint_invalid", "valid Fadada OpenAPI base URL is required")
	}
	if config(connection, "app_id") == "" {
		return permanent("app_id_required", "app_id is required")
	}
	timeout := intValue(connection.Config["timeout_seconds"], 30)
	if timeout < 1 || timeout > 120 {
		return permanent("timeout_invalid", "timeout_seconds must be between 1 and 120")
	}
	return nil
}
func (p *provider) create(ctx context.Context, r connector.TypedRequest[CreateEnvelopeInput]) (connector.TypedResult[map[string]any], error) {
	if len(r.Input.Input) == 0 {
		return empty(), permanent("input_required", "envelope input is required")
	}
	return p.business(ctx, r.Connection, r.Secrets, "/sign-task/create", r.Input.Input, true)
}
func (p *provider) get(ctx context.Context, r connector.TypedRequest[EnvelopeInput]) (connector.TypedResult[map[string]any], error) {
	return p.withID(ctx, r.Connection, r.Secrets, "/sign-task/app/get-detail", r.Input.EnvelopeID, nil, false)
}
func (p *provider) send(ctx context.Context, r connector.TypedRequest[EnvelopeInput]) (connector.TypedResult[map[string]any], error) {
	return p.withID(ctx, r.Connection, r.Secrets, "/sign-task/start", r.Input.EnvelopeID, nil, true)
}
func (p *provider) void(ctx context.Context, r connector.TypedRequest[VoidEnvelopeInput]) (connector.TypedResult[map[string]any], error) {
	body := map[string]any{}
	if reason := strings.TrimSpace(r.Input.Reason); reason != "" {
		body["reason"] = reason
	}
	return p.withID(ctx, r.Connection, r.Secrets, "/sign-task/cancel", r.Input.EnvelopeID, body, true)
}
func (p *provider) view(ctx context.Context, r connector.TypedRequest[RecipientViewInput]) (connector.TypedResult[map[string]any], error) {
	return p.withID(ctx, r.Connection, r.Secrets, "/sign-task/actor/get-url", r.Input.EnvelopeID, clone(r.Input.Input), true)
}
func (p *provider) withID(ctx context.Context, connection connector.Connection, secrets map[string]string, path, id string, body map[string]any, write bool) (connector.TypedResult[map[string]any], error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return empty(), permanent("sign_task_id_required", "envelope_id is required")
	}
	if body == nil {
		body = map[string]any{}
	}
	body["signTaskId"] = id
	return p.business(ctx, connection, secrets, path, body, write)
}
func (p *provider) test(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[map[string]any], error) {
	token, payload, err := p.accessToken(ctx, r.Connection, r.Secrets)
	if err != nil {
		return empty(), err
	}
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: "fadada:token:" + tokenRef(token)}, nil
}
func (p *provider) TestConnection(ctx context.Context, r connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.test(ctx, connector.TypedRequest[struct{}]{Connection: r.Connection, Secrets: r.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}
func (p *provider) accessToken(ctx context.Context, connection connector.Connection, secrets map[string]string) (string, map[string]any, error) {
	headers, secretHeaders, err := signedHeaders(connection, secrets, "", false)
	if err != nil {
		return "", nil, err
	}
	payload, _, err := p.request(ctx, connection, "/service/get-access-token", nil, headers, secretHeaders, false)
	if err != nil {
		return "", nil, err
	}
	data, _ := payload["data"].(map[string]any)
	token, _ := data["accessToken"].(string)
	token = strings.TrimSpace(token)
	if token == "" {
		return "", payload, permanent("access_token_missing", "Fadada access token is missing")
	}
	return token, payload, nil
}
func (p *provider) business(ctx context.Context, connection connector.Connection, secrets map[string]string, path string, body map[string]any, write bool) (connector.TypedResult[map[string]any], error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return empty(), permanent("request_invalid", "request body is invalid")
	}
	token, _, err := p.accessToken(ctx, connection, secrets)
	if err != nil {
		return empty(), err
	}
	headers, secretHeaders, err := signedHeaders(connection, secrets, string(raw), true)
	if err != nil {
		return empty(), err
	}
	secretHeaders["X-FASC-AccessToken"] = []string{token}
	payload, status, err := p.request(ctx, connection, path, raw, headers, secretHeaders, write)
	if err != nil {
		return empty(), err
	}
	ref := "fadada:http:" + strconv.Itoa(status)
	if data, ok := payload["data"].(map[string]any); ok {
		for _, key := range []string{"signTaskId", "actorSignTaskUrl", "url"} {
			if value, _ := data[key].(string); strings.TrimSpace(value) != "" {
				ref = "fadada:" + strings.TrimSpace(value)
				break
			}
		}
	}
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, nil
}
func (p *provider) request(ctx context.Context, connection connector.Connection, path string, body []byte, headers, secretHeaders map[string][]string, write bool) (map[string]any, int, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, 0, err
	}
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodPost, URL: strings.TrimRight(config(connection, "base_url"), "/") + path, Headers: headers, SecretHeaders: secretHeaders, Body: body, MaxResponseBytes: 4 << 20})
	if err != nil {
		if write {
			return nil, 0, connector.UncertainError("fadada.delivery_uncertain", err)
		}
		return nil, 0, connector.RetryableError("fadada.network_error", err)
	}
	payload := map[string]any{}
	if json.Unmarshal(response.Body, &payload) != nil {
		if write {
			return nil, response.StatusCode, connector.UncertainError("fadada.response_uncertain", errors.New("Fadada response is invalid"))
		}
		return nil, response.StatusCode, connector.RetryableError("fadada.response_invalid", errors.New("Fadada response is invalid"))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := errors.New("Fadada request failed")
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			return nil, response.StatusCode, connector.RetryableError(fmt.Sprintf("fadada.http_%d", response.StatusCode), cause)
		}
		return nil, response.StatusCode, connector.PermanentError(fmt.Sprintf("fadada.http_%d", response.StatusCode), cause)
	}
	code, _ := payload["code"].(string)
	if code != "100000" {
		if code == "" {
			code = "unknown"
		}
		return nil, response.StatusCode, connector.PermanentError("fadada."+strings.ToLower(code), errors.New("Fadada rejected the request"))
	}
	return payload, response.StatusCode, nil
}
func signedHeaders(connection connector.Connection, secrets map[string]string, bizContent string, business bool) (map[string][]string, map[string][]string, error) {
	secret := strings.TrimSpace(secrets["app_secret"])
	if secret == "" {
		return nil, nil, permanent("app_secret_required", "app_secret is required")
	}
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return nil, nil, connector.RetryableError("fadada.nonce_failed", err)
	}
	values := map[string]string{"X-FASC-App-Id": config(connection, "app_id"), "X-FASC-Sign-Type": "HMAC-SHA256", "X-FASC-Timestamp": timestamp, "X-FASC-Nonce": hex.EncodeToString(nonce), "X-FASC-Api-SubVersion": "5.1"}
	if business {
		values["bizContent"] = bizContent
	} else {
		values["X-FASC-Grant-Type"] = "client_credential"
	}
	keys := make([]string, 0, len(values))
	for key, value := range values {
		if value != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+values[key])
	}
	digest := sha256.Sum256([]byte(strings.Join(parts, "&")))
	temporary := hmacSHA256([]byte(secret), []byte(timestamp))
	signature := hex.EncodeToString(hmacSHA256(temporary, []byte(hex.EncodeToString(digest[:]))))
	headers := map[string][]string{"Accept": {"application/json"}}
	if business {
		headers["Content-Type"] = []string{"application/json"}
	}
	for key, value := range values {
		if key != "bizContent" {
			headers[key] = []string{value}
		}
	}
	return headers, map[string][]string{"X-FASC-Sign": {strings.ToLower(signature)}}, nil
}
func hmacSHA256(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(data)
	return mac.Sum(nil)
}
func clone(input map[string]any) map[string]any {
	result := make(map[string]any, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}
func tokenRef(token string) string {
	if len(token) <= 8 {
		return "issued"
	}
	return token[:8]
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
	return connector.PermanentError("fadada."+code, errors.New(message))
}
