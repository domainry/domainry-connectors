// Package http implements the reusable outbound HTTP webhook Provider.
package http

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	stdhttp "net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	ConnectorKey        = "webhook"
	ProviderKey         = "http"
	responseLimit int64 = 1 << 20
)

type SendInput map[string]any
type Response map[string]any

var (
	Send           = connector.EnqueueOperation[SendInput]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "send", ContractSHA256: "824c3f35b0f8917ba4fd37f5cd4a7269704676172d9cb3c83d89de6daccb3e18", Reliability: connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNone}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}}
	TestConnection = connector.CallOperation[struct{}, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection", ContractSHA256: "f22bc1f275ee012047c0bccc20745051a0eeafb7e73f3c496772d1f3fc07bbed", Reliability: connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}}
)

type provider struct {
	connector.Adapter
	transport connector.Transport
	now       func() time.Time
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("HTTP webhook transport is required")
	}
	p := &provider{transport: transport, now: time.Now}
	send, err := connector.BindEnqueueDelivery(Send, p.send)
	if err != nil {
		return nil, err
	}
	test, err := connector.BindCall(TestConnection, p.test)
	if err != nil {
		return nil, err
	}
	adapter, err := connector.NewProvider(schema(), send, test)
	if err != nil {
		return nil, err
	}
	p.Adapter = adapter
	return p, nil
}
func schema() connector.ProviderSchema {
	min, max := float64(1), float64(300)
	field := func(key, en, zh string, kind connector.ConfigFieldType) connector.ConfigField {
		return connector.ConfigField{Key: key, Name: en, Type: kind, I18n: i18n(en, zh)}
	}
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "url", Name: "Endpoint URL", Type: connector.ConfigFieldText, Required: true, I18n: i18n("Endpoint URL", "目标地址")}, {Key: "method", Name: "HTTP Method", Type: connector.ConfigFieldSelect, Default: json.RawMessage(`"POST"`), Validation: connector.ConfigValidation{Options: []string{"POST", "PUT", "PATCH"}}, I18n: i18n("HTTP Method", "HTTP 方法")}, field("headers", "Headers", "请求头", connector.ConfigFieldJSON), {Key: "timeout_seconds", Name: "Timeout Seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`10`), Validation: connector.ConfigValidation{Min: &min, Max: &max}, I18n: i18n("Timeout Seconds", "超时秒数")}, {Key: "signature_algorithm", Name: "Signature Algorithm", Type: connector.ConfigFieldSelect, Validation: connector.ConfigValidation{Options: []string{"hmac_sha256_hex", "feishu_sha256_hex"}}, I18n: i18n("Signature Algorithm", "签名算法")}, {Key: "signature_secret_ref_name", Name: "Signature Secret Name", Type: connector.ConfigFieldText, Default: json.RawMessage(`"webhook_secret"`), I18n: i18n("Signature Secret Name", "签名密钥名称")}}, SecretFields: []connector.SecretField{{Key: "webhook_secret", Name: "Webhook Secret", I18n: i18n("Webhook Secret", "Webhook 密钥"), CredentialKind: connector.SecretCredentialSigningSecret, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}
func i18n(en, zh string) map[string]connector.FieldLocalization {
	return map[string]connector.FieldLocalization{"en-US": {Name: en}, "zh-CN": {Name: zh}}
}
func (p *provider) ValidateConfig(c connector.Connection) error {
	endpoint, err := url.Parse(config(c, "url"))
	if err != nil || endpoint.Host == "" || endpoint.User != nil || endpoint.Fragment != "" || (endpoint.Scheme != "https" && !(endpoint.Scheme == "http" && loopback(endpoint.Hostname()))) {
		return permanent("url_invalid", "HTTPS or loopback HTTP endpoint is required")
	}
	method := strings.ToUpper(config(c, "method"))
	if method != "" && method != "POST" && method != "PUT" && method != "PATCH" {
		return permanent("method_unsupported", "HTTP method is unsupported")
	}
	if algorithm := config(c, "signature_algorithm"); algorithm != "" && algorithm != "hmac_sha256_hex" && algorithm != "feishu_sha256_hex" {
		return permanent("signature_algorithm_unsupported", "signature algorithm is unsupported")
	}
	return nil
}
func (p *provider) send(ctx context.Context, r connector.TypedRequest[SendInput]) (connector.DeliveryResult, error) {
	result, err := p.execute(ctx, r.Connection, r.Secrets, r.RequestRef, stringMap(r.Headers), map[string]any(r.Input), true)
	return connector.DeliveryResult{ResponseRef: result.ResponseRef}, err
}
func (p *provider) test(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	return p.execute(ctx, r.Connection, r.Secrets, r.RequestRef, stringMap(r.Headers), map[string]any{}, false)
}
func (p *provider) TestConnection(ctx context.Context, r connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.test(ctx, connector.TypedRequest[struct{}]{Connection: r.Connection, Secrets: r.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	raw, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: raw}, nil
}
func (p *provider) execute(ctx context.Context, c connector.Connection, secrets map[string]string, requestRef string, callerHeaders map[string]string, payload map[string]any, write bool) (connector.TypedResult[Response], error) {
	if err := p.ValidateConfig(c); err != nil {
		return empty(), err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return empty(), permanent("encode_failed", "request payload cannot be encoded")
	}
	method := strings.ToUpper(configDefault(c, "method", stdhttp.MethodPost))
	headers := map[string][]string{"Content-Type": {"application/json"}, "User-Agent": {"domainry-webhook-http/1.0"}, "X-Integration-Operation": {"send"}}
	if requestRef != "" {
		headers["X-Integration-Request-Ref"] = []string{requestRef}
	}
	for key, value := range configuredHeaders(c) {
		if key != "" && value != "" {
			headers[key] = []string{value}
		}
	}
	for key, value := range callerHeaders {
		if key != "" && value != "" {
			headers[key] = []string{value}
		}
	}
	secretHeaders, err := p.signatureHeaders(c, secrets, requestRef, "send", body)
	if err != nil {
		return empty(), err
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: config(c, "url"), Headers: headers, SecretHeaders: secretHeaders, Body: body, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		return empty(), transportFailure(write, "network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	output := Response{}
	valid := len(response.Body) == 0 || json.Unmarshal(response.Body, &output) == nil
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("webhook endpoint returned HTTP %d", response.StatusCode)
		if response.StatusCode == 429 {
			return connector.TypedResult[Response]{Output: output, ResponseRef: ref}, connector.RetryableError("webhook.http_429", cause)
		}
		if response.StatusCode >= 500 {
			return connector.TypedResult[Response]{Output: output, ResponseRef: ref}, transportFailure(write, "http_"+strconv.Itoa(response.StatusCode), cause)
		}
		return connector.TypedResult[Response]{Output: output, ResponseRef: ref}, connector.PermanentError("webhook.http_"+strconv.Itoa(response.StatusCode), cause)
	}
	if !valid {
		return connector.TypedResult[Response]{ResponseRef: ref}, transportFailure(write, "response_invalid", errors.New("webhook response is invalid JSON"))
	}
	if write {
		output = Response{"status_code": response.StatusCode}
	}
	return connector.TypedResult[Response]{Output: output, ResponseRef: ref}, nil
}
func (p *provider) signatureHeaders(c connector.Connection, secrets map[string]string, requestRef, operation string, body []byte) (map[string][]string, error) {
	algorithm := config(c, "signature_algorithm")
	if algorithm == "" {
		return nil, nil
	}
	name := configDefault(c, "signature_secret_ref_name", "webhook_secret")
	secret := strings.TrimSpace(secrets[name])
	if secret == "" {
		return nil, permanent("signature_secret_required", "resolved signature secret is required")
	}
	timestamp := strconv.FormatInt(p.now().UTC().Unix(), 10)
	nonce := strings.TrimSpace(requestRef)
	if nonce == "" {
		sum := sha256.Sum256([]byte(operation + timestamp))
		nonce = hex.EncodeToString(sum[:8])
	}
	signature := computeSignature(algorithm, secret, timestamp, nonce, body)
	return map[string][]string{configDefault(c, "signature_header", "X-Integration-Signature"): {signature}, configDefault(c, "signature_timestamp_header", "X-Integration-Timestamp"): {timestamp}, configDefault(c, "signature_nonce_header", "X-Integration-Nonce"): {nonce}, "X-Integration-Signature-Algorithm": {algorithm}}, nil
}
func computeSignature(algorithm, secret, timestamp, nonce string, body []byte) string {
	if algorithm == "feishu_sha256_hex" {
		sum := sha256.Sum256(append([]byte(timestamp+nonce+secret), body...))
		return hex.EncodeToString(sum[:])
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(timestamp))
	_, _ = mac.Write([]byte(nonce))
	_, _ = mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}
func configuredHeaders(c connector.Connection) map[string]string {
	result := map[string]string{}
	switch values := c.Config["headers"].(type) {
	case map[string]string:
		for k, v := range values {
			result[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	case map[string]any:
		for k, v := range values {
			result[strings.TrimSpace(k)] = clean(v)
		}
	}
	return result
}
func stringMap(values map[string]string) map[string]string { return values }
func config(c connector.Connection, key string) string     { return clean(c.Config[key]) }
func configDefault(c connector.Connection, key, fallback string) string {
	if value := config(c, key); value != "" {
		return value
	}
	return fallback
}
func clean(v any) string {
	value := strings.TrimSpace(fmt.Sprint(v))
	if value == "<nil>" {
		return ""
	}
	return value
}
func loopback(host string) bool {
	return host == "localhost" || strings.HasPrefix(host, "127.") || host == "::1"
}
func empty() connector.TypedResult[Response] { return connector.TypedResult[Response]{} }
func permanent(suffix, message string) error {
	return connector.PermanentError("webhook."+suffix, errors.New(message))
}
func transportFailure(write bool, suffix string, cause error) error {
	if write {
		return connector.UncertainError("webhook."+suffix, cause)
	}
	return connector.RetryableError("webhook."+suffix, cause)
}
