// Package metacloudapi implements the official WhatsApp Cloud API Provider.
package metacloudapi

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
	ConnectorKey         = "whatsapp"
	ProviderKey          = "meta_cloud_api"
	defaultBaseURL       = "https://graph.facebook.com"
	responseLimit  int64 = 4 << 20
)

type Response map[string]any
type SendMessageInput struct {
	Recipient       string         `json:"recipient"`
	Message         string         `json:"message"`
	ProviderPayload map[string]any `json:"provider_payload,omitempty"`
}
type ListTemplatesInput struct {
	Status string `json:"status,omitempty"`
}

var (
	ListTemplates  = operation[ListTemplatesInput]("list_templates", "02e9b6f198a1bf75c997c9bd79775e7f989a166a148923f29da731e29b21d568", connector.EffectRead, connector.IdempotencyNatural)
	SendMessage    = operation[SendMessageInput]("send_message", "3312e467ef1ca1118ba3be883436c597a8d1f1a8a4ecc019b4fe423073eb52b0", connector.EffectWrite, connector.IdempotencyNone)
	TestConnection = operation[struct{}]("test_connection", "3c4028bc879a87aab6cacbdfa44b36583693f053fdc805b01e92938d874b349f", connector.EffectRead, connector.IdempotencyNatural)
)

func operation[I any](key, hash string, effect connector.OperationEffect, idempotency connector.IdempotencyStrategy) connector.CallOperation[I, Response] {
	return connector.CallOperation[I, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: key, ContractSHA256: hash, Reliability: connector.ReliabilityContract{Effect: effect, Idempotency: connector.IdempotencyContract{Strategy: idempotency}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("WhatsApp Cloud API transport is required")
	}
	p := &provider{transport: transport}
	bindings := []func() (connector.BoundOperation, error){func() (connector.BoundOperation, error) { return connector.BindCall(ListTemplates, p.listTemplates) }, func() (connector.BoundOperation, error) { return connector.BindCall(SendMessage, p.sendMessage) }, func() (connector.BoundOperation, error) { return connector.BindCall(TestConnection, p.test) }}
	ops := make([]connector.BoundOperation, 0, len(bindings))
	for _, bind := range bindings {
		op, err := bind()
		if err != nil {
			return nil, err
		}
		ops = append(ops, op)
	}
	adapter, err := connector.NewProvider(schema(), ops...)
	if err != nil {
		return nil, err
	}
	p.Adapter = adapter
	return p, nil
}
func schema() connector.ProviderSchema {
	min, max := float64(1), float64(300)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "phone_number_id", Name: "Phone Number ID", Type: connector.ConfigFieldText, Required: true}, {Key: "business_account_id", Name: "WhatsApp Business Account ID", Type: connector.ConfigFieldText}, {Key: "graph_version", Name: "Graph API Version", Type: connector.ConfigFieldText, Default: json.RawMessage(`"v23.0"`)}, {Key: "base_url", Name: "Graph API Base URL", Type: connector.ConfigFieldText}, {Key: "timeout_seconds", Name: "Timeout Seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &min, Max: &max}}}, SecretFields: []connector.SecretField{secret("access_token", "Access Token", connector.SecretCredentialBearerToken, true), secret("app_secret", "App Secret", connector.SecretCredentialSigningSecret, false), secret("webhook_verify_token", "Webhook Verify Token", connector.SecretCredentialSigningSecret, false)}}
}
func secret(key, name string, kind connector.SecretCredentialKind, required bool) connector.SecretField {
	return connector.SecretField{Key: key, Name: name, Required: required, CredentialKind: kind, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}
}
func (p *provider) ValidateConfig(c connector.Connection) error {
	if config(c, "phone_number_id") == "" {
		return permanent("phone_number_id_required", "phone_number_id is required")
	}
	endpoint, err := url.Parse(baseURL(c))
	if err != nil || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || (endpoint.Scheme != "https" && !(endpoint.Scheme == "http" && loopback(endpoint.Hostname()))) {
		return permanent("endpoint_invalid", "HTTPS or loopback HTTP endpoint is required")
	}
	return nil
}
func (p *provider) test(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodGet, "/"+url.PathEscape(config(r.Connection, "phone_number_id"))+"?fields=id%2Cdisplay_phone_number%2Cverified_name", nil, false)
}
func (p *provider) TestConnection(ctx context.Context, r connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.test(ctx, connector.TypedRequest[struct{}]{Connection: r.Connection, Secrets: r.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	raw, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: raw}, nil
}
func (p *provider) sendMessage(ctx context.Context, r connector.TypedRequest[SendMessageInput]) (connector.TypedResult[Response], error) {
	recipient, message := strings.TrimSpace(r.Input.Recipient), strings.TrimSpace(r.Input.Message)
	if recipient == "" || message == "" {
		return empty(), permanent("message_invalid", "recipient and message are required")
	}
	payload := r.Input.ProviderPayload
	if payload == nil {
		payload = map[string]any{"type": "text", "text": map[string]any{"preview_url": false, "body": message}}
	}
	payload["messaging_product"], payload["recipient_type"], payload["to"] = "whatsapp", "individual", recipient
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodPost, "/"+url.PathEscape(config(r.Connection, "phone_number_id"))+"/messages", payload, true)
}
func (p *provider) listTemplates(ctx context.Context, r connector.TypedRequest[ListTemplatesInput]) (connector.TypedResult[Response], error) {
	account := config(r.Connection, "business_account_id")
	if account == "" {
		return empty(), permanent("business_account_id_required", "business_account_id is required")
	}
	query := url.Values{"fields": {"name,status,language,category,components"}, "limit": {"200"}}
	if status := strings.ToUpper(strings.TrimSpace(r.Input.Status)); status != "" {
		query.Set("status", status)
	}
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodGet, "/"+url.PathEscape(account)+"/message_templates?"+query.Encode(), nil, false)
}
func (p *provider) execute(ctx context.Context, c connector.Connection, secrets map[string]string, method, path string, payload map[string]any, write bool) (connector.TypedResult[Response], error) {
	if err := p.ValidateConfig(c); err != nil {
		return empty(), err
	}
	token := strings.TrimSpace(secrets["access_token"])
	if token == "" {
		return empty(), permanent("access_token_required", "resolved access token is required")
	}
	var body []byte
	var err error
	if payload != nil {
		body, err = json.Marshal(payload)
		if err != nil {
			return empty(), permanent("request_invalid", "request is invalid")
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	if payload != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: baseURL(c) + path, Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, Body: body, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		return empty(), transportFailure(write, "network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	output := Response{}
	valid := len(response.Body) == 0 || json.Unmarshal(response.Body, &output) == nil
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("WhatsApp returned HTTP %d", response.StatusCode)
		code := "http_" + strconv.Itoa(response.StatusCode)
		if response.StatusCode == 429 {
			return connector.TypedResult[Response]{Output: output, ResponseRef: ref}, connector.RetryableError("whatsapp."+code, cause)
		}
		if response.StatusCode >= 500 {
			return connector.TypedResult[Response]{Output: output, ResponseRef: ref}, transportFailure(write, code, cause)
		}
		return connector.TypedResult[Response]{Output: output, ResponseRef: ref}, connector.PermanentError("whatsapp."+code, cause)
	}
	if !valid {
		return connector.TypedResult[Response]{ResponseRef: ref}, transportFailure(write, "response_invalid", errors.New("WhatsApp response is invalid JSON"))
	}
	if messages, ok := output["messages"].([]any); ok && len(messages) > 0 {
		first, _ := messages[0].(map[string]any)
		if id := clean(first["id"]); id != "" {
			ref = "whatsapp:" + id
		}
	}
	if data, ok := output["data"].([]any); ok {
		output = Response{"templates": data, "paging": output["paging"]}
	}
	return connector.TypedResult[Response]{Output: output, ResponseRef: ref}, nil
}
func config(c connector.Connection, key string) string { return clean(c.Config[key]) }
func baseURL(c connector.Connection) string {
	if value := strings.TrimRight(config(c, "base_url"), "/"); value != "" {
		return value
	}
	version := config(c, "graph_version")
	if version == "" {
		version = "v23.0"
	}
	return defaultBaseURL + "/" + version
}
func loopback(host string) bool {
	return host == "localhost" || strings.HasPrefix(host, "127.") || host == "::1"
}
func clean(value any) string {
	result := strings.TrimSpace(fmt.Sprint(value))
	if result == "<nil>" {
		return ""
	}
	return result
}
func empty() connector.TypedResult[Response] { return connector.TypedResult[Response]{} }
func permanent(suffix, message string) error {
	return connector.PermanentError("whatsapp."+suffix, errors.New(message))
}
func transportFailure(write bool, suffix string, cause error) error {
	if write {
		return connector.UncertainError("whatsapp."+suffix, cause)
	}
	return connector.RetryableError("whatsapp."+suffix, cause)
}
