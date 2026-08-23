// Package facebookmessenger implements the official Facebook Messenger Provider.
package facebookmessenger

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
	ConnectorKey         = "social_message"
	ProviderKey          = "facebook_messenger"
	defaultBaseURL       = "https://graph.facebook.com"
	responseLimit  int64 = 4 << 20
)

type SendMessageInput struct {
	RecipientID string `json:"recipient_id"`
	Message     string `json:"message"`
}
type Response map[string]any

var (
	SendMessage    = connector.CallOperation[SendMessageInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "send_message", ContractSHA256: "a9b43aa09225808ca109a1772ce1dd9d99db3ef9759209a87e08e34126373d3c", Reliability: reliability(connector.EffectWrite, connector.IdempotencyNone)}
	TestConnection = connector.CallOperation[struct{}, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection", ContractSHA256: "eb6666daf4e750a585c727c6805da45c2978f4f59ea8ec57a385445d939d2ac2", Reliability: reliability(connector.EffectRead, connector.IdempotencyNatural)}
)

func reliability(effect connector.OperationEffect, strategy connector.IdempotencyStrategy) connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: effect, Idempotency: connector.IdempotencyContract{Strategy: strategy}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Facebook Messenger transport is required")
	}
	p := &provider{transport: transport}
	send, err := connector.BindCall(SendMessage, p.sendMessage)
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
	min, max := float64(1), float64(120)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{
		{Key: "page_id", Name: "Page ID", Type: connector.ConfigFieldText, Required: true}, {Key: "business_account_id", Name: "Business account ID", Type: connector.ConfigFieldText}, {Key: "ad_account_id", Name: "Ad account ID", Type: connector.ConfigFieldText}, {Key: "default_country", Name: "Default country", Type: connector.ConfigFieldText}, {Key: "default_language", Name: "Default language", Type: connector.ConfigFieldText}, {Key: "default_owner_queue", Name: "Default owner queue", Type: connector.ConfigFieldText}, {Key: "handoff_policy_key", Name: "Handoff policy key", Type: connector.ConfigFieldText}, {Key: "graph_version", Name: "Graph API version", Type: connector.ConfigFieldText, Default: json.RawMessage(`"v23.0"`)}, {Key: "base_url", Name: "Graph API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://graph.facebook.com"`)}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &min, Max: &max}},
	}, SecretFields: []connector.SecretField{secret("access_token", "Page access token", connector.SecretCredentialBearerToken, true), secret("app_secret", "App secret", connector.SecretCredentialOAuthClientSecret, false), secret("webhook_secret", "Webhook verify token", connector.SecretCredentialSigningSecret, false)}}
}

func secret(key, name string, kind connector.SecretCredentialKind, required bool) connector.SecretField {
	return connector.SecretField{Key: key, Name: name, Required: required, CredentialKind: kind, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}
}

func (p *provider) ValidateConfig(c connector.Connection) error {
	if config(c, "page_id") == "" {
		return permanent("page_id_required", "page_id is required")
	}
	u, err := url.Parse(baseURL(c))
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && !(u.Scheme == "http" && loopback(u.Hostname()))) {
		return permanent("endpoint_invalid", "HTTPS or loopback HTTP endpoint is required")
	}
	return nil
}

func (p *provider) sendMessage(ctx context.Context, r connector.TypedRequest[SendMessageInput]) (connector.TypedResult[Response], error) {
	if strings.TrimSpace(r.Input.RecipientID) == "" || strings.TrimSpace(r.Input.Message) == "" {
		return empty(), permanent("message_invalid", "recipient_id and message are required")
	}
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodPost, "/"+url.PathEscape(config(r.Connection, "page_id"))+"/messages", map[string]any{"messaging_type": "RESPONSE", "recipient": map[string]any{"id": r.Input.RecipientID}, "message": map[string]any{"text": r.Input.Message}}, true)
}

func (p *provider) test(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodGet, "/"+url.PathEscape(config(r.Connection, "page_id"))+"?fields=id%2Cname", nil, false)
}
func (p *provider) TestConnection(ctx context.Context, r connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.test(ctx, connector.TypedRequest[struct{}]{Connection: r.Connection, Secrets: r.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	raw, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: raw}, nil
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
		cause := fmt.Errorf("Facebook Messenger returned HTTP %d", response.StatusCode)
		code := "http_" + strconv.Itoa(response.StatusCode)
		if response.StatusCode == 429 {
			return connector.TypedResult[Response]{Output: output, ResponseRef: ref}, connector.RetryableError("facebook_messenger."+code, cause)
		}
		if response.StatusCode >= 500 {
			return connector.TypedResult[Response]{Output: output, ResponseRef: ref}, transportFailure(write, code, cause)
		}
		return connector.TypedResult[Response]{Output: output, ResponseRef: ref}, connector.PermanentError("facebook_messenger."+code, cause)
	}
	if !valid {
		return connector.TypedResult[Response]{ResponseRef: ref}, transportFailure(write, "response_invalid", errors.New("Facebook Messenger response is invalid JSON"))
	}
	if id := configMap(output, "message_id"); id != "" {
		ref = "facebook:" + id
	}
	return connector.TypedResult[Response]{Output: output, ResponseRef: ref}, nil
}

func config(c connector.Connection, key string) string {
	return strings.TrimSpace(fmt.Sprint(c.Config[key]))
}
func configMap(m map[string]any, key string) string {
	v := strings.TrimSpace(fmt.Sprint(m[key]))
	if v == "<nil>" {
		return ""
	}
	return v
}
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
func empty() connector.TypedResult[Response] { return connector.TypedResult[Response]{} }
func permanent(suffix string, cause string) error {
	return connector.PermanentError("facebook_messenger."+suffix, errors.New(cause))
}
func transportFailure(write bool, suffix string, cause error) error {
	if write {
		return connector.UncertainError("facebook_messenger."+suffix, cause)
	}
	return connector.RetryableError("facebook_messenger."+suffix, cause)
}
