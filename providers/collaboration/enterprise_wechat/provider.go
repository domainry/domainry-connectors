// Package enterprisewechat implements the official Enterprise WeChat collaboration Provider.
package enterprisewechat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	connector "github.com/domainry/domainry-connector-sdk"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const (
	ConnectorKey   = "collaboration"
	ProviderKey    = "enterprise_wechat"
	defaultAPIBase = "https://qyapi.weixin.qq.com"
	responseLimit  = 4 << 20
)

type SendMessageInput struct {
	Recipient       string         `json:"recipient"`
	Message         string         `json:"message"`
	Text            string         `json:"text,omitempty"`
	ProviderPayload map[string]any `json:"provider_payload,omitempty"`
}
type Response map[string]any

var (
	SendMessage    = connector.EnqueueOperation[SendMessageInput]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "send_message", ContractSHA256: "c8e3420b4832306a7f1ea9beec5ae3694c0a795a6a3f8bb28a568a8fb6d501cb", Reliability: writeReliability()}
	TestConnection = connector.CallOperation[struct{}, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection", ContractSHA256: "a354f65a655c7afc141199c44f831137989741466e5c064d4a872fdb8dec4916", Reliability: readReliability()}
)

func readReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}
func writeReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNone}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Enterprise WeChat transport is required")
	}
	p := &provider{transport: transport}
	send, err := connector.BindEnqueueDelivery(SendMessage, p.sendMessage)
	if err != nil {
		return nil, err
	}
	test, err := connector.BindCall(TestConnection, p.callTestConnection)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), send, test)
	if err != nil {
		return nil, err
	}
	p.Adapter = bound
	return p, nil
}
func schema() connector.ProviderSchema {
	min, max := float64(1), float64(60)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "api_base_url", Name: "Enterprise WeChat API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://qyapi.weixin.qq.com"`)}, {Key: "agent_id", Name: "Application agent ID", Type: connector.ConfigFieldInteger, Required: true, Validation: connector.ConfigValidation{Min: &min}}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`15`), Validation: connector.ConfigValidation{Min: &min, Max: &max}}}, SecretFields: []connector.SecretField{{Key: "corp_id", Name: "Enterprise WeChat corporation ID", Required: true, CredentialKind: connector.SecretCredentialIdentifier, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}, {Key: "corp_secret", Name: "Enterprise WeChat corporation secret", Required: true, CredentialKind: connector.SecretCredentialOAuthClientSecret, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(apiBase(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return permanent("endpoint_invalid", "valid Enterprise WeChat endpoint is required")
	}
	if !(parsed.Scheme == "http" && isLoopback(parsed.Hostname())) && (parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), "qyapi.weixin.qq.com") || (parsed.EscapedPath() != "" && parsed.EscapedPath() != "/")) {
		return permanent("endpoint_invalid", "official Enterprise WeChat endpoint or loopback HTTP is required")
	}
	if intValue(connection.Config["agent_id"], 0) < 1 {
		return permanent("agent_id_required", "agent_id must be positive")
	}
	if seconds := intValue(connection.Config["timeout_seconds"], 15); seconds < 1 || seconds > 60 {
		return permanent("timeout_invalid", "timeout_seconds must be between 1 and 60")
	}
	return nil
}
func (p *provider) sendMessage(ctx context.Context, request connector.TypedRequest[SendMessageInput]) (connector.DeliveryResult, error) {
	recipient, message := strings.TrimSpace(request.Input.Recipient), first(request.Input.Message, request.Input.Text)
	if recipient == "" || message == "" {
		return connector.DeliveryResult{}, permanent("message_fields_required", "recipient and message are required")
	}
	token, _, _, err := p.accessToken(ctx, request.Connection, request.Secrets)
	if err != nil {
		return connector.DeliveryResult{}, err
	}
	payload := request.Input.ProviderPayload
	if len(payload) == 0 {
		payload = map[string]any{"msgtype": "text", "text": map[string]any{"content": message}}
	}
	payload["touser"], payload["agentid"], payload["safe"] = recipient, intValue(request.Connection.Config["agent_id"], 0), 0
	result, ref, err := p.execute(ctx, request.Connection, http.MethodPost, apiBase(request.Connection)+"/cgi-bin/message/send", payload, map[string]string{"access_token": token}, true)
	if err != nil {
		return connector.DeliveryResult{ResponseRef: ref}, err
	}
	code := intValue(result["errcode"], -1)
	if code != 0 {
		return connector.DeliveryResult{ResponseRef: ref}, permanent("api_"+strconv.Itoa(code), "Enterprise WeChat rejected message")
	}
	if id := mapString(result, "msgid"); id != "" {
		ref = "enterprise_wechat:" + id
	} else {
		ref = "enterprise_wechat:message:accepted"
	}
	return connector.DeliveryResult{ResponseRef: ref}, nil
}
func (p *provider) accessToken(ctx context.Context, connection connector.Connection, secrets map[string]string) (string, Response, string, error) {
	corpID, corpSecret := strings.TrimSpace(secrets["corp_id"]), strings.TrimSpace(secrets["corp_secret"])
	if corpID == "" || corpSecret == "" {
		return "", nil, "", permanent("corp_credentials_required", "resolved corp_id and corp_secret are required")
	}
	payload, ref, err := p.execute(ctx, connection, http.MethodGet, apiBase(connection)+"/cgi-bin/gettoken", nil, map[string]string{"corpid": corpID, "corpsecret": corpSecret}, false)
	if err != nil {
		return "", payload, ref, err
	}
	if code := intValue(payload["errcode"], -1); code != 0 {
		return "", payload, ref, permanent("token_rejected", "Enterprise WeChat rejected corporation credentials")
	}
	token := mapString(payload, "access_token")
	if token == "" {
		return "", payload, ref, permanent("token_invalid", "Enterprise WeChat access token response is invalid")
	}
	return token, payload, ref, nil
}
func (p *provider) callTestConnection(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	_, payload, ref, err := p.accessToken(ctx, request.Connection, request.Secrets)
	if err != nil {
		return connector.TypedResult[Response]{ResponseRef: ref}, err
	}
	return connector.TypedResult[Response]{Output: Response{"connected": true, "expires_in": payload["expires_in"]}, ResponseRef: ref}, nil
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.callTestConnection(ctx, connector.TypedRequest[struct{}]{Connection: request.Connection, Secrets: request.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}
func (p *provider) execute(ctx context.Context, connection connector.Connection, method, endpoint string, body map[string]any, secretQuery map[string]string, write bool) (Response, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", err
	}
	var raw []byte
	var err error
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return nil, "", permanent("request_invalid", "Enterprise WeChat request body is invalid")
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint, Headers: headers, Body: raw, SecretQuery: secretQuery, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		if write {
			return nil, "", connector.UncertainError("enterprise_wechat.network_error", transportErr)
		}
		return nil, "", connector.RetryableError("enterprise_wechat.network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := Response{}
	valid := len(response.Body) == 0 || json.Unmarshal(response.Body, &payload) == nil
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		code := "enterprise_wechat.http_" + strconv.Itoa(response.StatusCode)
		if response.StatusCode == 429 {
			return payload, ref, connector.RetryableError(code, cause)
		}
		if response.StatusCode == 408 || response.StatusCode >= 500 {
			if write {
				return payload, ref, connector.UncertainError(code, cause)
			}
			return payload, ref, connector.RetryableError(code, cause)
		}
		return payload, ref, connector.PermanentError(code, cause)
	}
	if !valid {
		if write {
			return nil, ref, connector.UncertainError("enterprise_wechat.response_invalid", errors.New("Enterprise WeChat response is invalid JSON"))
		}
		return nil, ref, permanent("response_invalid", "Enterprise WeChat response is invalid JSON")
	}
	return payload, ref, nil
}
func apiBase(connection connector.Connection) string {
	return strings.TrimRight(config(connection, "api_base_url", defaultAPIBase), "/")
}
func config(connection connector.Connection, key, fallback string) string {
	if value := mapString(connection.Config, key); value != "" {
		return value
	}
	return fallback
}
func mapString(values map[string]any, key string) string {
	if values[key] == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(values[key]))
}
func intValue(value any, fallback int) int {
	if value == nil {
		return fallback
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(fmt.Sprint(value)))
	if err != nil {
		return 0
	}
	return parsed
}
func first(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
func permanent(code, message string) error {
	return connector.PermanentError("enterprise_wechat."+code, errors.New(message))
}
