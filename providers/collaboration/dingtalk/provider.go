// Package dingtalk implements the official DingTalk collaboration Provider.
package dingtalk

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/internal/notificationmessage"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	ConnectorKey   = "collaboration"
	ProviderKey    = "dingtalk"
	defaultAPIBase = "https://api.dingtalk.com"
	defaultBotBase = "https://oapi.dingtalk.com"
	responseLimit  = 4 << 20
)

type SendMessageInput struct {
	Recipient           string         `json:"recipient"`
	Message             string         `json:"message"`
	Text                string         `json:"text,omitempty"`
	ProviderPayload     map[string]any `json:"provider_payload,omitempty"`
	NotificationContent map[string]any `json:"notification_content,omitempty"`
}
type Response map[string]any

var (
	SendMessage    = connector.EnqueueOperation[SendMessageInput]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "send_message", ContractSHA256: "5dc35c6a31a177b460a433d8d33524d57f4c9cbbf08a8a84e0bda46ad512c324", Reliability: writeReliability()}
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
		return nil, errors.New("DingTalk transport is required")
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
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "api_base_url", Name: "DingTalk API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://api.dingtalk.com"`)}, {Key: "bot_base_url", Name: "DingTalk bot API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://oapi.dingtalk.com"`)}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`15`), Validation: connector.ConfigValidation{Min: &min, Max: &max}}}, SecretFields: []connector.SecretField{{Key: "client_id", Name: "DingTalk application client ID", Required: true, CredentialKind: connector.SecretCredentialIdentifier, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}, {Key: "client_secret", Name: "DingTalk application client secret", Required: true, CredentialKind: connector.SecretCredentialOAuthClientSecret, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}, {Key: "bot_token", Name: "DingTalk custom robot access token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}, {Key: "webhook_secret", Name: "DingTalk custom robot signing secret", CredentialKind: connector.SecretCredentialSigningSecret, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	for _, item := range []struct{ endpoint, host string }{{apiBase(connection), "api.dingtalk.com"}, {botBase(connection), "oapi.dingtalk.com"}} {
		parsed, err := url.Parse(item.endpoint)
		if err != nil || parsed.Host == "" || parsed.User != nil {
			return permanent("endpoint_invalid", "valid DingTalk endpoint is required")
		}
		if parsed.Scheme == "http" && isLoopback(parsed.Hostname()) {
			continue
		}
		if parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), item.host) || (parsed.EscapedPath() != "" && parsed.EscapedPath() != "/") {
			return permanent("endpoint_invalid", "official DingTalk endpoint or loopback HTTP is required")
		}
	}
	if seconds := intValue(connection.Config["timeout_seconds"], 15); seconds < 1 || seconds > 60 {
		return permanent("timeout_invalid", "timeout_seconds must be between 1 and 60")
	}
	return nil
}
func (p *provider) sendMessage(ctx context.Context, request connector.TypedRequest[SendMessageInput]) (connector.DeliveryResult, error) {
	input := request.Input
	message := first(input.Message, input.Text)
	if strings.TrimSpace(input.Recipient) == "" || message == "" {
		return connector.DeliveryResult{}, permanent("message_fields_required", "recipient and message are required")
	}
	token := strings.TrimSpace(request.Secrets["bot_token"])
	if token == "" {
		return connector.DeliveryResult{}, permanent("bot_token_required", "resolved DingTalk bot token is required")
	}
	payload, compileErr := notificationmessage.ResolveProviderPayload(ProviderKey, input.NotificationContent, input.ProviderPayload)
	if compileErr != nil {
		return connector.DeliveryResult{}, permanent("notification_content_invalid", compileErr.Error())
	}
	if len(payload) == 0 {
		payload = map[string]any{"msgtype": "text", "text": map[string]any{"content": message}}
	}
	query := url.Values{}
	if secret := strings.TrimSpace(request.Secrets["webhook_secret"]); secret != "" {
		timestamp := strconv.FormatInt(time.Now().UTC().UnixMilli(), 10)
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write([]byte(timestamp + "\n" + secret))
		query.Set("timestamp", timestamp)
		query.Set("sign", base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	}
	result, ref, err := p.execute(ctx, request.Connection, http.MethodPost, botBase(request.Connection)+"/robot/send", query, payload, nil, map[string]string{"access_token": token}, true)
	if err != nil {
		return connector.DeliveryResult{ResponseRef: ref}, err
	}
	if intValue(result["errcode"], -1) != 0 {
		return connector.DeliveryResult{ResponseRef: ref}, permanent("bot_rejected", fmt.Sprintf("DingTalk bot rejected message: %s", mapString(result, "errmsg")))
	}
	return connector.DeliveryResult{ResponseRef: "dingtalk:bot:accepted"}, nil
}
func (p *provider) callTestConnection(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	clientID, clientSecret := strings.TrimSpace(request.Secrets["client_id"]), strings.TrimSpace(request.Secrets["client_secret"])
	if clientID == "" || clientSecret == "" {
		return connector.TypedResult[Response]{}, permanent("oauth_credentials_required", "resolved DingTalk client_id and client_secret are required")
	}
	payload, ref, err := p.execute(ctx, request.Connection, http.MethodPost, apiBase(request.Connection)+"/v1.0/oauth2/accessToken", nil, nil, map[string]string{"appKey": clientID, "appSecret": clientSecret}, nil, false)
	if err != nil {
		return connector.TypedResult[Response]{ResponseRef: ref}, err
	}
	if mapString(payload, "accessToken") == "" {
		return connector.TypedResult[Response]{ResponseRef: ref}, permanent("token_invalid", "DingTalk access token response is invalid")
	}
	return connector.TypedResult[Response]{Output: Response{"connected": true, "expires_in": payload["expireIn"]}, ResponseRef: ref}, nil
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.callTestConnection(ctx, connector.TypedRequest[struct{}]{Connection: request.Connection, Secrets: request.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}
func (p *provider) execute(ctx context.Context, connection connector.Connection, method, endpoint string, query url.Values, body map[string]any, secretJSON, secretQuery map[string]string, write bool) (Response, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", err
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return nil, "", permanent("request_invalid", "DingTalk request URL is invalid")
	}
	parsed.RawQuery = query.Encode()
	var raw []byte
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return nil, "", permanent("request_invalid", "DingTalk request body is invalid")
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/json"}}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: parsed.String(), Headers: headers, Body: raw, SecretJSON: secretJSON, SecretQuery: secretQuery, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		if write {
			return nil, "", connector.UncertainError("dingtalk.network_error", transportErr)
		}
		return nil, "", connector.RetryableError("dingtalk.network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := Response{}
	validJSON := len(response.Body) == 0 || json.Unmarshal(response.Body, &payload) == nil
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		code := "dingtalk.http_" + strconv.Itoa(response.StatusCode)
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
	if !validJSON {
		if write {
			return nil, ref, connector.UncertainError("dingtalk.response_invalid", errors.New("DingTalk response is invalid JSON"))
		}
		return nil, ref, permanent("response_invalid", "DingTalk response is invalid JSON")
	}
	return payload, ref, nil
}
func apiBase(connection connector.Connection) string {
	return strings.TrimRight(config(connection, "api_base_url", defaultAPIBase), "/")
}
func botBase(connection connector.Connection) string {
	return strings.TrimRight(config(connection, "bot_base_url", defaultBotBase), "/")
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
	return connector.PermanentError("dingtalk."+code, errors.New(message))
}
