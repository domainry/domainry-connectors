// Package lineworks implements the official LINE WORKS collaboration Provider.
package lineworks

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
	"github.com/domainry/domainry-connectors/internal/notificationmessage"
)

const (
	ConnectorKey   = "collaboration"
	ProviderKey    = "line_works"
	defaultAPIBase = "https://www.worksapis.com/v1.0"
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
		return nil, errors.New("LINE WORKS transport is required")
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
	minimum, maximum := float64(1), float64(60)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "api_base_url", Name: "LINE WORKS API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://www.worksapis.com/v1.0"`)}, {Key: "bot_id", Name: "LINE WORKS bot ID", Type: connector.ConfigFieldText, Required: true}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`15`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}}}, SecretFields: []connector.SecretField{{Key: "bot_token", Name: "LINE WORKS bot access token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(apiBase(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return permanent("endpoint_invalid", "valid LINE WORKS endpoint is required")
	}
	if !(parsed.Scheme == "http" && isLoopback(parsed.Hostname())) && (parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), "www.worksapis.com") || strings.TrimRight(parsed.EscapedPath(), "/") != "/v1.0") {
		return permanent("endpoint_invalid", "official LINE WORKS API v1.0 endpoint or loopback HTTP is required")
	}
	if strings.TrimSpace(config(connection, "bot_id", "")) == "" {
		return permanent("bot_id_required", "LINE WORKS bot_id is required")
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
	payload, compileErr := notificationmessage.ResolveProviderPayload(ProviderKey, request.Input.NotificationContent, request.Input.ProviderPayload)
	if compileErr != nil {
		return connector.DeliveryResult{}, permanent("notification_content_invalid", compileErr.Error())
	}
	if len(payload) == 0 {
		payload = map[string]any{"content": map[string]any{"type": "text", "text": message}}
	}
	botID := config(request.Connection, "bot_id", "")
	result, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodPost, "/bots/"+url.PathEscape(botID)+"/users/"+url.PathEscape(recipient)+"/messages", payload, true)
	if err != nil {
		return connector.DeliveryResult{ResponseRef: ref}, err
	}
	if requestID := mapString(result, "requestId"); requestID != "" {
		ref = "line_works:" + requestID
	} else {
		ref = "line_works:message:accepted"
	}
	return connector.DeliveryResult{ResponseRef: ref}, nil
}
func (p *provider) callTestConnection(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/bots/"+url.PathEscape(config(request.Connection, "bot_id", "")), nil, false)
	return connector.TypedResult[Response]{Output: Response{"connected": err == nil, "bot": payload}, ResponseRef: ref}, err
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.callTestConnection(ctx, connector.TypedRequest[struct{}]{Connection: request.Connection, Secrets: request.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}
func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, method, path string, body map[string]any, write bool) (Response, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", err
	}
	token := strings.TrimSpace(secrets["bot_token"])
	if token == "" {
		return nil, "", permanent("bot_token_required", "resolved LINE WORKS bot token is required")
	}
	var raw []byte
	var err error
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return nil, "", permanent("request_invalid", "LINE WORKS request body is invalid")
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: apiBase(connection) + path, Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, Body: raw, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		if write {
			return nil, "", connector.UncertainError("line_works.network_error", transportErr)
		}
		return nil, "", connector.RetryableError("line_works.network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := Response{}
	valid := len(response.Body) == 0 || json.Unmarshal(response.Body, &payload) == nil
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		code := "line_works.http_" + strconv.Itoa(response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests {
			return payload, ref, connector.RetryableError(code, cause)
		}
		if response.StatusCode == http.StatusRequestTimeout || response.StatusCode >= 500 {
			if write {
				return payload, ref, connector.UncertainError(code, cause)
			}
			return payload, ref, connector.RetryableError(code, cause)
		}
		return payload, ref, connector.PermanentError(code, cause)
	}
	if !valid {
		if write {
			return nil, ref, connector.UncertainError("line_works.response_invalid", errors.New("LINE WORKS response is invalid JSON"))
		}
		return nil, ref, permanent("response_invalid", "LINE WORKS response is invalid JSON")
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
	if values == nil || values[key] == nil {
		return ""
	}
	value := strings.TrimSpace(fmt.Sprint(values[key]))
	if value == "<nil>" {
		return ""
	}
	return value
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
func cloneMap(source map[string]any) map[string]any {
	if len(source) == 0 {
		return nil
	}
	target := make(map[string]any, len(source))
	for key, value := range source {
		target[key] = value
	}
	return target
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
func permanent(code, message string) error {
	return connector.PermanentError("line_works."+code, errors.New(message))
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
