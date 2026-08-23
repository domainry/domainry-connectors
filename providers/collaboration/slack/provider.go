// Package slack implements the official Slack collaboration Provider.
package slack

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	ConnectorKey   = "collaboration"
	ProviderKey    = "slack"
	defaultAPIBase = "https://slack.com/api"
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
		return nil, errors.New("Slack transport is required")
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
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "api_base_url", Name: "Slack Web API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://slack.com/api"`)}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`15`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}}, {Key: "external_identity_mappings", Name: "External identity mappings", Type: connector.ConfigFieldJSON}}, SecretFields: []connector.SecretField{{Key: "bot_token", Name: "Slack bot token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}, {Key: "signing_secret", Name: "Slack signing secret", CredentialKind: connector.SecretCredentialSigningSecret, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(apiBase(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return permanent("endpoint_invalid", "valid Slack Web API endpoint is required")
	}
	if !(parsed.Scheme == "http" && isLoopback(parsed.Hostname())) && (parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), "slack.com") || strings.TrimRight(parsed.EscapedPath(), "/") != "/api") {
		return permanent("endpoint_invalid", "official Slack Web API endpoint or loopback HTTP is required")
	}
	if seconds := intValue(connection.Config["timeout_seconds"], 15); seconds < 1 || seconds > 60 {
		return permanent("timeout_invalid", "timeout_seconds must be between 1 and 60")
	}
	return nil
}
func (p *provider) sendMessage(ctx context.Context, request connector.TypedRequest[SendMessageInput]) (connector.DeliveryResult, error) {
	channel, message := strings.TrimSpace(request.Input.Recipient), first(request.Input.Message, request.Input.Text)
	if channel == "" || message == "" {
		return connector.DeliveryResult{}, permanent("message_fields_required", "recipient and message are required")
	}
	payload := cloneMap(request.Input.ProviderPayload)
	if len(payload) == 0 {
		payload = map[string]any{"text": message}
	}
	payload["channel"] = channel
	payload["client_msg_id"] = clientMessageID(request.RequestRef + "\x00" + channel + "\x00" + message)
	response, ref, err := p.call(ctx, request.Connection, request.Secrets, "chat.postMessage", payload, true)
	if err != nil {
		return connector.DeliveryResult{ResponseRef: ref}, err
	}
	timestamp := mapString(response, "ts")
	if timestamp == "" {
		return connector.DeliveryResult{ResponseRef: ref}, connector.UncertainError("slack.message_response_invalid", errors.New("Slack response lacks message timestamp"))
	}
	return connector.DeliveryResult{ResponseRef: "slack:" + timestamp}, nil
}
func (p *provider) callTestConnection(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	response, ref, err := p.call(ctx, request.Connection, request.Secrets, "auth.test", map[string]any{}, false)
	return connector.TypedResult[Response]{Output: Response{"connected": err == nil, "team_id": response["team_id"], "user_id": response["user_id"]}, ResponseRef: ref}, err
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.callTestConnection(ctx, connector.TypedRequest[struct{}]{Connection: request.Connection, Secrets: request.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}
func (p *provider) call(ctx context.Context, connection connector.Connection, secrets map[string]string, method string, payload map[string]any, write bool) (Response, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", err
	}
	token := strings.TrimSpace(secrets["bot_token"])
	if token == "" {
		return nil, "", permanent("bot_token_required", "resolved Slack bot token is required")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, "", permanent("request_invalid", "Slack request body is invalid")
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodPost, URL: apiBase(connection) + "/" + method, Headers: map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/json; charset=utf-8"}}, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, Body: body, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		if write {
			return nil, "", connector.UncertainError("slack.network_error", transportErr)
		}
		return nil, "", connector.RetryableError("slack.network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	parsed := Response{}
	valid := json.Unmarshal(response.Body, &parsed) == nil
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("Slack returned HTTP %d", response.StatusCode)
		code := "slack.http_" + strconv.Itoa(response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests {
			return parsed, ref, connector.RetryableError(code, cause)
		}
		if response.StatusCode == http.StatusRequestTimeout || response.StatusCode >= 500 {
			if write {
				return parsed, ref, connector.UncertainError(code, cause)
			}
			return parsed, ref, connector.RetryableError(code, cause)
		}
		return parsed, ref, connector.PermanentError(code, cause)
	}
	if !valid {
		if write {
			return nil, ref, connector.UncertainError("slack.response_invalid", errors.New("Slack response is invalid JSON"))
		}
		return nil, ref, permanent("response_invalid", "Slack response is invalid JSON")
	}
	if ok, _ := parsed["ok"].(bool); !ok {
		providerCode := mapString(parsed, "error")
		if providerCode == "ratelimited" {
			return parsed, ref, connector.RetryableError("slack.provider_ratelimited", errors.New("Slack rate limited the request"))
		}
		return parsed, ref, connector.PermanentError("slack.provider_"+errorPart(providerCode), errors.New("Slack rejected the request"))
	}
	return parsed, ref, nil
}
func (p *provider) VerifyWebhook(ctx context.Context, request connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	if err := ctx.Err(); err != nil {
		return connector.VerifiedWebhook{}, err
	}
	secret := strings.TrimSpace(request.Secrets["signing_secret"])
	if secret == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_signing_secret_required", "resolved Slack signing secret is required")
	}
	timestamp := headerValue(request.Headers, "X-Slack-Request-Timestamp")
	parsedTimestamp, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || parsedTimestamp <= 0 {
		return connector.VerifiedWebhook{}, permanent("webhook_timestamp_invalid", "Slack webhook timestamp is invalid")
	}
	receivedAt := request.ReceivedAt
	if receivedAt.IsZero() {
		receivedAt = time.Now().UTC()
	}
	if delta := receivedAt.Unix() - parsedTimestamp; delta > 300 || delta < -300 {
		return connector.VerifiedWebhook{}, permanent("webhook_timestamp_out_of_range", "Slack webhook timestamp is outside tolerance")
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte("v0:" + timestamp + ":"))
	_, _ = mac.Write(request.Body)
	expected := "v0=" + hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(headerValue(request.Headers, "X-Slack-Signature"))) {
		return connector.VerifiedWebhook{}, permanent("webhook_signature_invalid", "Slack webhook signature does not match")
	}
	payload := Response{}
	if json.Unmarshal(request.Body, &payload) != nil {
		return connector.VerifiedWebhook{}, permanent("webhook_payload_invalid", "Slack webhook payload is invalid JSON")
	}
	security := &connector.WebhookSecurityEvidence{SignatureVerified: true, EventTime: time.Unix(parsedTimestamp, 0).UTC()}
	if mapString(payload, "type") == "url_verification" {
		challenge := mapString(payload, "challenge")
		if challenge == "" {
			return connector.VerifiedWebhook{}, permanent("webhook_challenge_invalid", "Slack webhook challenge is missing")
		}
		return connector.VerifiedWebhook{EventType: "url_verification", ExternalID: "challenge:" + challenge, Payload: append(json.RawMessage(nil), request.Body...), Challenge: challenge, Security: security}, nil
	}
	externalID := mapString(payload, "event_id")
	event, _ := payload["event"].(map[string]any)
	eventType := mapString(event, "type")
	if externalID == "" || eventType == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_identity_missing", "Slack webhook event identity is missing")
	}
	verified := connector.VerifiedWebhook{EventType: eventType, ExternalID: externalID, Payload: append(json.RawMessage(nil), request.Body...), Security: security}
	if subject := mapString(event, "user"); subject != "" {
		verified.ExternalIdentity = &connector.WebhookExternalIdentity{Subject: subject, SubjectType: "slack_user", Name: mapString(event, "username"), Group: mapString(event, "channel")}
	}
	return verified, nil
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
func clientMessageID(value string) string {
	sum := sha256.Sum256([]byte(value))
	encoded := hex.EncodeToString(sum[:16])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}
func errorPart(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var out strings.Builder
	for _, char := range value {
		if char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '_' {
			out.WriteRune(char)
		}
	}
	if out.Len() == 0 {
		return "unknown"
	}
	return out.String()
}
func headerValue(headers map[string][]string, key string) string {
	for candidate, values := range headers {
		if strings.EqualFold(strings.TrimSpace(candidate), key) && len(values) > 0 {
			return strings.TrimSpace(values[0])
		}
	}
	return ""
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
func permanent(code, message string) error {
	return connector.PermanentError("slack."+code, errors.New(message))
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
var _ connector.WebhookVerifier = (*provider)(nil)
