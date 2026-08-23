// Package discord implements the official Discord collaboration Provider.
package discord

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
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
	ProviderKey    = "discord"
	defaultAPIBase = "https://discord.com/api/v10"
	responseLimit  = 4 << 20
)

type SendMessageInput struct {
	Recipient       string         `json:"recipient"`
	ChannelID       string         `json:"channel_id,omitempty"`
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
		return nil, errors.New("Discord transport is required")
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
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "api_base_url", Name: "Discord API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://discord.com/api/v10"`)}, {Key: "channel_id", Name: "Default channel ID", Type: connector.ConfigFieldText}, {Key: "application_public_key", Name: "Application public key", Type: connector.ConfigFieldText}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`15`), Validation: connector.ConfigValidation{Min: &min, Max: &max}}}, SecretFields: []connector.SecretField{{Key: "bot_token", Name: "Discord bot token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(apiBase(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return permanent("endpoint_invalid", "valid Discord endpoint is required")
	}
	if parsed.Scheme == "http" && isLoopback(parsed.Hostname()) {
		return nil
	}
	path := strings.TrimRight(parsed.EscapedPath(), "/")
	if parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), "discord.com") || path != "/api/v10" {
		return permanent("endpoint_invalid", "official Discord API v10 endpoint or loopback HTTP is required")
	}
	if seconds := intValue(connection.Config["timeout_seconds"], 15); seconds < 1 || seconds > 60 {
		return permanent("timeout_invalid", "timeout_seconds must be between 1 and 60")
	}
	return nil
}
func (p *provider) sendMessage(ctx context.Context, request connector.TypedRequest[SendMessageInput]) (connector.DeliveryResult, error) {
	input := request.Input
	channel := first(input.ChannelID, input.Recipient, config(request.Connection, "channel_id", ""))
	message := first(input.Message, input.Text)
	if channel == "" || message == "" {
		return connector.DeliveryResult{}, permanent("message_fields_required", "recipient or channel_id and message are required")
	}
	payload := input.ProviderPayload
	if len(payload) == 0 {
		payload = map[string]any{"content": message}
	}
	if request.RequestRef != "" {
		payload["nonce"], payload["enforce_nonce"] = request.RequestRef, true
	}
	result, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodPost, "/channels/"+url.PathEscape(channel)+"/messages", payload, true)
	if err != nil {
		return connector.DeliveryResult{ResponseRef: ref}, err
	}
	if id := mapString(result, "id"); id != "" {
		ref = "discord:" + id
	} else {
		ref = "discord:message:accepted"
	}
	return connector.DeliveryResult{ResponseRef: ref}, nil
}
func (p *provider) callTestConnection(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/users/@me", nil, false)
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
		return nil, "", permanent("bot_token_required", "resolved Discord bot token is required")
	}
	var raw []byte
	var err error
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return nil, "", permanent("request_invalid", "Discord request body is invalid")
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: apiBase(connection) + path, Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bot " + token}}, Body: raw, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		if write {
			return nil, "", connector.UncertainError("discord.network_error", transportErr)
		}
		return nil, "", connector.RetryableError("discord.network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := Response{}
	valid := len(response.Body) == 0 || json.Unmarshal(response.Body, &payload) == nil
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		code := "discord.http_" + strconv.Itoa(response.StatusCode)
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
			return nil, ref, connector.UncertainError("discord.response_invalid", errors.New("Discord response is invalid JSON"))
		}
		return nil, ref, permanent("response_invalid", "Discord response is invalid JSON")
	}
	return payload, ref, nil
}
func (p *provider) VerifyWebhook(ctx context.Context, request connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	if err := ctx.Err(); err != nil {
		return connector.VerifiedWebhook{}, err
	}
	publicKey, err := hex.DecodeString(strings.TrimSpace(config(request.Connection, "application_public_key", "")))
	if err != nil || len(publicKey) != ed25519.PublicKeySize {
		return connector.VerifiedWebhook{}, permanent("public_key_invalid", "Discord application public key is invalid")
	}
	timestamp := header(request.Headers, "X-Signature-Timestamp")
	signature, err := hex.DecodeString(header(request.Headers, "X-Signature-Ed25519"))
	if timestamp == "" || err != nil || len(signature) != ed25519.SignatureSize || !ed25519.Verify(ed25519.PublicKey(publicKey), append([]byte(timestamp), request.Body...), signature) {
		return connector.VerifiedWebhook{}, permanent("signature_invalid", "Discord interaction signature is invalid")
	}
	payload := Response{}
	if json.Unmarshal(request.Body, &payload) != nil {
		return connector.VerifiedWebhook{}, permanent("payload_invalid", "Discord interaction payload is invalid JSON")
	}
	if intValue(payload["type"], 0) == 1 {
		return connector.VerifiedWebhook{EventType: "ping", ExternalID: "ping:" + timestamp, Payload: append(json.RawMessage(nil), request.Body...), Challenge: `{"type":1}`, ChallengeFormat: "application/json", Security: &connector.WebhookSecurityEvidence{SignatureVerified: true}}, nil
	}
	id := mapString(payload, "id")
	if id == "" {
		return connector.VerifiedWebhook{}, permanent("identity_missing", "Discord interaction ID is required")
	}
	verified := connector.VerifiedWebhook{EventType: "interaction", ExternalID: "discord:" + id, Payload: append(json.RawMessage(nil), request.Body...), Security: &connector.WebhookSecurityEvidence{SignatureVerified: true}}
	if member, ok := payload["member"].(map[string]any); ok {
		if user, ok := member["user"].(map[string]any); ok {
			if subject := mapString(user, "id"); subject != "" {
				verified.ExternalIdentity = &connector.WebhookExternalIdentity{Subject: subject, SubjectType: "discord_user", Name: first(mapString(user, "global_name"), mapString(user, "username"))}
			}
		}
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
func header(headers map[string][]string, key string) string {
	for current, values := range headers {
		if strings.EqualFold(current, key) && len(values) > 0 {
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
	return connector.PermanentError("discord."+code, errors.New(message))
}
