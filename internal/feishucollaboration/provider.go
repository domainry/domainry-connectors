// Package feishucollaboration contains the private Feishu collaboration
// protocol kernel shared by the official public Provider identities.
package feishucollaboration

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
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
	internalfeishu "github.com/domainry/domainry-connectors/internal/feishu"
)

const (
	defaultAPIBase = "https://open.feishu.cn"
	responseLimit  = 4 << 20
)

type Identity struct {
	ConnectorKey       string
	ProviderKey        string
	ProviderName       string
	SendContractSHA256 string
	TestContractSHA256 string
}

type SendMessageInput struct {
	Recipient       string         `json:"recipient"`
	Message         string         `json:"message"`
	Text            string         `json:"text,omitempty"`
	ProviderPayload map[string]any `json:"provider_payload,omitempty"`
}
type Response map[string]any

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

func New(transport connector.Transport, identity Identity) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Feishu transport is required")
	}
	if strings.TrimSpace(identity.ConnectorKey) == "" || strings.TrimSpace(identity.ProviderKey) == "" || strings.TrimSpace(identity.SendContractSHA256) == "" || strings.TrimSpace(identity.TestContractSHA256) == "" {
		return nil, errors.New("Feishu Provider identity is required")
	}
	sendOperation := connector.EnqueueOperation[SendMessageInput]{ConnectorKey: identity.ConnectorKey, ProviderKey: identity.ProviderKey, Key: "send_message", ContractSHA256: identity.SendContractSHA256, Reliability: writeReliability()}
	testOperation := connector.CallOperation[struct{}, Response]{ConnectorKey: identity.ConnectorKey, ProviderKey: identity.ProviderKey, Key: "test_connection", ContractSHA256: identity.TestContractSHA256, Reliability: readReliability()}
	p := &provider{transport: transport}
	send, err := connector.BindEnqueueDelivery(sendOperation, p.sendMessage)
	if err != nil {
		return nil, err
	}
	test, err := connector.BindCall(testOperation, p.callTestConnection)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(identity), send, test)
	if err != nil {
		return nil, err
	}
	p.Adapter = bound
	return p, nil
}

func schema(identity Identity) connector.ProviderSchema {
	minimum, maximum := float64(1), float64(60)
	return connector.ProviderSchema{ConnectorKey: identity.ConnectorKey, ProviderKey: identity.ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{
		{Key: "app_id", Name: "Feishu app ID", Type: connector.ConfigFieldText, Required: true},
		{Key: "api_base_url", Name: "Feishu API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://open.feishu.cn"`)},
		{Key: "receive_id_type", Name: "Recipient ID type", Type: connector.ConfigFieldSelect, Default: json.RawMessage(`"open_id"`), Validation: connector.ConfigValidation{Options: []string{"open_id", "union_id", "user_id", "email", "chat_id"}}},
		{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`15`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}},
	}, SecretFields: []connector.SecretField{
		{Key: "app_secret", Name: "Feishu app secret", Required: true, CredentialKind: connector.SecretCredentialOAuthClientSecret, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound},
		{Key: "encrypt_key", Name: "Feishu event encrypt key", CredentialKind: connector.SecretCredentialGeneric, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound},
		{Key: "verification_token", Name: "Feishu event verification token", CredentialKind: connector.SecretCredentialSigningSecret, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound},
	}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	if config(connection, "app_id", "") == "" {
		return permanent("app_id_required", "Feishu app_id is required")
	}
	parsed, err := url.Parse(apiBase(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return permanent("endpoint_invalid", "valid Feishu endpoint is required")
	}
	if !(parsed.Scheme == "http" && isLoopback(parsed.Hostname())) && (parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), "open.feishu.cn") || (parsed.EscapedPath() != "" && parsed.EscapedPath() != "/")) {
		return permanent("endpoint_invalid", "official Feishu endpoint or loopback HTTP is required")
	}
	switch config(connection, "receive_id_type", "open_id") {
	case "open_id", "union_id", "user_id", "email", "chat_id":
	default:
		return permanent("receive_id_type_invalid", "unsupported Feishu recipient ID type")
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
	token, err := p.tenantToken(ctx, request.Connection, request.Secrets)
	if err != nil {
		return connector.DeliveryResult{}, err
	}
	payload := cloneMap(request.Input.ProviderPayload)
	if len(payload) == 0 {
		content, _ := json.Marshal(map[string]string{"text": message})
		payload = map[string]any{"msg_type": "text", "content": string(content)}
	}
	payload["receive_id"] = recipient
	if uuid := requestUUID(request.RequestRef); uuid != "" {
		payload["uuid"] = uuid
	}
	query := url.Values{"receive_id_type": {config(request.Connection, "receive_id_type", "open_id")}}
	result, ref, err := p.execute(ctx, request.Connection, http.MethodPost, "/open-apis/im/v1/messages", query, payload, token, true)
	if err != nil {
		return connector.DeliveryResult{ResponseRef: ref}, err
	}
	data, _ := result["data"].(map[string]any)
	messageID := mapString(data, "message_id")
	if messageID == "" {
		return connector.DeliveryResult{ResponseRef: ref}, connector.UncertainError("feishu.message_response_invalid", errors.New("Feishu response lacks message_id"))
	}
	return connector.DeliveryResult{ResponseRef: "feishu:" + messageID}, nil
}

func (p *provider) tenantToken(ctx context.Context, connection connector.Connection, secrets map[string]string) (string, error) {
	appSecret := strings.TrimSpace(secrets["app_secret"])
	if appSecret == "" {
		return "", permanent("app_secret_required", "resolved Feishu app_secret is required")
	}
	token, err := internalfeishu.FetchTenantToken(ctx, p.transport, internalfeishu.TenantTokenRequest{Endpoint: apiBase(connection) + "/open-apis/auth/v3/tenant_access_token/internal/", AppID: config(connection, "app_id", ""), AppSecret: appSecret, ErrorPrefix: "feishu"})
	if err != nil {
		return "", err
	}
	return token.AccessToken, nil
}

func (p *provider) callTestConnection(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	_, err := p.tenantToken(ctx, request.Connection, request.Secrets)
	if err != nil {
		return connector.TypedResult[Response]{}, err
	}
	return connector.TypedResult[Response]{Output: Response{"connected": true}, ResponseRef: "feishu:tenant-token"}, nil
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.callTestConnection(ctx, connector.TypedRequest[struct{}]{Connection: request.Connection, Secrets: request.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, method, path string, query url.Values, body map[string]any, token string, write bool) (Response, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", err
	}
	endpoint, err := url.Parse(apiBase(connection) + path)
	if err != nil {
		return nil, "", permanent("request_invalid", "Feishu request URL is invalid")
	}
	endpoint.RawQuery = query.Encode()
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, "", permanent("request_invalid", "Feishu request body is invalid")
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint.String(), Headers: map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/json; charset=utf-8"}}, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, Body: raw, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		if write {
			return nil, "", connector.UncertainError("feishu.network_error", transportErr)
		}
		return nil, "", connector.RetryableError("feishu.network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := Response{}
	valid := len(response.Body) == 0 || json.Unmarshal(response.Body, &payload) == nil
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		code := "feishu.http_" + strconv.Itoa(response.StatusCode)
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
			return nil, ref, connector.UncertainError("feishu.response_invalid", errors.New("Feishu response is invalid JSON"))
		}
		return nil, ref, permanent("response_invalid", "Feishu response is invalid JSON")
	}
	if code := intValue(payload["code"], 0); code != 0 {
		return payload, ref, connector.PermanentError("feishu.provider_code_"+strconv.Itoa(code), fmt.Errorf("Feishu returned code %d", code))
	}
	return payload, ref, nil
}

func (p *provider) VerifyWebhook(ctx context.Context, request connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	if err := ctx.Err(); err != nil {
		return connector.VerifiedWebhook{}, err
	}
	body := request.Body
	encryptKey := strings.TrimSpace(request.Secrets["encrypt_key"])
	verificationToken := strings.TrimSpace(request.Secrets["verification_token"])
	security := &connector.WebhookSecurityEvidence{}
	if encryptKey != "" {
		if err := verifySignature(request, encryptKey); err != nil {
			return connector.VerifiedWebhook{}, err
		}
		var envelope struct {
			Encrypt string `json:"encrypt"`
		}
		if json.Unmarshal(body, &envelope) != nil || strings.TrimSpace(envelope.Encrypt) == "" {
			return connector.VerifiedWebhook{}, permanent("webhook_payload_invalid", "encrypted Feishu webhook payload is invalid")
		}
		var err error
		body, err = decryptEvent(envelope.Encrypt, encryptKey)
		if err != nil {
			return connector.VerifiedWebhook{}, err
		}
		security.SignatureVerified = true
	}
	var payload Response
	if json.Unmarshal(body, &payload) != nil {
		return connector.VerifiedWebhook{}, permanent("webhook_payload_invalid", "Feishu webhook payload is invalid JSON")
	}
	if encryptKey == "" && verificationToken == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_verification_required", "Feishu webhook verification is not configured")
	}
	if verificationToken != "" && !hmac.Equal([]byte(verificationToken), []byte(payloadToken(payload))) {
		return connector.VerifiedWebhook{}, permanent("webhook_token_invalid", "Feishu webhook token does not match")
	}
	if verificationToken != "" {
		security.SignatureVerified = true
	}
	if mapString(payload, "type") == "url_verification" {
		challenge := mapString(payload, "challenge")
		if challenge == "" {
			return connector.VerifiedWebhook{}, permanent("webhook_challenge_invalid", "Feishu webhook challenge is missing")
		}
		return connector.VerifiedWebhook{EventType: "url_verification", ExternalID: "challenge:" + challenge, Payload: append(json.RawMessage(nil), body...), Challenge: challenge, Security: security}, nil
	}
	header, _ := payload["header"].(map[string]any)
	event, _ := payload["event"].(map[string]any)
	eventID, eventType := mapString(header, "event_id"), mapString(header, "event_type")
	if eventID == "" || eventType == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_identity_missing", "Feishu webhook event identity is missing")
	}
	verified := connector.VerifiedWebhook{EventType: eventType, ExternalID: eventID, Payload: append(json.RawMessage(nil), body...), Security: security}
	if sender, ok := event["sender"].(map[string]any); ok {
		if ids, ok := sender["sender_id"].(map[string]any); ok {
			if subject := mapString(ids, "open_id"); subject != "" {
				verified.ExternalIdentity = &connector.WebhookExternalIdentity{Subject: subject, SubjectType: "feishu_user", Name: mapString(sender, "sender_type")}
			}
		}
	}
	return verified, nil
}

func verifySignature(request connector.VerifyWebhookRequest, encryptKey string) error {
	timestamp, nonce, signature := headerValue(request.Headers, "X-Lark-Request-Timestamp"), headerValue(request.Headers, "X-Lark-Request-Nonce"), headerValue(request.Headers, "X-Lark-Signature")
	parsed, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || nonce == "" || signature == "" {
		return permanent("webhook_signature_invalid", "Feishu webhook signature headers are invalid")
	}
	now := request.ReceivedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if delta := now.Unix() - parsed; delta > 300 || delta < -300 {
		return permanent("webhook_timestamp_invalid", "Feishu webhook timestamp is outside tolerance")
	}
	sum := sha256.Sum256(append([]byte(timestamp+nonce+encryptKey), request.Body...))
	if !hmac.Equal([]byte(hex.EncodeToString(sum[:])), []byte(strings.ToLower(signature))) {
		return permanent("webhook_signature_invalid", "Feishu webhook signature does not match")
	}
	return nil
}

func decryptEvent(encoded, encryptKey string) ([]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(raw) < 2*aes.BlockSize || len(raw)%aes.BlockSize != 0 {
		return nil, permanent("webhook_decrypt_failed", "Feishu webhook ciphertext is invalid")
	}
	key := sha256.Sum256([]byte(encryptKey))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, permanent("webhook_decrypt_failed", "Feishu webhook key is invalid")
	}
	plain := make([]byte, len(raw)-aes.BlockSize)
	cipher.NewCBCDecrypter(block, raw[:aes.BlockSize]).CryptBlocks(plain, raw[aes.BlockSize:])
	padding := int(plain[len(plain)-1])
	if padding < 1 || padding > aes.BlockSize || padding > len(plain) {
		return nil, permanent("webhook_decrypt_failed", "Feishu webhook padding is invalid")
	}
	for _, value := range plain[len(plain)-padding:] {
		if int(value) != padding {
			return nil, permanent("webhook_decrypt_failed", "Feishu webhook padding is invalid")
		}
	}
	return plain[:len(plain)-padding], nil
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
func requestUUID(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= 50 {
		return value
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:32]
}
func payloadToken(payload map[string]any) string {
	if value := mapString(payload, "token"); value != "" {
		return value
	}
	header, _ := payload["header"].(map[string]any)
	return mapString(header, "token")
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
	return connector.PermanentError("feishu."+code, errors.New(message))
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
var _ connector.WebhookVerifier = (*provider)(nil)
