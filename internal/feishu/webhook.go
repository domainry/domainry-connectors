package feishu

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
	"strconv"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

// VerifyWebhook authenticates and decrypts one Feishu event callback. It
// supports both Encrypt Key signatures and Verification Token callbacks and
// returns the decrypted provider payload for Provider-specific normalization.
func VerifyWebhook(ctx context.Context, request connector.VerifyWebhookRequest, errorPrefix string) (connector.VerifiedWebhook, error) {
	if err := ctx.Err(); err != nil {
		return connector.VerifiedWebhook{}, err
	}
	prefix := strings.TrimSpace(errorPrefix)
	if prefix == "" {
		prefix = "feishu"
	}
	body := request.Body
	encryptKey := strings.TrimSpace(request.Secrets["encrypt_key"])
	verificationToken := strings.TrimSpace(request.Secrets["verification_token"])
	security := &connector.WebhookSecurityEvidence{}
	if encryptKey != "" {
		if err := VerifyWebhookSignature(request, encryptKey, prefix); err != nil {
			return connector.VerifiedWebhook{}, err
		}
		var envelope struct {
			Encrypt string `json:"encrypt"`
		}
		if json.Unmarshal(body, &envelope) != nil || strings.TrimSpace(envelope.Encrypt) == "" {
			return connector.VerifiedWebhook{}, webhookPermanent(prefix, "webhook_payload_invalid", "encrypted Feishu webhook payload is invalid")
		}
		var err error
		body, err = DecryptWebhookEvent(envelope.Encrypt, encryptKey, prefix)
		if err != nil {
			return connector.VerifiedWebhook{}, err
		}
		security.SignatureVerified = true
		security.Nonce = webhookHeader(request.Headers, "X-Lark-Request-Nonce")
	}
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil || payload == nil {
		return connector.VerifiedWebhook{}, webhookPermanent(prefix, "webhook_payload_invalid", "Feishu webhook payload is invalid JSON")
	}
	if encryptKey == "" && verificationToken == "" {
		return connector.VerifiedWebhook{}, webhookPermanent(prefix, "webhook_verification_required", "Feishu webhook verification is not configured")
	}
	if verificationToken != "" && !hmac.Equal([]byte(verificationToken), []byte(webhookPayloadToken(payload))) {
		return connector.VerifiedWebhook{}, webhookPermanent(prefix, "webhook_token_invalid", "Feishu webhook token does not match")
	}
	if verificationToken != "" {
		security.SignatureVerified = true
	}
	if webhookMapString(payload, "type") == "url_verification" {
		challenge := webhookMapString(payload, "challenge")
		if challenge == "" {
			return connector.VerifiedWebhook{}, webhookPermanent(prefix, "webhook_challenge_invalid", "Feishu webhook challenge is missing")
		}
		return connector.VerifiedWebhook{EventType: "url_verification", ExternalID: "challenge:" + challenge, Payload: append(json.RawMessage(nil), body...), Challenge: challenge, ChallengeFormat: "json", Security: security}, nil
	}
	header, _ := payload["header"].(map[string]any)
	event, _ := payload["event"].(map[string]any)
	eventID, eventType := webhookMapString(header, "event_id"), webhookMapString(header, "event_type")
	if eventID == "" || eventType == "" {
		return connector.VerifiedWebhook{}, webhookPermanent(prefix, "webhook_identity_missing", "Feishu webhook event identity is missing")
	}
	verified := connector.VerifiedWebhook{EventType: eventType, ExternalID: eventID, Payload: append(json.RawMessage(nil), body...), Security: security}
	if sender, ok := event["sender"].(map[string]any); ok {
		if ids, ok := sender["sender_id"].(map[string]any); ok {
			if subject := webhookMapString(ids, "open_id"); subject != "" {
				verified.ExternalIdentity = &connector.WebhookExternalIdentity{Subject: subject, SubjectType: "feishu_user", Name: webhookMapString(sender, "sender_type")}
			}
		}
	}
	return verified, nil
}

func VerifyWebhookSignature(request connector.VerifyWebhookRequest, encryptKey, errorPrefix string) error {
	timestamp := webhookHeader(request.Headers, "X-Lark-Request-Timestamp")
	nonce := webhookHeader(request.Headers, "X-Lark-Request-Nonce")
	signature := webhookHeader(request.Headers, "X-Lark-Signature")
	parsed, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || nonce == "" || signature == "" {
		return webhookPermanent(errorPrefix, "webhook_signature_invalid", "Feishu webhook signature headers are invalid")
	}
	now := request.ReceivedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if delta := now.Unix() - parsed; delta > 300 || delta < -300 {
		return webhookPermanent(errorPrefix, "webhook_timestamp_invalid", "Feishu webhook timestamp is outside tolerance")
	}
	sum := sha256.Sum256(append([]byte(timestamp+nonce+encryptKey), request.Body...))
	if !hmac.Equal([]byte(hex.EncodeToString(sum[:])), []byte(strings.ToLower(signature))) {
		return webhookPermanent(errorPrefix, "webhook_signature_invalid", "Feishu webhook signature does not match")
	}
	return nil
}

func DecryptWebhookEvent(encoded, encryptKey, errorPrefix string) ([]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(raw) < 2*aes.BlockSize || len(raw)%aes.BlockSize != 0 {
		return nil, webhookPermanent(errorPrefix, "webhook_decrypt_failed", "Feishu webhook ciphertext is invalid")
	}
	key := sha256.Sum256([]byte(encryptKey))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, webhookPermanent(errorPrefix, "webhook_decrypt_failed", "Feishu webhook key is invalid")
	}
	plain := make([]byte, len(raw)-aes.BlockSize)
	cipher.NewCBCDecrypter(block, raw[:aes.BlockSize]).CryptBlocks(plain, raw[aes.BlockSize:])
	padding := int(plain[len(plain)-1])
	if padding < 1 || padding > aes.BlockSize || padding > len(plain) {
		return nil, webhookPermanent(errorPrefix, "webhook_decrypt_failed", "Feishu webhook padding is invalid")
	}
	for _, value := range plain[len(plain)-padding:] {
		if int(value) != padding {
			return nil, webhookPermanent(errorPrefix, "webhook_decrypt_failed", "Feishu webhook padding is invalid")
		}
	}
	return plain[:len(plain)-padding], nil
}

func webhookPayloadToken(payload map[string]any) string {
	if value := webhookMapString(payload, "token"); value != "" {
		return value
	}
	header, _ := payload["header"].(map[string]any)
	return webhookMapString(header, "token")
}

func webhookMapString(values map[string]any, key string) string {
	if values == nil || values[key] == nil {
		return ""
	}
	value := strings.TrimSpace(fmt.Sprint(values[key]))
	if value == "<nil>" {
		return ""
	}
	return value
}

func webhookHeader(headers map[string][]string, key string) string {
	for candidate, values := range headers {
		if strings.EqualFold(strings.TrimSpace(candidate), key) && len(values) > 0 {
			return strings.TrimSpace(values[0])
		}
	}
	return ""
}

func webhookPermanent(prefix, code, message string) error {
	prefix = strings.TrimSuffix(strings.TrimSpace(prefix), ".")
	if prefix == "" {
		prefix = "feishu"
	}
	return connector.PermanentError(prefix+"."+code, errors.New(message))
}
