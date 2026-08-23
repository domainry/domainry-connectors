package facebookmessenger

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
)

func (p *provider) VerifyWebhook(ctx context.Context, r connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	if err := ctx.Err(); err != nil {
		return connector.VerifiedWebhook{}, err
	}
	if challenge := value(r.Query, "hub.challenge"); challenge != "" {
		token := r.Secrets["webhook_secret"]
		if value(r.Query, "hub.mode") != "subscribe" || token == "" || !hmac.Equal([]byte(token), []byte(value(r.Query, "hub.verify_token"))) {
			return connector.VerifiedWebhook{}, permanent("webhook_verify_token_invalid", "webhook verify token is invalid")
		}
		return connector.VerifiedWebhook{EventType: "url_verification", ExternalID: "challenge:" + challenge, Challenge: challenge, ChallengeFormat: "text/plain"}, nil
	}
	secret := r.Secrets["app_secret"]
	if secret == "" {
		return connector.VerifiedWebhook{}, permanent("app_secret_required", "resolved app secret is required")
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(r.Body)
	expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(strings.ToLower(value(r.Headers, "X-Hub-Signature-256"))), []byte(expected)) {
		return connector.VerifiedWebhook{}, permanent("webhook_signature_invalid", "webhook signature is invalid")
	}
	payload := map[string]any{}
	if json.Unmarshal(r.Body, &payload) != nil {
		return connector.VerifiedWebhook{}, permanent("webhook_payload_invalid", "webhook payload is invalid")
	}
	eventType, id, sender := identity(payload)
	if id == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_identity_missing", "webhook identity is missing")
	}
	raw, _ := json.Marshal(payload)
	event := connector.VerifiedWebhook{EventType: eventType, ExternalID: id, Payload: raw}
	if sender != "" {
		event.ExternalIdentity = &connector.WebhookExternalIdentity{Subject: sender, SubjectType: "social_user"}
	}
	return event, nil
}
func identity(payload map[string]any) (string, string, string) {
	entries, _ := payload["entry"].([]any)
	for _, rawEntry := range entries {
		entry, _ := rawEntry.(map[string]any)
		messages, _ := entry["messaging"].([]any)
		for _, rawMessage := range messages {
			message, _ := rawMessage.(map[string]any)
			senderMap, _ := message["sender"].(map[string]any)
			sender := clean(senderMap["id"])
			if content, ok := message["message"].(map[string]any); ok {
				return "message", clean(content["mid"]), sender
			}
			if delivery, ok := message["delivery"].(map[string]any); ok {
				mids, _ := delivery["mids"].([]any)
				if len(mids) > 0 {
					return "message.delivery", clean(mids[0]), sender
				}
				return "message.delivery", "watermark:" + clean(delivery["watermark"]), sender
			}
		}
	}
	return "", "", ""
}
func value(values map[string][]string, key string) string {
	for current, list := range values {
		if strings.EqualFold(current, key) && len(list) > 0 {
			return strings.TrimSpace(list[0])
		}
	}
	return ""
}
func clean(input any) string {
	result := strings.TrimSpace(fmt.Sprint(input))
	if result == "<nil>" {
		return ""
	}
	return result
}

var _ connector.WebhookVerifier = (*provider)(nil)
