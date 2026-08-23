package metacloudapi

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

func (p *provider) VerifyWebhook(ctx context.Context, r connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	if err := ctx.Err(); err != nil {
		return connector.VerifiedWebhook{}, err
	}
	if challenge := value(r.Query, "hub.challenge"); challenge != "" {
		token := r.Secrets["webhook_verify_token"]
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
	eventType, id, identity, receipt := webhookIdentity(payload)
	if eventType == "" || id == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_identity_missing", "webhook identity is missing")
	}
	raw, _ := json.Marshal(payload)
	return connector.VerifiedWebhook{EventType: eventType, ExternalID: id, Payload: raw, ExternalIdentity: identity, DeliveryReceipt: receipt}, nil
}
func webhookIdentity(payload map[string]any) (string, string, *connector.WebhookExternalIdentity, *connector.WebhookDeliveryReceipt) {
	entries, _ := payload["entry"].([]any)
	for _, rawEntry := range entries {
		entry, _ := rawEntry.(map[string]any)
		changes, _ := entry["changes"].([]any)
		for _, rawChange := range changes {
			change, _ := rawChange.(map[string]any)
			v, _ := change["value"].(map[string]any)
			if messages, ok := v["messages"].([]any); ok && len(messages) > 0 {
				message, _ := messages[0].(map[string]any)
				id, from := clean(message["id"]), clean(message["from"])
				var identity *connector.WebhookExternalIdentity
				if from != "" {
					identity = &connector.WebhookExternalIdentity{Subject: from, SubjectType: "phone"}
				}
				return "message", id, identity, nil
			}
			if statuses, ok := v["statuses"].([]any); ok && len(statuses) > 0 {
				status, _ := statuses[0].(map[string]any)
				id, state := clean(status["id"]), clean(status["status"])
				seconds, _ := strconv.ParseInt(clean(status["timestamp"]), 10, 64)
				return "message_status." + state, id, nil, &connector.WebhookDeliveryReceipt{ResponseRef: "whatsapp:" + id, Status: state, Error: statusError(status), OccurredAt: time.Unix(seconds, 0).UTC()}
			}
		}
	}
	return "", "", nil, nil
}
func statusError(status map[string]any) string {
	items, _ := status["errors"].([]any)
	if len(items) == 0 {
		return ""
	}
	first, _ := items[0].(map[string]any)
	for _, key := range []string{"title", "message", "code"} {
		if result := clean(first[key]); result != "" {
			return result
		}
	}
	return ""
}
func value(values map[string][]string, key string) string {
	for current, list := range values {
		if strings.EqualFold(current, key) && len(list) > 0 {
			return strings.TrimSpace(list[0])
		}
	}
	return ""
}

var _ connector.WebhookVerifier = (*provider)(nil)
