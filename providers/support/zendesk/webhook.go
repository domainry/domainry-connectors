package zendesk

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
)

func (p *provider) VerifyWebhook(ctx context.Context, r connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	if err := ctx.Err(); err != nil {
		return connector.VerifiedWebhook{}, err
	}
	secret := r.Secrets["webhook_secret"]
	if secret == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_secret_required", "resolved webhook secret is required")
	}
	timestamp, actual := header(r.Headers, "X-Zendesk-Webhook-Signature-Timestamp"), header(r.Headers, "X-Zendesk-Webhook-Signature")
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(append([]byte(timestamp), r.Body...))
	expected := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	if timestamp == "" || !hmac.Equal([]byte(expected), []byte(strings.TrimSpace(actual))) {
		return connector.VerifiedWebhook{}, permanent("webhook_signature_invalid", "webhook signature is invalid")
	}
	payload := map[string]any{}
	if json.Unmarshal(r.Body, &payload) != nil {
		return connector.VerifiedWebhook{}, permanent("webhook_payload_invalid", "webhook payload is invalid")
	}
	id := clean(payload["id"])
	if id == "" {
		id = clean(payload["ticket_id"])
	}
	eventType := clean(payload["type"])
	if eventType == "" {
		eventType = "ticket.updated"
	}
	if id == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_identity_missing", "webhook identity is missing")
	}
	raw, _ := json.Marshal(payload)
	return connector.VerifiedWebhook{EventType: eventType, ExternalID: id, Payload: raw}, nil
}
func header(headers map[string][]string, key string) string {
	for current, values := range headers {
		if strings.EqualFold(current, key) && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}

var _ connector.WebhookVerifier = (*provider)(nil)
