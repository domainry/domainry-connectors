package twilio

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	connector "github.com/domainry/domainry-connector-sdk"
	"net/url"
	"sort"
	"strings"
)

func (p *provider) VerifyWebhook(ctx context.Context, r connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	if err := ctx.Err(); err != nil {
		return connector.VerifiedWebhook{}, err
	}
	token := r.Secrets["auth_token"]
	if token == "" {
		return connector.VerifiedWebhook{}, permanent("auth_token_required", "resolved auth token is required")
	}
	form, err := url.ParseQuery(string(r.Body))
	if err != nil {
		return connector.VerifiedWebhook{}, permanent("webhook_payload_invalid", "webhook payload is invalid")
	}
	canonicalURL := config(r.Connection, "webhook_url")
	if canonicalURL == "" || !validSignature(token, canonicalURL, form, header(r.Headers, "X-Twilio-Signature")) {
		return connector.VerifiedWebhook{}, permanent("webhook_signature_invalid", "webhook signature is invalid")
	}
	id := first(form, "MessageSid", "CallSid", "SmsSid")
	if id == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_identity_missing", "webhook identity is missing")
	}
	eventType := "message.status." + strings.ToLower(first(form, "MessageStatus", "SmsStatus"))
	if strings.HasSuffix(eventType, ".") {
		eventType = "call.status." + strings.ToLower(first(form, "CallStatus"))
	}
	payload := map[string]any{}
	for key, values := range form {
		if len(values) == 1 {
			payload[key] = values[0]
		} else {
			payload[key] = append([]string(nil), values...)
		}
	}
	raw, _ := json.Marshal(payload)
	event := connector.VerifiedWebhook{EventType: eventType, ExternalID: id, Payload: raw}
	if from := first(form, "From"); from != "" {
		event.ExternalIdentity = &connector.WebhookExternalIdentity{Subject: from, SubjectType: "phone"}
	}
	return event, nil
}
func validSignature(secret, canonicalURL string, form url.Values, actual string) bool {
	keys := make([]string, 0, len(form))
	for key := range form {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	value := canonicalURL
	for _, key := range keys {
		values := append([]string(nil), form[key]...)
		sort.Strings(values)
		for _, item := range values {
			value += key + item
		}
	}
	mac := hmac.New(sha1.New, []byte(secret))
	_, _ = mac.Write([]byte(value))
	return hmac.Equal([]byte(strings.TrimSpace(actual)), []byte(base64.StdEncoding.EncodeToString(mac.Sum(nil))))
}
func first(form url.Values, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(form.Get(key)); value != "" {
			return value
		}
	}
	return ""
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
