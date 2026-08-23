package stripe

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

// VerifyWebhook authenticates and normalizes one request after Runtime has
// enforced ingress size and connection resolution. Runtime still owns replay
// protection, persistence, audit, and the HTTP response.
func (*provider) VerifyWebhook(ctx context.Context, request connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	if err := ctx.Err(); err != nil {
		return connector.VerifiedWebhook{}, err
	}
	secretValue, err := secret(request.Secrets, "webhook_secret")
	if err != nil {
		return connector.VerifiedWebhook{}, err
	}
	timestamp, signatures, err := parseSignature(headerValue(request.Headers, "Stripe-Signature"))
	if err != nil {
		return connector.VerifiedWebhook{}, webhookError("stripe.webhook_signature_invalid", err)
	}
	receivedAt := request.ReceivedAt
	if receivedAt.IsZero() {
		receivedAt = time.Now().UTC()
	}
	tolerance := time.Duration(configInt(request.Connection.Config, 300, "webhook_tolerance_seconds")) * time.Second
	eventTime := time.Unix(timestamp, 0)
	if tolerance <= 0 || receivedAt.Sub(eventTime).Abs() > tolerance {
		return connector.VerifiedWebhook{}, webhookError("stripe.webhook_timestamp_out_of_range", errors.New("Stripe event timestamp is outside tolerance"))
	}
	mac := hmac.New(sha256.New, []byte(secretValue))
	_, _ = fmt.Fprintf(mac, "%d.%s", timestamp, request.Body)
	expected := hex.EncodeToString(mac.Sum(nil))
	verified := false
	for _, signature := range signatures {
		if hmac.Equal([]byte(expected), []byte(signature)) {
			verified = true
			break
		}
	}
	if !verified {
		return connector.VerifiedWebhook{}, webhookError("stripe.webhook_signature_invalid", errors.New("Stripe signature does not match"))
	}
	payload := map[string]any{}
	if err := json.Unmarshal(request.Body, &payload); err != nil {
		return connector.VerifiedWebhook{}, webhookError("stripe.webhook_payload_invalid", err)
	}
	eventID, eventType := configString(payload, "id"), configString(payload, "type")
	if eventID == "" || eventType == "" {
		return connector.VerifiedWebhook{}, webhookError("stripe.webhook_identity_missing", errors.New("Stripe event id and type are required"))
	}
	var identity *connector.WebhookExternalIdentity
	if customer := configString(nestedObject(payload), "customer"); customer != "" {
		identity = &connector.WebhookExternalIdentity{Subject: customer, SubjectType: "stripe_customer"}
	}
	return connector.VerifiedWebhook{
		EventType: eventType, ExternalID: eventID, Payload: append([]byte(nil), request.Body...),
		Security:         &connector.WebhookSecurityEvidence{SignatureVerified: true, EventTime: eventTime},
		ExternalIdentity: identity,
	}, nil
}

func parseSignature(value string) (int64, []string, error) {
	var timestamp int64
	var signatures []string
	for _, part := range strings.Split(value, ",") {
		key, raw, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		switch key {
		case "t":
			timestamp, _ = strconv.ParseInt(raw, 10, 64)
		case "v1":
			signatures = append(signatures, raw)
		}
	}
	if timestamp <= 0 || len(signatures) == 0 {
		return 0, nil, errors.New("Stripe signature header is invalid")
	}
	return timestamp, signatures, nil
}

func nestedObject(payload map[string]any) map[string]any {
	data, _ := payload["data"].(map[string]any)
	object, _ := data["object"].(map[string]any)
	return object
}

func headerValue(headers map[string][]string, key string) string {
	for current, values := range headers {
		if strings.EqualFold(current, key) && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}

func webhookError(code string, cause error) error { return connector.PermanentError(code, cause) }

var _ connector.WebhookVerifier = (*provider)(nil)
