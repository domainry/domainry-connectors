package alipay

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"net/url"
	"strings"
	"time"
)

func (p *provider) VerifyWebhook(ctx context.Context, r connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	if err := ctx.Err(); err != nil {
		return connector.VerifiedWebhook{}, err
	}
	values, err := url.ParseQuery(string(r.Body))
	if err != nil {
		return connector.VerifiedWebhook{}, permanent("webhook_payload_invalid", "webhook payload is invalid")
	}
	signatureText, signType := values.Get("sign"), strings.ToUpper(values.Get("sign_type"))
	if signatureText == "" || (signType != "" && signType != "RSA2") {
		return connector.VerifiedWebhook{}, permanent("webhook_signature_invalid", "webhook signature is invalid")
	}
	key, err := parsePublicKey([]byte(r.Secrets["platform_public_key"]))
	if err != nil {
		return connector.VerifiedWebhook{}, permanent("platform_key_invalid", "platform public key is invalid")
	}
	signature, err := base64.StdEncoding.DecodeString(signatureText)
	if err != nil {
		return connector.VerifiedWebhook{}, permanent("webhook_signature_invalid", "webhook signature is invalid")
	}
	digest := sha256.Sum256([]byte(canonical(values, map[string]bool{"sign": true, "sign_type": true})))
	if rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature) != nil {
		return connector.VerifiedWebhook{}, permanent("webhook_signature_invalid", "webhook signature does not match")
	}
	if app := config(r.Connection, "app_id"); app != "" && values.Get("app_id") != app {
		return connector.VerifiedWebhook{}, permanent("webhook_app_id_mismatch", "webhook app ID does not match")
	}
	order, eventID, eventType := strings.TrimSpace(values.Get("out_trade_no")), strings.TrimSpace(values.Get("notify_id")), strings.TrimSpace(values.Get("trade_status"))
	if order == "" || eventType == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_identity_missing", "webhook identity is missing")
	}
	if eventID == "" {
		eventID = order + ":" + eventType + ":" + values.Get("gmt_payment")
	}
	occurred := r.ReceivedAt.UTC()
	if occurred.IsZero() {
		occurred = time.Now().UTC()
	}
	if raw := values.Get("notify_time"); raw != "" {
		if parsed, parseErr := time.ParseInLocation("2006-01-02 15:04:05", raw, time.FixedZone("CST", 8*60*60)); parseErr == nil {
			occurred = parsed.UTC()
		}
	}
	payload := map[string]any{}
	for key := range values {
		if key != "sign" {
			payload[key] = values.Get(key)
		}
	}
	transaction := strings.TrimSpace(values.Get("trade_no"))
	if transaction == "" {
		transaction = order
	}
	bodyDigest := sha256.Sum256(r.Body)
	payload["_payment"] = map[string]any{"event_type": eventType, "merchant_order_no": order, "provider_transaction_id": transaction, "provider_status": eventType, "paid_amount": strings.TrimSpace(values.Get("total_amount")), "currency": "CNY", "occurred_at": occurred.Format(time.RFC3339), "callback_digest": hex.EncodeToString(bodyDigest[:])}
	raw, _ := json.Marshal(payload)
	event := connector.VerifiedWebhook{EventType: eventType, ExternalID: eventID, Payload: raw, Security: &connector.WebhookSecurityEvidence{SignatureVerified: true, Nonce: eventID, DeviceIdentity: values.Get("seller_id"), EventTime: occurred}, ExternalIdentity: &connector.WebhookExternalIdentity{Subject: order, SubjectType: "merchant_order"}}
	switch eventType {
	case "TRADE_SUCCESS", "TRADE_FINISHED":
		event.DeliveryReceipt = &connector.WebhookDeliveryReceipt{ResponseRef: "alipay:" + order, Status: "delivered", OccurredAt: occurred}
	case "TRADE_CLOSED":
		event.DeliveryReceipt = &connector.WebhookDeliveryReceipt{ResponseRef: "alipay:" + order, Status: "failed", Error: "trade_closed", OccurredAt: occurred}
	}
	return event, nil
}
func parsePublicKey(value []byte) (*rsa.PublicKey, error) {
	block, _ := pem.Decode(value)
	if block == nil {
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(value)))
		if err != nil {
			return nil, err
		}
		block = &pem.Block{Bytes: decoded}
	}
	if parsed, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
		key, ok := parsed.(*rsa.PublicKey)
		if !ok {
			return nil, errors.New("key is not RSA")
		}
		return key, nil
	}
	return x509.ParsePKCS1PublicKey(block.Bytes)
}

var _ connector.WebhookVerifier = (*provider)(nil)
