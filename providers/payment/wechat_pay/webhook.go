package wechatpay

import (
	"context"
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	connector "github.com/domainry/domainry-connector-sdk"
	"strconv"
	"strings"
	"time"
)

func (p *provider) VerifyWebhook(ctx context.Context, r connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	if err := ctx.Err(); err != nil {
		return connector.VerifiedWebhook{}, err
	}
	timestamp, nonce, signatureText, serial := header(r.Headers, "Wechatpay-Timestamp"), header(r.Headers, "Wechatpay-Nonce"), header(r.Headers, "Wechatpay-Signature"), header(r.Headers, "Wechatpay-Serial")
	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || nonce == "" || signatureText == "" || serial == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_signature_invalid", "signature headers are invalid")
	}
	received := r.ReceivedAt
	if received.IsZero() {
		received = time.Now().UTC()
	}
	tolerance := time.Duration(integer(r.Connection.Config["webhook_tolerance_seconds"], 300)) * time.Second
	if tolerance <= 0 || received.Sub(time.Unix(seconds, 0)).Abs() > tolerance {
		return connector.VerifiedWebhook{}, permanent("webhook_timestamp_out_of_range", "webhook timestamp is outside tolerance")
	}
	key, err := parsePublicKey([]byte(r.Secrets["platform_public_key"]))
	if err != nil {
		return connector.VerifiedWebhook{}, permanent("platform_key_invalid", "platform key is invalid")
	}
	signature, err := base64.StdEncoding.DecodeString(signatureText)
	if err != nil {
		return connector.VerifiedWebhook{}, permanent("webhook_signature_invalid", "signature is invalid")
	}
	digest := sha256.Sum256([]byte(timestamp + "\n" + nonce + "\n" + string(r.Body) + "\n"))
	if rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature) != nil {
		return connector.VerifiedWebhook{}, permanent("webhook_signature_invalid", "signature does not match")
	}
	var envelope struct {
		ID        string `json:"id"`
		EventType string `json:"event_type"`
		Resource  struct {
			Algorithm      string `json:"algorithm"`
			Ciphertext     string `json:"ciphertext"`
			Nonce          string `json:"nonce"`
			AssociatedData string `json:"associated_data"`
		} `json:"resource"`
	}
	if json.Unmarshal(r.Body, &envelope) != nil || envelope.ID == "" || envelope.EventType == "" || envelope.Resource.Algorithm != "AEAD_AES_256_GCM" {
		return connector.VerifiedWebhook{}, permanent("webhook_payload_invalid", "webhook envelope is invalid")
	}
	plain, err := decrypt([]byte(r.Secrets["api_v3_key"]), envelope.Resource.Nonce, envelope.Resource.AssociatedData, envelope.Resource.Ciphertext)
	if err != nil {
		return connector.VerifiedWebhook{}, permanent("webhook_decrypt_failed", "webhook resource cannot be decrypted")
	}
	payload := map[string]any{}
	if json.Unmarshal(plain, &payload) != nil {
		return connector.VerifiedWebhook{}, permanent("webhook_payload_invalid", "webhook payload is invalid")
	}
	if config(r.Connection, "merchant_id") == "" || mapString(payload, "mchid") != config(r.Connection, "merchant_id") {
		return connector.VerifiedWebhook{}, permanent("webhook_merchant_id_mismatch", "merchant ID does not match")
	}
	callbackApp := mapString(payload, "appid")
	if config(r.Connection, "app_id") == "" || (callbackApp != "" && callbackApp != config(r.Connection, "app_id")) || (strings.HasPrefix(strings.ToUpper(envelope.EventType), "TRANSACTION.") && callbackApp != config(r.Connection, "app_id")) {
		return connector.VerifiedWebhook{}, permanent("webhook_app_id_mismatch", "app ID does not match")
	}
	bodyDigest := sha256.Sum256(r.Body)
	payload["_payment"] = normalizedWebhook(payload, envelope.EventType, hex.EncodeToString(bodyDigest[:]))
	order := firstString(payload, "out_trade_no", "out_refund_no")
	if order == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_identity_missing", "payment identity is missing")
	}
	raw, _ := json.Marshal(payload)
	eventTime := time.Unix(seconds, 0).UTC()
	event := connector.VerifiedWebhook{EventType: envelope.EventType, ExternalID: envelope.ID, Payload: raw, Security: &connector.WebhookSecurityEvidence{SignatureVerified: true, Nonce: nonce, DeviceIdentity: serial, EventTime: eventTime}, ExternalIdentity: &connector.WebhookExternalIdentity{Subject: order, SubjectType: "merchant_order"}}
	status := strings.ToUpper(firstString(payload, "trade_state", "refund_status"))
	switch status {
	case "SUCCESS":
		occurred, _ := time.Parse(time.RFC3339, mapString(payload, "success_time"))
		event.DeliveryReceipt = &connector.WebhookDeliveryReceipt{ResponseRef: "wechat_pay:" + order, Status: "delivered", OccurredAt: occurred}
	case "CLOSED", "REVOKED", "PAYERROR", "ABNORMAL":
		event.DeliveryReceipt = &connector.WebhookDeliveryReceipt{ResponseRef: "wechat_pay:" + order, Status: "failed", Error: strings.ToLower(status)}
	}
	return event, nil
}
func normalizedWebhook(payload map[string]any, eventType, digest string) map[string]any {
	amount, _ := payload["amount"].(map[string]any)
	result := map[string]any{"event_type": strings.TrimSpace(eventType), "merchant_order_no": mapString(payload, "out_trade_no"), "merchant_refund_no": mapString(payload, "out_refund_no"), "provider_transaction_id": mapString(payload, "transaction_id"), "provider_refund_id": mapString(payload, "refund_id"), "provider_status": firstString(payload, "trade_state", "refund_status"), "currency": mapString(amount, "currency"), "occurred_at": mapString(payload, "success_time"), "callback_digest": digest}
	if value := intValue(amount, "payer_total", "total"); value >= 0 {
		result["paid_amount"] = fmt.Sprintf("%d.%02d", value/100, value%100)
	}
	if value := intValue(amount, "refund", "payer_refund"); value > 0 {
		result["refunded_amount"] = fmt.Sprintf("%d.%02d", value/100, value%100)
	}
	return result
}
func decrypt(key []byte, nonce, associated, ciphertext string) ([]byte, error) {
	if len(key) != 32 {
		return nil, errors.New("invalid key")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	encrypted, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return nil, err
	}
	return gcm.Open(nil, []byte(nonce), encrypted, []byte(associated))
}
func parsePublicKey(value []byte) (*rsa.PublicKey, error) {
	block, _ := pem.Decode(value)
	if block == nil {
		return nil, errors.New("PEM missing")
	}
	if certificate, err := x509.ParseCertificate(block.Bytes); err == nil {
		key, ok := certificate.PublicKey.(*rsa.PublicKey)
		if !ok {
			return nil, errors.New("key is not RSA")
		}
		return key, nil
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
func header(headers map[string][]string, key string) string {
	for current, values := range headers {
		if strings.EqualFold(current, key) && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}

var _ connector.WebhookVerifier = (*provider)(nil)
