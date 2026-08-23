package wechatpay

import (
	"context"
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
	"strconv"
	"strings"
	"testing"
	"time"
)

type recordingTransport struct {
	requests []connector.HTTPRequest
	respond  func(connector.HTTPRequest) (connector.HTTPResponse, error)
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, r connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, r)
	if t.respond != nil {
		return t.respond(r)
	}
	return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"out_trade_no":"ORDER","code_url":"weixin://pay"}`)}, nil
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func TestSignedDeliveryUsesRuntimeOnlyAuthorization(t *testing.T) {
	merchant, _ := rsa.GenerateKey(rand.Reader, 2048)
	transport := &recordingTransport{}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	p := adapter.(*provider)
	p.now = func() time.Time { return time.Unix(1700000000, 0) }
	p.nonce = func(target []byte) (int, error) {
		for i := range target {
			target[i] = byte(i)
		}
		return len(target), nil
	}
	payload, _ := json.Marshal(CreateNativePaymentOrderInput{MerchantOrderNo: "ORDER", AmountMinor: 1234, Subject: "Order"})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CreateNativePaymentOrder.Key, ContractSHA256: CreateNativePaymentOrder.ContractSHA256, Mode: connector.ModeEnqueue, Delivery: true, Connection: validConnection(), Secrets: map[string]string{"merchant_private_key": privatePEM(merchant)}, Payload: payload})
	if err != nil || result.ResponseRef != "wechat_pay:ORDER" || len(result.Payload) != 0 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	request := transport.requests[0]
	if request.SecretHeaders["Authorization"][0] == "" || request.Headers["Authorization"] != nil {
		t.Fatalf("request=%+v", request)
	}
	raw, _ := json.Marshal(request)
	if strings.Contains(string(raw), "WECHATPAY2") || strings.Contains(string(raw), "PRIVATE KEY") {
		t.Fatalf("request leaks secret: %s", raw)
	}
}
func TestEncryptedWebhookVerification(t *testing.T) {
	merchant, _ := rsa.GenerateKey(rand.Reader, 2048)
	platform, _ := rsa.GenerateKey(rand.Reader, 2048)
	adapter, _ := New(&recordingTransport{})
	apiKey := []byte("0123456789abcdef0123456789abcdef")
	plain, _ := json.Marshal(map[string]any{"mchid": "merchant", "appid": "app", "out_trade_no": "ORDER", "transaction_id": "WX", "trade_state": "SUCCESS", "success_time": "2026-08-24T00:00:00Z", "amount": map[string]any{"total": 1234, "currency": "CNY"}})
	nonce, associated := "123456789012", "payment"
	block, _ := aes.NewCipher(apiKey)
	gcm, _ := cipher.NewGCM(block)
	ciphertext := base64.StdEncoding.EncodeToString(gcm.Seal(nil, []byte(nonce), plain, []byte(associated)))
	body, _ := json.Marshal(map[string]any{"id": "event", "event_type": "TRANSACTION.SUCCESS", "resource": map[string]any{"algorithm": "AEAD_AES_256_GCM", "ciphertext": ciphertext, "nonce": nonce, "associated_data": associated}})
	received := time.Now().UTC()
	timestamp := strconv.FormatInt(received.Unix(), 10)
	webhookNonce := "webhook-nonce"
	digest := sha256.Sum256([]byte(timestamp + "\n" + webhookNonce + "\n" + string(body) + "\n"))
	signature, _ := rsa.SignPKCS1v15(rand.Reader, platform, crypto.SHA256, digest[:])
	verified, err := adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Connection: validConnection(), Secrets: map[string]string{"platform_public_key": publicPEM(&platform.PublicKey), "api_v3_key": string(apiKey)}, Headers: map[string][]string{"Wechatpay-Timestamp": {timestamp}, "Wechatpay-Nonce": {webhookNonce}, "Wechatpay-Signature": {base64.StdEncoding.EncodeToString(signature)}, "Wechatpay-Serial": {"serial"}}, Body: body, ReceivedAt: received})
	if err != nil || verified.ExternalID != "event" || verified.Security == nil || !verified.Security.SignatureVerified || verified.DeliveryReceipt.Status != "delivered" {
		t.Fatalf("verified=%+v err=%v", verified, err)
	}
	body[0] ^= 1
	if _, err := adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Connection: validConnection(), Secrets: map[string]string{"platform_public_key": publicPEM(&platform.PublicKey), "api_v3_key": string(apiKey)}, Headers: map[string][]string{"Wechatpay-Timestamp": {timestamp}, "Wechatpay-Nonce": {webhookNonce}, "Wechatpay-Signature": {base64.StdEncoding.EncodeToString(signature)}, "Wechatpay-Serial": {"serial"}}, Body: body, ReceivedAt: received}); err == nil {
		t.Fatal("tampered webhook accepted")
	}
	_ = merchant
}
func TestWriteNetworkFailureIsUncertain(t *testing.T) {
	merchant, _ := rsa.GenerateKey(rand.Reader, 2048)
	transport := &recordingTransport{respond: func(connector.HTTPRequest) (connector.HTTPResponse, error) {
		return connector.HTTPResponse{}, errors.New("reset")
	}}
	adapter, _ := New(transport)
	payload, _ := json.Marshal(CreateNativePaymentOrderInput{MerchantOrderNo: "ORDER", AmountMinor: 1, Subject: "Order"})
	_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CreateNativePaymentOrder.Key, ContractSHA256: CreateNativePaymentOrder.ContractSHA256, Mode: connector.ModeEnqueue, Delivery: true, Connection: validConnection(), Secrets: map[string]string{"merchant_private_key": privatePEM(merchant)}, Payload: payload})
	classification, ok := connector.ErrorClassificationOf(err)
	if !ok || classification != connector.ErrorUncertain {
		t.Fatalf("classification=%q err=%v", classification, err)
	}
}
func validConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "http://127.0.0.1:8080", "app_id": "app", "merchant_id": "merchant", "merchant_serial_no": "serial", "notify_url": "https://example.test/webhook", "currency": "CNY", "webhook_tolerance_seconds": 300, "timeout_seconds": 15}}
}
func privatePEM(key *rsa.PrivateKey) string {
	raw, _ := x509.MarshalPKCS8PrivateKey(key)
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: raw}))
}
func publicPEM(key *rsa.PublicKey) string {
	raw, _ := x509.MarshalPKIXPublicKey(key)
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: raw}))
}
