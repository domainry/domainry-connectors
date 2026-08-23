package alipay

import (
	"context"
	"crypto"
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
	"net/url"
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
	return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"alipay_trade_query_response":{"code":"10000","out_trade_no":"ORDER"}}`)}, nil
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func TestSignedDeliveryAndTypedContracts(t *testing.T) {
	merchant, _ := rsa.GenerateKey(rand.Reader, 2048)
	transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		values, _ := url.ParseQuery(string(r.Body))
		if values.Get("sign") != "" || r.SecretForm["sign"] == "" {
			t.Fatalf("request=%+v", r)
		}
		digest := sha256.Sum256([]byte(canonical(values, nil)))
		signature, _ := base64.StdEncoding.DecodeString(r.SecretForm["sign"])
		if rsa.VerifyPKCS1v15(&merchant.PublicKey, crypto.SHA256, digest[:], signature) != nil {
			t.Fatal("invalid request signature")
		}
		return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"alipay_trade_precreate_response":{"code":"10000","out_trade_no":"ORDER","qr_code":"https://qr"}}`)}, nil
	}}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	adapter.(*provider).now = func() time.Time { return time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC) }
	payload, _ := json.Marshal(CreateNativePaymentOrderInput{MerchantOrderNo: "ORDER", AmountMinor: 1234, Subject: "Order"})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CreateNativePaymentOrder.Key, ContractSHA256: CreateNativePaymentOrder.ContractSHA256, Mode: connector.ModeEnqueue, Delivery: true, Connection: validConnection(), Secrets: map[string]string{"merchant_private_key": privatePEM(merchant)}, Payload: payload})
	if err != nil || result.ResponseRef != "alipay:ORDER" || len(result.Payload) != 0 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	raw, _ := json.Marshal(transport.requests[0])
	if strings.Contains(string(raw), "PRIVATE KEY") || strings.Contains(string(raw), "sign") {
		t.Fatalf("request leaks secret: %s", raw)
	}
}
func TestWebhookVerificationAndFailureSemantics(t *testing.T) {
	merchant, _ := rsa.GenerateKey(rand.Reader, 2048)
	platform, _ := rsa.GenerateKey(rand.Reader, 2048)
	adapter, _ := New(&recordingTransport{})
	values := url.Values{"app_id": {"app"}, "out_trade_no": {"ORDER"}, "trade_no": {"ALI"}, "notify_id": {"event"}, "trade_status": {"TRADE_SUCCESS"}, "total_amount": {"12.34"}, "sign_type": {"RSA2"}}
	digest := sha256.Sum256([]byte(canonical(values, map[string]bool{"sign": true, "sign_type": true})))
	signature, _ := rsa.SignPKCS1v15(rand.Reader, platform, crypto.SHA256, digest[:])
	values.Set("sign", base64.StdEncoding.EncodeToString(signature))
	verified, err := adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Connection: validConnection(), Secrets: map[string]string{"platform_public_key": publicPEM(&platform.PublicKey)}, Body: []byte(values.Encode()), ReceivedAt: time.Now().UTC()})
	if err != nil || verified.ExternalID != "event" || verified.Security == nil || !verified.Security.SignatureVerified || verified.DeliveryReceipt.Status != "delivered" {
		t.Fatalf("verified=%+v err=%v", verified, err)
	}
	values.Set("out_trade_no", "tampered")
	if _, err := adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Connection: validConnection(), Secrets: map[string]string{"platform_public_key": publicPEM(&platform.PublicKey)}, Body: []byte(values.Encode())}); err == nil {
		t.Fatal("tampered webhook accepted")
	}
	transport := &recordingTransport{respond: func(connector.HTTPRequest) (connector.HTTPResponse, error) {
		return connector.HTTPResponse{}, errors.New("reset")
	}}
	adapter, _ = New(transport)
	payload, _ := json.Marshal(CreateNativePaymentOrderInput{MerchantOrderNo: "ORDER", AmountMinor: 1, Subject: "Order"})
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CreateNativePaymentOrder.Key, ContractSHA256: CreateNativePaymentOrder.ContractSHA256, Mode: connector.ModeEnqueue, Delivery: true, Connection: validConnection(), Secrets: map[string]string{"merchant_private_key": privatePEM(merchant)}, Payload: payload})
	classification, ok := connector.ErrorClassificationOf(err)
	if !ok || classification != connector.ErrorUncertain {
		t.Fatalf("classification=%q err=%v", classification, err)
	}
}
func validConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "http://127.0.0.1:8080/gateway.do", "app_id": "app", "notify_url": "https://example.test/webhook", "charset": "utf-8", "sign_type": "RSA2", "timeout_seconds": 15}}
}
func privatePEM(key *rsa.PrivateKey) string {
	raw, _ := x509.MarshalPKCS8PrivateKey(key)
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: raw}))
}
func publicPEM(key *rsa.PublicKey) string {
	raw, _ := x509.MarshalPKIXPublicKey(key)
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: raw}))
}
