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
	"fmt"
	connector "github.com/domainry/domainry-connector-sdk"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

func (p *provider) createPayment(ctx context.Context, r connector.TypedRequest[CreateNativePaymentOrderInput]) (connector.DeliveryResult, error) {
	i := r.Input
	if strings.TrimSpace(i.MerchantOrderNo) == "" || strings.TrimSpace(i.Subject) == "" || i.AmountMinor <= 0 {
		return connector.DeliveryResult{}, permanent("payment_fields_required", "merchant_order_no, subject and positive amount are required")
	}
	biz := map[string]any{"out_trade_no": i.MerchantOrderNo, "total_amount": minorAmountText(i.AmountMinor), "subject": i.Subject}
	if i.Description != "" {
		biz["body"] = i.Description
	}
	if i.ExpiresAt != "" {
		if parsed, err := time.Parse(time.RFC3339, i.ExpiresAt); err == nil {
			remaining := parsed.Sub(p.now())
			if remaining > 0 {
				minutes := int(remaining.Round(time.Minute) / time.Minute)
				if minutes < 1 {
					minutes = 1
				}
				biz["timeout_express"] = fmt.Sprintf("%dm", minutes)
			}
		}
	}
	return p.deliver(ctx, r.Connection, r.Secrets, "create_native_payment_order", i.MerchantOrderNo, "", "alipay.trade.precreate", "alipay_trade_precreate_response", biz, true)
}
func (p *provider) closePayment(ctx context.Context, r connector.TypedRequest[PaymentOrderInput]) (connector.DeliveryResult, error) {
	id := strings.TrimSpace(r.Input.MerchantOrderNo)
	if id == "" {
		return connector.DeliveryResult{}, permanent("merchant_order_no_required", "merchant_order_no is required")
	}
	return p.deliver(ctx, r.Connection, r.Secrets, "close_payment_order", id, "", "alipay.trade.close", "alipay_trade_close_response", map[string]any{"out_trade_no": id}, true)
}
func (p *provider) createRefund(ctx context.Context, r connector.TypedRequest[CreatePaymentRefundInput]) (connector.DeliveryResult, error) {
	i := r.Input
	if strings.TrimSpace(i.MerchantOrderNo) == "" || strings.TrimSpace(i.MerchantRefundNo) == "" || i.RefundAmountMinor <= 0 || i.TotalAmountMinor <= 0 || i.RefundAmountMinor > i.TotalAmountMinor {
		return connector.DeliveryResult{}, permanent("refund_fields_invalid", "refund fields and amounts are invalid")
	}
	biz := map[string]any{"out_trade_no": i.MerchantOrderNo, "out_request_no": i.MerchantRefundNo, "refund_amount": minorAmountText(i.RefundAmountMinor)}
	if i.Reason != "" {
		biz["refund_reason"] = i.Reason
	}
	return p.deliver(ctx, r.Connection, r.Secrets, "create_payment_refund", i.MerchantOrderNo, i.MerchantRefundNo, "alipay.trade.refund", "alipay_trade_refund_response", biz, true)
}
func (p *provider) queryPayment(ctx context.Context, r connector.TypedRequest[PaymentOrderInput]) (connector.TypedResult[Response], error) {
	id := strings.TrimSpace(r.Input.MerchantOrderNo)
	if id == "" {
		return empty(), permanent("merchant_order_no_required", "merchant_order_no is required")
	}
	return p.execute(ctx, r.Connection, r.Secrets, "query_payment_order", id, "", "alipay.trade.query", "alipay_trade_query_response", map[string]any{"out_trade_no": id}, false)
}
func (p *provider) queryRefund(ctx context.Context, r connector.TypedRequest[QueryPaymentRefundInput]) (connector.TypedResult[Response], error) {
	i := r.Input
	if strings.TrimSpace(i.MerchantOrderNo) == "" || strings.TrimSpace(i.MerchantRefundNo) == "" {
		return empty(), permanent("refund_query_fields_required", "merchant order and refund numbers are required")
	}
	return p.execute(ctx, r.Connection, r.Secrets, "query_payment_refund", i.MerchantOrderNo, i.MerchantRefundNo, "alipay.trade.fastpay.refund.query", "alipay_trade_fastpay_refund_query_response", map[string]any{"out_trade_no": i.MerchantOrderNo, "out_request_no": i.MerchantRefundNo}, false)
}
func (p *provider) test(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	result, err := p.execute(ctx, r.Connection, r.Secrets, "query_payment_order", "domainry-connection-probe", "", "alipay.trade.query", "alipay_trade_query_response", map[string]any{"out_trade_no": "domainry-connection-probe"}, false)
	if code, ok := connector.ProviderErrorCodeOf(err); ok && strings.Contains(code, "ACQ.TRADE_NOT_EXIST") {
		return connector.TypedResult[Response]{Output: Response{"connected": true}, ResponseRef: "alipay:connected"}, nil
	}
	return result, err
}
func (p *provider) TestConnection(ctx context.Context, r connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.test(ctx, connector.TypedRequest[struct{}]{Connection: r.Connection, Secrets: r.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}
func (p *provider) deliver(ctx context.Context, c connector.Connection, secrets map[string]string, operation, order, refund, method, responseKey string, biz map[string]any, write bool) (connector.DeliveryResult, error) {
	result, err := p.execute(ctx, c, secrets, operation, order, refund, method, responseKey, biz, write)
	return connector.DeliveryResult{ResponseRef: result.ResponseRef, SecretUpdates: result.SecretUpdates, ResourceHealth: result.ResourceHealth}, err
}
func (p *provider) execute(ctx context.Context, c connector.Connection, secrets map[string]string, operation, order, refund, apiMethod, responseKey string, biz map[string]any, write bool) (connector.TypedResult[Response], error) {
	if err := p.ValidateConfig(c); err != nil {
		return empty(), err
	}
	bizJSON, err := json.Marshal(biz)
	if err != nil {
		return empty(), permanent("request_invalid", "request is invalid")
	}
	charset := configDefault(c, "charset", "utf-8")
	values := url.Values{"app_id": {config(c, "app_id")}, "method": {apiMethod}, "format": {"JSON"}, "charset": {charset}, "sign_type": {"RSA2"}, "timestamp": {p.now().In(time.FixedZone("CST", 8*60*60)).Format("2006-01-02 15:04:05")}, "version": {"1.0"}, "notify_url": {config(c, "notify_url")}, "biz_content": {string(bizJSON)}}
	key, err := parsePrivateKey([]byte(secrets["merchant_private_key"]))
	if err != nil {
		return empty(), permanent("private_key_invalid", "merchant private key is invalid")
	}
	digest := sha256.Sum256([]byte(canonical(values, nil)))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		return empty(), permanent("sign_failed", "request signing failed")
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodPost, URL: configDefault(c, "base_url", defaultGatewayURL), Headers: map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/x-www-form-urlencoded;charset=" + charset}}, Body: []byte(values.Encode()), SecretForm: map[string]string{"sign": base64.StdEncoding.EncodeToString(signature)}, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		return empty(), transportFailure(write, "network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := map[string]any{}
	valid := json.Unmarshal(response.Body, &payload) == nil
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("Alipay returned HTTP %d", response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests {
			return connector.TypedResult[Response]{ResponseRef: ref}, connector.RetryableError("alipay.http_429", cause)
		}
		if response.StatusCode >= 500 {
			return connector.TypedResult[Response]{ResponseRef: ref}, transportFailure(write, "http_"+strconv.Itoa(response.StatusCode), cause)
		}
		return connector.TypedResult[Response]{ResponseRef: ref}, connector.PermanentError("alipay.http_"+strconv.Itoa(response.StatusCode), cause)
	}
	if !valid {
		return connector.TypedResult[Response]{ResponseRef: ref}, transportFailure(write, "response_invalid", errors.New("Alipay response is invalid JSON"))
	}
	providerResponse, ok := payload[responseKey].(map[string]any)
	if !ok {
		return connector.TypedResult[Response]{ResponseRef: ref}, transportFailure(write, "response_invalid", errors.New("Alipay response envelope is invalid"))
	}
	if mapString(providerResponse, "code") != "10000" {
		sub := mapString(providerResponse, "sub_code")
		if sub == "" {
			sub = "business_error"
		}
		return connector.TypedResult[Response]{Output: providerResponse, ResponseRef: ref}, connector.PermanentError("alipay."+sub, errors.New("Alipay rejected the request"))
	}
	if id := firstString(providerResponse, "trade_no", "out_trade_no", "out_request_no"); id != "" {
		ref = "alipay:" + id
	} else if order != "" {
		ref = "alipay:" + order
	} else if refund != "" {
		ref = "alipay:" + refund
	}
	return connector.TypedResult[Response]{Output: normalize(operation, order, refund, providerResponse), ResponseRef: ref}, nil
}
func canonical(values url.Values, excluded map[string]bool) string {
	keys := make([]string, 0, len(values))
	for key := range values {
		if excluded == nil || !excluded[key] {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	parts := []string{}
	for _, key := range keys {
		if value := values.Get(key); value != "" {
			parts = append(parts, key+"="+value)
		}
	}
	return strings.Join(parts, "&")
}
func parsePrivateKey(value []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(value)
	if block == nil {
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(value)))
		if err != nil {
			return nil, err
		}
		block = &pem.Block{Bytes: decoded}
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("key is not RSA")
	}
	return key, nil
}
