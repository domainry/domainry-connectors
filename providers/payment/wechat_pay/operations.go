package wechatpay

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
	"strconv"
	"strings"
)

func (p *provider) createPayment(ctx context.Context, r connector.TypedRequest[CreateNativePaymentOrderInput]) (connector.DeliveryResult, error) {
	i := r.Input
	if strings.TrimSpace(i.MerchantOrderNo) == "" || strings.TrimSpace(i.Subject) == "" || i.AmountMinor <= 0 {
		return connector.DeliveryResult{}, permanent("payment_fields_required", "merchant_order_no, subject and positive amount are required")
	}
	currency := currency(r.Connection, i.Currency)
	body := map[string]any{"appid": config(r.Connection, "app_id"), "mchid": config(r.Connection, "merchant_id"), "description": i.Subject, "out_trade_no": i.MerchantOrderNo, "notify_url": config(r.Connection, "notify_url"), "amount": map[string]any{"total": i.AmountMinor, "currency": currency}}
	if i.ExpiresAt != "" {
		body["time_expire"] = i.ExpiresAt
	}
	return p.deliver(ctx, r.Connection, r.Secrets, "create_native_payment_order", i.MerchantOrderNo, "", http.MethodPost, "/v3/pay/transactions/native", body, true)
}
func (p *provider) closePayment(ctx context.Context, r connector.TypedRequest[PaymentOrderInput]) (connector.DeliveryResult, error) {
	id := strings.TrimSpace(r.Input.MerchantOrderNo)
	if id == "" {
		return connector.DeliveryResult{}, permanent("merchant_order_no_required", "merchant_order_no is required")
	}
	return p.deliver(ctx, r.Connection, r.Secrets, "close_payment_order", id, "", http.MethodPost, "/v3/pay/transactions/out-trade-no/"+url.PathEscape(id)+"/close", map[string]any{"mchid": config(r.Connection, "merchant_id")}, true)
}
func (p *provider) createRefund(ctx context.Context, r connector.TypedRequest[CreatePaymentRefundInput]) (connector.DeliveryResult, error) {
	i := r.Input
	if strings.TrimSpace(i.MerchantOrderNo) == "" || strings.TrimSpace(i.MerchantRefundNo) == "" || i.RefundAmountMinor <= 0 || i.TotalAmountMinor <= 0 || i.RefundAmountMinor > i.TotalAmountMinor {
		return connector.DeliveryResult{}, permanent("refund_fields_invalid", "refund fields and amounts are invalid")
	}
	body := map[string]any{"out_trade_no": i.MerchantOrderNo, "out_refund_no": i.MerchantRefundNo, "reason": i.Reason, "amount": map[string]any{"refund": i.RefundAmountMinor, "total": i.TotalAmountMinor, "currency": currency(r.Connection, i.Currency)}}
	if notify := config(r.Connection, "refund_notify_url"); notify != "" {
		body["notify_url"] = notify
	}
	return p.deliver(ctx, r.Connection, r.Secrets, "create_payment_refund", i.MerchantOrderNo, i.MerchantRefundNo, http.MethodPost, "/v3/refund/domestic/refunds", body, true)
}
func (p *provider) queryPayment(ctx context.Context, r connector.TypedRequest[PaymentOrderInput]) (connector.TypedResult[Response], error) {
	id := strings.TrimSpace(r.Input.MerchantOrderNo)
	if id == "" {
		return empty(), permanent("merchant_order_no_required", "merchant_order_no is required")
	}
	return p.execute(ctx, r.Connection, r.Secrets, "query_payment_order", id, "", http.MethodGet, "/v3/pay/transactions/out-trade-no/"+url.PathEscape(id)+"?mchid="+url.QueryEscape(config(r.Connection, "merchant_id")), nil, false)
}
func (p *provider) queryRefund(ctx context.Context, r connector.TypedRequest[QueryPaymentRefundInput]) (connector.TypedResult[Response], error) {
	id := strings.TrimSpace(r.Input.MerchantRefundNo)
	if id == "" {
		return empty(), permanent("merchant_refund_no_required", "merchant_refund_no is required")
	}
	return p.execute(ctx, r.Connection, r.Secrets, "query_payment_refund", "", id, http.MethodGet, "/v3/refund/domestic/refunds/"+url.PathEscape(id), nil, false)
}
func (p *provider) test(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	result, err := p.execute(ctx, r.Connection, r.Secrets, "query_payment_order", "domainry-connection-probe", "", http.MethodGet, "/v3/pay/transactions/out-trade-no/domainry-connection-probe?mchid="+url.QueryEscape(config(r.Connection, "merchant_id")), nil, false)
	if code, ok := connector.ProviderErrorCodeOf(err); ok && strings.Contains(strings.ToUpper(code), "ORDER_NOT_EXIST") {
		return connector.TypedResult[Response]{Output: Response{"connected": true}, ResponseRef: "wechat_pay:connected"}, nil
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
func (p *provider) deliver(ctx context.Context, c connector.Connection, secrets map[string]string, operation, order, refund, method, path string, body map[string]any, write bool) (connector.DeliveryResult, error) {
	result, err := p.execute(ctx, c, secrets, operation, order, refund, method, path, body, write)
	return connector.DeliveryResult{ResponseRef: result.ResponseRef}, err
}
func (p *provider) execute(ctx context.Context, c connector.Connection, secrets map[string]string, operation, order, refund, method, path string, payload map[string]any, write bool) (connector.TypedResult[Response], error) {
	if err := p.ValidateConfig(c); err != nil {
		return empty(), err
	}
	body := []byte{}
	var err error
	if payload != nil {
		body, err = json.Marshal(payload)
		if err != nil {
			return empty(), permanent("request_invalid", "request is invalid")
		}
	}
	key, err := parsePrivateKey([]byte(secrets["merchant_private_key"]))
	if err != nil {
		return empty(), permanent("private_key_invalid", "merchant private key is invalid")
	}
	timestamp := strconv.FormatInt(p.now().UTC().Unix(), 10)
	nonceBytes := make([]byte, 18)
	if _, err := p.nonce(nonceBytes); err != nil {
		return empty(), permanent("nonce_failed", "nonce generation failed")
	}
	nonce := base64.RawURLEncoding.EncodeToString(nonceBytes)
	message := method + "\n" + path + "\n" + timestamp + "\n" + nonce + "\n" + string(body) + "\n"
	digest := sha256.Sum256([]byte(message))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		return empty(), permanent("sign_failed", "request signing failed")
	}
	auth := fmt.Sprintf(`WECHATPAY2-SHA256-RSA2048 mchid="%s",nonce_str="%s",timestamp="%s",serial_no="%s",signature="%s"`, config(c, "merchant_id"), nonce, timestamp, config(c, "merchant_serial_no"), base64.StdEncoding.EncodeToString(signature))
	headers := map[string][]string{"Accept": {"application/json"}}
	if method != http.MethodGet {
		headers["Content-Type"] = []string{"application/json"}
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: strings.TrimRight(configDefault(c, "base_url", defaultBaseURL), "/") + path, Headers: headers, SecretHeaders: map[string][]string{"Authorization": {auth}}, Body: body, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		return empty(), transportFailure(write, "network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	responseBody := Response{}
	valid := len(response.Body) == 0 || json.Unmarshal(response.Body, &responseBody) == nil
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code := mapString(responseBody, "code")
		if code == "" {
			code = "http_" + strconv.Itoa(response.StatusCode)
		}
		cause := fmt.Errorf("WeChat Pay returned HTTP %d", response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests {
			return connector.TypedResult[Response]{Output: responseBody, ResponseRef: ref}, connector.RetryableError("wechat_pay."+code, cause)
		}
		if response.StatusCode >= 500 {
			return connector.TypedResult[Response]{Output: responseBody, ResponseRef: ref}, transportFailure(write, code, cause)
		}
		return connector.TypedResult[Response]{Output: responseBody, ResponseRef: ref}, connector.PermanentError("wechat_pay."+code, cause)
	}
	if !valid {
		return connector.TypedResult[Response]{ResponseRef: ref}, transportFailure(write, "response_invalid", errors.New("WeChat Pay response is invalid JSON"))
	}
	if id := firstString(responseBody, "out_trade_no", "out_refund_no", "transaction_id", "refund_id"); id != "" {
		ref = "wechat_pay:" + id
	} else if order != "" {
		ref = "wechat_pay:" + order
	} else if refund != "" {
		ref = "wechat_pay:" + refund
	}
	return connector.TypedResult[Response]{Output: normalize(operation, order, refund, responseBody), ResponseRef: ref}, nil
}
func parsePrivateKey(value []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(value)
	if block == nil {
		return nil, errors.New("PEM is missing")
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
