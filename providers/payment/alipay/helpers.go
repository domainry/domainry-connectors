package alipay

import (
	"encoding/json"
	"errors"
	"fmt"
	connector "github.com/domainry/domainry-connector-sdk"
	"strconv"
	"strings"
)

func normalize(operation, order, refund string, response map[string]any) Response {
	result := Response{}
	if order == "" {
		order = mapString(response, "out_trade_no")
	}
	if refund == "" {
		refund = mapString(response, "out_request_no")
	}
	if order != "" {
		result["merchant_order_no"] = order
	}
	if refund != "" {
		result["merchant_refund_no"] = refund
	}
	if value := mapString(response, "trade_no"); value != "" {
		result["provider_order_no"] = value
	}
	if value := mapString(response, "refund_fee"); value != "" {
		if minor, err := decimalToMinor(value); err == nil {
			result["refund_amount_minor"] = minor
		}
	}
	if value := mapString(response, "qr_code"); value != "" {
		result["code_url"] = value
	}
	status := firstString(response, "trade_status", "refund_status")
	if status == "" && operation == "create_native_payment_order" {
		status = "WAIT_BUYER_PAY"
	}
	if status == "" && operation == "close_payment_order" {
		status = "TRADE_CLOSED"
	}
	if status == "" && operation == "create_payment_refund" && mapString(response, "fund_change") == "Y" {
		status = "REFUND_SUCCESS"
	}
	if status != "" {
		result["status"] = strings.ToLower(status)
	}
	if value := firstString(response, "send_pay_date", "gmt_refund_pay"); value != "" {
		if operation == "query_payment_refund" {
			result["refunded_at"] = value
		} else {
			result["paid_at"] = value
		}
	}
	if value := mapString(response, "total_amount"); value != "" {
		if minor, err := decimalToMinor(value); err == nil {
			result["amount_minor"] = minor
		}
	}
	result["currency"] = "CNY"
	return result
}
func minorAmountText(amount int64) string { return fmt.Sprintf("%d.%02d", amount/100, amount%100) }
func decimalToMinor(value string) (int64, error) {
	parts := strings.Split(strings.TrimSpace(value), ".")
	if len(parts) > 2 {
		return 0, errors.New("invalid decimal")
	}
	whole, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, err
	}
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
	}
	if len(fraction) > 2 {
		return 0, errors.New("precision exceeds cents")
	}
	fraction += strings.Repeat("0", 2-len(fraction))
	cents, err := strconv.ParseInt(fraction, 10, 64)
	if err != nil {
		return 0, err
	}
	return whole*100 + cents, nil
}
func config(c connector.Connection, key string) string {
	value, _ := c.Config[key].(string)
	return strings.TrimSpace(value)
}
func configDefault(c connector.Connection, key, fallback string) string {
	if value := config(c, key); value != "" {
		return value
	}
	return fallback
}
func integer(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		return typed
	case float64:
		return int(typed)
	case json.Number:
		if parsed, err := strconv.Atoi(typed.String()); err == nil {
			return parsed
		}
	}
	return fallback
}
func mapString(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return strings.TrimSpace(value)
}
func firstString(values map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := mapString(values, key); value != "" {
			return value
		}
	}
	return ""
}
func loopback(host string) bool              { return host == "localhost" || host == "127.0.0.1" || host == "::1" }
func empty() connector.TypedResult[Response] { return connector.TypedResult[Response]{} }
func permanent(code, message string) error {
	return connector.PermanentError("alipay."+code, errors.New(message))
}
func transportFailure(write bool, suffix string, cause error) error {
	if write {
		return connector.UncertainError("alipay."+suffix, cause)
	}
	return connector.RetryableError("alipay."+suffix, cause)
}
