package wechatpay

import (
	"encoding/json"
	"errors"
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
		refund = mapString(response, "out_refund_no")
	}
	if order != "" {
		result["merchant_order_no"] = order
	}
	if refund != "" {
		result["merchant_refund_no"] = refund
	}
	if value := mapString(response, "transaction_id"); value != "" {
		result["provider_order_no"] = value
	}
	if value := mapString(response, "refund_id"); value != "" {
		result["provider_refund_no"] = value
	}
	if value := mapString(response, "code_url"); value != "" {
		result["code_url"] = value
	}
	status := firstString(response, "trade_state", "status")
	if status == "" && operation == "create_native_payment_order" {
		status = "NOTPAY"
	}
	if status == "" && operation == "close_payment_order" {
		status = "CLOSED"
	}
	if status != "" {
		result["status"] = strings.ToLower(status)
	}
	if value := mapString(response, "success_time"); value != "" {
		result["paid_at"] = value
	}
	if amount, ok := response["amount"].(map[string]any); ok {
		if total := intValue(amount, "total", "payer_total", "refund"); total > 0 {
			result["amount_minor"] = total
		}
		if value := mapString(amount, "currency"); value != "" {
			result["currency"] = value
		}
	}
	return result
}
func currency(c connector.Connection, input string) string {
	value := strings.ToUpper(strings.TrimSpace(input))
	if value == "" {
		value = strings.ToUpper(configDefault(c, "currency", "CNY"))
	}
	return value
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
func intValue(values map[string]any, keys ...string) int64 {
	for _, key := range keys {
		switch value := values[key].(type) {
		case int:
			return int64(value)
		case int64:
			return value
		case float64:
			if value == float64(int64(value)) {
				return int64(value)
			}
		case json.Number:
			parsed, _ := value.Int64()
			return parsed
		}
	}
	return 0
}
func loopback(host string) bool              { return host == "localhost" || host == "127.0.0.1" || host == "::1" }
func empty() connector.TypedResult[Response] { return connector.TypedResult[Response]{} }
func permanent(code, message string) error {
	return connector.PermanentError("wechat_pay."+code, errors.New(message))
}
func transportFailure(write bool, suffix string, cause error) error {
	if write {
		return connector.UncertainError("wechat_pay."+suffix, cause)
	}
	return connector.RetryableError("wechat_pay."+suffix, cause)
}
