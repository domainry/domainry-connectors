package stripe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
)

const responseLimit int64 = 2 << 20

func (a *provider) execute(ctx context.Context, request connector.CallRequest, method, path string, values url.Values) (map[string]any, string, error) {
	baseURL := configString(request.Connection.Config, "base_url")
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	endpoint := strings.TrimRight(baseURL, "/") + path
	var body []byte
	if values != nil && method != http.MethodGet {
		body = []byte(values.Encode())
	}
	if method == http.MethodGet && len(values) > 0 {
		endpoint += "?" + values.Encode()
	}
	apiKey, err := secret(request.Secrets, "api_key")
	if err != nil {
		return nil, "", err
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	secretHeaders := map[string][]string{"Authorization": {"Bearer " + apiKey}}
	if len(body) > 0 {
		headers["Content-Type"] = []string{"application/x-www-form-urlencoded"}
	}
	if requestRef := strings.TrimSpace(request.RequestRef); requestRef != "" && method == http.MethodPost {
		headers["Idempotency-Key"] = []string{requestRef}
	}
	response, err := a.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint, Headers: headers, SecretHeaders: secretHeaders, Body: body, MaxResponseBytes: responseLimit})
	if err != nil {
		if method == http.MethodGet {
			return nil, "", connector.RetryableError("stripe.network_error", err)
		}
		return nil, "", connector.UncertainError("stripe.network_error", err)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := map[string]any{}
	if len(response.Body) > 0 {
		if err := json.Unmarshal(response.Body, &payload); err != nil {
			return nil, ref, connector.PermanentError("stripe.response_invalid", err)
		}
	}
	if id := configString(payload, "id"); id != "" {
		ref = "stripe:" + id
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return payload, ref, nil
	}
	code := stripeErrorCode(payload, response.StatusCode)
	cause := fmt.Errorf("Stripe returned HTTP %d", response.StatusCode)
	if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
		return nil, ref, connector.RetryableError(code, cause)
	}
	return nil, ref, connector.PermanentError(code, cause)
}

func stripeErrorCode(payload map[string]any, status int) string {
	if detail, ok := payload["error"].(map[string]any); ok {
		if code := configString(detail, "code", "type"); code != "" {
			return "stripe." + code
		}
	}
	return "stripe.http_" + strconv.Itoa(status)
}

func positiveAmount(input map[string]any, key string) (int64, error) {
	amount, err := strconv.ParseInt(configString(input, key), 10, 64)
	if err != nil || amount <= 0 {
		return 0, connector.PermanentError("stripe.amount_invalid", errors.New("positive integer amount is required"))
	}
	return amount, nil
}

func secret(values map[string]string, key string) (string, error) {
	value := strings.TrimSpace(values[key])
	if value == "" {
		return "", connector.PermanentError("stripe.secret_missing", fmt.Errorf("resolved secret %s is required", key))
	}
	return value, nil
}

func configString(values map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(fmt.Sprint(values[key])); value != "" && value != "<nil>" {
			return value
		}
	}
	return ""
}

func configInt(values map[string]any, fallback int, key string) int {
	value, err := strconv.Atoi(configString(values, key))
	if err != nil {
		return fallback
	}
	return value
}

func copyQuery(destination url.Values, source map[string]any, keys ...string) {
	for _, key := range keys {
		if value := configString(source, key); value != "" {
			destination.Set(key, value)
		}
	}
}

func stringList(value any) []string {
	items, ok := value.([]any)
	if !ok {
		if typed, typedOK := value.([]string); typedOK {
			items = make([]any, len(typed))
			for index := range typed {
				items[index] = typed[index]
			}
		}
	}
	result := make([]string, 0, len(items))
	for _, item := range items {
		if text := strings.TrimSpace(fmt.Sprint(item)); text != "" {
			result = append(result, text)
		}
	}
	return result
}
