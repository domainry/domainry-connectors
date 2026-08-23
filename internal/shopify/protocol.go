package shopify

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
)

const DefaultAPIVersion = "2026-04"
const responseLimit = 4 << 20

var versionPattern = regexp.MustCompile(`^[0-9]{4}-(0[1-9]|1[0-2])$`)

type Config struct{ ShopDomain, APIVersion string }
type Webhook struct {
	Topic, DeliveryID, ShopDomain string
	Payload                       map[string]any
	Raw                           json.RawMessage
}

func Validate(config Config) error {
	domain := strings.ToLower(strings.TrimSpace(config.ShopDomain))
	parsed, err := url.Parse("https://" + domain)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Host != domain {
		return errors.New("valid Shopify shop domain is required")
	}
	if !strings.HasSuffix(parsed.Hostname(), ".myshopify.com") && !isLoopback(parsed.Hostname()) {
		return errors.New("myshopify.com shop domain or loopback host is required")
	}
	version := strings.TrimSpace(config.APIVersion)
	if version == "" {
		version = DefaultAPIVersion
	}
	if !versionPattern.MatchString(version) {
		return errors.New("Shopify API version must use YYYY-MM")
	}
	return nil
}
func Endpoint(config Config) string {
	version := strings.TrimSpace(config.APIVersion)
	if version == "" {
		version = DefaultAPIVersion
	}
	scheme := "https"
	if isLoopback(strings.Split(strings.TrimSpace(config.ShopDomain), ":")[0]) {
		scheme = "http"
	}
	return scheme + "://" + strings.ToLower(strings.TrimSpace(config.ShopDomain)) + "/admin/api/" + version + "/graphql.json"
}
func Execute(ctx context.Context, transport connector.Transport, config Config, accessToken, query string, variables map[string]any, write bool) (map[string]any, string, error) {
	if err := Validate(config); err != nil {
		return nil, "", connector.PermanentError("shopify.endpoint_invalid", err)
	}
	token := strings.TrimSpace(accessToken)
	if token == "" {
		return nil, "", connector.PermanentError("shopify.access_token_required", errors.New("resolved Shopify access token is required"))
	}
	raw, err := json.Marshal(map[string]any{"query": query, "variables": variables})
	if err != nil {
		return nil, "", connector.PermanentError("shopify.request_invalid", err)
	}
	response, transportErr := transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodPost, URL: Endpoint(config), Headers: map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/json"}}, SecretHeaders: map[string][]string{"X-Shopify-Access-Token": {token}}, Body: raw, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		if write {
			return nil, "", connector.UncertainError("shopify.network_error", transportErr)
		}
		return nil, "", connector.RetryableError("shopify.network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := map[string]any{}
	valid := len(response.Body) == 0 || json.Unmarshal(response.Body, &payload) == nil
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("Shopify returned HTTP %d", response.StatusCode)
		code := "shopify.http_" + strconv.Itoa(response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests {
			return payload, ref, connector.RetryableError(code, cause)
		}
		if response.StatusCode == http.StatusRequestTimeout || response.StatusCode >= 500 {
			if write {
				return payload, ref, connector.UncertainError(code, cause)
			}
			return payload, ref, connector.RetryableError(code, cause)
		}
		return payload, ref, connector.PermanentError(code, cause)
	}
	if !valid {
		if write {
			return nil, ref, connector.UncertainError("shopify.response_invalid", errors.New("Shopify response is invalid JSON"))
		}
		return nil, ref, connector.PermanentError("shopify.response_invalid", errors.New("Shopify response is invalid JSON"))
	}
	if list, ok := payload["errors"].([]any); ok && len(list) > 0 {
		return payload, ref, connector.PermanentError("shopify.graphql_error", errors.New("Shopify rejected the GraphQL operation"))
	}
	return payload, ref, nil
}
func VerifyWebhook(ctx context.Context, request connector.VerifyWebhookRequest) (Webhook, error) {
	if err := ctx.Err(); err != nil {
		return Webhook{}, err
	}
	secret := strings.TrimSpace(request.Secrets["webhook_secret"])
	if secret == "" {
		return Webhook{}, connector.PermanentError("shopify.webhook_secret_required", errors.New("resolved Shopify webhook secret is required"))
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(request.Body)
	expected := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(header(request.Headers, "X-Shopify-Hmac-Sha256"))) {
		return Webhook{}, connector.PermanentError("shopify.webhook_signature_invalid", errors.New("Shopify webhook signature does not match"))
	}
	payload := map[string]any{}
	if json.Unmarshal(request.Body, &payload) != nil {
		return Webhook{}, connector.PermanentError("shopify.webhook_payload_invalid", errors.New("Shopify webhook payload is invalid JSON"))
	}
	topic := strings.ToLower(header(request.Headers, "X-Shopify-Topic"))
	if topic == "" {
		return Webhook{}, connector.PermanentError("shopify.webhook_identity_missing", errors.New("Shopify webhook topic is missing"))
	}
	return Webhook{Topic: topic, DeliveryID: header(request.Headers, "X-Shopify-Webhook-Id"), ShopDomain: strings.ToLower(header(request.Headers, "X-Shopify-Shop-Domain")), Payload: payload, Raw: append(json.RawMessage(nil), request.Body...)}, nil
}
func GID(kind, value string) string {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "gid://shopify/") {
		return value
	}
	return "gid://shopify/" + kind + "/" + value
}
func IDFromGID(value string) string {
	parts := strings.Split(strings.TrimSpace(value), "/")
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}
func header(headers map[string][]string, key string) string {
	for candidate, values := range headers {
		if strings.EqualFold(candidate, key) && len(values) > 0 {
			return strings.TrimSpace(values[0])
		}
	}
	return ""
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
