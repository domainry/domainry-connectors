// Package tally owns the repository-private Tally wire protocol shared by
// connector-specific official Providers.
package tally

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
	"strconv"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	DefaultBaseURL          = "https://api.tally.so"
	DefaultAPIVersion       = "2026-02-05"
	responseLimit     int64 = 4 << 20
)

type Protocol struct{ Transport connector.Transport }

func (p Protocol) Validate(connection connector.Connection) error {
	if strings.TrimSpace(ConfigString(connection.Config, "form_id")) == "" {
		return Permanent("form_id_required", "form ID is required")
	}
	parsed, err := url.Parse(BaseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && IsLoopback(parsed.Hostname()))) {
		return Permanent("endpoint_invalid", "HTTPS or loopback HTTP endpoint is required")
	}
	if _, err = time.Parse("2006-01-02", APIVersion(connection)); err != nil {
		return Permanent("api_version_invalid", "Tally API version must be YYYY-MM-DD")
	}
	return nil
}

func (p Protocol) TestConnection(ctx context.Context, connection connector.Connection, secrets map[string]string) (map[string]any, string, error) {
	if p.Transport == nil {
		return nil, "", errors.New("Tally transport is required")
	}
	if err := p.Validate(connection); err != nil {
		return nil, "", err
	}
	token := strings.TrimSpace(secrets["api_token"])
	if token == "" {
		return nil, "", Permanent("api_token_required", "resolved API token is required")
	}
	formID := url.PathEscape(ConfigString(connection.Config, "form_id"))
	response, err := p.Transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodGet, URL: BaseURL(connection) + "/forms/" + formID, Headers: map[string][]string{"Accept": {"application/json"}, "tally-version": {APIVersion(connection)}}, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, MaxResponseBytes: responseLimit})
	if err != nil {
		return nil, "", connector.RetryableError("tally.network_error", err)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := map[string]any{}
	if json.Unmarshal(response.Body, &payload) != nil {
		return nil, ref, Permanent("response_invalid", "provider response is invalid JSON")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code, cause := "tally.http_"+strconv.Itoa(response.StatusCode), fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			return payload, ref, connector.RetryableError(code, cause)
		}
		return payload, ref, connector.PermanentError(code, cause)
	}
	if id := ConfigString(payload, "id"); id != "" {
		ref = "tally:form:" + id
	}
	return payload, ref, nil
}

func VerifyWebhook(ctx context.Context, request connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	if err := ctx.Err(); err != nil {
		return connector.VerifiedWebhook{}, err
	}
	secret := strings.TrimSpace(request.Secrets["webhook_secret"])
	if secret == "" {
		return connector.VerifiedWebhook{}, Permanent("webhook_secret_required", "resolved webhook signing secret is required")
	}
	actual := strings.TrimSpace(HeaderValue(request.Headers, "Tally-Signature"))
	actual = strings.TrimSpace(strings.TrimPrefix(actual, "sha256="))
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(request.Body)
	expected := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(actual), []byte(expected)) {
		return connector.VerifiedWebhook{}, Permanent("webhook_signature_invalid", "webhook signature is invalid")
	}
	payload := map[string]any{}
	if json.Unmarshal(request.Body, &payload) != nil {
		return connector.VerifiedWebhook{}, Permanent("webhook_payload_invalid", "webhook payload is invalid JSON")
	}
	data, _ := payload["data"].(map[string]any)
	externalID := FirstValue(payload["eventId"], data["responseId"], data["submissionId"])
	if externalID == "" {
		return connector.VerifiedWebhook{}, Permanent("webhook_identity_missing", "webhook event identity is missing")
	}
	eventType := strings.ToLower(FirstValue(payload["eventType"], "FORM_RESPONSE"))
	raw, err := json.Marshal(payload)
	if err != nil {
		return connector.VerifiedWebhook{}, Permanent("webhook_payload_invalid", err.Error())
	}
	return connector.VerifiedWebhook{EventType: eventType, ExternalID: externalID, Payload: raw, Security: &connector.WebhookSecurityEvidence{SignatureVerified: true}, ExternalIdentity: Identity(data)}, nil
}

func Identity(data map[string]any) *connector.WebhookExternalIdentity {
	fields, _ := data["fields"].([]any)
	for _, raw := range fields {
		field, _ := raw.(map[string]any)
		kind := strings.ToLower(FirstValue(field["type"], field["key"], field["label"]))
		if !strings.Contains(kind, "email") {
			continue
		}
		if value := FirstValue(field["value"]); strings.Contains(value, "@") {
			return &connector.WebhookExternalIdentity{Subject: value, SubjectType: "email"}
		}
	}
	return nil
}

func HeaderValue(headers map[string][]string, key string) string {
	for candidate, values := range headers {
		if strings.EqualFold(candidate, key) && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}
func FirstValue(values ...any) string {
	for _, value := range values {
		result := strings.TrimSpace(fmt.Sprint(value))
		if result != "" && result != "<nil>" {
			return result
		}
	}
	return ""
}
func BaseURL(connection connector.Connection) string {
	if value := strings.TrimRight(ConfigString(connection.Config, "base_url"), "/"); value != "" {
		return value
	}
	return DefaultBaseURL
}
func APIVersion(connection connector.Connection) string {
	if value := ConfigString(connection.Config, "api_version"); value != "" {
		return value
	}
	return DefaultAPIVersion
}
func ConfigString(values map[string]any, key string) string {
	if values[key] == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(values[key]))
}
func IsLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
func Permanent(code, message string) error {
	return connector.PermanentError("tally."+code, errors.New(message))
}
