// Package zoomwebinar implements the official Zoom Webinars Provider.
package zoomwebinar

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	connector "github.com/domainry/domainry-connector-sdk"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	ConnectorKey   = "event_webinar"
	ProviderKey    = "zoom_webinar"
	defaultBaseURL = "https://api.zoom.us/v2"
)

type ListWebinarsInput struct {
	UserID        string `json:"user_id,omitempty"`
	PageSize      int    `json:"page_size,omitempty"`
	NextPageToken string `json:"next_page_token,omitempty"`
	Type          string `json:"type,omitempty"`
	Status        string `json:"status,omitempty"`
}
type WebinarInput struct {
	WebinarID string `json:"webinar_id"`
}
type ListRegistrantsInput struct {
	WebinarID     string `json:"webinar_id"`
	PageSize      int    `json:"page_size,omitempty"`
	NextPageToken string `json:"next_page_token,omitempty"`
	Status        string `json:"status,omitempty"`
}

var (
	ListWebinars    = readOp[ListWebinarsInput]("list_webinars", "2294b19886fd4558fe4e93799db4658e6627d2e2bbc07c9dcce34513501b8f3d")
	GetWebinar      = readOp[WebinarInput]("get_webinar", "da36b84d0419c549c0e6f46e634d22b759404a68bbad21da2511cc3a80234969")
	ListRegistrants = readOp[ListRegistrantsInput]("list_registrants", "abfb0c88ce7cc1304cb070a7e322c431a1f4733e58e7e2a1bcffa60d2bae9480")
	TestConnection  = readOp[struct{}]("test_connection", "716ab5c39dae386ae28b7895342303b1beb226176127e3c730bc2322ca5cbf6f")
)

func readOp[I any](key, hash string) connector.CallOperation[I, map[string]any] {
	return connector.CallOperation[I, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: key, ContractSHA256: hash, Reliability: connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Zoom Webinar transport is required")
	}
	p := &provider{transport: transport}
	ops := make([]connector.BoundOperation, 0, 4)
	bind := func(bound connector.BoundOperation, err error) error { ops = append(ops, bound); return err }
	if err := bind(connector.BindCall(GetWebinar, p.get)); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(ListRegistrants, p.registrants)); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(ListWebinars, p.list)); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(TestConnection, p.test)); err != nil {
		return nil, err
	}
	adapter, err := connector.NewProvider(schema(), ops...)
	if err != nil {
		return nil, err
	}
	p.Adapter = adapter
	return p, nil
}
func schema() connector.ProviderSchema {
	min, max := float64(1), float64(120)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "base_url", Name: "Zoom API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://api.zoom.us/v2"`)}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &min, Max: &max}}}, SecretFields: []connector.SecretField{{Key: "api_token", Name: "OAuth access token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}, {Key: "webhook_secret", Name: "Webhook secret token", CredentialKind: connector.SecretCredentialSigningSecret, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(config(connection, "base_url", defaultBaseURL))
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return permanent("endpoint_invalid", "valid Zoom API base URL is required")
	}
	timeout := intValue(connection.Config["timeout_seconds"], 30)
	if timeout < 1 || timeout > 120 {
		return permanent("timeout_invalid", "timeout_seconds must be between 1 and 120")
	}
	return nil
}
func (p *provider) list(ctx context.Context, r connector.TypedRequest[ListWebinarsInput]) (connector.TypedResult[map[string]any], error) {
	user := strings.TrimSpace(r.Input.UserID)
	if user == "" {
		user = "me"
	}
	query := pagination(r.Input.PageSize, r.Input.NextPageToken)
	if value := strings.TrimSpace(r.Input.Type); value != "" {
		query.Set("type", value)
	}
	if value := strings.TrimSpace(r.Input.Status); value != "" {
		query.Set("status", value)
	}
	return p.execute(ctx, r.Connection, r.Secrets, "/users/"+url.PathEscape(user)+"/webinars", query)
}
func (p *provider) get(ctx context.Context, r connector.TypedRequest[WebinarInput]) (connector.TypedResult[map[string]any], error) {
	id, err := webinarID(r.Input.WebinarID)
	if err != nil {
		return empty(), err
	}
	return p.execute(ctx, r.Connection, r.Secrets, "/webinars/"+url.PathEscape(id), nil)
}
func (p *provider) registrants(ctx context.Context, r connector.TypedRequest[ListRegistrantsInput]) (connector.TypedResult[map[string]any], error) {
	id, err := webinarID(r.Input.WebinarID)
	if err != nil {
		return empty(), err
	}
	query := pagination(r.Input.PageSize, r.Input.NextPageToken)
	if value := strings.TrimSpace(r.Input.Status); value != "" {
		query.Set("status", value)
	}
	return p.execute(ctx, r.Connection, r.Secrets, "/webinars/"+url.PathEscape(id)+"/registrants", query)
}
func (p *provider) test(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[map[string]any], error) {
	return p.execute(ctx, r.Connection, r.Secrets, "/users/me", nil)
}
func (p *provider) TestConnection(ctx context.Context, r connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.test(ctx, connector.TypedRequest[struct{}]{Connection: r.Connection, Secrets: r.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(map[string]any{"connected": true, "user": result.Output})
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}
func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, path string, query url.Values) (connector.TypedResult[map[string]any], error) {
	if err := p.ValidateConfig(connection); err != nil {
		return empty(), err
	}
	token := strings.TrimSpace(secrets["api_token"])
	if token == "" {
		return empty(), permanent("api_token_required", "api_token is required")
	}
	endpoint := strings.TrimRight(config(connection, "base_url", defaultBaseURL), "/") + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodGet, URL: endpoint, Headers: map[string][]string{"Accept": {"application/json"}}, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, MaxResponseBytes: 4 << 20})
	if err != nil {
		return empty(), connector.RetryableError("zoom_webinar.network_error", err)
	}
	payload := map[string]any{}
	if len(response.Body) > 0 && json.Unmarshal(response.Body, &payload) != nil {
		return empty(), connector.RetryableError("zoom_webinar.response_invalid", errors.New("Zoom response is invalid"))
	}
	ref := fmt.Sprintf("http:%d", response.StatusCode)
	if id := stringValue(payload["id"]); id != "" {
		ref = "zoom_webinar:" + id
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, nil
	}
	cause := errors.New("Zoom rejected the request")
	if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
		return connector.TypedResult[map[string]any]{ResponseRef: ref}, connector.RetryableError(fmt.Sprintf("zoom_webinar.http_%d", response.StatusCode), cause)
	}
	return connector.TypedResult[map[string]any]{ResponseRef: ref}, connector.PermanentError(fmt.Sprintf("zoom_webinar.http_%d", response.StatusCode), cause)
}
func (p *provider) VerifyWebhook(ctx context.Context, request connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	if err := ctx.Err(); err != nil {
		return connector.VerifiedWebhook{}, err
	}
	secret := strings.TrimSpace(request.Secrets["webhook_secret"])
	if secret == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_secret_required", "resolved webhook secret is required")
	}
	timestamp := header(request.Headers, "X-Zm-Request-Timestamp")
	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || seconds <= 0 {
		return connector.VerifiedWebhook{}, permanent("webhook_timestamp_invalid", "Zoom webhook timestamp is invalid")
	}
	received := request.ReceivedAt
	if received.IsZero() {
		received = time.Now().UTC()
	}
	delta := received.Unix() - seconds
	if delta > 300 || delta < -300 {
		return connector.VerifiedWebhook{}, permanent("webhook_timestamp_out_of_range", "Zoom webhook timestamp is outside tolerance")
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte("v0:" + timestamp + ":"))
	_, _ = mac.Write(request.Body)
	expected := "v0=" + hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(header(request.Headers, "X-Zm-Signature"))) {
		return connector.VerifiedWebhook{}, permanent("webhook_signature_invalid", "Zoom webhook signature does not match")
	}
	payload := map[string]any{}
	if json.Unmarshal(request.Body, &payload) != nil {
		return connector.VerifiedWebhook{}, permanent("webhook_payload_invalid", "Zoom webhook payload is invalid")
	}
	event := stringValue(payload["event"])
	body, _ := payload["payload"].(map[string]any)
	object, _ := body["object"].(map[string]any)
	id := stringValue(object["id"])
	if id == "" {
		id = stringValue(object["uuid"])
	}
	if event == "" || id == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_identity_missing", "Zoom webhook identity is missing")
	}
	return connector.VerifiedWebhook{EventType: event, ExternalID: event + ":" + id, Payload: append(json.RawMessage(nil), request.Body...), Security: &connector.WebhookSecurityEvidence{SignatureVerified: true, EventTime: time.Unix(seconds, 0).UTC()}}, nil
}
func pagination(size int, token string) url.Values {
	query := url.Values{}
	if size > 0 {
		query.Set("page_size", strconv.Itoa(size))
	}
	if token = strings.TrimSpace(token); token != "" {
		query.Set("next_page_token", token)
	}
	return query
}
func webinarID(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", permanent("webinar_id_required", "webinar_id is required")
	}
	return value, nil
}
func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	}
	return ""
}
func header(headers map[string][]string, name string) string {
	for key, values := range headers {
		if strings.EqualFold(key, name) && len(values) > 0 {
			return strings.TrimSpace(values[0])
		}
	}
	return ""
}
func empty() connector.TypedResult[map[string]any] { return connector.TypedResult[map[string]any]{} }
func config(connection connector.Connection, key, fallback string) string {
	value, _ := connection.Config[key].(string)
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return fallback
}
func intValue(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		return typed
	case float64:
		return int(typed)
	case json.Number:
		parsed, err := strconv.Atoi(typed.String())
		if err == nil {
			return parsed
		}
	}
	return fallback
}
func isLoopback(host string) bool {
	return strings.EqualFold(host, "localhost") || (net.ParseIP(host) != nil && net.ParseIP(host).IsLoopback())
}
func permanent(code, message string) error {
	return connector.PermanentError("zoom_webinar."+code, errors.New(message))
}
