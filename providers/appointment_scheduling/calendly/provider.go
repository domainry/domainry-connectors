// Package calendly implements the official Calendly appointment-scheduling Provider.
package calendly

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
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
	ConnectorKey   = "appointment_scheduling"
	ProviderKey    = "calendly"
	defaultBaseURL = "https://api.calendly.com"
	responseLimit  = 4 << 20
)

type ListEventTypesInput struct {
	User         string `json:"user,omitempty"`
	Organization string `json:"organization,omitempty"`
	Active       *bool  `json:"active,omitempty"`
	Count        int    `json:"count,omitempty"`
	PageToken    string `json:"page_token,omitempty"`
}

type ListScheduledEventsInput struct {
	User         string `json:"user,omitempty"`
	Organization string `json:"organization,omitempty"`
	InviteeEmail string `json:"invitee_email,omitempty"`
	Status       string `json:"status,omitempty"`
	MinStartTime string `json:"min_start_time,omitempty"`
	MaxStartTime string `json:"max_start_time,omitempty"`
	Count        int    `json:"count,omitempty"`
	PageToken    string `json:"page_token,omitempty"`
}

type CreateSchedulingLinkInput struct {
	Owner         string `json:"owner"`
	OwnerType     string `json:"owner_type"`
	MaxEventCount int    `json:"max_event_count,omitempty"`
}

type CancelScheduledEventInput struct {
	EventUUID string `json:"event_uuid,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

type Response map[string]any

var (
	ListEventTypes       = connector.CallOperation[ListEventTypesInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_event_types", ContractSHA256: "5cc6e50948aa95cf6c9acbe02050ba4ef273f59b19fb67771fe5c00542bbc02b", Reliability: readReliability()}
	ListScheduledEvents  = connector.CallOperation[ListScheduledEventsInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_scheduled_events", ContractSHA256: "03718b1dd1f9c62875e87ab34f8c29d7b15ae3ec62aebe8d10ddf1748a8a6e94", Reliability: readReliability()}
	CreateSchedulingLink = connector.CallOperation[CreateSchedulingLinkInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "create_scheduling_link", ContractSHA256: "6bf835923d29858351a34ef988ed3697664743ae497ec783bd0598a68133571d", Reliability: writeReliability()}
	CancelScheduledEvent = connector.CallOperation[CancelScheduledEventInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "cancel_scheduled_event", ContractSHA256: "5bda3d58d0b1996439bfd85bd778e4e598a721b70f585cd0b06e42a8cbc5360f", Reliability: writeReliability()}
	TestConnection       = connector.CallOperation[struct{}, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection", ContractSHA256: "7fb4c29cab409b99b675ce439ae7c376287217e18c25d5466e6c4cbd4c45b891", Reliability: readReliability()}
)

func readReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

func writeReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNone}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Calendly transport is required")
	}
	p := &provider{transport: transport}
	operations := make([]connector.BoundOperation, 0, 5)
	for _, bind := range []func() (connector.BoundOperation, error){
		func() (connector.BoundOperation, error) { return connector.BindCall(ListEventTypes, p.listEventTypes) },
		func() (connector.BoundOperation, error) {
			return connector.BindCall(ListScheduledEvents, p.listScheduledEvents)
		},
		func() (connector.BoundOperation, error) {
			return connector.BindCall(CreateSchedulingLink, p.createSchedulingLink)
		},
		func() (connector.BoundOperation, error) {
			return connector.BindCall(CancelScheduledEvent, p.cancelScheduledEvent)
		},
		func() (connector.BoundOperation, error) {
			return connector.BindCall(TestConnection, p.callTestConnection)
		},
	} {
		operation, err := bind()
		if err != nil {
			return nil, err
		}
		operations = append(operations, operation)
	}
	bound, err := connector.NewProvider(schema(), operations...)
	if err != nil {
		return nil, err
	}
	p.Adapter = bound
	return p, nil
}

func schema() connector.ProviderSchema {
	minimum, maximum, webhookMaximum := float64(1), float64(120), float64(3600)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{
		{Key: "base_url", Name: "Calendly API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://api.calendly.com"`)},
		{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}},
		{Key: "webhook_tolerance_seconds", Name: "Webhook timestamp tolerance seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`300`), Validation: connector.ConfigValidation{Min: &minimum, Max: &webhookMaximum}},
	}, SecretFields: []connector.SecretField{
		{Key: "access_token", Name: "Personal access or OAuth token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound},
		{Key: "webhook_signing_key", Name: "Webhook signing key", Required: false, CredentialKind: connector.SecretCredentialSigningSecret, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound},
	}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(baseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return permanent("endpoint_invalid", "valid Calendly API endpoint is required")
	}
	if parsed.Scheme == "http" && isLoopback(parsed.Hostname()) {
		return nil
	}
	if parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), "api.calendly.com") {
		return permanent("endpoint_invalid", "official Calendly API endpoint or loopback HTTP is required")
	}
	return nil
}

func (p *provider) listEventTypes(ctx context.Context, request connector.TypedRequest[ListEventTypesInput]) (connector.TypedResult[Response], error) {
	input := request.Input
	if strings.TrimSpace(input.User) == "" && strings.TrimSpace(input.Organization) == "" {
		return connector.TypedResult[Response]{}, permanent("scope_required", "user or organization URI is required")
	}
	query, err := pageQuery(input.Count, input.PageToken)
	if err != nil {
		return connector.TypedResult[Response]{}, err
	}
	set(query, "user", input.User)
	set(query, "organization", input.Organization)
	if input.Active != nil {
		query.Set("active", strconv.FormatBool(*input.Active))
	}
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/event_types", query, nil, false)
}

func (p *provider) listScheduledEvents(ctx context.Context, request connector.TypedRequest[ListScheduledEventsInput]) (connector.TypedResult[Response], error) {
	input := request.Input
	if strings.TrimSpace(input.User) == "" && strings.TrimSpace(input.Organization) == "" {
		return connector.TypedResult[Response]{}, permanent("scope_required", "user or organization URI is required")
	}
	query, err := pageQuery(input.Count, input.PageToken)
	if err != nil {
		return connector.TypedResult[Response]{}, err
	}
	status := strings.TrimSpace(input.Status)
	if status != "" && status != "active" && status != "canceled" {
		return connector.TypedResult[Response]{}, permanent("status_invalid", "status must be active or canceled")
	}
	if err = validateTimeRange(input.MinStartTime, input.MaxStartTime); err != nil {
		return connector.TypedResult[Response]{}, err
	}
	set(query, "user", input.User)
	set(query, "organization", input.Organization)
	set(query, "invitee_email", input.InviteeEmail)
	set(query, "status", status)
	set(query, "min_start_time", input.MinStartTime)
	set(query, "max_start_time", input.MaxStartTime)
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/scheduled_events", query, nil, false)
}

func (p *provider) createSchedulingLink(ctx context.Context, request connector.TypedRequest[CreateSchedulingLinkInput]) (connector.TypedResult[Response], error) {
	input := request.Input
	if strings.TrimSpace(input.Owner) == "" {
		return connector.TypedResult[Response]{}, permanent("owner_required", "owner URI is required")
	}
	if input.OwnerType != "EventType" {
		return connector.TypedResult[Response]{}, permanent("owner_type_invalid", "owner_type must be EventType")
	}
	if input.MaxEventCount < 0 {
		return connector.TypedResult[Response]{}, permanent("max_event_count_invalid", "max_event_count must be positive when set")
	}
	body := map[string]any{"owner": strings.TrimSpace(input.Owner), "owner_type": input.OwnerType}
	if input.MaxEventCount > 0 {
		body["max_event_count"] = input.MaxEventCount
	}
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodPost, "/scheduling_links", nil, body, true)
}

func (p *provider) cancelScheduledEvent(ctx context.Context, request connector.TypedRequest[CancelScheduledEventInput]) (connector.TypedResult[Response], error) {
	uuid := strings.TrimSpace(request.Input.EventUUID)
	if uuid == "" {
		return connector.TypedResult[Response]{}, permanent("event_uuid_required", "event_uuid is required")
	}
	body := map[string]any{}
	if reason := strings.TrimSpace(request.Input.Reason); reason != "" {
		body["reason"] = reason
	}
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodPost, "/scheduled_events/"+url.PathEscape(uuid)+"/cancellation", nil, body, true)
}

func (p *provider) callTestConnection(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/users/me", nil, nil, false)
}

func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/users/me", nil, nil, false)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, err := json.Marshal(map[string]any{"resource": result.Output["resource"], "response_ref": result.ResponseRef})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, method, path string, query url.Values, body map[string]any, write bool) (connector.TypedResult[Response], error) {
	if err := p.ValidateConfig(connection); err != nil {
		return connector.TypedResult[Response]{}, err
	}
	token := strings.TrimSpace(secrets["access_token"])
	if token == "" {
		return connector.TypedResult[Response]{}, permanent("access_token_required", "resolved Calendly access token is required")
	}
	endpoint, err := url.Parse(baseURL(connection) + path)
	if err != nil {
		return connector.TypedResult[Response]{}, permanent("request_invalid", "Calendly request URL is invalid")
	}
	endpoint.RawQuery = query.Encode()
	var raw []byte
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return connector.TypedResult[Response]{}, permanent("request_invalid", "Calendly request body is invalid")
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint.String(), Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, Body: raw, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		if write {
			return connector.TypedResult[Response]{}, connector.UncertainError("calendly.network_error", transportErr)
		}
		return connector.TypedResult[Response]{}, connector.RetryableError("calendly.network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := Response{}
	validJSON := len(response.Body) == 0 || json.Unmarshal(response.Body, &payload) == nil
	result := connector.TypedResult[Response]{Output: payload, ResponseRef: ref}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		code := "calendly.http_" + strconv.Itoa(response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests {
			return result, connector.RetryableError(code, cause)
		}
		if response.StatusCode == http.StatusRequestTimeout || response.StatusCode >= 500 {
			if write {
				return result, connector.UncertainError(code, cause)
			}
			return result, connector.RetryableError(code, cause)
		}
		return result, connector.PermanentError(code, cause)
	}
	if !validJSON {
		if write {
			return connector.TypedResult[Response]{ResponseRef: ref}, connector.UncertainError("calendly.response_invalid", errors.New("provider response is invalid JSON"))
		}
		return connector.TypedResult[Response]{ResponseRef: ref}, permanent("response_invalid", "provider response is invalid JSON")
	}
	if resource, ok := payload["resource"].(map[string]any); ok {
		if uri := mapString(resource, "uri"); uri != "" {
			result.ResponseRef = "calendly:" + uri
		}
		if bookingURL := mapString(resource, "booking_url"); bookingURL != "" {
			result.ResponseRef = "calendly:" + bookingURL
		}
	}
	return result, nil
}

func (p *provider) VerifyWebhook(ctx context.Context, request connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	if err := ctx.Err(); err != nil {
		return connector.VerifiedWebhook{}, err
	}
	secret := strings.TrimSpace(request.Secrets["webhook_signing_key"])
	if secret == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_signing_key_required", "resolved webhook signing key is required")
	}
	timestamp, signatures, err := parseSignature(header(request.Headers, "Calendly-Webhook-Signature"))
	if err != nil {
		return connector.VerifiedWebhook{}, permanent("webhook_signature_invalid", err.Error())
	}
	receivedAt := request.ReceivedAt
	if receivedAt.IsZero() {
		receivedAt = time.Now().UTC()
	}
	tolerance := time.Duration(configInt(request.Connection, "webhook_tolerance_seconds", 300)) * time.Second
	eventTime := time.Unix(timestamp, 0)
	if tolerance <= 0 || receivedAt.Sub(eventTime).Abs() > tolerance {
		return connector.VerifiedWebhook{}, permanent("webhook_timestamp_invalid", "webhook timestamp is outside tolerance")
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = fmt.Fprintf(mac, "%d.%s", timestamp, request.Body)
	expected := hex.EncodeToString(mac.Sum(nil))
	valid := false
	for _, signature := range signatures {
		if hmac.Equal([]byte(expected), []byte(signature)) {
			valid = true
			break
		}
	}
	if !valid {
		return connector.VerifiedWebhook{}, permanent("webhook_signature_invalid", "webhook signature does not match")
	}
	payload := Response{}
	if json.Unmarshal(request.Body, &payload) != nil {
		return connector.VerifiedWebhook{}, permanent("webhook_payload_invalid", "webhook payload is invalid JSON")
	}
	eventType := mapString(payload, "event")
	detail, _ := payload["payload"].(map[string]any)
	eventURI := mapString(detail, "uri")
	if eventURI == "" {
		nested, _ := detail["event"].(map[string]any)
		eventURI = mapString(nested, "uri")
	}
	if eventType == "" || eventURI == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_identity_missing", "webhook event identity is missing")
	}
	verified := connector.VerifiedWebhook{EventType: eventType, ExternalID: eventURI + ":" + eventType, Payload: append(json.RawMessage(nil), request.Body...), Security: &connector.WebhookSecurityEvidence{SignatureVerified: true, EventTime: eventTime}}
	if email := mapString(detail, "email"); email != "" {
		verified.ExternalIdentity = &connector.WebhookExternalIdentity{Subject: email, SubjectType: "email", Name: mapString(detail, "name")}
	}
	return verified, nil
}

func pageQuery(count int, token string) (url.Values, error) {
	if count < 0 || count > 100 {
		return nil, permanent("count_invalid", "count must be between 1 and 100 when set")
	}
	query := url.Values{}
	if count > 0 {
		query.Set("count", strconv.Itoa(count))
	}
	set(query, "page_token", token)
	return query, nil
}
func validateTimeRange(minimum, maximum string) error {
	var minTime, maxTime time.Time
	var err error
	if strings.TrimSpace(minimum) != "" {
		minTime, err = time.Parse(time.RFC3339, minimum)
		if err != nil {
			return permanent("min_start_time_invalid", "min_start_time must be RFC3339")
		}
	}
	if strings.TrimSpace(maximum) != "" {
		maxTime, err = time.Parse(time.RFC3339, maximum)
		if err != nil {
			return permanent("max_start_time_invalid", "max_start_time must be RFC3339")
		}
	}
	if !minTime.IsZero() && !maxTime.IsZero() && minTime.After(maxTime) {
		return permanent("start_time_range_invalid", "min_start_time must not exceed max_start_time")
	}
	return nil
}
func parseSignature(value string) (int64, []string, error) {
	var timestamp int64
	signatures := []string{}
	for _, part := range strings.Split(value, ",") {
		key, raw, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		if key == "t" {
			timestamp, _ = strconv.ParseInt(raw, 10, 64)
		}
		if key == "v1" && raw != "" {
			signatures = append(signatures, raw)
		}
	}
	if timestamp <= 0 || len(signatures) == 0 {
		return 0, nil, errors.New("signature header is invalid")
	}
	return timestamp, signatures, nil
}
func set(query url.Values, key, value string) {
	if value = strings.TrimSpace(value); value != "" {
		query.Set(key, value)
	}
}
func baseURL(connection connector.Connection) string {
	if value := strings.TrimRight(config(connection, "base_url", ""), "/"); value != "" {
		return value
	}
	return defaultBaseURL
}
func config(connection connector.Connection, key, fallback string) string {
	if value := mapString(connection.Config, key); value != "" {
		return value
	}
	return fallback
}
func configInt(connection connector.Connection, key string, fallback int) int {
	value := config(connection, key, "")
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}
func mapString(values map[string]any, key string) string {
	if values[key] == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(values[key]))
}
func header(headers map[string][]string, key string) string {
	for current, values := range headers {
		if strings.EqualFold(current, key) && len(values) > 0 {
			return strings.TrimSpace(values[0])
		}
	}
	return ""
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
func permanent(code, message string) error {
	return connector.PermanentError("calendly."+code, errors.New(message))
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
var _ connector.WebhookVerifier = (*provider)(nil)
