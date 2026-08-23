// Package calcom implements the official Cal.com appointment-scheduling Provider.
package calcom

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
	ProviderKey    = "cal_com"
	defaultBaseURL = "https://api.cal.com"
	responseLimit  = 4 << 20
	versionEvents  = "2024-06-14"
	versionWrites  = "2026-02-25"
	versionList    = "2026-05-01"
)

type ListEventTypesInput struct {
	Username      string `json:"username,omitempty"`
	EventSlug     string `json:"eventSlug,omitempty"`
	Usernames     string `json:"usernames,omitempty"`
	OrgSlug       string `json:"orgSlug,omitempty"`
	OrgID         int    `json:"orgId,omitempty"`
	SortCreatedAt string `json:"sortCreatedAt,omitempty"`
}
type ListBookingsInput struct {
	Status        string `json:"status,omitempty"`
	AttendeeEmail string `json:"attendeeEmail,omitempty"`
	AttendeeName  string `json:"attendeeName,omitempty"`
	BookingUID    string `json:"bookingUid,omitempty"`
	Cursor        string `json:"cursor,omitempty"`
}
type CreateBookingInput struct {
	Start                   string         `json:"start"`
	Attendee                map[string]any `json:"attendee"`
	BookingFieldsResponses  map[string]any `json:"bookingFieldsResponses,omitempty"`
	EventTypeID             int            `json:"eventTypeId,omitempty"`
	EventTypeSlug           string         `json:"eventTypeSlug,omitempty"`
	Username                string         `json:"username,omitempty"`
	TeamSlug                string         `json:"teamSlug,omitempty"`
	OrganizationSlug        string         `json:"organizationSlug,omitempty"`
	Guests                  []string       `json:"guests,omitempty"`
	MeetingURL              string         `json:"meetingUrl,omitempty"`
	Location                map[string]any `json:"location,omitempty"`
	Metadata                map[string]any `json:"metadata,omitempty"`
	LengthInMinutes         int            `json:"lengthInMinutes,omitempty"`
	Routing                 map[string]any `json:"routing,omitempty"`
	EmailVerificationCode   string         `json:"emailVerificationCode,omitempty"`
	AllowConflicts          *bool          `json:"allowConflicts,omitempty"`
	AllowBookingOutOfBounds *bool          `json:"allowBookingOutOfBounds,omitempty"`
}
type CancelBookingInput struct {
	BookingUID string `json:"booking_uid,omitempty"`
	EventUUID  string `json:"event_uuid,omitempty"`
	Reason     string `json:"reason,omitempty"`
}
type RescheduleBookingInput struct {
	BookingUID            string `json:"booking_uid"`
	Start                 string `json:"start"`
	RescheduledBy         string `json:"rescheduledBy,omitempty"`
	ReschedulingReason    string `json:"reschedulingReason,omitempty"`
	EmailVerificationCode string `json:"emailVerificationCode,omitempty"`
}
type Response map[string]any

var (
	ListEventTypes           = connector.CallOperation[ListEventTypesInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_event_types", ContractSHA256: "5cc6e50948aa95cf6c9acbe02050ba4ef273f59b19fb67771fe5c00542bbc02b", Reliability: readReliability()}
	ListScheduledEvents      = connector.CallOperation[ListBookingsInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_scheduled_events", ContractSHA256: "03718b1dd1f9c62875e87ab34f8c29d7b15ae3ec62aebe8d10ddf1748a8a6e94", Reliability: readReliability()}
	CreateBooking            = connector.CallOperation[CreateBookingInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "create_booking", ContractSHA256: "928778cc82cac4f36f946f071d8b588e37eb555170f9878281aca249e29c66e1", Reliability: writeReliability()}
	CancelScheduledEvent     = connector.CallOperation[CancelBookingInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "cancel_scheduled_event", ContractSHA256: "09d521d641dcd334b09676d0c9d2f8bc1771ed648206cef4c5fdc231d32fa082", Reliability: writeReliability()}
	RescheduleScheduledEvent = connector.CallOperation[RescheduleBookingInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "reschedule_scheduled_event", ContractSHA256: "4a086743e91ae464065a1075fa4493efeb5c94fda5d60b1446bd1033b81a0b76", Reliability: writeReliability()}
	TestConnection           = connector.CallOperation[struct{}, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection", ContractSHA256: "7fb4c29cab409b99b675ce439ae7c376287217e18c25d5466e6c4cbd4c45b891", Reliability: readReliability()}
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
		return nil, errors.New("Cal.com transport is required")
	}
	p := &provider{transport: transport}
	operations := make([]connector.BoundOperation, 0, 6)
	for _, bind := range []func() (connector.BoundOperation, error){
		func() (connector.BoundOperation, error) { return connector.BindCall(ListEventTypes, p.listEventTypes) },
		func() (connector.BoundOperation, error) {
			return connector.BindCall(ListScheduledEvents, p.listBookings)
		},
		func() (connector.BoundOperation, error) { return connector.BindCall(CreateBooking, p.createBooking) },
		func() (connector.BoundOperation, error) {
			return connector.BindCall(CancelScheduledEvent, p.cancelBooking)
		},
		func() (connector.BoundOperation, error) {
			return connector.BindCall(RescheduleScheduledEvent, p.rescheduleBooking)
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
	min, max := float64(1), float64(120)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{
		{Key: "base_url", Name: "Cal.com API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://api.cal.com"`)},
		{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &min, Max: &max}},
	}, SecretFields: []connector.SecretField{
		{Key: "access_token", Name: "API key, managed-user token, or OAuth access token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound},
		{Key: "webhook_signing_key", Name: "Webhook signing key", Required: false, CredentialKind: connector.SecretCredentialSigningSecret, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound},
	}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(baseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return permanent("endpoint_invalid", "valid Cal.com API endpoint is required")
	}
	if parsed.Scheme == "http" && isLoopback(parsed.Hostname()) {
		return nil
	}
	if parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), "api.cal.com") {
		return permanent("endpoint_invalid", "official Cal.com API endpoint or loopback HTTP is required")
	}
	return nil
}

func (p *provider) listEventTypes(ctx context.Context, request connector.TypedRequest[ListEventTypesInput]) (connector.TypedResult[Response], error) {
	i := request.Input
	if i.EventSlug != "" && i.Username == "" {
		return connector.TypedResult[Response]{}, permanent("username_required", "username is required with eventSlug")
	}
	if i.OrgID < 0 {
		return connector.TypedResult[Response]{}, permanent("org_id_invalid", "orgId must be positive when set")
	}
	if i.SortCreatedAt != "" && i.SortCreatedAt != "asc" && i.SortCreatedAt != "desc" {
		return connector.TypedResult[Response]{}, permanent("sort_invalid", "sortCreatedAt must be asc or desc")
	}
	q := url.Values{}
	set(q, "username", i.Username)
	set(q, "eventSlug", i.EventSlug)
	set(q, "usernames", i.Usernames)
	set(q, "orgSlug", i.OrgSlug)
	set(q, "sortCreatedAt", i.SortCreatedAt)
	if i.OrgID > 0 {
		q.Set("orgId", strconv.Itoa(i.OrgID))
	}
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/v2/event-types", q, nil, versionEvents, false)
}
func (p *provider) listBookings(ctx context.Context, request connector.TypedRequest[ListBookingsInput]) (connector.TypedResult[Response], error) {
	i := request.Input
	if !validBookingStatus(i.Status) {
		return connector.TypedResult[Response]{}, permanent("status_invalid", "unsupported booking status")
	}
	q := url.Values{}
	set(q, "status", i.Status)
	set(q, "attendeeEmail", i.AttendeeEmail)
	set(q, "attendeeName", i.AttendeeName)
	set(q, "bookingUid", i.BookingUID)
	set(q, "cursor", i.Cursor)
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/v2/bookings", q, nil, versionList, false)
}
func (p *provider) createBooking(ctx context.Context, request connector.TypedRequest[CreateBookingInput]) (connector.TypedResult[Response], error) {
	i := request.Input
	if err := validateUTC(i.Start, "start"); err != nil {
		return connector.TypedResult[Response]{}, err
	}
	if len(i.Attendee) == 0 || mapString(i.Attendee, "email") == "" || mapString(i.Attendee, "name") == "" || mapString(i.Attendee, "timeZone") == "" {
		return connector.TypedResult[Response]{}, permanent("attendee_invalid", "attendee name, email, and timeZone are required")
	}
	if i.EventTypeID < 1 && strings.TrimSpace(i.EventTypeSlug) == "" {
		return connector.TypedResult[Response]{}, permanent("event_type_required", "eventTypeId or eventTypeSlug is required")
	}
	if i.EventTypeID < 0 || i.LengthInMinutes < 0 {
		return connector.TypedResult[Response]{}, permanent("numeric_field_invalid", "numeric fields must be positive when set")
	}
	raw, _ := json.Marshal(i)
	body := map[string]any{}
	_ = json.Unmarshal(raw, &body)
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodPost, "/v2/bookings", nil, body, versionWrites, true)
}
func (p *provider) cancelBooking(ctx context.Context, request connector.TypedRequest[CancelBookingInput]) (connector.TypedResult[Response], error) {
	uid := strings.TrimSpace(request.Input.BookingUID)
	if uid == "" {
		uid = strings.TrimSpace(request.Input.EventUUID)
	}
	if uid == "" {
		return connector.TypedResult[Response]{}, permanent("booking_uid_required", "booking_uid is required")
	}
	body := map[string]any{}
	if reason := strings.TrimSpace(request.Input.Reason); reason != "" {
		body["cancellationReason"] = reason
	}
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodPost, "/v2/bookings/"+url.PathEscape(uid)+"/cancel", nil, body, versionWrites, true)
}
func (p *provider) rescheduleBooking(ctx context.Context, request connector.TypedRequest[RescheduleBookingInput]) (connector.TypedResult[Response], error) {
	i := request.Input
	uid := strings.TrimSpace(i.BookingUID)
	if uid == "" {
		return connector.TypedResult[Response]{}, permanent("booking_uid_required", "booking_uid is required")
	}
	if err := validateUTC(i.Start, "start"); err != nil {
		return connector.TypedResult[Response]{}, err
	}
	body := map[string]any{"start": i.Start}
	setAny(body, "rescheduledBy", i.RescheduledBy)
	setAny(body, "reschedulingReason", i.ReschedulingReason)
	setAny(body, "emailVerificationCode", i.EmailVerificationCode)
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodPost, "/v2/bookings/"+url.PathEscape(uid)+"/reschedule", nil, body, versionWrites, true)
}
func (p *provider) callTestConnection(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/v2/event-types", nil, nil, versionEvents, false)
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/v2/event-types", nil, nil, versionEvents, false)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, err := json.Marshal(map[string]any{"status": result.Output["status"], "response_ref": result.ResponseRef})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, method, path string, query url.Values, body map[string]any, version string, write bool) (connector.TypedResult[Response], error) {
	if err := p.ValidateConfig(connection); err != nil {
		return connector.TypedResult[Response]{}, err
	}
	token := strings.TrimSpace(secrets["access_token"])
	if token == "" {
		return connector.TypedResult[Response]{}, permanent("access_token_required", "resolved Cal.com access token is required")
	}
	endpoint, err := url.Parse(baseURL(connection) + path)
	if err != nil {
		return connector.TypedResult[Response]{}, permanent("request_invalid", "Cal.com request URL is invalid")
	}
	endpoint.RawQuery = query.Encode()
	var raw []byte
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return connector.TypedResult[Response]{}, permanent("request_invalid", "Cal.com request body is invalid")
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}, "cal-api-version": {version}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint.String(), Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, Body: raw, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		if write {
			return connector.TypedResult[Response]{}, connector.UncertainError("cal_com.network_error", transportErr)
		}
		return connector.TypedResult[Response]{}, connector.RetryableError("cal_com.network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := Response{}
	validJSON := len(response.Body) == 0 || json.Unmarshal(response.Body, &payload) == nil
	result := connector.TypedResult[Response]{Output: payload, ResponseRef: ref}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		code := "cal_com.http_" + strconv.Itoa(response.StatusCode)
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
			return connector.TypedResult[Response]{ResponseRef: ref}, connector.UncertainError("cal_com.response_invalid", errors.New("provider response is invalid JSON"))
		}
		return connector.TypedResult[Response]{ResponseRef: ref}, permanent("response_invalid", "provider response is invalid JSON")
	}
	if data, ok := payload["data"].(map[string]any); ok {
		if uid := firstString(data, "uid", "id"); uid != "" {
			result.ResponseRef = "cal_com:" + uid
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
	provided := strings.TrimPrefix(strings.ToLower(header(request.Headers, "x-cal-signature-256")), "sha256=")
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(request.Body)
	expected := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(provided)) {
		return connector.VerifiedWebhook{}, permanent("webhook_signature_invalid", "webhook signature does not match")
	}
	payload := Response{}
	if json.Unmarshal(request.Body, &payload) != nil {
		return connector.VerifiedWebhook{}, permanent("webhook_payload_invalid", "webhook payload is invalid JSON")
	}
	eventType := mapString(payload, "triggerEvent")
	detail, _ := payload["payload"].(map[string]any)
	if detail == nil {
		detail = payload
	}
	uid := firstString(detail, "uid", "bookingUid")
	if eventType == "" || uid == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_identity_missing", "webhook event identity is missing")
	}
	verified := connector.VerifiedWebhook{EventType: strings.ToLower(eventType), ExternalID: uid + ":" + eventType, Payload: append(json.RawMessage(nil), request.Body...), Security: &connector.WebhookSecurityEvidence{SignatureVerified: true}}
	if attendees, ok := detail["attendees"].([]any); ok && len(attendees) > 0 {
		if attendee, ok := attendees[0].(map[string]any); ok {
			if email := mapString(attendee, "email"); email != "" {
				verified.ExternalIdentity = &connector.WebhookExternalIdentity{Subject: email, SubjectType: "email", Name: mapString(attendee, "name")}
			}
		}
	}
	return verified, nil
}

func validBookingStatus(value string) bool {
	switch strings.TrimSpace(value) {
	case "", "upcoming", "recurring", "unconfirmed", "past", "cancelled":
		return true
	}
	return false
}
func validateUTC(value, key string) error {
	parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(value))
	if err != nil || parsed.Location() != time.UTC {
		return permanent(key+"_invalid", key+" must be RFC3339 in UTC")
	}
	return nil
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
func mapString(values map[string]any, key string) string {
	if values[key] == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(values[key]))
}
func firstString(values map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := mapString(values, key); value != "" {
			return value
		}
	}
	return ""
}
func set(values url.Values, key, value string) {
	if value = strings.TrimSpace(value); value != "" {
		values.Set(key, value)
	}
}
func setAny(values map[string]any, key, value string) {
	if value = strings.TrimSpace(value); value != "" {
		values[key] = value
	}
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
	return connector.PermanentError("cal_com."+code, errors.New(message))
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
var _ connector.WebhookVerifier = (*provider)(nil)
