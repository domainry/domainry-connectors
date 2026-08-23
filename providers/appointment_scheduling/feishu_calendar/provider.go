// Package feishucalendar implements the official Feishu Calendar Provider.
package feishucalendar

import (
	"context"
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
	internalfeishu "github.com/domainry/domainry-connectors/internal/feishu"
)

const (
	ConnectorKey         = "appointment_scheduling"
	ProviderKey          = "feishu_calendar"
	defaultBaseURL       = "https://open.feishu.cn"
	responseLimit  int64 = 1 << 20
)

type Attendee struct {
	Email  string `json:"email,omitempty"`
	UserID string `json:"user_id,omitempty"`
}
type CreateBookingInput struct {
	Start       string     `json:"start"`
	End         string     `json:"end"`
	TimeZone    string     `json:"timeZone,omitempty"`
	Summary     string     `json:"summary,omitempty"`
	Description string     `json:"description,omitempty"`
	Attendee    *Attendee  `json:"attendee,omitempty"`
	Attendees   []Attendee `json:"attendees,omitempty"`
}
type EnqueueBookingInput struct {
	Start       string         `json:"start"`
	End         string         `json:"end"`
	TimeZone    string         `json:"time_zone,omitempty"`
	Summary     string         `json:"summary,omitempty"`
	Description string         `json:"description,omitempty"`
	Attendees   []Attendee     `json:"attendees"`
	Metadata    map[string]any `json:"metadata"`
}
type Response map[string]any

var (
	CreateBooking  = connector.CallOperation[CreateBookingInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "create_booking", ContractSHA256: "928778cc82cac4f36f946f071d8b588e37eb555170f9878281aca249e29c66e1", Reliability: writeReliability()}
	EnqueueBooking = connector.EnqueueOperation[EnqueueBookingInput]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "enqueue_booking", ContractSHA256: "425b4b8fdc9a5865424285463c073951321d757d50dfbf1dc01b6f05eb507146", Reliability: writeReliability()}
	TestConnection = connector.CallOperation[struct{}, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection", ContractSHA256: "7fb4c29cab409b99b675ce439ae7c376287217e18c25d5466e6c4cbd4c45b891", Reliability: readReliability()}
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
		return nil, errors.New("Feishu Calendar transport is required")
	}
	p := &provider{transport: transport}
	create, err := connector.BindCall(CreateBooking, p.createBooking)
	if err != nil {
		return nil, err
	}
	enqueue, err := connector.BindEnqueueDelivery(EnqueueBooking, p.enqueueBooking)
	if err != nil {
		return nil, err
	}
	test, err := connector.BindCall(TestConnection, p.callTestConnection)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), create, enqueue, test)
	if err != nil {
		return nil, err
	}
	p.Adapter = bound
	return p, nil
}
func schema() connector.ProviderSchema {
	min, max := float64(1), float64(120)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "calendar_id", Name: "Calendar ID", Type: connector.ConfigFieldText}, {Key: "app_id", Name: "Feishu app ID", Type: connector.ConfigFieldText}, {Key: "base_url", Name: "Feishu API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://open.feishu.cn"`)}, {Key: "default_timezone", Name: "Default time zone", Type: connector.ConfigFieldText, Default: json.RawMessage(`"Asia/Shanghai"`)}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &min, Max: &max}}}, SecretFields: []connector.SecretField{{Key: "access_token", Name: "User or tenant access token", Required: false, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}, {Key: "app_secret", Name: "Feishu app secret", Required: false, CredentialKind: connector.SecretCredentialOAuthClientSecret, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(baseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return permanent("endpoint_invalid", "valid Feishu API endpoint is required")
	}
	if !(parsed.Scheme == "http" && isLoopback(parsed.Hostname())) && (parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), "open.feishu.cn")) {
		return permanent("endpoint_invalid", "official Feishu API endpoint or loopback HTTP is required")
	}
	return nil
}

func (p *provider) createBooking(ctx context.Context, request connector.TypedRequest[CreateBookingInput]) (connector.TypedResult[Response], error) {
	attendees := append([]Attendee(nil), request.Input.Attendees...)
	if request.Input.Attendee != nil {
		attendees = append(attendees, *request.Input.Attendee)
	}
	output, ref, err := p.createMeeting(ctx, request.Connection, request.Secrets, request.RequestRef, request.Input.Start, request.Input.End, request.Input.TimeZone, request.Input.Summary, request.Input.Description, attendees)
	return connector.TypedResult[Response]{Output: output, ResponseRef: ref}, err
}
func (p *provider) enqueueBooking(ctx context.Context, request connector.TypedRequest[EnqueueBookingInput]) (connector.DeliveryResult, error) {
	if len(request.Input.Metadata) == 0 {
		return connector.DeliveryResult{}, permanent("metadata_required", "metadata is required")
	}
	_, ref, err := p.createMeeting(ctx, request.Connection, request.Secrets, request.RequestRef, request.Input.Start, request.Input.End, request.Input.TimeZone, request.Input.Summary, request.Input.Description, request.Input.Attendees)
	return connector.DeliveryResult{ResponseRef: ref}, err
}
func (p *provider) callTestConnection(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	token, err := p.accessToken(ctx, request.Connection, request.Secrets)
	if err != nil {
		return connector.TypedResult[Response]{}, err
	}
	calendarID, err := p.calendarID(ctx, request.Connection, token)
	if err != nil {
		return connector.TypedResult[Response]{}, err
	}
	output, ref, _, err := p.execute(ctx, request.Connection, token, http.MethodGet, "/open-apis/calendar/v4/calendars/"+url.PathEscape(calendarID), nil, nil, nil, false)
	return connector.TypedResult[Response]{Output: output, ResponseRef: ref}, err
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	token, err := p.accessToken(ctx, request.Connection, request.Secrets)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	calendarID, err := p.calendarID(ctx, request.Connection, token)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	output, ref, _, err := p.execute(ctx, request.Connection, token, http.MethodGet, "/open-apis/calendar/v4/calendars/"+url.PathEscape(calendarID), nil, nil, nil, false)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, err := json.Marshal(map[string]any{"calendar": output, "response_ref": ref})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) createMeeting(ctx context.Context, connection connector.Connection, secrets map[string]string, requestRef, start, end, zone, summary, description string, attendees []Attendee) (Response, string, error) {
	startAt, endAt, err := validateRange(start, end)
	if err != nil {
		return nil, "", err
	}
	normalized, err := normalizeAttendees(attendees)
	if err != nil {
		return nil, "", err
	}
	requestRef = strings.TrimSpace(requestRef)
	if requestRef == "" {
		return nil, "", permanent("request_ref_required", "request_ref is required for Feishu idempotency")
	}
	token, err := p.accessToken(ctx, connection, secrets)
	if err != nil {
		return nil, "", err
	}
	calendarID, err := p.calendarID(ctx, connection, token)
	if err != nil {
		return nil, "", err
	}
	if zone = strings.TrimSpace(zone); zone == "" {
		zone = config(connection, "default_timezone", "Asia/Shanghai")
	}
	path := "/open-apis/calendar/v4/calendars/" + url.PathEscape(calendarID) + "/events"
	query := url.Values{"idempotency_key": {idempotencyKey(requestRef)}}
	body := map[string]any{"summary": strings.TrimSpace(summary), "description": strings.TrimSpace(description), "need_notification": true, "attendee_ability": "can_see_others", "free_busy_status": "busy", "start_time": map[string]any{"timestamp": strconv.FormatInt(startAt.Unix(), 10), "timezone": zone}, "end_time": map[string]any{"timestamp": strconv.FormatInt(endAt.Unix(), 10), "timezone": zone}, "vchat": map[string]any{"vc_type": "vc", "allow_attendees_start": true}}
	created, ref, _, err := p.execute(ctx, connection, token, http.MethodPost, path, query, body, nil, true)
	if err != nil {
		return nil, ref, err
	}
	event := nested(created, "data", "event")
	eventID := mapString(event, "event_id")
	if eventID == "" {
		return nil, ref, connector.UncertainError("feishu_calendar.event_response_invalid", errors.New("created event response lacks event_id"))
	}
	eventPath := path + "/" + url.PathEscape(eventID)
	attendeeBody := map[string]any{"attendees": normalized, "need_notification": true}
	if _, _, classification, attendeeErr := p.execute(ctx, connection, token, http.MethodPost, eventPath+"/attendees", nil, attendeeBody, nil, true); attendeeErr != nil {
		return nil, "feishu_calendar:" + eventID, p.compensatedError(ctx, connection, token, eventPath, attendeeErr, classification)
	}
	current, _, classification, getErr := p.execute(ctx, connection, token, http.MethodGet, eventPath, nil, nil, nil, false)
	if getErr != nil {
		return nil, "feishu_calendar:" + eventID, p.compensatedError(ctx, connection, token, eventPath, getErr, classification)
	}
	joinURL := mapString(nested(current, "data", "event", "vchat"), "meeting_url")
	if joinURL == "" {
		return nil, "feishu_calendar:" + eventID, p.compensatedError(ctx, connection, token, eventPath, errors.New("meeting URL is missing"), connector.ErrorPermanent)
	}
	return Response{"event_id": eventID, "join_url": joinURL, "attendees": normalized, "provider": ProviderKey}, "feishu_calendar:" + eventID, nil
}
func (p *provider) compensatedError(ctx context.Context, connection connector.Connection, token, eventPath string, cause error, classification connector.ErrorClassification) error {
	_, _, _, deleteErr := p.execute(ctx, connection, token, http.MethodDelete, eventPath, url.Values{"need_notification": {"false"}}, nil, nil, true)
	if deleteErr != nil {
		return connector.UncertainError("feishu_calendar.compensation_failed", fmt.Errorf("original failure: %v; compensation: %w", cause, deleteErr))
	}
	if classification == connector.ErrorRetryable {
		return connector.RetryableError("feishu_calendar.meeting_creation_rolled_back", cause)
	}
	return connector.PermanentError("feishu_calendar.meeting_creation_rolled_back", cause)
}

func (p *provider) accessToken(ctx context.Context, connection connector.Connection, secrets map[string]string) (string, error) {
	if token := strings.TrimSpace(secrets["access_token"]); token != "" {
		return token, nil
	}
	appID, appSecret := config(connection, "app_id", ""), strings.TrimSpace(secrets["app_secret"])
	if appID == "" || appSecret == "" {
		return "", permanent("credentials_required", "access_token or app_id plus app_secret is required")
	}
	token, err := internalfeishu.FetchTenantToken(ctx, p.transport, internalfeishu.TenantTokenRequest{Endpoint: baseURL(connection) + "/open-apis/auth/v3/tenant_access_token/internal/", AppID: appID, AppSecret: appSecret, ErrorPrefix: "feishu_calendar"})
	if err != nil {
		return "", err
	}
	return token.AccessToken, nil
}
func (p *provider) calendarID(ctx context.Context, connection connector.Connection, token string) (string, error) {
	if id := config(connection, "calendar_id", ""); id != "" {
		return id, nil
	}
	output, _, _, err := p.execute(ctx, connection, token, http.MethodPost, "/open-apis/calendar/v4/calendars/primary", url.Values{"user_id_type": {"open_id"}}, map[string]any{}, nil, false)
	if err != nil {
		return "", err
	}
	data := nested(output, "data")
	items, _ := data["calendars"].([]any)
	for _, item := range items {
		entry, _ := item.(map[string]any)
		calendar := nested(entry, "calendar")
		role := strings.ToLower(mapString(calendar, "role"))
		deleted, _ := calendar["is_deleted"].(bool)
		if deleted || (role != "owner" && role != "writer") {
			continue
		}
		if id := mapString(calendar, "calendar_id"); id != "" {
			return id, nil
		}
	}
	return "", permanent("primary_calendar_unavailable", "writable primary calendar is unavailable")
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, token, method, path string, query url.Values, body map[string]any, secretJSON map[string]string, write bool) (Response, string, connector.ErrorClassification, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", connector.ErrorPermanent, err
	}
	endpoint, err := url.Parse(baseURL(connection) + path)
	if err != nil {
		return nil, "", connector.ErrorPermanent, permanent("request_invalid", "request URL is invalid")
	}
	endpoint.RawQuery = query.Encode()
	var raw []byte
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return nil, "", connector.ErrorPermanent, permanent("request_invalid", "request body is invalid")
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json; charset=utf-8"}
	}
	secretHeaders := map[string][]string{}
	if token != "" {
		secretHeaders["Authorization"] = []string{"Bearer " + token}
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint.String(), Headers: headers, SecretHeaders: secretHeaders, SecretJSON: secretJSON, Body: raw, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		if write {
			return nil, "", connector.ErrorUncertain, connector.UncertainError("feishu_calendar.network_error", transportErr)
		}
		return nil, "", connector.ErrorRetryable, connector.RetryableError("feishu_calendar.network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	output := Response{}
	validJSON := len(response.Body) == 0 || json.Unmarshal(response.Body, &output) == nil
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		code := "feishu_calendar.http_" + strconv.Itoa(response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests {
			return output, ref, connector.ErrorRetryable, connector.RetryableError(code, cause)
		}
		if response.StatusCode == http.StatusRequestTimeout || response.StatusCode >= 500 {
			if write {
				return output, ref, connector.ErrorUncertain, connector.UncertainError(code, cause)
			}
			return output, ref, connector.ErrorRetryable, connector.RetryableError(code, cause)
		}
		return output, ref, connector.ErrorPermanent, connector.PermanentError(code, cause)
	}
	if !validJSON {
		if write {
			return nil, ref, connector.ErrorUncertain, connector.UncertainError("feishu_calendar.response_invalid", errors.New("provider response is invalid JSON"))
		}
		return nil, ref, connector.ErrorPermanent, permanent("response_invalid", "provider response is invalid JSON")
	}
	if code := intValue(output, "code"); code != 0 {
		return output, ref, connector.ErrorPermanent, connector.PermanentError("feishu_calendar.provider_code_"+strconv.Itoa(code), fmt.Errorf("provider returned code %d", code))
	}
	return output, ref, "", nil
}

func validateRange(start, end string) (time.Time, time.Time, error) {
	startAt, err := time.Parse(time.RFC3339, strings.TrimSpace(start))
	if err != nil {
		return time.Time{}, time.Time{}, permanent("start_invalid", "start must be RFC3339")
	}
	endAt, err := time.Parse(time.RFC3339, strings.TrimSpace(end))
	if err != nil || !startAt.Before(endAt) {
		return time.Time{}, time.Time{}, permanent("end_invalid", "end must be RFC3339 and after start")
	}
	return startAt, endAt, nil
}
func normalizeAttendees(input []Attendee) ([]any, error) {
	if len(input) == 0 {
		return nil, permanent("attendees_required", "at least one attendee is required")
	}
	output := make([]any, 0, len(input))
	for _, attendee := range input {
		if email := strings.TrimSpace(attendee.Email); email != "" {
			output = append(output, map[string]any{"type": "third_party", "third_party_email": email})
			continue
		}
		if id := strings.TrimSpace(attendee.UserID); id != "" {
			output = append(output, map[string]any{"type": "user", "user_id": id})
			continue
		}
		return nil, permanent("attendee_invalid", "attendee email or user_id is required")
	}
	return output, nil
}
func idempotencyKey(value string) string {
	value = strings.TrimSpace(value)
	if len(value) >= 32 && len(value) <= 128 {
		return value
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
func nested(input map[string]any, keys ...string) map[string]any {
	current := input
	for _, key := range keys {
		next, _ := current[key].(map[string]any)
		if next == nil {
			return map[string]any{}
		}
		current = next
	}
	return current
}
func intValue(values map[string]any, key string) int {
	switch value := values[key].(type) {
	case float64:
		return int(value)
	case int:
		return value
	case json.Number:
		parsed, _ := strconv.Atoi(string(value))
		return parsed
	}
	return 0
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
	value := strings.TrimSpace(fmt.Sprint(values[key]))
	if value == "<nil>" {
		return ""
	}
	return value
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
func permanent(code, message string) error {
	return connector.PermanentError("feishu_calendar."+code, errors.New(message))
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
