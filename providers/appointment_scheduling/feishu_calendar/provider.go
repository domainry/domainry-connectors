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
	calendarList, err := connector.BindCall(CalendarList, p.calendarList)
	if err != nil {
		return nil, err
	}
	calendarEvents, err := connector.BindCall(CalendarEvents, p.calendarEvents)
	if err != nil {
		return nil, err
	}
	calendarEvent, err := connector.BindCall(CalendarEvent, p.calendarEvent)
	if err != nil {
		return nil, err
	}
	calendarAvailability, err := connector.BindCall(CalendarAvailability, p.calendarAvailability)
	if err != nil {
		return nil, err
	}
	meetingContent, err := connector.BindCall(FetchMeetingContent, p.fetchMeetingContent)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), create, enqueue, test, calendarList, calendarEvents, calendarEvent, calendarAvailability, meetingContent)
	if err != nil {
		return nil, err
	}
	p.Adapter = bound
	return p, nil
}
func schema() connector.ProviderSchema {
	min, max := float64(1), float64(120)
	zero, historyMax, futureMax := float64(0), float64(90), float64(92)
	reconcileMin, reconcileMax := float64(300), float64(86400)
	secret := func(key, name string, kind connector.SecretCredentialKind, rotation connector.SecretRotationPolicy) connector.SecretField {
		return connector.SecretField{Key: key, Name: name, CredentialKind: kind, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: rotation, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}
	}
	return connector.ProviderSchema{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "2.3.0",
		ConfigFields: []connector.ConfigField{
			{Key: "calendar_id", Name: "Calendar ID", Type: connector.ConfigFieldText},
			{Key: "app_id", Name: "Feishu app ID", Type: connector.ConfigFieldText},
			{Key: "base_url", Name: "Feishu API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://open.feishu.cn"`)},
			{Key: "oauth_base_url", Name: "Feishu OAuth base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://accounts.feishu.cn"`)},
			{Key: "default_timezone", Name: "Default time zone", Type: connector.ConfigFieldText, Default: json.RawMessage(`"Asia/Shanghai"`)},
			{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &min, Max: &max}},
			{Key: "calendar_sync_enabled", Name: "Enable calendar sync", Type: connector.ConfigFieldBoolean, Default: json.RawMessage(`false`)},
			{Key: "calendar_subscription_enabled", Name: "Enable calendar event subscription", Type: connector.ConfigFieldBoolean, Default: json.RawMessage(`true`)},
			{Key: "calendar_history_days", Name: "Calendar history days", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &zero, Max: &historyMax}},
			{Key: "calendar_future_days", Name: "Calendar future days", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`62`), Validation: connector.ConfigValidation{Min: &min, Max: &futureMax}},
			{Key: "calendar_reconcile_seconds", Name: "Calendar reconciliation interval", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`900`), Validation: connector.ConfigValidation{Min: &reconcileMin, Max: &reconcileMax}},
		},
		SecretFields: []connector.SecretField{
			secret("access_token", "User or tenant access token", connector.SecretCredentialBearerToken, connector.SecretRotationOAuthRefresh),
			secret("refresh_token", "User refresh token", connector.SecretCredentialRefreshToken, connector.SecretRotationOAuthRefresh),
			secret("client_id", "OAuth client ID", connector.SecretCredentialIdentifier, connector.SecretRotationManual),
			secret("client_secret", "OAuth client secret", connector.SecretCredentialOAuthClientSecret, connector.SecretRotationManual),
			secret("app_secret", "Feishu tenant app secret", connector.SecretCredentialOAuthClientSecret, connector.SecretRotationManual),
			secret("encrypt_key", "Feishu event encrypt key", connector.SecretCredentialGeneric, connector.SecretRotationManual),
			secret("verification_token", "Feishu event verification token", connector.SecretCredentialSigningSecret, connector.SecretRotationManual),
		},
	}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	for _, endpoint := range []struct {
		value, host string
	}{{baseURL(connection), "open.feishu.cn"}, {oauthBaseURL(connection), "accounts.feishu.cn"}} {
		parsed, err := url.Parse(endpoint.value)
		if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" || parsed.RawQuery != "" || (!(parsed.Scheme == "http" && isLoopback(parsed.Hostname())) && (parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), endpoint.host))) {
			return permanent("endpoint_invalid", "official Feishu endpoints or loopback HTTP are required")
		}
	}
	historyDays := feishuConfigInt(connection.Config, "calendar_history_days", 30)
	futureDays := feishuConfigInt(connection.Config, "calendar_future_days", 62)
	reconcileSeconds := feishuConfigInt(connection.Config, "calendar_reconcile_seconds", 900)
	if historyDays < 0 || historyDays > 90 || futureDays < 1 || futureDays > 92 || historyDays+futureDays > 92 {
		return permanent("sync_window_invalid", "Feishu Calendar history and future window must total no more than 92 days")
	}
	if reconcileSeconds < 300 || reconcileSeconds > 86400 {
		return permanent("reconcile_interval_invalid", "Feishu Calendar reconciliation interval must be between 300 and 86400 seconds")
	}
	return nil
}

func (p *provider) createBooking(ctx context.Context, request connector.TypedRequest[CreateBookingInput]) (connector.TypedResult[Response], error) {
	attendees := append([]Attendee(nil), request.Input.Attendees...)
	if request.Input.Attendee != nil {
		attendees = append(attendees, *request.Input.Attendee)
	}
	output, ref, updates, err := p.createMeeting(ctx, request.Connection, request.Secrets, request.RequestRef, request.Input.Start, request.Input.End, request.Input.TimeZone, request.Input.Summary, request.Input.Description, attendees)
	return connector.TypedResult[Response]{Output: output, ResponseRef: ref, SecretUpdates: updates}, err
}
func (p *provider) enqueueBooking(ctx context.Context, request connector.TypedRequest[EnqueueBookingInput]) (connector.DeliveryResult, error) {
	if len(request.Input.Metadata) == 0 {
		return connector.DeliveryResult{}, permanent("metadata_required", "metadata is required")
	}
	_, ref, updates, err := p.createMeeting(ctx, request.Connection, request.Secrets, request.RequestRef, request.Input.Start, request.Input.End, request.Input.TimeZone, request.Input.Summary, request.Input.Description, request.Input.Attendees)
	return connector.DeliveryResult{ResponseRef: ref, SecretUpdates: updates}, err
}
func (p *provider) callTestConnection(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	session := p.newAPISession(request.Connection, request.Secrets)
	calendarID, err := p.calendarID(ctx, session)
	if err != nil {
		return connector.TypedResult[Response]{SecretUpdates: cloneStringMap(session.updates)}, err
	}
	return session.call(ctx, http.MethodGet, "/open-apis/calendar/v4/calendars/"+url.PathEscape(calendarID), nil, nil, false)
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	session := p.newAPISession(request.Connection, request.Secrets)
	calendarID, err := p.calendarID(ctx, session)
	if err != nil {
		return connector.TestConnectionResult{SecretUpdates: cloneStringMap(session.updates)}, err
	}
	calendarResult, err := session.call(ctx, http.MethodGet, "/open-apis/calendar/v4/calendars/"+url.PathEscape(calendarID), nil, nil, false)
	if err != nil {
		return connector.TestConnectionResult{SecretUpdates: cloneStringMap(session.updates)}, err
	}
	detail := map[string]any{"calendar": calendarResult.Output, "response_ref": calendarResult.ResponseRef}
	if strings.TrimSpace(request.Secrets["refresh_token"]) != "" {
		userResult, userErr := session.call(ctx, http.MethodGet, "/open-apis/authen/v1/user_info", nil, nil, false)
		if userErr != nil {
			return connector.TestConnectionResult{SecretUpdates: cloneStringMap(session.updates)}, userErr
		}
		user := nested(userResult.Output, "data")
		openID, unionID := mapString(user, "open_id"), mapString(user, "union_id")
		subject := unionID
		if subject == "" {
			subject = openID
		}
		if subject == "" || openID == "" {
			return connector.TestConnectionResult{SecretUpdates: cloneStringMap(session.updates)}, permanent("provider_account_invalid", "Feishu user profile lacks a stable identity")
		}
		routes := []map[string]string{{"kind": "open_id", "value": openID}}
		if unionID != "" && unionID != openID {
			routes = append(routes, map[string]string{"kind": "union_id", "value": unionID})
		}
		if userID := strings.TrimSpace(mapString(user, "user_id")); userID != "" {
			routes = append(routes, map[string]string{"kind": "user_id", "value": userID})
		}
		if email := strings.ToLower(strings.TrimSpace(mapString(user, "email"))); email != "" {
			routes = append(routes, map[string]string{"kind": "email", "value": email})
		}
		detail["provider_account"] = map[string]any{"subject": subject, "routes": routes}
		detail["user"] = user
	}
	details, err := json.Marshal(detail)
	if err != nil {
		return connector.TestConnectionResult{SecretUpdates: cloneStringMap(session.updates)}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details, SecretUpdates: cloneStringMap(session.updates)}, nil
}

func (p *provider) createMeeting(ctx context.Context, connection connector.Connection, secrets map[string]string, requestRef, start, end, zone, summary, description string, attendees []Attendee) (Response, string, map[string]string, error) {
	startAt, endAt, err := validateRange(start, end)
	if err != nil {
		return nil, "", nil, err
	}
	normalized, err := normalizeAttendees(attendees)
	if err != nil {
		return nil, "", nil, err
	}
	requestRef = strings.TrimSpace(requestRef)
	if requestRef == "" {
		return nil, "", nil, permanent("request_ref_required", "request_ref is required for Feishu idempotency")
	}
	session := p.newAPISession(connection, secrets)
	calendarID, err := p.calendarID(ctx, session)
	if err != nil {
		return nil, "", cloneStringMap(session.updates), err
	}
	if zone = strings.TrimSpace(zone); zone == "" {
		zone = config(connection, "default_timezone", "Asia/Shanghai")
	}
	path := "/open-apis/calendar/v4/calendars/" + url.PathEscape(calendarID) + "/events"
	query := url.Values{"idempotency_key": {idempotencyKey(requestRef)}}
	body := map[string]any{"summary": strings.TrimSpace(summary), "description": strings.TrimSpace(description), "need_notification": true, "attendee_ability": "can_see_others", "free_busy_status": "busy", "start_time": map[string]any{"timestamp": strconv.FormatInt(startAt.Unix(), 10), "timezone": zone}, "end_time": map[string]any{"timestamp": strconv.FormatInt(endAt.Unix(), 10), "timezone": zone}, "vchat": map[string]any{"vc_type": "vc", "allow_attendees_start": true}}
	createdResult, err := session.call(ctx, http.MethodPost, path, query, body, true)
	created, ref := createdResult.Output, createdResult.ResponseRef
	if err != nil {
		return nil, ref, cloneStringMap(session.updates), err
	}
	event := nested(created, "data", "event")
	eventID := mapString(event, "event_id")
	if eventID == "" {
		return nil, ref, cloneStringMap(session.updates), connector.UncertainError("feishu_calendar.event_response_invalid", errors.New("created event response lacks event_id"))
	}
	eventPath := path + "/" + url.PathEscape(eventID)
	attendeeBody := map[string]any{"attendees": normalized, "need_notification": true}
	if _, attendeeErr := session.call(ctx, http.MethodPost, eventPath+"/attendees", nil, attendeeBody, true); attendeeErr != nil {
		classification, _ := connector.ErrorClassificationOf(attendeeErr)
		compensated := p.compensatedError(ctx, session, eventPath, attendeeErr, classification)
		return nil, "feishu_calendar:" + eventID, cloneStringMap(session.updates), compensated
	}
	currentResult, getErr := session.call(ctx, http.MethodGet, eventPath, nil, nil, false)
	current := currentResult.Output
	if getErr != nil {
		classification, _ := connector.ErrorClassificationOf(getErr)
		compensated := p.compensatedError(ctx, session, eventPath, getErr, classification)
		return nil, "feishu_calendar:" + eventID, cloneStringMap(session.updates), compensated
	}
	joinURL := mapString(nested(current, "data", "event", "vchat"), "meeting_url")
	if joinURL == "" {
		compensated := p.compensatedError(ctx, session, eventPath, errors.New("meeting URL is missing"), connector.ErrorPermanent)
		return nil, "feishu_calendar:" + eventID, cloneStringMap(session.updates), compensated
	}
	return Response{"event_id": eventID, "join_url": joinURL, "attendees": normalized, "provider": ProviderKey}, "feishu_calendar:" + eventID, cloneStringMap(session.updates), nil
}
func (p *provider) compensatedError(ctx context.Context, session *apiSession, eventPath string, cause error, classification connector.ErrorClassification) error {
	_, deleteErr := session.call(ctx, http.MethodDelete, eventPath, url.Values{"need_notification": {"false"}}, nil, true)
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
func (p *provider) calendarID(ctx context.Context, session *apiSession) (string, error) {
	if id := config(session.connection, "calendar_id", ""); id != "" {
		return id, nil
	}
	result, err := session.call(ctx, http.MethodPost, "/open-apis/calendar/v4/calendars/primary", url.Values{"user_id_type": {"open_id"}}, map[string]any{}, false)
	if err != nil {
		return "", err
	}
	output := result.Output
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
