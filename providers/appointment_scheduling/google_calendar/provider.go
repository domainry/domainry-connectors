// Package googlecalendar implements the official Google Calendar Provider.
package googlecalendar

import (
	"context"
	"crypto/hmac"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	internaloauth2 "github.com/domainry/domainry-connectors/internal/oauth2"
)

const (
	ConnectorKey          = "appointment_scheduling"
	ProviderKey           = "google_calendar"
	defaultBaseURL        = "https://www.googleapis.com"
	defaultTokenURL       = "https://oauth2.googleapis.com/token"
	responseLimit   int64 = 4 << 20
)

type ListEventsInput struct {
	PageToken    string `json:"pageToken,omitempty"`
	SyncToken    string `json:"syncToken,omitempty"`
	TimeMin      string `json:"timeMin,omitempty"`
	TimeMax      string `json:"timeMax,omitempty"`
	UpdatedMin   string `json:"updatedMin,omitempty"`
	Query        string `json:"q,omitempty"`
	MaxResults   int    `json:"maxResults,omitempty"`
	SingleEvents *bool  `json:"singleEvents,omitempty"`
	ShowDeleted  *bool  `json:"showDeleted,omitempty"`
}
type CreateBookingInput struct {
	Start               string           `json:"start"`
	End                 string           `json:"end"`
	TimeZone            string           `json:"timeZone,omitempty"`
	Attendee            map[string]any   `json:"attendee,omitempty"`
	Attendees           []map[string]any `json:"attendees,omitempty"`
	Summary             string           `json:"summary,omitempty"`
	Description         string           `json:"description,omitempty"`
	Location            string           `json:"location,omitempty"`
	CreateOnlineMeeting bool             `json:"createOnlineMeeting,omitempty"`
	ConferenceRequestID string           `json:"conferenceRequestId,omitempty"`
	SendUpdates         string           `json:"sendUpdates,omitempty"`
}
type EnqueueBookingInput struct {
	Start               string           `json:"start"`
	End                 string           `json:"end"`
	TimeZone            string           `json:"time_zone,omitempty"`
	Attendees           []map[string]any `json:"attendees"`
	Summary             string           `json:"summary,omitempty"`
	Description         string           `json:"description,omitempty"`
	Metadata            map[string]any   `json:"metadata"`
	CreateOnlineMeeting bool             `json:"create_online_meeting,omitempty"`
	ConferenceRequestID string           `json:"conference_request_id,omitempty"`
}
type CancelEventInput struct {
	EventUUID   string `json:"event_uuid"`
	SendUpdates string `json:"sendUpdates,omitempty"`
}
type RescheduleEventInput struct {
	EventUUID   string `json:"event_uuid"`
	Start       string `json:"start"`
	End         string `json:"end"`
	TimeZone    string `json:"timeZone,omitempty"`
	SendUpdates string `json:"sendUpdates,omitempty"`
}
type WatchEventsInput struct {
	ChannelID  string `json:"channel_id"`
	Address    string `json:"address"`
	Expiration int64  `json:"expiration,omitempty"`
}
type Response map[string]any

var (
	ListScheduledEvents      = connector.CallOperation[ListEventsInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_scheduled_events", ContractSHA256: "03718b1dd1f9c62875e87ab34f8c29d7b15ae3ec62aebe8d10ddf1748a8a6e94", Reliability: readReliability()}
	CreateBooking            = connector.CallOperation[CreateBookingInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "create_booking", ContractSHA256: "928778cc82cac4f36f946f071d8b588e37eb555170f9878281aca249e29c66e1", Reliability: writeReliability()}
	EnqueueBooking           = connector.EnqueueOperation[EnqueueBookingInput]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "enqueue_booking", ContractSHA256: "425b4b8fdc9a5865424285463c073951321d757d50dfbf1dc01b6f05eb507146", Reliability: writeReliability()}
	CancelScheduledEvent     = connector.CallOperation[CancelEventInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "cancel_scheduled_event", ContractSHA256: "09d521d641dcd334b09676d0c9d2f8bc1771ed648206cef4c5fdc231d32fa082", Reliability: writeReliability()}
	RescheduleScheduledEvent = connector.CallOperation[RescheduleEventInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "reschedule_scheduled_event", ContractSHA256: "4a086743e91ae464065a1075fa4493efeb5c94fda5d60b1446bd1033b81a0b76", Reliability: writeReliability()}
	WatchScheduledEvents     = connector.CallOperation[WatchEventsInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "watch_scheduled_events", ContractSHA256: "19cd60e96e5ac3aa0b44eeb5a6329d19cf2416233256f9ffd1f5b0ee943ce25a", Reliability: writeReliability()}
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
		return nil, errors.New("Google Calendar transport is required")
	}
	p := &provider{transport: transport}
	operations := make([]connector.BoundOperation, 0, 7)
	for _, bind := range []func() (connector.BoundOperation, error){func() (connector.BoundOperation, error) { return connector.BindCall(ListScheduledEvents, p.listEvents) }, func() (connector.BoundOperation, error) { return connector.BindCall(CreateBooking, p.createBooking) }, func() (connector.BoundOperation, error) {
		return connector.BindEnqueueDelivery(EnqueueBooking, p.enqueueBooking)
	}, func() (connector.BoundOperation, error) {
		return connector.BindCall(CancelScheduledEvent, p.cancelEvent)
	}, func() (connector.BoundOperation, error) {
		return connector.BindCall(RescheduleScheduledEvent, p.rescheduleEvent)
	}, func() (connector.BoundOperation, error) {
		return connector.BindCall(WatchScheduledEvents, p.watchEvents)
	}, func() (connector.BoundOperation, error) {
		return connector.BindCall(TestConnection, p.callTestConnection)
	}} {
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
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "calendar_id", Name: "Calendar ID", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"primary"`)}, {Key: "base_url", Name: "Google Calendar API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://www.googleapis.com"`)}, {Key: "token_url", Name: "Google OAuth token URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://oauth2.googleapis.com/token"`)}, {Key: "default_timezone", Name: "Default time zone", Type: connector.ConfigFieldText, Default: json.RawMessage(`"UTC"`)}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &min, Max: &max}}}, SecretFields: []connector.SecretField{{Key: "access_token", Name: "OAuth access token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationOAuthRefresh, ExpiryPolicy: connector.SecretExpiryRequired, TestRequirement: connector.SecretTestWhenBound}, {Key: "refresh_token", Name: "OAuth refresh token", Required: false, CredentialKind: connector.SecretCredentialRefreshToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationOAuthRefresh, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}, {Key: "client_id", Name: "OAuth client ID", Required: false, CredentialKind: connector.SecretCredentialIdentifier, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}, {Key: "client_secret", Name: "OAuth client secret", Required: false, CredentialKind: connector.SecretCredentialOAuthClientSecret, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}, {Key: "channel_token", Name: "Push channel verification token", Required: false, CredentialKind: connector.SecretCredentialSigningSecret, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	if strings.TrimSpace(config(connection, "calendar_id", "")) == "" {
		return permanent("calendar_id_required", "calendar_id is required")
	}
	for _, item := range []struct{ value, host string }{{baseURL(connection), "www.googleapis.com"}, {tokenURL(connection), "oauth2.googleapis.com"}} {
		parsed, err := url.Parse(item.value)
		if err != nil || parsed.Host == "" || parsed.User != nil {
			return permanent("endpoint_invalid", "valid Google endpoint is required")
		}
		if parsed.Scheme == "http" && isLoopback(parsed.Hostname()) {
			continue
		}
		if parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), item.host) {
			return permanent("endpoint_invalid", "official Google endpoint or loopback HTTP is required")
		}
	}
	return nil
}

func (p *provider) listEvents(ctx context.Context, request connector.TypedRequest[ListEventsInput]) (connector.TypedResult[Response], error) {
	i := request.Input
	if i.MaxResults < 0 || i.MaxResults > 2500 {
		return connector.TypedResult[Response]{}, permanent("max_results_invalid", "maxResults must be between 1 and 2500 when set")
	}
	if i.SyncToken != "" && (i.TimeMin != "" || i.TimeMax != "" || i.UpdatedMin != "" || i.Query != "") {
		return connector.TypedResult[Response]{}, permanent("sync_token_filters_invalid", "syncToken cannot be combined with time or query filters")
	}
	for key, value := range map[string]string{"timeMin": i.TimeMin, "timeMax": i.TimeMax, "updatedMin": i.UpdatedMin} {
		if value != "" {
			if _, err := time.Parse(time.RFC3339, value); err != nil {
				return connector.TypedResult[Response]{}, permanent("time_filter_invalid", key+" must be RFC3339")
			}
		}
	}
	q := url.Values{}
	set(q, "pageToken", i.PageToken)
	set(q, "syncToken", i.SyncToken)
	set(q, "timeMin", i.TimeMin)
	set(q, "timeMax", i.TimeMax)
	set(q, "updatedMin", i.UpdatedMin)
	set(q, "q", i.Query)
	if i.MaxResults > 0 {
		q.Set("maxResults", strconv.Itoa(i.MaxResults))
	}
	if i.SingleEvents != nil {
		q.Set("singleEvents", strconv.FormatBool(*i.SingleEvents))
	}
	if i.ShowDeleted != nil {
		q.Set("showDeleted", strconv.FormatBool(*i.ShowDeleted))
	}
	output, ref, updates, _, err := p.executeWithRefresh(ctx, request.Connection, request.Secrets, http.MethodGet, eventPath(request.Connection), q, nil, nil, false)
	return connector.TypedResult[Response]{Output: output, ResponseRef: ref, SecretUpdates: updates}, err
}
func (p *provider) createBooking(ctx context.Context, request connector.TypedRequest[CreateBookingInput]) (connector.TypedResult[Response], error) {
	body, q, err := bookingBody(request.Connection, request.Input.Start, request.Input.End, request.Input.TimeZone, request.Input.Summary, request.Input.Description, request.Input.Location, request.Input.Attendee, request.Input.Attendees, request.Input.CreateOnlineMeeting, request.Input.ConferenceRequestID, request.RequestRef, request.Input.SendUpdates)
	if err != nil {
		return connector.TypedResult[Response]{}, err
	}
	output, ref, updates, _, err := p.executeWithRefresh(ctx, request.Connection, request.Secrets, http.MethodPost, eventPath(request.Connection), q, body, nil, true)
	return connector.TypedResult[Response]{Output: output, ResponseRef: ref, SecretUpdates: updates}, err
}
func (p *provider) enqueueBooking(ctx context.Context, request connector.TypedRequest[EnqueueBookingInput]) (connector.DeliveryResult, error) {
	i := request.Input
	if len(i.Attendees) == 0 || len(i.Metadata) == 0 {
		return connector.DeliveryResult{}, permanent("enqueue_fields_required", "attendees and metadata are required")
	}
	body, q, err := bookingBody(request.Connection, i.Start, i.End, i.TimeZone, i.Summary, i.Description, "", nil, i.Attendees, i.CreateOnlineMeeting, i.ConferenceRequestID, request.RequestRef, "all")
	if err != nil {
		return connector.DeliveryResult{}, err
	}
	body["extendedProperties"] = map[string]any{"private": stringMap(i.Metadata)}
	_, ref, updates, _, err := p.executeWithRefresh(ctx, request.Connection, request.Secrets, http.MethodPost, eventPath(request.Connection), q, body, nil, true)
	return connector.DeliveryResult{ResponseRef: ref, SecretUpdates: updates}, err
}
func (p *provider) cancelEvent(ctx context.Context, request connector.TypedRequest[CancelEventInput]) (connector.TypedResult[Response], error) {
	id := strings.TrimSpace(request.Input.EventUUID)
	if id == "" {
		return connector.TypedResult[Response]{}, permanent("event_uuid_required", "event_uuid is required")
	}
	q, err := sendUpdatesQuery(request.Input.SendUpdates)
	if err != nil {
		return connector.TypedResult[Response]{}, err
	}
	output, ref, updates, _, err := p.executeWithRefresh(ctx, request.Connection, request.Secrets, http.MethodDelete, eventPath(request.Connection)+"/"+url.PathEscape(id), q, nil, nil, true)
	return connector.TypedResult[Response]{Output: output, ResponseRef: ref, SecretUpdates: updates}, err
}
func (p *provider) rescheduleEvent(ctx context.Context, request connector.TypedRequest[RescheduleEventInput]) (connector.TypedResult[Response], error) {
	i := request.Input
	id := strings.TrimSpace(i.EventUUID)
	if id == "" {
		return connector.TypedResult[Response]{}, permanent("event_uuid_required", "event_uuid is required")
	}
	if err := validateRange(i.Start, i.End); err != nil {
		return connector.TypedResult[Response]{}, err
	}
	q, err := sendUpdatesQuery(i.SendUpdates)
	if err != nil {
		return connector.TypedResult[Response]{}, err
	}
	zone := strings.TrimSpace(i.TimeZone)
	if zone == "" {
		zone = config(request.Connection, "default_timezone", "UTC")
	}
	body := map[string]any{"start": map[string]any{"dateTime": i.Start, "timeZone": zone}, "end": map[string]any{"dateTime": i.End, "timeZone": zone}}
	output, ref, updates, _, err := p.executeWithRefresh(ctx, request.Connection, request.Secrets, http.MethodPatch, eventPath(request.Connection)+"/"+url.PathEscape(id), q, body, nil, true)
	return connector.TypedResult[Response]{Output: output, ResponseRef: ref, SecretUpdates: updates}, err
}
func (p *provider) watchEvents(ctx context.Context, request connector.TypedRequest[WatchEventsInput]) (connector.TypedResult[Response], error) {
	i := request.Input
	if strings.TrimSpace(i.ChannelID) == "" || strings.TrimSpace(i.Address) == "" {
		return connector.TypedResult[Response]{}, permanent("watch_fields_required", "channel_id and address are required")
	}
	address, err := url.Parse(i.Address)
	if err != nil || address.Scheme != "https" || address.Host == "" {
		return connector.TypedResult[Response]{}, permanent("watch_address_invalid", "watch address must be an absolute HTTPS URL")
	}
	token := strings.TrimSpace(request.Secrets["channel_token"])
	if token == "" {
		return connector.TypedResult[Response]{}, permanent("channel_token_required", "resolved channel token is required")
	}
	body := map[string]any{"id": i.ChannelID, "type": "web_hook", "address": i.Address}
	if i.Expiration > 0 {
		body["expiration"] = i.Expiration
	}
	output, ref, updates, _, err := p.executeWithRefresh(ctx, request.Connection, request.Secrets, http.MethodPost, eventPath(request.Connection)+"/watch", nil, body, map[string]string{"token": token}, true)
	return connector.TypedResult[Response]{Output: output, ResponseRef: ref, SecretUpdates: updates}, err
}
func (p *provider) callTestConnection(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	output, ref, updates, _, err := p.executeWithRefresh(ctx, request.Connection, request.Secrets, http.MethodGet, "/calendar/v3/calendars/"+url.PathEscape(config(request.Connection, "calendar_id", "primary")), nil, nil, nil, false)
	return connector.TypedResult[Response]{Output: output, ResponseRef: ref, SecretUpdates: updates}, err
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	output, ref, updates, _, err := p.executeWithRefresh(ctx, request.Connection, request.Secrets, http.MethodGet, "/calendar/v3/calendars/"+url.PathEscape(config(request.Connection, "calendar_id", "primary")), nil, nil, nil, false)
	if err != nil {
		return connector.TestConnectionResult{SecretUpdates: updates}, err
	}
	details, err := json.Marshal(map[string]any{"calendar": output, "response_ref": ref})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details, SecretUpdates: updates}, nil
}

func (p *provider) executeWithRefresh(ctx context.Context, connection connector.Connection, secrets map[string]string, method, path string, query url.Values, body map[string]any, secretJSON map[string]string, write bool) (Response, string, map[string]string, int, error) {
	token := strings.TrimSpace(secrets["access_token"])
	if token == "" {
		return nil, "", nil, 0, permanent("access_token_required", "resolved access token is required")
	}
	output, ref, status, err := p.execute(ctx, connection, token, method, path, query, body, secretJSON, write)
	if status != http.StatusUnauthorized {
		return output, ref, nil, status, err
	}
	refresh := strings.TrimSpace(secrets["refresh_token"])
	clientID := strings.TrimSpace(secrets["client_id"])
	if refresh == "" || clientID == "" {
		return output, ref, nil, status, err
	}
	updated, refreshErr := internaloauth2.Refresh(ctx, p.transport, internaloauth2.RefreshRequest{Endpoint: tokenURL(connection), RefreshToken: refresh, ClientID: clientID, ClientSecret: strings.TrimSpace(secrets["client_secret"]), ClientSecretOptional: true, ClientAuthentication: internaloauth2.ClientAuthenticationForm, ErrorPrefix: "google_calendar.oauth"})
	if refreshErr != nil {
		return nil, "oauth:refresh_failed", nil, status, refreshErr
	}
	updates := map[string]string{"access_token": updated.AccessToken}
	if updated.RefreshToken != "" {
		updates["refresh_token"] = updated.RefreshToken
	}
	output, ref, status, err = p.execute(ctx, connection, updated.AccessToken, method, path, query, body, secretJSON, write)
	return output, ref, updates, status, err
}
func (p *provider) execute(ctx context.Context, connection connector.Connection, token, method, path string, query url.Values, body map[string]any, secretJSON map[string]string, write bool) (Response, string, int, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", 0, err
	}
	endpoint, err := url.Parse(baseURL(connection) + path)
	if err != nil {
		return nil, "", 0, permanent("request_invalid", "request URL is invalid")
	}
	endpoint.RawQuery = query.Encode()
	var raw []byte
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return nil, "", 0, permanent("request_invalid", "request body is invalid")
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint.String(), Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, SecretJSON: secretJSON, Body: raw, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		if write {
			return nil, "", 0, connector.UncertainError("google_calendar.network_error", transportErr)
		}
		return nil, "", 0, connector.RetryableError("google_calendar.network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	output := Response{}
	validJSON := len(response.Body) == 0 || json.Unmarshal(response.Body, &output) == nil
	if id := mapString(output, "id"); id != "" {
		ref = "google_calendar:" + id
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		code := "google_calendar.http_" + strconv.Itoa(response.StatusCode)
		if response.StatusCode == http.StatusUnauthorized {
			return output, ref, response.StatusCode, connector.PermanentError(code, cause)
		}
		if response.StatusCode == http.StatusTooManyRequests {
			return output, ref, response.StatusCode, connector.RetryableError(code, cause)
		}
		if response.StatusCode == http.StatusRequestTimeout || response.StatusCode >= 500 {
			if write {
				return output, ref, response.StatusCode, connector.UncertainError(code, cause)
			}
			return output, ref, response.StatusCode, connector.RetryableError(code, cause)
		}
		return output, ref, response.StatusCode, connector.PermanentError(code, cause)
	}
	if !validJSON {
		if write {
			return nil, ref, response.StatusCode, connector.UncertainError("google_calendar.response_invalid", errors.New("provider response is invalid JSON"))
		}
		return nil, ref, response.StatusCode, permanent("response_invalid", "provider response is invalid JSON")
	}
	return output, ref, response.StatusCode, nil
}

func (p *provider) VerifyWebhook(ctx context.Context, request connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	if err := ctx.Err(); err != nil {
		return connector.VerifiedWebhook{}, err
	}
	token := strings.TrimSpace(request.Secrets["channel_token"])
	if token == "" {
		return connector.VerifiedWebhook{}, permanent("channel_token_required", "resolved channel token is required")
	}
	if !hmac.Equal([]byte(token), []byte(header(request.Headers, "X-Goog-Channel-Token"))) {
		return connector.VerifiedWebhook{}, permanent("webhook_token_invalid", "push channel token does not match")
	}
	channel, message, resource, state := header(request.Headers, "X-Goog-Channel-ID"), header(request.Headers, "X-Goog-Message-Number"), header(request.Headers, "X-Goog-Resource-ID"), header(request.Headers, "X-Goog-Resource-State")
	if channel == "" || message == "" || resource == "" || state == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_identity_missing", "Google push identity headers are required")
	}
	payload, _ := json.Marshal(map[string]any{"channel_id": channel, "message_number": message, "resource_id": resource, "resource_state": state, "resource_uri": header(request.Headers, "X-Goog-Resource-URI")})
	return connector.VerifiedWebhook{EventType: "calendar_events." + strings.ToLower(state), ExternalID: channel + ":" + message, Payload: payload, Security: &connector.WebhookSecurityEvidence{SignatureVerified: true, DeviceIdentity: resource}}, nil
}

func bookingBody(connection connector.Connection, start, end, zone, summary, description, location string, attendee map[string]any, attendees []map[string]any, online bool, conferenceID, requestRef, sendUpdates string) (map[string]any, url.Values, error) {
	if err := validateRange(start, end); err != nil {
		return nil, nil, err
	}
	if zone = strings.TrimSpace(zone); zone == "" {
		zone = config(connection, "default_timezone", "UTC")
	}
	body := map[string]any{"start": map[string]any{"dateTime": start, "timeZone": zone}, "end": map[string]any{"dateTime": end, "timeZone": zone}}
	setAny(body, "summary", summary)
	setAny(body, "description", description)
	setAny(body, "location", location)
	if len(attendees) > 0 {
		body["attendees"] = attendees
	} else if attendee != nil {
		body["attendees"] = []map[string]any{attendee}
	}
	q, err := sendUpdatesQuery(sendUpdates)
	if err != nil {
		return nil, nil, err
	}
	if online {
		if conferenceID = strings.TrimSpace(conferenceID); conferenceID == "" {
			conferenceID = strings.TrimSpace(requestRef)
		}
		if conferenceID == "" {
			return nil, nil, permanent("conference_request_id_required", "conference request ID is required")
		}
		body["conferenceData"] = map[string]any{"createRequest": map[string]any{"requestId": conferenceID, "conferenceSolutionKey": map[string]any{"type": "hangoutsMeet"}}}
		q.Set("conferenceDataVersion", "1")
	}
	return body, q, nil
}
func sendUpdatesQuery(value string) (url.Values, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		value = "all"
	}
	if value != "all" && value != "externalOnly" && value != "none" {
		return nil, permanent("send_updates_invalid", "sendUpdates must be all, externalOnly, or none")
	}
	return url.Values{"sendUpdates": {value}}, nil
}
func validateRange(start, end string) error {
	startTime, err := time.Parse(time.RFC3339, strings.TrimSpace(start))
	if err != nil {
		return permanent("start_invalid", "start must be RFC3339")
	}
	endTime, err := time.Parse(time.RFC3339, strings.TrimSpace(end))
	if err != nil {
		return permanent("end_invalid", "end must be RFC3339")
	}
	if !startTime.Before(endTime) {
		return permanent("time_range_invalid", "start must be before end")
	}
	return nil
}
func stringMap(values map[string]any) map[string]string {
	result := map[string]string{}
	for key, value := range values {
		result[key] = fmt.Sprint(value)
	}
	return result
}
func eventPath(connection connector.Connection) string {
	return "/calendar/v3/calendars/" + url.PathEscape(config(connection, "calendar_id", "primary")) + "/events"
}
func baseURL(connection connector.Connection) string {
	if value := strings.TrimRight(config(connection, "base_url", ""), "/"); value != "" {
		return value
	}
	return defaultBaseURL
}
func tokenURL(connection connector.Connection) string {
	return config(connection, "token_url", defaultTokenURL)
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
	return connector.PermanentError("google_calendar."+code, errors.New(message))
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
var _ connector.WebhookVerifier = (*provider)(nil)
