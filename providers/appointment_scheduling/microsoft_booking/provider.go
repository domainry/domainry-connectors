// Package microsoftbooking implements the official Microsoft Bookings Provider.
package microsoftbooking

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
	internaloauth2 "github.com/domainry/domainry-connectors/internal/oauth2"
)

const (
	ConnectorKey    = "appointment_scheduling"
	ProviderKey     = "microsoft_booking"
	defaultBaseURL  = "https://graph.microsoft.com/v1.0"
	defaultTokenURL = "https://login.microsoftonline.com/common/oauth2/v2.0/token"
	responseLimit   = 4 << 20
)

type ListAppointmentsInput struct {
	Count  *bool  `json:"$count,omitempty"`
	Expand string `json:"$expand,omitempty"`
	Top    int    `json:"$top,omitempty"`
	Skip   int    `json:"$skip,omitempty"`
}

type CreateBookingInput struct {
	Start                 string           `json:"start"`
	End                   string           `json:"end"`
	TimeZone              string           `json:"timeZone,omitempty"`
	ServiceID             string           `json:"serviceId,omitempty"`
	ServiceName           string           `json:"serviceName,omitempty"`
	CustomerEmailAddress  string           `json:"customerEmailAddress,omitempty"`
	CustomerName          string           `json:"customerName,omitempty"`
	CustomerPhone         string           `json:"customerPhone,omitempty"`
	CustomerTimeZone      string           `json:"customerTimeZone,omitempty"`
	AdditionalInformation string           `json:"additionalInformation,omitempty"`
	IsLocationOnline      *bool            `json:"isLocationOnline,omitempty"`
	StaffMemberIDs        []string         `json:"staffMemberIds,omitempty"`
	Customers             []map[string]any `json:"customers,omitempty"`
}

type CancelAppointmentInput struct {
	EventUUID  string `json:"event_uuid,omitempty"`
	BookingUID string `json:"booking_uid,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

type RescheduleAppointmentInput struct {
	EventUUID  string `json:"event_uuid,omitempty"`
	BookingUID string `json:"booking_uid,omitempty"`
	Start      string `json:"start"`
	End        string `json:"end"`
	TimeZone   string `json:"timeZone,omitempty"`
}

type Response map[string]any

var (
	ListScheduledEvents      = connector.CallOperation[ListAppointmentsInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_scheduled_events", ContractSHA256: "03718b1dd1f9c62875e87ab34f8c29d7b15ae3ec62aebe8d10ddf1748a8a6e94", Reliability: readReliability()}
	CreateBooking            = connector.CallOperation[CreateBookingInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "create_booking", ContractSHA256: "928778cc82cac4f36f946f071d8b588e37eb555170f9878281aca249e29c66e1", Reliability: writeReliability()}
	CancelScheduledEvent     = connector.CallOperation[CancelAppointmentInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "cancel_scheduled_event", ContractSHA256: "09d521d641dcd334b09676d0c9d2f8bc1771ed648206cef4c5fdc231d32fa082", Reliability: writeReliability()}
	RescheduleScheduledEvent = connector.CallOperation[RescheduleAppointmentInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "reschedule_scheduled_event", ContractSHA256: "4a086743e91ae464065a1075fa4493efeb5c94fda5d60b1446bd1033b81a0b76", Reliability: writeReliability()}
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
		return nil, errors.New("Microsoft Bookings transport is required")
	}
	p := &provider{transport: transport}
	operations := make([]connector.BoundOperation, 0, 5)
	for _, bind := range []func() (connector.BoundOperation, error){
		func() (connector.BoundOperation, error) {
			return connector.BindCall(ListScheduledEvents, p.listAppointments)
		},
		func() (connector.BoundOperation, error) { return connector.BindCall(CreateBooking, p.createBooking) },
		func() (connector.BoundOperation, error) {
			return connector.BindCall(CancelScheduledEvent, p.cancelAppointment)
		},
		func() (connector.BoundOperation, error) {
			return connector.BindCall(RescheduleScheduledEvent, p.rescheduleAppointment)
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
		{Key: "business_id", Name: "Booking business ID", Type: connector.ConfigFieldText, Required: true},
		{Key: "base_url", Name: "Microsoft Graph base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://graph.microsoft.com/v1.0"`)},
		{Key: "token_url", Name: "Microsoft identity token URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://login.microsoftonline.com/common/oauth2/v2.0/token"`)},
		{Key: "default_timezone", Name: "Default time zone", Type: connector.ConfigFieldText, Default: json.RawMessage(`"UTC"`)},
		{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &min, Max: &max}},
	}, SecretFields: []connector.SecretField{
		{Key: "access_token", Name: "OAuth access token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationOAuthRefresh, ExpiryPolicy: connector.SecretExpiryRequired, TestRequirement: connector.SecretTestWhenBound},
		{Key: "refresh_token", Name: "OAuth refresh token", CredentialKind: connector.SecretCredentialRefreshToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationOAuthRefresh, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound},
		{Key: "client_id", Name: "OAuth client ID", CredentialKind: connector.SecretCredentialIdentifier, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound},
		{Key: "client_secret", Name: "OAuth client secret", CredentialKind: connector.SecretCredentialOAuthClientSecret, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound},
	}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	if strings.TrimSpace(config(connection, "business_id", "")) == "" {
		return permanent("business_id_required", "business_id is required")
	}
	for _, item := range []struct{ endpoint, host string }{{baseURL(connection), "graph.microsoft.com"}, {tokenURL(connection), "login.microsoftonline.com"}} {
		parsed, err := url.Parse(item.endpoint)
		if err != nil || parsed.Host == "" || parsed.User != nil {
			return permanent("endpoint_invalid", "valid Microsoft endpoint is required")
		}
		if parsed.Scheme == "http" && isLoopback(parsed.Hostname()) {
			continue
		}
		if parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), item.host) {
			return permanent("endpoint_invalid", "official Microsoft endpoint or loopback HTTP is required")
		}
	}
	return nil
}

func (p *provider) listAppointments(ctx context.Context, request connector.TypedRequest[ListAppointmentsInput]) (connector.TypedResult[Response], error) {
	i := request.Input
	if i.Top < 0 || i.Skip < 0 {
		return connector.TypedResult[Response]{}, permanent("paging_invalid", "$top and $skip must be non-negative")
	}
	query := url.Values{}
	if i.Count != nil {
		query.Set("$count", strconv.FormatBool(*i.Count))
	}
	set(query, "$expand", i.Expand)
	if i.Top > 0 {
		query.Set("$top", strconv.Itoa(i.Top))
	}
	if i.Skip > 0 {
		query.Set("$skip", strconv.Itoa(i.Skip))
	}
	return p.executeWithRefresh(ctx, request.Connection, request.Secrets, http.MethodGet, appointmentsPath(request.Connection), query, nil, false)
}

func (p *provider) createBooking(ctx context.Context, request connector.TypedRequest[CreateBookingInput]) (connector.TypedResult[Response], error) {
	i := request.Input
	if strings.TrimSpace(i.Start) == "" || strings.TrimSpace(i.End) == "" {
		return connector.TypedResult[Response]{}, permanent("time_range_required", "start and end are required")
	}
	zone := strings.TrimSpace(i.TimeZone)
	if zone == "" {
		zone = config(request.Connection, "default_timezone", "UTC")
	}
	body := map[string]any{"start": map[string]any{"dateTime": i.Start, "timeZone": zone}, "end": map[string]any{"dateTime": i.End, "timeZone": zone}}
	setAny(body, "serviceId", i.ServiceID)
	setAny(body, "serviceName", i.ServiceName)
	setAny(body, "customerEmailAddress", i.CustomerEmailAddress)
	setAny(body, "customerName", i.CustomerName)
	setAny(body, "customerPhone", i.CustomerPhone)
	setAny(body, "customerTimeZone", i.CustomerTimeZone)
	setAny(body, "additionalInformation", i.AdditionalInformation)
	if i.IsLocationOnline != nil {
		body["isLocationOnline"] = *i.IsLocationOnline
	}
	if len(i.StaffMemberIDs) > 0 {
		body["staffMemberIds"] = i.StaffMemberIDs
	}
	if len(i.Customers) > 0 {
		body["customers"] = i.Customers
	}
	return p.executeWithRefresh(ctx, request.Connection, request.Secrets, http.MethodPost, appointmentsPath(request.Connection), nil, body, true)
}

func (p *provider) cancelAppointment(ctx context.Context, request connector.TypedRequest[CancelAppointmentInput]) (connector.TypedResult[Response], error) {
	id := appointmentID(request.Input.EventUUID, request.Input.BookingUID)
	if id == "" {
		return connector.TypedResult[Response]{}, permanent("appointment_id_required", "event_uuid or booking_uid is required")
	}
	result, err := p.executeWithRefresh(ctx, request.Connection, request.Secrets, http.MethodPost, appointmentsPath(request.Connection)+"/"+url.PathEscape(id)+"/cancel", nil, map[string]any{"cancellationMessage": strings.TrimSpace(request.Input.Reason)}, true)
	if err == nil {
		result.ResponseRef = "microsoft_booking:appointment:" + id + ":cancelled"
	}
	return result, err
}

func (p *provider) rescheduleAppointment(ctx context.Context, request connector.TypedRequest[RescheduleAppointmentInput]) (connector.TypedResult[Response], error) {
	i := request.Input
	id := appointmentID(i.EventUUID, i.BookingUID)
	if id == "" {
		return connector.TypedResult[Response]{}, permanent("appointment_id_required", "event_uuid or booking_uid is required")
	}
	if strings.TrimSpace(i.Start) == "" || strings.TrimSpace(i.End) == "" {
		return connector.TypedResult[Response]{}, permanent("time_range_required", "start and end are required")
	}
	zone := strings.TrimSpace(i.TimeZone)
	if zone == "" {
		zone = config(request.Connection, "default_timezone", "UTC")
	}
	body := map[string]any{"start": map[string]any{"dateTime": i.Start, "timeZone": zone}, "end": map[string]any{"dateTime": i.End, "timeZone": zone}}
	return p.executeWithRefresh(ctx, request.Connection, request.Secrets, http.MethodPatch, appointmentsPath(request.Connection)+"/"+url.PathEscape(id), nil, body, true)
}

func (p *provider) callTestConnection(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	return p.executeWithRefresh(ctx, request.Connection, request.Secrets, http.MethodGet, businessPath(request.Connection), nil, nil, false)
}

func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.executeWithRefresh(ctx, request.Connection, request.Secrets, http.MethodGet, businessPath(request.Connection), nil, nil, false)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, err := json.Marshal(map[string]any{"status": result.Output["status"], "response_ref": result.ResponseRef})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details, SecretUpdates: result.SecretUpdates}, nil
}

func (p *provider) executeWithRefresh(ctx context.Context, connection connector.Connection, secrets map[string]string, method, path string, query url.Values, body map[string]any, write bool) (connector.TypedResult[Response], error) {
	token := strings.TrimSpace(secrets["access_token"])
	if token == "" {
		return connector.TypedResult[Response]{}, permanent("access_token_required", "resolved Microsoft access token is required")
	}
	result, status, err := p.execute(ctx, connection, token, method, path, query, body, write)
	if err == nil || status != http.StatusUnauthorized {
		return result, err
	}
	refreshToken, clientID := strings.TrimSpace(secrets["refresh_token"]), strings.TrimSpace(secrets["client_id"])
	if refreshToken == "" || clientID == "" {
		return result, err
	}
	updated, refreshErr := internaloauth2.Refresh(ctx, p.transport, internaloauth2.RefreshRequest{Endpoint: tokenURL(connection), RefreshToken: refreshToken, ClientID: clientID, ClientSecret: strings.TrimSpace(secrets["client_secret"]), ClientSecretOptional: true, Scope: "https://graph.microsoft.com/.default offline_access", ClientAuthentication: internaloauth2.ClientAuthenticationForm, ErrorPrefix: "microsoft_booking.oauth"})
	if refreshErr != nil {
		return connector.TypedResult[Response]{ResponseRef: "oauth:refresh_failed"}, refreshErr
	}
	result, _, err = p.execute(ctx, connection, updated.AccessToken, method, path, query, body, write)
	result.SecretUpdates = map[string]string{"access_token": updated.AccessToken}
	if updated.RefreshToken != "" {
		result.SecretUpdates["refresh_token"] = updated.RefreshToken
	}
	return result, err
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, token, method, path string, query url.Values, body map[string]any, write bool) (connector.TypedResult[Response], int, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return connector.TypedResult[Response]{}, 0, err
	}
	endpoint, err := url.Parse(baseURL(connection) + path)
	if err != nil {
		return connector.TypedResult[Response]{}, 0, permanent("request_invalid", "Microsoft Graph request URL is invalid")
	}
	endpoint.RawQuery = query.Encode()
	var raw []byte
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return connector.TypedResult[Response]{}, 0, permanent("request_invalid", "Microsoft Graph request body is invalid")
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint.String(), Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, Body: raw, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		if write {
			return connector.TypedResult[Response]{}, 0, connector.UncertainError("microsoft_booking.network_error", transportErr)
		}
		return connector.TypedResult[Response]{}, 0, connector.RetryableError("microsoft_booking.network_error", transportErr)
	}
	status := response.StatusCode
	ref := "http:" + strconv.Itoa(status)
	payload := Response{}
	validJSON := len(response.Body) == 0 || json.Unmarshal(response.Body, &payload) == nil
	result := connector.TypedResult[Response]{Output: payload, ResponseRef: ref}
	if status < 200 || status >= 300 {
		cause := fmt.Errorf("provider returned HTTP %d", status)
		code := "microsoft_booking.http_" + strconv.Itoa(status)
		if status == http.StatusTooManyRequests {
			return result, status, connector.RetryableError(code, cause)
		}
		if status == http.StatusRequestTimeout || status >= 500 {
			if write {
				return result, status, connector.UncertainError(code, cause)
			}
			return result, status, connector.RetryableError(code, cause)
		}
		return result, status, connector.PermanentError(code, cause)
	}
	if !validJSON {
		if write {
			return result, status, connector.UncertainError("microsoft_booking.response_invalid", errors.New("provider response is invalid JSON"))
		}
		return result, status, permanent("response_invalid", "provider response is invalid JSON")
	}
	if id := mapString(payload, "id"); id != "" {
		result.ResponseRef = "microsoft_booking:" + id
	}
	return result, status, nil
}

func businessPath(connection connector.Connection) string {
	return "/solutions/bookingBusinesses/" + url.PathEscape(config(connection, "business_id", ""))
}
func appointmentsPath(connection connector.Connection) string {
	return businessPath(connection) + "/appointments"
}
func appointmentID(eventUUID, bookingUID string) string {
	if value := strings.TrimSpace(eventUUID); value != "" {
		return value
	}
	return strings.TrimSpace(bookingUID)
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
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
func permanent(code, message string) error {
	return connector.PermanentError("microsoft_booking."+code, errors.New(message))
}
