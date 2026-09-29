package feishucalendar

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	calendar "github.com/domainry/domainry-connector-sdk/calendar"
)

func oauthConnection() connector.Connection {
	value := connection("http://localhost:8080", true)
	value.ConnectorKey = ConnectorKey
	value.ProviderKey = ProviderKey
	return value
}

func TestOAuthV2KeepsCredentialsPrivateAndDeclaresCalendarScopes(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: http.StatusOK, Body: []byte(`{"access_token":"private-access","refresh_token":"private-refresh","token_type":"Bearer","expires_in":7200,"scope":"calendar:calendar:readonly"}`)}}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	authorizer, ok := connector.ResolveOAuthAuthorizer(adapter)
	if !ok {
		t.Fatal("Feishu Calendar OAuth capability is missing")
	}
	verifier := strings.Repeat("v", 43)
	digest := sha256.Sum256([]byte(verifier))
	request := connector.OAuthAuthorizationRequest{
		Connection: oauthConnection(), ClientID: "client-id", RedirectURI: "https://crm.example.test/integration/oauth/callback",
		Scopes: []string{"calendar:calendar:readonly"}, State: strings.Repeat("s", 43), CodeChallenge: base64.RawURLEncoding.EncodeToString(digest[:]),
	}
	rawURL, err := authorizer.AuthorizationURL(request)
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := url.Parse(rawURL)
	if parsed.Hostname() != "accounts.feishu.cn" || parsed.Path != "/open-apis/authen/v1/authorize" || parsed.Query().Get("code_challenge_method") != "S256" || parsed.Query().Get("state") != request.State {
		t.Fatalf("authorization URL=%s", rawURL)
	}
	tokens, err := authorizer.ExchangeAuthorizationCode(t.Context(), connector.OAuthCodeExchangeRequest{
		Connection: request.Connection, ClientID: request.ClientID, ClientSecret: "private-client-secret", RedirectURI: request.RedirectURI,
		RequestedScopes: request.Scopes, Code: "private-code", CodeVerifier: verifier,
	})
	if err != nil || tokens.AccessToken != "private-access" || tokens.RefreshToken != "private-refresh" || tokens.TokenType != "Bearer" || tokens.ExpiresInSeconds != 7200 {
		t.Fatalf("tokens=%+v err=%v", tokens, err)
	}
	httpRequest := transport.requests[0]
	if httpRequest.URL != "http://localhost:8080/open-apis/authen/v2/oauth/token" || httpRequest.Headers["Content-Type"][0] != "application/json; charset=utf-8" || httpRequest.SecretJSON["client_id"] != "client-id" || httpRequest.SecretJSON["client_secret"] != "private-client-secret" || httpRequest.SecretJSON["code"] != "private-code" || httpRequest.SecretJSON["code_verifier"] != verifier {
		t.Fatalf("token request=%+v", httpRequest)
	}
	serialized, _ := json.Marshal(httpRequest)
	if strings.Contains(string(serialized), "private-") || strings.Contains(string(serialized), verifier) {
		t.Fatalf("serialized token request leaked credentials: %s", serialized)
	}
	for _, key := range []string{CalendarList.Key, CalendarEvents.Key, CalendarEvent.Key, CalendarAvailability.Key} {
		scopes, declared := connector.ResolveOAuthOperationScopes(adapter, key)
		if !declared || len(scopes) != 1 || len(scopes[0]) != 1 || scopes[0][0] != "calendar:calendar:readonly" {
			t.Fatalf("scopes for %s=%v declared=%v", key, scopes, declared)
		}
	}
}

func TestOAuthTokenURLUsesOfficialV2EndpointByDefault(t *testing.T) {
	connection := connector.Connection{Config: map[string]any{}}
	if got := oauthTokenURL(connection); got != "https://open.feishu.cn/open-apis/authen/v2/oauth/token" {
		t.Fatalf("token URL=%s", got)
	}
}

func TestConnectionRefreshesUserTokenAndReturnsProviderAccountRoute(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{
		{StatusCode: http.StatusUnauthorized, Body: []byte(`{"code":99991677}`)},
		{StatusCode: http.StatusOK, Body: []byte(`{"access_token":"fresh-access","refresh_token":"fresh-refresh","token_type":"Bearer","expires_in":7200}`)},
		{StatusCode: http.StatusOK, Body: []byte(`{"code":0,"data":{"calendar":{"calendar_id":"calendar-primary"}}}`)},
		{StatusCode: http.StatusOK, Body: []byte(`{"code":0,"data":{"open_id":"ou_42","union_id":"on_42","email":"Person@Example.Test"}}`)},
	}}
	adapter, _ := New(transport)
	provider := adapter.(connector.ConnectionTester)
	result, err := provider.TestConnection(t.Context(), connector.TestConnectionRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Connection: oauthConnection(),
		Secrets: map[string]string{"access_token": "expired", "refresh_token": "private-refresh", "client_id": "private-client", "client_secret": "private-secret"},
	})
	if err != nil || !result.Connected || result.SecretUpdates["access_token"] != "fresh-access" || result.SecretUpdates["refresh_token"] != "fresh-refresh" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if len(transport.requests) != 4 || transport.requests[1].SecretJSON["refresh_token"] != "private-refresh" || strings.Contains(string(transport.requests[1].Body), "private-") || transport.requests[2].SecretHeaders["Authorization"][0] != "Bearer fresh-access" || transport.requests[3].SecretHeaders["Authorization"][0] != "Bearer fresh-access" {
		t.Fatalf("requests=%+v", transport.requests)
	}
	var details struct {
		ProviderAccount struct {
			Subject string `json:"subject"`
			Routes  []struct {
				Kind  string `json:"kind"`
				Value string `json:"value"`
			} `json:"routes"`
		} `json:"provider_account"`
	}
	if json.Unmarshal(result.Details, &details) != nil || details.ProviderAccount.Subject != "on_42" || len(details.ProviderAccount.Routes) != 3 || details.ProviderAccount.Routes[0].Value != "ou_42" || details.ProviderAccount.Routes[1].Value != "on_42" || details.ProviderAccount.Routes[2].Value != "person@example.test" {
		t.Fatalf("details=%s", result.Details)
	}
}

func TestCalendarReadsNormalizePaginationAllDayAndMeetingLink(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{
		{StatusCode: http.StatusOK, Body: []byte(`{"code":0,"data":{"has_more":true,"page_token":"calendar-next","calendar_list":[{"calendar_id":"calendar-primary","summary":"Sales","summary_alias":"My Sales","type":"primary","role":"owner","is_deleted":false}]}}`)},
		{StatusCode: http.StatusOK, Body: []byte(`{"code":0,"data":{"has_more":true,"page_token":"event-next","items":[{"event_id":"all-day","summary":"Conference","start_time":{"date":"2026-09-26","timezone":"UTC"},"end_time":{"date":"2026-09-28","timezone":"UTC"},"status":"confirmed","free_busy_status":"busy"},{"event_id":"meeting-42","summary":"Customer call","description":"Discovery","start_time":{"timestamp":"1790391600","timezone":"Asia/Shanghai"},"end_time":{"timestamp":"1790395200","timezone":"Asia/Shanghai"},"status":"confirmed","free_busy_status":"busy","vchat":{"meeting_url":"https://vc.feishu.cn/j/42"}}]}}`)},
		{StatusCode: http.StatusOK, Body: []byte(`{"code":0,"data":{"event":{"event_id":"meeting-42","organizer_calendar_id":"calendar-owner","summary":"Customer call","description":"Discovery","app_link":"https://applink.feishu.cn/client/calendar/event/detail?eventId=42","start_time":{"date_time":"2026-09-26T09:00:00+08:00","timezone":"Asia/Shanghai"},"end_time":{"date_time":"2026-09-26T10:00:00+08:00","timezone":"Asia/Shanghai"},"status":"confirmed","free_busy_status":"busy","recurrence":"FREQ=WEEKLY","event_organizer":{"user_id":"ou_owner","display_name":"Owner"},"attendees":[{"type":"user","attendee_id":"attendee-1","user_id":"ou_guest","display_name":"Guest","rsvp_status":"accept","is_optional":true,"is_external":false}],"vchat":{"meeting_url":"https://vc.feishu.cn/j/42"}}}}`)},
		{StatusCode: http.StatusOK, Body: []byte(`{"code":0,"data":{"has_more":false,"items":[{"type":"user","attendee_id":"attendee-1","user_id":"ou_guest","display_name":"Guest","rsvp_status":"accept","is_optional":true,"is_external":false}]}}`)},
	}}
	adapter, _ := New(transport)
	call := func(operationKey, hash string, input any) connector.CallResult {
		raw, _ := json.Marshal(input)
		result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: operationKey, ContractSHA256: hash, Mode: connector.ModeCall, Connection: oauthConnection(), Secrets: map[string]string{"access_token": "token"}, Payload: raw})
		if err != nil {
			t.Fatalf("call %s: %v", operationKey, err)
		}
		return result
	}
	listResult := call(CalendarList.Key, CalendarList.ContractSHA256, calendar.PageRequest{Limit: 25})
	var calendars calendar.CalendarsPage
	if json.Unmarshal(listResult.Payload, &calendars) != nil || len(calendars.Items) != 1 || calendars.Items[0].Name != "My Sales" || !calendars.Items[0].Primary || calendars.NextCursor != "calendar-next" || calendars.Complete {
		t.Fatalf("calendars=%s", listResult.Payload)
	}
	if transport.requests[0].URL != "http://localhost:8080/open-apis/calendar/v4/calendars?page_size=50" {
		t.Fatalf("calendar list URL=%s", transport.requests[0].URL)
	}
	eventsResult := call(CalendarEvents.Key, CalendarEvents.ContractSHA256, calendar.EventsRequest{CalendarID: "calendar-primary", Window: calendar.Window{Start: "2026-09-01T00:00:00+08:00", End: "2026-10-01T00:00:00+08:00"}, TimeZone: "Asia/Shanghai", Limit: 10})
	var events calendar.EventsPage
	if json.Unmarshal(eventsResult.Payload, &events) != nil || len(events.Items) != 2 || events.Items[0].Start.Date != "2026-09-26" || events.Items[0].End.Date != "2026-09-28" || events.Items[1].MeetingURL != "https://vc.feishu.cn/j/42" || events.NextCursor != "event-next" || events.Complete {
		t.Fatalf("events=%s", eventsResult.Payload)
	}
	eventListURL, _ := url.Parse(transport.requests[1].URL)
	if transport.requests[1].Method != http.MethodGet || eventListURL.Path != "/open-apis/calendar/v4/calendars/calendar-primary/events" || eventListURL.Query().Get("start_time") != "1788192000" || eventListURL.Query().Get("end_time") != "1790784000" || eventListURL.Query().Get("page_size") != "10" || eventListURL.Query().Get("user_id_type") != "open_id" || len(transport.requests[1].Body) != 0 {
		t.Fatalf("event list request=%+v", transport.requests[1])
	}
	eventResult := call(CalendarEvent.Key, CalendarEvent.ContractSHA256, calendar.EventRequest{CalendarID: "calendar-primary", EventID: "meeting-42", TimeZone: "Asia/Shanghai"})
	var event calendar.Event
	if json.Unmarshal(eventResult.Payload, &event) != nil || event.ID != "meeting-42" || event.SeriesID != "meeting-42" || event.URL == "" || event.MeetingURL != "https://vc.feishu.cn/j/42" || len(event.Recurrence) != 1 || event.Description != "Discovery" || event.Organizer == nil || event.Organizer.ID != "ou_owner" || len(event.Attendees) != 1 || event.Attendees[0].Role != "optional" || event.Attendees[0].ResponseStatus != "accepted" {
		t.Fatalf("event=%s", eventResult.Payload)
	}
	if query, _ := url.Parse(transport.requests[2].URL); query.Query().Has("need_attendee") || query.Query().Has("max_attendee_num") || query.Query().Get("user_id_type") != "open_id" {
		t.Fatalf("event query=%s", transport.requests[2].URL)
	}
	attendeeListURL, _ := url.Parse(transport.requests[3].URL)
	if attendeeListURL.Path != "/open-apis/calendar/v4/calendars/calendar-primary/events/meeting-42/attendees" || attendeeListURL.Query().Get("page_size") != "100" || attendeeListURL.Query().Get("user_id_type") != "open_id" {
		t.Fatalf("attendee list query=%s", transport.requests[3].URL)
	}
}

func TestCalendarEventPagesMoreThanTwoHundredAttendees(t *testing.T) {
	attendeePage := func(start, count int, hasMore bool, next string) connector.HTTPResponse {
		items := make([]map[string]any, count)
		for index := range items {
			value := start + index
			items[index] = map[string]any{"type": "user", "attendee_id": "attendee-" + strconv.Itoa(value), "user_id": "ou_" + strconv.Itoa(value), "display_name": "Guest " + strconv.Itoa(value), "rsvp_status": "accept"}
		}
		data := map[string]any{"has_more": hasMore, "items": items}
		if next != "" {
			data["page_token"] = next
		}
		body, _ := json.Marshal(map[string]any{"code": 0, "data": data})
		return connector.HTTPResponse{StatusCode: http.StatusOK, Body: body}
	}
	transport := &recordingTransport{responses: []connector.HTTPResponse{
		{StatusCode: http.StatusOK, Body: []byte(`{"code":0,"data":{"event":{"event_id":"meeting-large","summary":"Town hall","start_time":{"timestamp":"1790391600","timezone":"Asia/Shanghai"},"end_time":{"timestamp":"1790395200","timezone":"Asia/Shanghai"},"status":"confirmed"}}}`)},
		attendeePage(0, 100, true, "page-2"), attendeePage(100, 100, true, "page-3"), attendeePage(200, 1, false, ""),
	}}
	adapter, _ := New(transport)
	payload, _ := json.Marshal(calendar.EventRequest{CalendarID: "calendar-primary", EventID: "meeting-large", TimeZone: "Asia/Shanghai"})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CalendarEvent.Key, ContractSHA256: CalendarEvent.ContractSHA256, Mode: connector.ModeCall, Connection: oauthConnection(), Secrets: map[string]string{"access_token": "token"}, Payload: payload})
	var event calendar.Event
	if err != nil || json.Unmarshal(result.Payload, &event) != nil || len(event.Attendees) != 201 || event.Attendees[200].ID != "ou_200" {
		t.Fatalf("attendees=%d err=%v payload=%s", len(event.Attendees), err, result.Payload)
	}
	for index, token := range []string{"", "page-2", "page-3"} {
		requestURL, _ := url.Parse(transport.requests[index+1].URL)
		if requestURL.Query().Get("page_token") != token {
			t.Fatalf("attendee page %d query=%s", index+1, requestURL.RawQuery)
		}
	}
}

func TestCalendarEventsUsesPageTokenWithoutWindowBoundsAfterFirstPage(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: http.StatusOK, Body: []byte(`{"code":0,"data":{"has_more":false,"items":[]}}`)}}}
	adapter, _ := New(transport)
	input := calendar.EventsRequest{CalendarID: "calendar-primary", Cursor: "event-next", Window: calendar.Window{Start: "2026-09-01T00:00:00+08:00", End: "2026-10-01T00:00:00+08:00"}, TimeZone: "Asia/Shanghai", Limit: 10}
	payload, _ := json.Marshal(input)
	if _, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CalendarEvents.Key, ContractSHA256: CalendarEvents.ContractSHA256, Mode: connector.ModeCall, Connection: oauthConnection(), Secrets: map[string]string{"access_token": "token"}, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	requestURL, _ := url.Parse(transport.requests[0].URL)
	if requestURL.Query().Get("page_token") != "event-next" || requestURL.Query().Has("start_time") || requestURL.Query().Has("end_time") {
		t.Fatalf("paged event list query=%s", requestURL.RawQuery)
	}
}

func TestCalendarAvailabilityUsesBoundedInstanceWindowsAndNeverInventsBusyGaps(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: http.StatusOK, Body: []byte(`{"code":0,"data":{"items":[{"event_id":"busy-1","start_time":{"date_time":"2026-09-26T10:00:00+08:00","timezone":"Asia/Shanghai"},"end_time":{"date_time":"2026-09-26T11:00:00+08:00","timezone":"Asia/Shanghai"},"status":"confirmed","free_busy_status":"busy"},{"event_id":"free-1","start_time":{"date_time":"2026-09-26T12:00:00+08:00","timezone":"Asia/Shanghai"},"end_time":{"date_time":"2026-09-26T13:00:00+08:00","timezone":"Asia/Shanghai"},"status":"confirmed","free_busy_status":"free"}]}}`)}}}
	adapter, _ := New(transport)
	input := calendar.AvailabilityRequest{CalendarIDs: []string{"calendar-primary"}, Window: calendar.Window{Start: "2026-09-26T09:00:00+08:00", End: "2026-09-26T14:00:00+08:00"}, TimeZone: "Asia/Shanghai"}
	payload, _ := json.Marshal(input)
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CalendarAvailability.Key, ContractSHA256: CalendarAvailability.ContractSHA256, Mode: connector.ModeCall, Connection: oauthConnection(), Secrets: map[string]string{"access_token": "token"}, Payload: payload})
	var availability calendar.Availability
	if err != nil || json.Unmarshal(result.Payload, &availability) != nil || !availability.Complete || len(availability.Calendars) != 1 || len(availability.Calendars[0].Busy) != 1 || len(availability.Free) != 2 {
		t.Fatalf("availability=%s err=%v", result.Payload, err)
	}
	if len(transport.requests) != 1 || !strings.Contains(transport.requests[0].URL, "/events/instance_view?") {
		t.Fatalf("requests=%+v", transport.requests)
	}
}
