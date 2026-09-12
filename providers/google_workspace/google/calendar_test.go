package google

import (
	"encoding/json"
	"net/url"
	"reflect"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	calendar "github.com/domainry/domainry-connector-sdk/calendar"
)

func calendarCall[I, O any](t *testing.T, a connector.Adapter, op connector.CallOperation[I, O], in I, secrets map[string]string) (O, connector.CallResult, error) {
	t.Helper()
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	result, err := a.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: op.Key, ContractSHA256: op.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Payload: raw, Secrets: secrets})
	var out O
	if err == nil {
		if e := json.Unmarshal(result.Payload, &out); e != nil {
			t.Fatal(e)
		}
	}
	return out, result, err
}

func TestCalendarReadRangePaginationAllDayAndEventDetails(t *testing.T) {
	transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		u, _ := url.Parse(r.URL)
		q := u.Query()
		if r.Method != "GET" || r.SecretHeaders["Authorization"][0] != "Bearer account-token" {
			t.Fatal("read escaped host credential transport")
		}
		switch {
		case strings.HasSuffix(u.Path, "/calendarList"):
			if q.Get("pageToken") != "opaque/+==" || q.Get("maxResults") != "2" || q.Get("showHidden") != "false" {
				t.Fatal("calendar paging missing", q)
			}
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"kind":"calendar#calendarList","nextPageToken":"next/opaque","items":[{"id":"team@example.test","summary":"Team","summaryOverride":"我的团队","timeZone":"Asia/Shanghai","primary":true,"accessRole":"reader"}]}`)}, nil
		case strings.HasSuffix(u.Path, "/events/event/a"):
			if !strings.Contains(u.EscapedPath(), "event%2Fa") || q.Get("timeZone") != "Asia/Shanghai" {
				t.Fatal("event identity/path not escaped", u)
			}
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"kind":"calendar#event","id":"event/a","summary":"评审","description":"实际详情","location":"Room A","htmlLink":"https://calendar.google.com/event","status":"confirmed","start":{"dateTime":"2026-09-11T10:00:00+08:00"},"end":{"dateTime":"2026-09-11T11:00:00+08:00"}}`)}, nil
		default:
			if q.Get("timeMin") != "2026-03-08T00:00:00-08:00" || q.Get("timeMax") != "2026-03-09T00:00:00-07:00" || q.Get("singleEvents") != "true" || q.Get("orderBy") != "startTime" || q.Get("pageToken") != "next/opaque" || q.Has("syncToken") {
				t.Fatal("bounded recurrence query missing", q)
			}
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"kind":"calendar#events","timeZone":"America/Los_Angeles","nextPageToken":"more","items":[{"id":"holiday","summary":"假期","start":{"date":"2026-03-08"},"end":{"date":"2026-03-10"}},{"id":"instance","summary":"例会","recurringEventId":"series","originalStartTime":{"dateTime":"2026-03-08T09:00:00-07:00"},"start":{"dateTime":"2026-03-08T10:00:00-07:00","timeZone":"America/Los_Angeles"},"end":{"dateTime":"2026-03-08T11:00:00-07:00","timeZone":"America/Los_Angeles"}}]}`)}, nil
		}
	}}
	a, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	registry := connector.NewRegistry()
	if err = registry.Register(a); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()
	a, _ = registry.Provider(ConnectorKey, ProviderKey)
	secrets := map[string]string{"access_token": "account-token"}
	list, _, err := calendarCall(t, a, CalendarList, calendar.PageRequest{Limit: 2, Cursor: "opaque/+=="}, secrets)
	if err != nil || list.Complete || list.NextCursor != "next/opaque" || len(list.Items) != 1 || list.Items[0].Name != "我的团队" {
		t.Fatal(list, err)
	}
	events, _, err := calendarCall(t, a, CalendarEvents, calendar.EventsRequest{CalendarID: list.Items[0].ID, Window: calendar.Window{Start: "2026-03-08T00:00:00-08:00", End: "2026-03-09T00:00:00-07:00"}, TimeZone: "America/Los_Angeles", Limit: 2, Cursor: list.NextCursor}, secrets)
	if err != nil || events.Complete || len(events.Items) != 2 || events.Items[0].Start.Date != "2026-03-08" || events.Items[0].Start.DateTime != "" || events.Items[0].End.Date != "2026-03-10" || events.Items[1].SeriesID != "series" || events.Items[1].OriginalStart == nil {
		t.Fatal(events, err)
	}
	detail, _, err := calendarCall(t, a, CalendarEvent, calendar.EventRequest{CalendarID: list.Items[0].ID, EventID: "event/a", TimeZone: "Asia/Shanghai"}, secrets)
	if err != nil || detail.Description != "实际详情" || detail.Start.DateTime != "2026-09-11T10:00:00+08:00" {
		t.Fatal(detail, err)
	}
	if len(transport.requests) != 3 {
		t.Fatal("unexpected extra request")
	}
}

func TestCalendarAvailabilityRequiresEveryPositiveCalendarResult(t *testing.T) {
	request := calendar.AvailabilityRequest{CalendarIDs: []string{"a", "b"}, Window: calendar.Window{Start: "2026-09-11T09:00:00Z", End: "2026-09-11T18:00:00Z"}, TimeZone: "UTC"}
	for _, tc := range []struct {
		name, calendars string
		complete        bool
		free            int
	}{
		{"both free", `{"a":{"busy":[]},"b":{"busy":[]}}`, true, 1},
		{"overlaps", `{"a":{"busy":[{"start":"2026-09-11T10:00:00Z","end":"2026-09-11T11:00:00Z"}]},"b":{"busy":[{"start":"2026-09-11T10:30:00Z","end":"2026-09-11T12:00:00Z"}]}}`, true, 2},
		{"denied", `{"a":{"busy":[]},"b":{"errors":[{"reason":"notFound"}]}}`, false, 0},
		{"omitted", `{"a":{"busy":[]}}`, false, 0},
		{"no positive empty list", `{"a":{},"b":{"busy":[]}}`, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
				if r.Method != "POST" || !strings.HasSuffix(r.URL, "/calendar/v3/freeBusy") {
					t.Fatal("wrong availability request")
				}
				var body struct {
					Items []struct {
						ID string `json:"id"`
					} `json:"items"`
					TimeMin string `json:"timeMin"`
					TimeMax string `json:"timeMax"`
				}
				if json.Unmarshal(r.Body, &body) != nil || len(body.Items) != 2 || body.Items[0].ID != "a" || body.TimeMin != request.Window.Start || body.TimeMax != request.Window.End {
					t.Fatal("availability scope changed")
				}
				return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"timeMin":"2026-09-11T09:00:00Z","timeMax":"2026-09-11T18:00:00Z","calendars":` + tc.calendars + `}`)}, nil
			}}
			a, _ := New(transport)
			out, _, err := calendarCall(t, a, CalendarAvailability, request, map[string]string{"access_token": "t"})
			if err != nil || out.Complete != tc.complete || len(out.Free) != tc.free || len(out.Calendars) != 2 {
				t.Fatal(out, err)
			}
		})
	}
}

func TestCalendarInvalidInputsAndResponsesFailBeforeDisclosure(t *testing.T) {
	transport := &recordingTransport{}
	a, _ := New(transport)
	_, _, err := calendarCall(t, a, CalendarEvents, calendar.EventsRequest{CalendarID: "a", Window: calendar.Window{Start: "2026-09-11T09:00:00", End: "2026-09-11T18:00:00Z"}, TimeZone: "UTC"}, map[string]string{"access_token": "t"})
	if err == nil || len(transport.requests) != 0 {
		t.Fatal("offset-free query dispatched")
	}
	for _, body := range []string{`{}`, `{"kind":"calendar#events","items":[{"id":"broken","start":{"date":"2026-09-11"},"end":{"dateTime":"2026-09-12T00:00:00Z"}}]}`, `{"kind":"calendar#events","items":[{"id":"private-event-title"}]}`} {
		transport.respond = func(connector.HTTPRequest) (connector.HTTPResponse, error) {
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(body)}, nil
		}
		_, result, err := calendarCall(t, a, CalendarEvents, calendar.EventsRequest{CalendarID: "a", Window: calendar.Window{Start: "2026-09-11T09:00:00Z", End: "2026-09-11T18:00:00Z"}, TimeZone: "UTC"}, map[string]string{"access_token": "t"})
		if err == nil || len(result.Payload) != 0 || strings.Contains(err.Error(), "private-event-title") {
			t.Fatal("malformed response disclosed", err)
		}
	}
}

func TestCalendarNormalizationFailureStillPreservesCompletedCredentialRefresh(t *testing.T) {
	calls := 0
	transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		calls++
		if strings.HasSuffix(r.URL, "/token") {
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"access_token":"fresh-access","refresh_token":"fresh-refresh"}`)}, nil
		}
		if r.SecretHeaders["Authorization"][0] == "Bearer expired" {
			return connector.HTTPResponse{StatusCode: 401, Body: []byte(`{}`)}, nil
		}
		return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"kind":"calendar#event","id":"wrong-identity"}`)}, nil
	}}
	a, _ := New(transport)
	_, result, err := calendarCall(t, a, CalendarEvent, calendar.EventRequest{CalendarID: "a", EventID: "wanted", TimeZone: "UTC"}, map[string]string{"access_token": "expired", "refresh_token": "old-refresh", "client_id": "client", "client_secret": "secret"})
	if err == nil || len(result.Payload) != 0 || !reflect.DeepEqual(result.SecretUpdates, map[string]string{"access_token": "fresh-access", "refresh_token": "fresh-refresh"}) || calls != 3 {
		t.Fatal("refresh lost after normalized response rejection", result, err, calls)
	}
}

func TestCalendarSourceWallTimeUsesDeclaredZoneAndRejectsDSTGuess(t *testing.T) {
	source := googleCalendarEvent{ID: "local", Start: googleCalendarMoment{DateTime: "2026-09-11T10:00:00", TimeZone: "Asia/Shanghai"}, End: googleCalendarMoment{DateTime: "2026-09-11T11:00:00", TimeZone: "Asia/Shanghai"}}
	event, err := source.event("a")
	if err != nil || event.Start.DateTime != "2026-09-11T10:00:00+08:00" {
		t.Fatal(event, err)
	}
	source.Start = googleCalendarMoment{DateTime: "2026-11-01T01:30:00", TimeZone: "America/Los_Angeles"}
	if _, err = source.event("a"); err == nil {
		t.Fatal("ambiguous wall-clock time was guessed")
	}
}
