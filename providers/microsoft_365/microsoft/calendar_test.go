package microsoft

import (
	"encoding/base64"
	"encoding/json"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	calendar "github.com/domainry/domainry-connector-sdk/calendar"
)

func calendarCall[I, O any](t *testing.T, a connector.Adapter, op connector.CallOperation[I, O], in I, secrets map[string]string) (O, connector.CallResult, error) {
	t.Helper()
	payload, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	r, err := a.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: op.Key, ContractSHA256: op.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Payload: payload, Secrets: secrets})
	var out O
	if err == nil {
		if err := json.Unmarshal(r.Payload, &out); err != nil {
			t.Fatal(err)
		}
	}
	return out, r, err
}

func graphResponse(value any) (connector.HTTPResponse, error) {
	b, _ := json.Marshal(value)
	return connector.HTTPResponse{StatusCode: 200, Body: b}, nil
}

func graphEvent(id, start, end, showAs string) map[string]any {
	return map[string]any{"id": id, "subject": "评审", "isAllDay": false, "isCancelled": false, "type": "singleInstance", "showAs": showAs,
		"start": map[string]string{"dateTime": start, "timeZone": "UTC"}, "end": map[string]string{"dateTime": end, "timeZone": "UTC"}}
}

func calendarTestWindow() calendar.Window {
	return calendar.Window{Start: "2026-09-11T09:00:00Z", End: "2026-09-11T18:00:00Z"}
}
func calendarTestSecrets() map[string]string {
	return map[string]string{"access_token": "account-token"}
}

func nextLink(requestURL, token string) string {
	u, _ := url.Parse(requestURL)
	q := u.Query()
	q.Set("$skiptoken", token)
	u.RawQuery = q.Encode()
	return u.String()
}

func TestCalendarReadPublicBindingsPagingDSTAndDetails(t *testing.T) {
	transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		u, _ := url.Parse(r.URL)
		q := u.Query()
		if r.Method != "GET" || r.SecretHeaders["Authorization"][0] != "Bearer account-token" || !strings.Contains(r.Headers["Prefer"][0], `IdType="ImmutableId"`) || !strings.Contains(r.Headers["Prefer"][0], `outlook.timezone="UTC"`) {
			t.Fatal("read left host transport or stable identity")
		}
		switch {
		case strings.HasSuffix(u.Path, "/calendars"):
			if q.Get("$top") != "2" {
				t.Fatal(q)
			}
			if q.Get("$skiptoken") == "" {
				return graphResponse(map[string]any{"value": []any{}, "@odata.nextLink": nextLink(r.URL, "opaque/+==")})
			}
			if q.Get("$skiptoken") != "opaque/+==" {
				t.Fatal(q)
			}
			return graphResponse(map[string]any{"value": []any{map[string]any{"id": "default", "name": "默认", "isDefaultCalendar": true}, map[string]any{"id": "team/a", "name": "团队", "isDefaultCalendar": false}}})
		case strings.HasSuffix(u.Path, "/calendarView"):
			if !strings.Contains(u.EscapedPath(), "team%2Fa") || q.Get("startDateTime") != "2026-03-08T08:00:00Z" || q.Get("endDateTime") != "2026-03-09T07:00:00Z" || q.Get("$top") != "2" || strings.Contains(q.Get("$select"), ",body") {
				t.Fatal("range/selection escaped", u)
			}
			e := graphEvent("instance", "2026-03-08T09:30:00.0000000", "2026-03-08T10:30:00.0000000", "busy")
			e["type"], e["seriesMasterId"], e["originalStart"] = "exception", "series", "2026-03-08T09:00:00Z"
			return graphResponse(map[string]any{"value": []any{e}, "@odata.nextLink": nextLink(r.URL, "events-next")})
		default:
			if !strings.HasSuffix(u.EscapedPath(), "/team%2Fa/events/event%2Fa") || !strings.HasSuffix(q.Get("$select"), ",body") || !strings.Contains(r.Headers["Prefer"][0], `outlook.body-content-type="text"`) {
				t.Fatal(u, r.Headers)
			}
			e := graphEvent("event/a", "2026-09-11T02:00:00.1234567", "2026-09-11T03:00:00", "free")
			e["body"] = map[string]string{"contentType": "text", "content": "实际详情"}
			return graphResponse(e)
		}
	}}
	a, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	registry := connector.NewRegistry()
	if err := registry.Register(a); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()
	a, _ = registry.Provider(ConnectorKey, ProviderKey)
	first, _, err := calendarCall(t, a, CalendarList, calendar.PageRequest{Limit: 2}, calendarTestSecrets())
	if err != nil || first.Complete || len(first.Items) != 0 || first.NextCursor == "" {
		t.Fatal(first, err)
	}
	list, _, err := calendarCall(t, a, CalendarList, calendar.PageRequest{Limit: 2, Cursor: first.NextCursor}, calendarTestSecrets())
	if err != nil || !list.Complete || len(list.Items) != 2 || !list.Items[0].Primary || list.Items[1].Primary || list.Items[1].TimeZone != "" {
		t.Fatal(list, err)
	}
	window := calendar.Window{Start: "2026-03-08T00:00:00-08:00", End: "2026-03-09T00:00:00-07:00"}
	events, _, err := calendarCall(t, a, CalendarEvents, calendar.EventsRequest{CalendarID: list.Items[1].ID, Window: window, TimeZone: "America/Los_Angeles", Limit: 2}, calendarTestSecrets())
	if err != nil || events.Complete || events.NextCursor == "" || len(events.Items) != 1 {
		t.Fatal(events, err)
	}
	e := events.Items[0]
	if e.Start.DateTime != "2026-03-08T01:30:00-08:00" || e.End.DateTime != "2026-03-08T03:30:00-07:00" || e.SeriesID != "series" || e.OriginalStart == nil || e.OriginalStart.DateTime != "2026-03-08T01:00:00-08:00" {
		t.Fatal(e)
	}
	detail, _, err := calendarCall(t, a, CalendarEvent, calendar.EventRequest{CalendarID: "team/a", EventID: "event/a", TimeZone: "Asia/Shanghai"}, calendarTestSecrets())
	if err != nil || detail.Description != "实际详情" || detail.Start.DateTime != "2026-09-11T10:00:00.1234567+08:00" || detail.Transparency != "transparent" {
		t.Fatal(detail, err)
	}
}

func allDayGraphEvent(zone string, local bool) map[string]any {
	a, b := "2026-09-10T16:00:00.0000000", "2026-09-12T16:00:00.0000000"
	if local {
		a, b = "2026-09-11T00:00:00.0000000", "2026-09-13T00:00:00.0000000"
	}
	e := graphEvent("holiday", a, b, "oof")
	e["isAllDay"], e["originalStartTimeZone"], e["originalEndTimeZone"], e["changeKey"] = true, zone, zone, "revision-1"
	if local {
		e["start"].(map[string]string)["timeZone"], e["end"].(map[string]string)["timeZone"] = zone, zone
	}
	return e
}

func TestCalendarAllDaySourceDatesAndSnapshotGuard(t *testing.T) {
	for _, tc := range []string{"valid", "changed", "missing-zone", "zone-injection", "not-midnight", "zone-ignored"} {
		t.Run(tc, func(t *testing.T) {
			transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
				if strings.Contains(r.URL, "/calendarView?") {
					e := allDayGraphEvent("China Standard Time", false)
					if tc == "missing-zone" {
						delete(e, "originalStartTimeZone")
					}
					if tc == "zone-injection" {
						e["originalStartTimeZone"], e["originalEndTimeZone"] = "bad\"\r\nX:bad", "bad\"\r\nX:bad"
					}
					return graphResponse(map[string]any{"value": []any{e}})
				}
				if !strings.Contains(r.Headers["Prefer"][0], `outlook.timezone="China Standard Time"`) || strings.Contains(r.URL, "%2Cbody") {
					t.Fatal(r.Headers, r.URL)
				}
				e := allDayGraphEvent("China Standard Time", true)
				switch tc {
				case "changed":
					e["changeKey"] = "revision-2"
				case "not-midnight":
					e["start"].(map[string]string)["dateTime"] = "2026-09-11T01:00:00"
				case "zone-ignored":
					e = allDayGraphEvent("China Standard Time", false)
				}
				return graphResponse(e)
			}}
			a, _ := New(transport)
			out, result, err := calendarCall(t, a, CalendarEvents, calendar.EventsRequest{CalendarID: "team", Window: calendarTestWindow(), TimeZone: "America/Los_Angeles"}, calendarTestSecrets())
			if tc == "valid" {
				if err != nil || len(out.Items) != 1 || out.Items[0].Start.Date != "2026-09-11" || out.Items[0].End.Date != "2026-09-13" || out.Items[0].Start.DateTime != "" || len(transport.requests) != 2 {
					t.Fatal(out, err)
				}
			} else {
				if err == nil || len(result.Payload) != 0 {
					t.Fatal("accepted inconsistent dates", out, err)
				}
				if (tc == "missing-zone" || tc == "zone-injection") && len(transport.requests) != 1 {
					t.Fatal("untrusted zone reached transport")
				}
				if tc == "changed" {
					class, _ := connector.ErrorClassificationOf(err)
					if class != connector.ErrorRetryable {
						t.Fatal(err)
					}
				}
			}
		})
	}
}

func TestCalendarNextLinkCannotChangeDestinationOrQuery(t *testing.T) {
	for _, tc := range []string{"host", "path", "userinfo", "fragment", "filter", "window", "duplicate", "missing-select", "negative-skip", "two-pagers"} {
		t.Run(tc, func(t *testing.T) {
			transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
				u, _ := url.Parse(nextLink(r.URL, "next"))
				q := u.Query()
				switch tc {
				case "host":
					u.Host = "attacker.example"
				case "path":
					u.Path = "/v1.0/me/messages"
				case "userinfo":
					u.User = url.User("attacker")
				case "fragment":
					u.Fragment = "fragment"
				case "filter":
					q.Set("$filter", "true")
				case "window":
					q.Set("startDateTime", "2020-01-01T00:00:00Z")
				case "duplicate":
					q.Add("$top", "100")
				case "missing-select":
					q.Del("$select")
				case "negative-skip":
					q.Del("$skiptoken")
					q.Set("$skip", "-1")
				case "two-pagers":
					q.Set("$skip", "2")
				}
				u.RawQuery = q.Encode()
				return graphResponse(map[string]any{"value": []any{}, "@odata.nextLink": u.String()})
			}}
			a, _ := New(transport)
			_, r, err := calendarCall(t, a, CalendarEvents, calendar.EventsRequest{CalendarID: "team", Window: calendarTestWindow(), TimeZone: "UTC"}, calendarTestSecrets())
			if err == nil || len(r.Payload) != 0 || len(transport.requests) != 1 {
				t.Fatal("unsafe continuation accepted", err)
			}
		})
	}
}

func TestCalendarCursorBoundToAccountWindowAndSelection(t *testing.T) {
	transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		return graphResponse(map[string]any{"value": []any{}, "@odata.nextLink": nextLink(r.URL, "next")})
	}}
	a, _ := New(transport)
	in := calendar.EventsRequest{CalendarID: "team", Window: calendarTestWindow(), TimeZone: "UTC"}
	out, _, err := calendarCall(t, a, CalendarEvents, in, calendarTestSecrets())
	if err != nil {
		t.Fatal(err)
	}
	in.Cursor = out.NextCursor
	for _, tc := range []string{"calendar", "window", "zone", "limit", "account", "workspace", "malformed", "forged-query"} {
		t.Run(tc, func(t *testing.T) {
			next := in
			connection := validConnection()
			switch tc {
			case "calendar":
				next.CalendarID = "other"
			case "window":
				next.Window.Start = "2026-09-11T08:00:00Z"
			case "zone":
				next.TimeZone = "Asia/Shanghai"
			case "limit":
				next.Limit = 50
			case "account":
				connection.Key = "other"
			case "workspace":
				connection.WorkspaceID = "other"
			case "malformed":
				next.Cursor = "https://attacker.example"
			case "forged-query":
				b, _ := base64.RawURLEncoding.DecodeString(next.Cursor)
				var c calendarCursor
				_ = json.Unmarshal(b, &c)
				c.Query += "&%24filter=true"
				b, _ = json.Marshal(c)
				next.Cursor = base64.RawURLEncoding.EncodeToString(b)
			}
			payload, _ := json.Marshal(next)
			_, err := a.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CalendarEvents.Key, ContractSHA256: CalendarEvents.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Payload: payload, Secrets: calendarTestSecrets()})
			if err == nil || len(transport.requests) != 1 {
				t.Fatal("cursor escaped scope before I/O", err)
			}
		})
	}
}

func TestCalendarAvailabilitySecondaryCalendarsAndIncomplete(t *testing.T) {
	for _, tc := range []string{"complete", "unknown", "page-limit", "loop", "missing-value", "null-value", "denied", "non-utc"} {
		t.Run(tc, func(t *testing.T) {
			pages := 0
			transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
				u, _ := url.Parse(r.URL)
				if !strings.Contains(u.Path, "/calendarView") || u.Query().Get("$select") != calendarBusyFields || r.Method != "GET" {
					t.Fatal("busy query included content or used default mailbox schedule")
				}
				if strings.Contains(u.Path, "/primary/") {
					return graphResponse(map[string]any{"value": []any{graphEvent("a", "2026-09-11T10:00:00", "2026-09-11T11:00:00", "busy")}})
				}
				pages++
				e := graphEvent("b", "2026-09-11T10:30:00", "2026-09-11T12:00:00", "tentative")
				switch tc {
				case "unknown":
					e["showAs"] = "unknown"
				case "non-utc":
					e["start"].(map[string]string)["timeZone"] = "Pacific Standard Time"
				case "missing-value":
					return graphResponse(map[string]any{})
				case "null-value":
					return graphResponse(map[string]any{"value": nil})
				case "denied":
					return connector.HTTPResponse{StatusCode: 403, Body: []byte(`{"error":{"message":"private vendor text"}}`)}, nil
				}
				body := map[string]any{"value": []any{e, graphEvent("free", "2026-09-11T12:00:00", "2026-09-11T13:00:00", "workingElsewhere")}}
				if pages == 1 || tc == "page-limit" || tc == "loop" {
					body["@odata.nextLink"] = nextLink(r.URL, strconv.Itoa(pages))
					body["value"] = []any{}
				}
				if tc == "loop" {
					body["@odata.nextLink"] = nextLink(r.URL, "1")
				}
				return graphResponse(body)
			}}
			a, _ := New(transport)
			out, result, err := calendarCall(t, a, CalendarAvailability, calendar.AvailabilityRequest{CalendarIDs: []string{"primary", "secondary"}, Window: calendarTestWindow(), TimeZone: "UTC"}, calendarTestSecrets())
			switch tc {
			case "complete":
				want := []calendar.Window{{Start: "2026-09-11T09:00:00Z", End: "2026-09-11T10:00:00Z"}, {Start: "2026-09-11T12:00:00Z", End: "2026-09-11T18:00:00Z"}}
				if err != nil || !out.Complete || !reflect.DeepEqual(out.Free, want) || pages != 2 {
					t.Fatal(out, err)
				}
			case "unknown", "page-limit", "non-utc":
				if err != nil || out.Complete || len(out.Free) != 0 || len(out.Calendars[1].ErrorCodes) == 0 {
					t.Fatal("invented availability", out, err)
				}
				if tc == "page-limit" && pages != calendarAvailabilityPageLimit {
					t.Fatal(pages)
				}
			default:
				if err == nil || len(result.Payload) != 0 || strings.Contains(err.Error(), "private vendor text") {
					t.Fatal("failure disclosed data or availability", out, err)
				}
			}
		})
	}
}

func TestCalendarRotationsAcrossPagesSurviveLaterNormalizationFailure(t *testing.T) {
	rotations, pages := 0, 0
	transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		if r.Method == "POST" {
			rotations++
			want := "refresh"
			if rotations == 2 {
				want = "refresh-1"
			}
			if r.SecretForm["refresh_token"] != want {
				t.Fatal("subrequest used stale refresh credential")
			}
			return graphResponse(map[string]any{"access_token": "token-" + strconv.Itoa(rotations), "refresh_token": "refresh-" + strconv.Itoa(rotations)})
		}
		u, _ := url.Parse(r.URL)
		token := r.SecretHeaders["Authorization"][0]
		if token == "Bearer stale" || (u.Query().Get("$skiptoken") != "" && token == "Bearer token-1") {
			return connector.HTTPResponse{StatusCode: 401, Body: []byte(`{}`)}, nil
		}
		pages++
		if pages == 1 {
			return graphResponse(map[string]any{"value": []any{}, "@odata.nextLink": nextLink(r.URL, "next")})
		}
		return graphResponse(map[string]any{"value": "not an array"})
	}}
	a, _ := New(transport)
	secrets := map[string]string{"access_token": "stale", "refresh_token": "refresh", "client_id": "client"}
	_, result, err := calendarCall(t, a, CalendarAvailability, calendar.AvailabilityRequest{CalendarIDs: []string{"secondary"}, Window: calendarTestWindow(), TimeZone: "UTC"}, secrets)
	if err == nil || len(result.Payload) != 0 || rotations != 2 || result.SecretUpdates["access_token"] != "token-2" || result.SecretUpdates["refresh_token"] != "refresh-2" || secrets["access_token"] != "stale" || secrets["refresh_token"] != "refresh" {
		t.Fatal("rotation lost or input mutated", result.SecretUpdates, err)
	}
}

func TestCalendarInvalidRequestsAndDetailResponses(t *testing.T) {
	transport := &recordingTransport{}
	a, _ := New(transport)
	_, _, e1 := calendarCall(t, a, CalendarList, calendar.PageRequest{Limit: 101}, calendarTestSecrets())
	_, _, e2 := calendarCall(t, a, CalendarEvents, calendar.EventsRequest{CalendarID: "team", Window: calendarTestWindow(), TimeZone: "Local"}, calendarTestSecrets())
	_, _, e3 := calendarCall(t, a, CalendarEvent, calendar.EventRequest{CalendarID: "..", EventID: "event", TimeZone: "UTC"}, calendarTestSecrets())
	_, _, e4 := calendarCall(t, a, CalendarAvailability, calendar.AvailabilityRequest{CalendarIDs: []string{"a", "a"}, Window: calendarTestWindow(), TimeZone: "UTC"}, calendarTestSecrets())
	if e1 == nil || e2 == nil || e3 == nil || e4 == nil || len(transport.requests) != 0 {
		t.Fatal("invalid request reached transport")
	}
	for _, tc := range []string{"wrong-id", "cancelled", "html", "missing-body", "missing-flags", "wrong-offset", "reversed"} {
		t.Run(tc, func(t *testing.T) {
			transport.respond = func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
				e := graphEvent("event", "2026-09-11T10:00:00", "2026-09-11T11:00:00", "busy")
				e["body"] = map[string]string{"contentType": "text", "content": "private event text"}
				switch tc {
				case "wrong-id":
					e["id"] = "other"
				case "cancelled":
					e["isCancelled"] = true
				case "html":
					e["body"].(map[string]string)["contentType"] = "html"
				case "missing-body":
					delete(e, "body")
				case "missing-flags":
					delete(e, "isAllDay")
				case "wrong-offset":
					e["start"].(map[string]string)["dateTime"] = "2026-09-11T10:00:00+08:00"
				case "reversed":
					e["end"].(map[string]string)["dateTime"] = "2026-09-11T09:00:00"
				}
				return graphResponse(e)
			}
			_, r, err := calendarCall(t, a, CalendarEvent, calendar.EventRequest{CalendarID: "team", EventID: "event", TimeZone: "UTC"}, calendarTestSecrets())
			if err == nil || len(r.Payload) != 0 || strings.Contains(err.Error(), "private event text") {
				t.Fatal("invalid detail accepted/disclosed", err)
			}
		})
	}
}

func TestCalendarAllDayRefetchRetainsRefreshOnChangedSnapshot(t *testing.T) {
	transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		if r.Method == "POST" {
			return graphResponse(map[string]any{"access_token": "fresh", "refresh_token": "rotated"})
		}
		if strings.Contains(r.URL, "/calendarView?") {
			return graphResponse(map[string]any{"value": []any{allDayGraphEvent("China Standard Time", false)}})
		}
		if r.SecretHeaders["Authorization"][0] == "Bearer stale" {
			return connector.HTTPResponse{StatusCode: 401, Body: []byte(`{}`)}, nil
		}
		if !strings.Contains(r.Headers["Prefer"][0], `outlook.timezone="China Standard Time"`) {
			t.Fatal("refresh replay lost source timezone")
		}
		e := allDayGraphEvent("China Standard Time", true)
		e["changeKey"] = "changed-during-refetch"
		return graphResponse(e)
	}}
	a, _ := New(transport)
	_, result, err := calendarCall(t, a, CalendarEvents, calendar.EventsRequest{CalendarID: "team", Window: calendarTestWindow(), TimeZone: "UTC"}, map[string]string{"access_token": "stale", "refresh_token": "refresh", "client_id": "client"})
	if err == nil || len(result.Payload) != 0 || result.SecretUpdates["access_token"] != "fresh" || result.SecretUpdates["refresh_token"] != "rotated" || len(transport.requests) != 4 {
		t.Fatal("refetch dropped rotation or returned changed data", result.SecretUpdates, err)
	}
}

func TestCalendarUTCAllDayDoesNotRefetchAndFallBackInstantsStayDistinct(t *testing.T) {
	transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		if !strings.Contains(r.URL, "/calendarView?") {
			t.Fatal("UTC all-day caused unnecessary refetch")
		}
		return graphResponse(map[string]any{"value": []any{allDayGraphEvent("UTC", true), graphEvent("repeated-hour", "2026-11-01T08:30:00", "2026-11-01T09:30:00", "busy")}})
	}}
	a, _ := New(transport)
	out, _, err := calendarCall(t, a, CalendarEvents, calendar.EventsRequest{CalendarID: "team", Window: calendar.Window{Start: "2026-09-10T00:00:00Z", End: "2026-11-02T00:00:00Z"}, TimeZone: "America/Los_Angeles"}, calendarTestSecrets())
	if err != nil || len(out.Items) != 2 || out.Items[0].Start.Date != "2026-09-11" || out.Items[1].Start.DateTime != "2026-11-01T01:30:00-07:00" || out.Items[1].End.DateTime != "2026-11-01T01:30:00-08:00" || len(transport.requests) != 1 {
		t.Fatal(out, err)
	}
}
