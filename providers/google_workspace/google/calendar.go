package google

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	calendar "github.com/domainry/domainry-connector-sdk/calendar"
)

var (
	CalendarList         = calendarRead[calendar.PageRequest, calendar.CalendarsPage](calendar.ListOperationKey)
	CalendarEvents       = calendarRead[calendar.EventsRequest, calendar.EventsPage](calendar.EventsOperationKey)
	CalendarEvent        = calendarRead[calendar.EventRequest, calendar.Event](calendar.EventOperationKey)
	CalendarAvailability = calendarRead[calendar.AvailabilityRequest, calendar.Availability](calendar.AvailabilityOperationKey)
)

func calendarRead[I, O any](key string) connector.CallOperation[I, O] {
	return connector.CallOperation[I, O]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: key, ContractSHA256: calendar.OperationSHA256(key), Reliability: readReliability()}
}

func decodeCalendarResponse(raw Response, value any) error {
	b, err := json.Marshal(raw)
	if err == nil {
		err = json.Unmarshal(b, value)
	}
	if err != nil {
		return permanent("calendar.invalid_response", "invalid calendar response")
	}
	return nil
}

func (p *provider) calendarList(ctx context.Context, r connector.TypedRequest[calendar.PageRequest]) (connector.TypedResult[calendar.CalendarsPage], error) {
	var out calendar.CalendarsPage
	if err := r.Input.Validate(); err != nil {
		return providerResult(connector.TypedResult[Response]{}, out, permanent("calendar.invalid_request", err.Error()))
	}
	q := url.Values{"maxResults": {strconv.Itoa(r.Input.PageSize())}, "showDeleted": {"false"}, "showHidden": {"false"}, "fields": {"kind,nextPageToken,items(id,summary,summaryOverride,timeZone,primary,accessRole)"}}
	set(q, "pageToken", r.Input.Cursor)
	raw, err := p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodGet, apiBase(r.Connection)+"/calendar/v3/users/me/calendarList", q, nil, false)
	if err != nil {
		return providerResult(raw, out, err)
	}
	var body struct {
		Kind  string `json:"kind"`
		Next  string `json:"nextPageToken"`
		Items []struct {
			ID       string `json:"id"`
			Summary  string `json:"summary"`
			Override string `json:"summaryOverride"`
			TimeZone string `json:"timeZone"`
			Primary  bool   `json:"primary"`
			Role     string `json:"accessRole"`
		} `json:"items"`
	}
	if err = decodeCalendarResponse(raw.Output, &body); err != nil {
		return providerResult(raw, out, err)
	}
	if body.Kind != "calendar#calendarList" || len(body.Items) > r.Input.PageSize() || len(body.Next) > 8192 {
		return providerResult(raw, out, permanent("calendar.invalid_response", "invalid calendar page"))
	}
	out = calendar.CalendarsPage{Items: []calendar.Calendar{}, NextCursor: body.Next, Complete: body.Next == ""}
	for _, item := range body.Items {
		if !calendar.ValidID(item.ID) {
			return providerResult(raw, calendar.CalendarsPage{}, permanent("calendar.invalid_response", "calendar identifier missing"))
		}
		name := item.Summary
		if item.Override != "" {
			name = item.Override
		}
		out.Items = append(out.Items, calendar.Calendar{ID: item.ID, Name: name, TimeZone: item.TimeZone, Primary: item.Primary, AccessRole: item.Role})
	}
	return providerResult(raw, out, nil)
}

type googleCalendarMoment struct {
	Date     string `json:"date"`
	DateTime string `json:"dateTime"`
	TimeZone string `json:"timeZone"`
}

func (m googleCalendarMoment) moment() calendar.Moment {
	return calendar.Moment{Date: m.Date, DateTime: m.DateTime, TimeZone: m.TimeZone}
}

type googleCalendarEvent struct {
	Kind          string                `json:"kind"`
	ID            string                `json:"id"`
	Summary       string                `json:"summary"`
	Description   string                `json:"description"`
	Location      string                `json:"location"`
	URL           string                `json:"htmlLink"`
	Status        string                `json:"status"`
	Start         googleCalendarMoment  `json:"start"`
	End           googleCalendarMoment  `json:"end"`
	SeriesID      string                `json:"recurringEventId"`
	OriginalStart *googleCalendarMoment `json:"originalStartTime"`
	Transparency  string                `json:"transparency"`
}

func (e googleCalendarEvent) event(calendarID string) (calendar.Event, error) {
	out := calendar.Event{ID: e.ID, CalendarID: calendarID, Title: e.Summary, Description: e.Description, Location: e.Location, URL: e.URL, Status: e.Status, Start: e.Start.moment(), End: e.End.moment(), SeriesID: e.SeriesID, Transparency: e.Transparency}
	if e.OriginalStart != nil {
		m := e.OriginalStart.moment()
		out.OriginalStart = &m
	}
	for _, moment := range []*calendar.Moment{&out.Start, &out.End, out.OriginalStart} {
		if moment == nil || moment.DateTime == "" {
			continue
		}
		if _, err := time.Parse(time.RFC3339, moment.DateTime); err == nil {
			continue
		}
		instant, err := calendar.LocalDateTime(moment.DateTime, moment.TimeZone)
		if err != nil {
			return calendar.Event{}, permanent("calendar.invalid_response", "calendar event time cannot be resolved")
		}
		moment.DateTime = instant
	}
	if err := out.Validate(); err != nil {
		return calendar.Event{}, permanent("calendar.invalid_response", "invalid calendar event")
	}
	return out, nil
}

func (p *provider) calendarEvents(ctx context.Context, r connector.TypedRequest[calendar.EventsRequest]) (connector.TypedResult[calendar.EventsPage], error) {
	var out calendar.EventsPage
	if err := r.Input.Validate(); err != nil {
		return providerResult(connector.TypedResult[Response]{}, out, permanent("calendar.invalid_request", err.Error()))
	}
	q := url.Values{"timeMin": {r.Input.Window.Start}, "timeMax": {r.Input.Window.End}, "timeZone": {r.Input.TimeZone}, "singleEvents": {"true"}, "orderBy": {"startTime"}, "showDeleted": {"false"}, "maxResults": {strconv.Itoa((calendar.PageRequest{Limit: r.Input.Limit}).PageSize())}, "fields": {"kind,timeZone,nextPageToken,items(id,summary,location,htmlLink,status,start,end,recurringEventId,originalStartTime,transparency)"}}
	set(q, "pageToken", r.Input.Cursor)
	raw, err := p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodGet, apiBase(r.Connection)+"/calendar/v3/calendars/"+url.PathEscape(r.Input.CalendarID)+"/events", q, nil, false)
	if err != nil {
		return providerResult(raw, out, err)
	}
	var body struct {
		Kind     string                `json:"kind"`
		TimeZone string                `json:"timeZone"`
		Next     string                `json:"nextPageToken"`
		Items    []googleCalendarEvent `json:"items"`
	}
	if err = decodeCalendarResponse(raw.Output, &body); err != nil {
		return providerResult(raw, out, err)
	}
	if body.Kind != "calendar#events" || len(body.Items) > (calendar.PageRequest{Limit: r.Input.Limit}).PageSize() || len(body.Next) > 8192 {
		return providerResult(raw, out, permanent("calendar.invalid_response", "invalid event page"))
	}
	out = calendar.EventsPage{Items: []calendar.Event{}, NextCursor: body.Next, Complete: body.Next == "", TimeZone: body.TimeZone}
	for _, item := range body.Items {
		if item.Status == "cancelled" {
			continue
		}
		event, err := item.event(r.Input.CalendarID)
		if err != nil {
			return providerResult(raw, calendar.EventsPage{}, err)
		}
		out.Items = append(out.Items, event)
	}
	return providerResult(raw, out, nil)
}

func (p *provider) calendarEvent(ctx context.Context, r connector.TypedRequest[calendar.EventRequest]) (connector.TypedResult[calendar.Event], error) {
	var out calendar.Event
	if err := r.Input.Validate(); err != nil {
		return providerResult(connector.TypedResult[Response]{}, out, permanent("calendar.invalid_request", err.Error()))
	}
	raw, err := p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodGet, apiBase(r.Connection)+"/calendar/v3/calendars/"+url.PathEscape(r.Input.CalendarID)+"/events/"+url.PathEscape(r.Input.EventID), url.Values{"timeZone": {r.Input.TimeZone}}, nil, false)
	if err != nil {
		return providerResult(raw, out, err)
	}
	var body googleCalendarEvent
	if err = decodeCalendarResponse(raw.Output, &body); err != nil {
		return providerResult(raw, out, err)
	}
	if body.Kind != "calendar#event" || body.ID != r.Input.EventID {
		return providerResult(raw, out, permanent("calendar.invalid_response", "event identity mismatch"))
	}
	if body.Status == "cancelled" {
		return providerResult(raw, out, permanent("calendar.event_cancelled", "calendar event is cancelled"))
	}
	out, err = body.event(r.Input.CalendarID)
	return providerResult(raw, out, err)
}

func (p *provider) calendarAvailability(ctx context.Context, r connector.TypedRequest[calendar.AvailabilityRequest]) (connector.TypedResult[calendar.Availability], error) {
	var out calendar.Availability
	if err := r.Input.Validate(); err != nil {
		return providerResult(connector.TypedResult[Response]{}, out, permanent("calendar.invalid_request", err.Error()))
	}
	items := []map[string]string{}
	for _, id := range r.Input.CalendarIDs {
		items = append(items, map[string]string{"id": id})
	}
	body := map[string]any{"timeMin": r.Input.Window.Start, "timeMax": r.Input.Window.End, "timeZone": r.Input.TimeZone, "calendarExpansionMax": calendar.MaximumCalendars, "items": items}
	raw, err := p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodPost, apiBase(r.Connection)+"/calendar/v3/freeBusy", nil, body, false)
	if err != nil {
		return providerResult(raw, out, err)
	}
	var result struct {
		TimeMin   string `json:"timeMin"`
		TimeMax   string `json:"timeMax"`
		Calendars map[string]struct {
			Busy   *[]calendar.Window `json:"busy"`
			Errors []struct {
				Reason string `json:"reason"`
			} `json:"errors"`
		} `json:"calendars"`
	}
	if err = decodeCalendarResponse(raw.Output, &result); err != nil {
		return providerResult(raw, out, err)
	}
	start, end, err := (calendar.Window{Start: result.TimeMin, End: result.TimeMax}).Instants()
	a, b, _ := r.Input.Window.Instants()
	if err != nil || !start.Equal(a) || !end.Equal(b) {
		return providerResult(raw, out, permanent("calendar.invalid_response", "availability window mismatch"))
	}
	values := []calendar.CalendarBusy{}
	for id, c := range result.Calendars {
		value := calendar.CalendarBusy{CalendarID: id, Busy: []calendar.Window{}, Complete: c.Busy != nil && len(c.Errors) == 0}
		if c.Busy != nil {
			value.Busy = *c.Busy
		}
		for _, e := range c.Errors {
			code := e.Reason
			if code == "" {
				code = "unavailable"
			}
			value.ErrorCodes = append(value.ErrorCodes, code)
		}
		values = append(values, value)
	}
	out, err = calendar.ResolveAvailability(r.Input, values)
	if err != nil {
		err = permanent("calendar.invalid_response", "invalid availability response")
	}
	return providerResult(raw, out, err)
}
