package microsoft

import (
	"context"
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

func (p *provider) calendarList(ctx context.Context, r connector.TypedRequest[calendar.PageRequest]) (connector.TypedResult[calendar.CalendarsPage], error) {
	s := newCalendarSession(p, r.Connection, r.Secrets)
	var out calendar.CalendarsPage
	if err := r.Input.Validate(); err != nil {
		return calendarResult(s, out, permanent("calendar.invalid_request", err.Error()))
	}
	q := url.Values{"$top": {strconv.Itoa(r.Input.PageSize())}, "$select": {"id,name,isDefaultCalendar"}}
	type item struct {
		ID      string `json:"id"`
		Name    string `json:"name"`
		Primary bool   `json:"isDefaultCalendar"`
	}
	items, next, err := readCalendarPage[item](ctx, s, graphBase(r.Connection)+"/me/calendars", q, "UTC", r.Input.Cursor, r.Input.PageSize())
	if err != nil {
		return calendarResult(s, out, err)
	}
	out = calendar.CalendarsPage{Items: []calendar.Calendar{}, NextCursor: next, Complete: next == ""}
	seen := map[string]bool{}
	for _, item := range items {
		if !calendar.ValidID(item.ID) || seen[item.ID] {
			return calendarResult(s, calendar.CalendarsPage{}, calendarInvalidResponse("invalid calendar identity"))
		}
		seen[item.ID] = true
		out.Items = append(out.Items, calendar.Calendar{ID: item.ID, Name: item.Name, Primary: item.Primary})
	}
	return calendarResult(s, out, nil)
}

const calendarEventFields = "id,subject,location,webLink,isCancelled,isAllDay,start,end,seriesMasterId,originalStart,originalStartTimeZone,originalEndTimeZone,showAs,changeKey,type"
const calendarBusyFields = "id,isCancelled,start,end,showAs,type"
const calendarAvailabilityPageLimit = 10

func calendarPath(c connector.Connection, id string) string {
	return graphBase(c) + "/me/calendars/" + url.PathEscape(id)
}
func calendarViewQuery(window calendar.Window, limit int, fields string) url.Values {
	start, end, _ := window.Instants()
	return url.Values{"startDateTime": {start.UTC().Format(time.RFC3339Nano)}, "endDateTime": {end.UTC().Format(time.RFC3339Nano)}, "$top": {strconv.Itoa(limit)}, "$select": {fields}}
}

func (p *provider) calendarEvents(ctx context.Context, r connector.TypedRequest[calendar.EventsRequest]) (connector.TypedResult[calendar.EventsPage], error) {
	s := newCalendarSession(p, r.Connection, r.Secrets)
	var out calendar.EventsPage
	if err := r.Input.Validate(); err != nil {
		return calendarResult(s, out, permanent("calendar.invalid_request", err.Error()))
	}
	limit := (calendar.PageRequest{Limit: r.Input.Limit}).PageSize()
	q := calendarViewQuery(r.Input.Window, limit, calendarEventFields)
	items, next, err := readCalendarPage[graphCalendarEvent](ctx, s, calendarPath(r.Connection, r.Input.CalendarID)+"/calendarView", q, r.Input.TimeZone, r.Input.Cursor, limit)
	if err != nil {
		return calendarResult(s, out, err)
	}
	out = calendar.EventsPage{Items: []calendar.Event{}, NextCursor: next, Complete: next == "", TimeZone: r.Input.TimeZone}
	seen := map[string]bool{}
	for _, item := range items {
		if item.Cancelled != nil && *item.Cancelled {
			continue
		}
		if seen[item.ID] || item.Type == "seriesMaster" {
			return calendarResult(s, calendar.EventsPage{}, calendarInvalidResponse("invalid calendar view event"))
		}
		seen[item.ID] = true
		event, err := s.event(ctx, item, r.Input.CalendarID, r.Input.TimeZone, false)
		if err != nil {
			return calendarResult(s, calendar.EventsPage{}, err)
		}
		out.Items = append(out.Items, event)
	}
	return calendarResult(s, out, nil)
}

func (p *provider) calendarEvent(ctx context.Context, r connector.TypedRequest[calendar.EventRequest]) (connector.TypedResult[calendar.Event], error) {
	s := newCalendarSession(p, r.Connection, r.Secrets)
	var out calendar.Event
	if err := r.Input.Validate(); err != nil {
		return calendarResult(s, out, permanent("calendar.invalid_request", err.Error()))
	}
	var item graphCalendarEvent
	err := s.get(ctx, calendarPath(r.Connection, r.Input.CalendarID)+"/events/"+url.PathEscape(r.Input.EventID), url.Values{"$select": {calendarEventFields + ",body"}}, "UTC", &item)
	if err != nil {
		return calendarResult(s, out, err)
	}
	if item.ID != r.Input.EventID {
		return calendarResult(s, out, calendarInvalidResponse("event identity mismatch"))
	}
	out, err = s.event(ctx, item, r.Input.CalendarID, r.Input.TimeZone, true)
	return calendarResult(s, out, err)
}

// getSchedule addresses mailbox schedules, not selected secondary calendars.
// Enumerate each selected calendarView, with a strict page bound and no Free
// conclusion unless every page and every busy status is positively known.
func (p *provider) calendarAvailability(ctx context.Context, r connector.TypedRequest[calendar.AvailabilityRequest]) (connector.TypedResult[calendar.Availability], error) {
	s := newCalendarSession(p, r.Connection, r.Secrets)
	var out calendar.Availability
	if err := r.Input.Validate(); err != nil {
		return calendarResult(s, out, permanent("calendar.invalid_request", err.Error()))
	}
	calendars := make([]calendar.CalendarBusy, 0, len(r.Input.CalendarIDs))
	for _, id := range r.Input.CalendarIDs {
		busy := calendar.CalendarBusy{CalendarID: id, Busy: []calendar.Window{}}
		q := calendarViewQuery(r.Input.Window, 100, calendarBusyFields)
		cursor, seen := "", map[string]bool{}
		seenEvents := map[string]bool{}
		for page := 0; page < calendarAvailabilityPageLimit; page++ {
			items, next, err := readCalendarPage[graphCalendarEvent](ctx, s, calendarPath(r.Connection, id)+"/calendarView", q, "UTC", cursor, 100)
			if err != nil {
				return calendarResult(s, out, err)
			}
			for _, item := range items {
				if item.Cancelled != nil && *item.Cancelled {
					continue
				}
				if !calendar.ValidID(item.ID) || item.Cancelled == nil || item.Type == "seriesMaster" || seenEvents[item.ID] {
					busy.ErrorCodes = append(busy.ErrorCodes, "invalid_event")
					continue
				}
				seenEvents[item.ID] = true
				transparency := calendarTransparency(item.ShowAs)
				if transparency == "unknown" {
					busy.ErrorCodes = append(busy.ErrorCodes, "unknown_busy_status")
					continue
				}
				if transparency == "transparent" {
					continue
				}
				a, errA := item.Start.utcInstant()
				b, errB := item.End.utcInstant()
				if errA != nil || errB != nil || b.Before(a) {
					busy.ErrorCodes = append(busy.ErrorCodes, "invalid_event_time")
					continue
				}
				if b.After(a) {
					busy.Busy = append(busy.Busy, calendar.Window{Start: a.Format(time.RFC3339Nano), End: b.Format(time.RFC3339Nano)})
				}
			}
			if next == "" {
				busy.Complete = len(busy.ErrorCodes) == 0
				break
			}
			if seen[next] {
				busy.ErrorCodes = append(busy.ErrorCodes, "pagination_loop")
				break
			}
			seen[next], cursor = true, next
			if page == calendarAvailabilityPageLimit-1 {
				busy.ErrorCodes = append(busy.ErrorCodes, "page_limit")
			}
		}
		calendars = append(calendars, busy)
	}
	out, err := calendar.ResolveAvailability(r.Input, calendars)
	return calendarResult(s, out, err)
}
