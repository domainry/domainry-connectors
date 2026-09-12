package microsoft

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"time"
	"unicode"

	connector "github.com/domainry/domainry-connector-sdk"
	calendar "github.com/domainry/domainry-connector-sdk/calendar"
)

type graphCalendarMoment struct {
	DateTime string `json:"dateTime"`
	TimeZone string `json:"timeZone"`
}

func (m graphCalendarMoment) utcInstant() (time.Time, error) {
	if m.TimeZone != "UTC" {
		return time.Time{}, calendarInvalidResponse("calendar response did not use requested UTC zone")
	}
	value, err := calendar.LocalDateTime(m.DateTime, "UTC")
	if err == nil {
		return time.Parse(time.RFC3339Nano, value)
	}
	instant, err := time.Parse(time.RFC3339Nano, m.DateTime)
	if err != nil {
		return time.Time{}, calendarInvalidResponse("invalid calendar event time")
	}
	_, offset := instant.Zone()
	if offset != 0 {
		return time.Time{}, calendarInvalidResponse("calendar event UTC offset mismatch")
	}
	return instant.UTC(), nil
}

type graphCalendarEvent struct {
	ID       string `json:"id"`
	Title    string `json:"subject"`
	Location struct {
		Name string `json:"displayName"`
	} `json:"location"`
	URL           string              `json:"webLink"`
	Cancelled     *bool               `json:"isCancelled"`
	AllDay        *bool               `json:"isAllDay"`
	Start         graphCalendarMoment `json:"start"`
	End           graphCalendarMoment `json:"end"`
	SeriesID      string              `json:"seriesMasterId"`
	OriginalStart string              `json:"originalStart"`
	StartZone     string              `json:"originalStartTimeZone"`
	EndZone       string              `json:"originalEndTimeZone"`
	ShowAs        string              `json:"showAs"`
	ChangeKey     string              `json:"changeKey"`
	Type          string              `json:"type"`
	Body          *struct {
		Type    string `json:"contentType"`
		Content string `json:"content"`
	} `json:"body"`
}

func calendarTransparency(showAs string) string {
	switch showAs {
	case "free", "workingElsewhere":
		return "transparent"
	case "busy", "tentative", "oof":
		return "opaque"
	default:
		return "unknown"
	}
}

func calendarSourceZoneValid(zone string) bool {
	return zone != "" && len(zone) <= 128 && strings.TrimSpace(zone) == zone &&
		!strings.ContainsAny(zone, "\"\\") && strings.IndexFunc(zone, unicode.IsControl) < 0
}

func calendarDate(m graphCalendarMoment, zone string) (calendar.Moment, error) {
	t, err := time.Parse("2006-01-02T15:04:05", m.DateTime)
	if err != nil || m.TimeZone != zone || t.Hour() != 0 || t.Minute() != 0 || t.Second() != 0 || t.Nanosecond() != 0 {
		return calendar.Moment{}, calendarInvalidResponse("all-day event is not midnight in its source zone")
	}
	return calendar.Moment{Date: t.Format(time.DateOnly), TimeZone: zone}, nil
}

func (s *calendarSession) allDay(ctx context.Context, item graphCalendarEvent, calendarID string) (calendar.Moment, calendar.Moment, error) {
	if !calendarSourceZoneValid(item.StartZone) || item.EndZone != item.StartZone {
		return calendar.Moment{}, calendar.Moment{}, calendarInvalidResponse("all-day source zone is unavailable")
	}
	zone := item.StartZone
	if zone != "UTC" {
		// Graph resolves its own Windows/IANA/custom zone vocabulary. Do not guess
		// offsets or derive dates from the caller's unrelated display time zone.
		if item.ChangeKey == "" {
			return calendar.Moment{}, calendar.Moment{}, calendarInvalidResponse("all-day source revision is unavailable")
		}
		var local graphCalendarEvent
		err := s.get(ctx, calendarPath(s.connection, calendarID)+"/events/"+url.PathEscape(item.ID),
			url.Values{"$select": {"id,changeKey,isAllDay,isCancelled,start,end,originalStartTimeZone,originalEndTimeZone"}}, zone, &local)
		if err != nil {
			return calendar.Moment{}, calendar.Moment{}, err
		}
		if local.ID != item.ID || local.ChangeKey != item.ChangeKey || local.Cancelled == nil || *local.Cancelled || local.AllDay == nil || !*local.AllDay || local.StartZone != zone || local.EndZone != zone {
			return calendar.Moment{}, calendar.Moment{}, connector.RetryableError("microsoft.calendar.event_changed", errors.New("all-day event changed while reading its source dates"))
		}
		item = local
	}
	a, err := calendarDate(item.Start, zone)
	if err != nil {
		return calendar.Moment{}, calendar.Moment{}, err
	}
	b, err := calendarDate(item.End, zone)
	return a, b, err
}

func (s *calendarSession) event(ctx context.Context, item graphCalendarEvent, calendarID, zone string, detail bool) (calendar.Event, error) {
	if !calendar.ValidID(item.ID) || item.AllDay == nil || item.Cancelled == nil {
		return calendar.Event{}, calendarInvalidResponse("calendar event identity or flags missing")
	}
	if *item.Cancelled {
		return calendar.Event{}, permanent("calendar.event_cancelled", "calendar event is cancelled")
	}
	out := calendar.Event{ID: item.ID, CalendarID: calendarID, Title: item.Title, Location: item.Location.Name, URL: item.URL,
		Status: "confirmed", SeriesID: item.SeriesID, Transparency: calendarTransparency(item.ShowAs)}
	if detail {
		if item.Body == nil || !strings.EqualFold(item.Body.Type, "text") {
			return calendar.Event{}, calendarInvalidResponse("calendar event text body is unavailable")
		}
		out.Description = item.Body.Content
	}
	location, err := time.LoadLocation(zone)
	if err != nil {
		return calendar.Event{}, calendarInvalidResponse("invalid calendar display zone")
	}
	if *item.AllDay {
		out.Start, out.End, err = s.allDay(ctx, item, calendarID)
		if err != nil {
			return calendar.Event{}, err
		}
	} else {
		a, errA := item.Start.utcInstant()
		b, errB := item.End.utcInstant()
		if errA != nil || errB != nil {
			return calendar.Event{}, calendarInvalidResponse("invalid calendar event time")
		}
		out.Start = calendar.Moment{DateTime: a.In(location).Format(time.RFC3339Nano), TimeZone: zone}
		out.End = calendar.Moment{DateTime: b.In(location).Format(time.RFC3339Nano), TimeZone: zone}
	}
	if item.OriginalStart != "" {
		original, err := time.Parse(time.RFC3339Nano, item.OriginalStart)
		if err != nil {
			return calendar.Event{}, calendarInvalidResponse("invalid recurring event original start")
		}
		out.OriginalStart = &calendar.Moment{DateTime: original.In(location).Format(time.RFC3339Nano), TimeZone: zone}
	}
	if err := out.Validate(); err != nil {
		return calendar.Event{}, calendarInvalidResponse("invalid calendar event")
	}
	return out, nil
}
