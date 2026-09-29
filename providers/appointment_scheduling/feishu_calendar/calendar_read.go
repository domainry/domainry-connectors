package feishucalendar

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	calendar "github.com/domainry/domainry-connector-sdk/calendar"
)

var (
	CalendarList         = calendarReadOperation[calendar.PageRequest, calendar.CalendarsPage](calendar.ListOperationKey)
	CalendarEvents       = calendarReadOperation[calendar.EventsRequest, calendar.EventsPage](calendar.EventsOperationKey)
	CalendarEvent        = calendarReadOperation[calendar.EventRequest, calendar.Event](calendar.EventOperationKey)
	CalendarAvailability = calendarReadOperation[calendar.AvailabilityRequest, calendar.Availability](calendar.AvailabilityOperationKey)
)

func calendarReadOperation[I, O any](key string) connector.CallOperation[I, O] {
	return connector.CallOperation[I, O]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: key, ContractSHA256: calendar.OperationSHA256(key), Reliability: readReliability()}
}

func readResult[O any](raw connector.TypedResult[Response], output O, err error) (connector.TypedResult[O], error) {
	return connector.TypedResult[O]{Output: output, ResponseRef: raw.ResponseRef, SecretUpdates: raw.SecretUpdates, ResourceHealth: raw.ResourceHealth}, err
}

func (p *provider) calendarList(ctx context.Context, request connector.TypedRequest[calendar.PageRequest]) (connector.TypedResult[calendar.CalendarsPage], error) {
	var output calendar.CalendarsPage
	if err := request.Input.Validate(); err != nil {
		return readResult(connector.TypedResult[Response]{}, output, permanent("calendar.invalid_request", err.Error()))
	}
	pageSize := request.Input.PageSize()
	providerPageSize := pageSize
	if providerPageSize < 50 {
		providerPageSize = 50
	}
	query := url.Values{"page_size": {strconv.Itoa(providerPageSize)}}
	if request.Input.Cursor != "" {
		query.Set("page_token", request.Input.Cursor)
	}
	session := p.newAPISession(request.Connection, request.Secrets)
	raw, err := session.call(ctx, http.MethodGet, "/open-apis/calendar/v4/calendars", query, nil, false)
	if err != nil {
		return readResult(raw, output, err)
	}
	data := nested(raw.Output, "data")
	items, itemsErr := responseMaps(data["calendar_list"])
	hasMore, hasMoreOK := data["has_more"].(bool)
	next := mapString(data, "page_token")
	if itemsErr != nil || !hasMoreOK || len(items) > providerPageSize || len(next) > 8192 || hasMore && next == "" || !hasMore && next != "" {
		return readResult(raw, output, permanent("calendar.invalid_response", "invalid Feishu calendar page"))
	}
	output = calendar.CalendarsPage{Items: []calendar.Calendar{}, NextCursor: next, Complete: !hasMore}
	for _, item := range items {
		id := mapString(item, "calendar_id")
		if !calendar.ValidID(id) {
			return readResult(raw, calendar.CalendarsPage{}, permanent("calendar.invalid_response", "Feishu calendar identifier is invalid"))
		}
		if deleted, _ := item["is_deleted"].(bool); deleted {
			continue
		}
		name := mapString(item, "summary_alias")
		if name == "" {
			name = mapString(item, "summary")
		}
		output.Items = append(output.Items, calendar.Calendar{
			ID: id, Name: name, Primary: strings.EqualFold(mapString(item, "type"), "primary"), AccessRole: strings.ToLower(mapString(item, "role")),
		})
	}
	return readResult(raw, output, nil)
}

type feishuTimeInfo struct {
	Date      string `json:"date"`
	DateTime  string `json:"date_time"`
	Timestamp string `json:"timestamp"`
	TimeZone  string `json:"timezone"`
}

type feishuAttendee struct {
	Type            string `json:"type"`
	AttendeeID      string `json:"attendee_id"`
	UserID          string `json:"user_id"`
	ThirdPartyEmail string `json:"third_party_email"`
	DisplayName     string `json:"display_name"`
	RSVPStatus      string `json:"rsvp_status"`
	IsOptional      bool   `json:"is_optional"`
	IsOrganizer     bool   `json:"is_organizer"`
	IsExternal      bool   `json:"is_external"`
}

type feishuEvent struct {
	ID                  string          `json:"event_id"`
	OrganizerCalendarID string          `json:"organizer_calendar_id"`
	Summary             string          `json:"summary"`
	Description         string          `json:"description"`
	DescriptionRich     string          `json:"description_rich"`
	Start               *feishuTimeInfo `json:"start_time"`
	End                 *feishuTimeInfo `json:"end_time"`
	Status              string          `json:"status"`
	Recurrence          string          `json:"recurrence"`
	RecurringEventID    string          `json:"recurring_event_id"`
	IsException         bool            `json:"is_exception"`
	AppLink             string          `json:"app_link"`
	FreeBusyStatus      string          `json:"free_busy_status"`
	Location            struct {
		Name    string `json:"name"`
		Address string `json:"address"`
	} `json:"location"`
	VChat struct {
		MeetingURL string `json:"meeting_url"`
	} `json:"vchat"`
	EventOrganizer *struct {
		UserID      string `json:"user_id"`
		DisplayName string `json:"display_name"`
	} `json:"event_organizer"`
	Attendees []feishuAttendee `json:"attendees"`
}

func feishuResponseStatus(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "needs_action", "needsaction":
		return "needs_action"
	case "accept", "accepted":
		return "accepted"
	case "tentative":
		return "tentative"
	case "decline", "declined":
		return "declined"
	case "":
		return ""
	default:
		return "unknown"
	}
}

func decodeFeishuEvents(value any) ([]feishuEvent, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var events []feishuEvent
	if err = json.Unmarshal(raw, &events); err != nil {
		return nil, err
	}
	if events == nil {
		events = []feishuEvent{}
	}
	return events, nil
}

func decodeFeishuEvent(value any) (feishuEvent, error) {
	raw, err := json.Marshal(value)
	var event feishuEvent
	if err == nil {
		err = json.Unmarshal(raw, &event)
	}
	return event, err
}

func decodeFeishuAttendees(value any) ([]feishuAttendee, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var attendees []feishuAttendee
	if err = json.Unmarshal(raw, &attendees); err != nil {
		return nil, err
	}
	if attendees == nil {
		attendees = []feishuAttendee{}
	}
	return attendees, nil
}

func (value feishuEvent) normalized(calendarID, fallbackZone string) (calendar.Event, error) {
	if !calendar.ValidID(value.ID) || !calendar.ValidID(calendarID) || value.Start == nil || value.End == nil {
		return calendar.Event{}, permanent("calendar.invalid_response", "Feishu event identity or time is missing")
	}
	start, err := value.Start.moment(fallbackZone)
	if err != nil {
		return calendar.Event{}, err
	}
	end, err := value.End.moment(fallbackZone)
	if err != nil {
		return calendar.Event{}, err
	}
	description := value.Description
	if description == "" {
		description = value.DescriptionRich
	}
	location := value.Location.Name
	if location == "" {
		location = value.Location.Address
	}
	seriesID := value.RecurringEventID
	if seriesID == "" && strings.TrimSpace(value.Recurrence) != "" {
		seriesID = value.ID
	}
	transparency := "opaque"
	if strings.EqualFold(value.FreeBusyStatus, "free") {
		transparency = "transparent"
	}
	output := calendar.Event{
		ID: value.ID, CalendarID: calendarID, Title: value.Summary, Description: description,
		Location: location, URL: value.AppLink, MeetingURL: value.VChat.MeetingURL, Status: strings.ToLower(value.Status), Start: start, End: end,
		SeriesID: seriesID, Transparency: transparency,
	}
	if strings.TrimSpace(value.Recurrence) != "" {
		output.Recurrence = []string{value.Recurrence}
	}
	if value.EventOrganizer != nil {
		id := strings.TrimSpace(value.EventOrganizer.UserID)
		if id == "" {
			id = strings.TrimSpace(value.OrganizerCalendarID)
		}
		if id != "" {
			organizer := calendar.Participant{ID: id, DisplayName: value.EventOrganizer.DisplayName, Role: "organizer"}
			output.Organizer = &organizer
		}
	}
	for _, attendee := range value.Attendees {
		id := strings.TrimSpace(attendee.UserID)
		if id == "" {
			id = strings.TrimSpace(attendee.AttendeeID)
		}
		role := "required"
		switch {
		case attendee.IsOrganizer:
			role = "organizer"
		case strings.EqualFold(attendee.Type, "resource"):
			role = "resource"
		case attendee.IsOptional:
			role = "optional"
		}
		output.Attendees = append(output.Attendees, calendar.Participant{
			ID: id, Email: strings.ToLower(strings.TrimSpace(attendee.ThirdPartyEmail)), DisplayName: attendee.DisplayName,
			Role: role, ResponseStatus: feishuResponseStatus(attendee.RSVPStatus), External: attendee.IsExternal,
		})
	}
	if value.IsException && value.RecurringEventID != "" {
		if original, ok := feishuOriginalStart(value.ID, start.TimeZone, fallbackZone); ok {
			output.OriginalStart = &original
		}
	}
	if err = output.Validate(); err != nil {
		return calendar.Event{}, permanent("calendar.invalid_response", "Feishu event is invalid")
	}
	return output, nil
}

func (value *feishuTimeInfo) moment(fallbackZone string) (calendar.Moment, error) {
	if value == nil {
		return calendar.Moment{}, permanent("calendar.invalid_response", "Feishu event time is missing")
	}
	zone := strings.TrimSpace(value.TimeZone)
	if zone == "" {
		zone = fallbackZone
	}
	if value.Date != "" {
		moment := calendar.Moment{Date: value.Date, TimeZone: zone}
		if err := moment.Validate(); err != nil {
			return calendar.Moment{}, permanent("calendar.invalid_response", "Feishu all-day event date is invalid")
		}
		return moment, nil
	}
	dateTime := strings.TrimSpace(value.DateTime)
	if dateTime != "" {
		if _, err := time.Parse(time.RFC3339, dateTime); err != nil {
			dateTime, err = calendar.LocalDateTime(dateTime, zone)
			if err != nil {
				return calendar.Moment{}, permanent("calendar.invalid_response", "Feishu event local time is ambiguous")
			}
		}
		moment := calendar.Moment{DateTime: dateTime, TimeZone: zone}
		if err := moment.Validate(); err != nil {
			return calendar.Moment{}, permanent("calendar.invalid_response", "Feishu event time is invalid")
		}
		return moment, nil
	}
	seconds, err := strconv.ParseInt(strings.TrimSpace(value.Timestamp), 10, 64)
	if err != nil || seconds <= 0 {
		return calendar.Moment{}, permanent("calendar.invalid_response", "Feishu event timestamp is invalid")
	}
	location, err := time.LoadLocation(zone)
	if err != nil {
		return calendar.Moment{}, permanent("calendar.invalid_response", "Feishu event timezone is invalid")
	}
	return calendar.Moment{DateTime: time.Unix(seconds, 0).In(location).Format(time.RFC3339), TimeZone: zone}, nil
}

func feishuOriginalStart(eventID, zone, fallbackZone string) (calendar.Moment, bool) {
	index := strings.LastIndexByte(eventID, '_')
	if index < 0 || index == len(eventID)-1 {
		return calendar.Moment{}, false
	}
	seconds, err := strconv.ParseInt(eventID[index+1:], 10, 64)
	if err != nil || seconds <= 0 {
		return calendar.Moment{}, false
	}
	if zone == "" {
		zone = fallbackZone
	}
	location, err := time.LoadLocation(zone)
	if err != nil {
		return calendar.Moment{}, false
	}
	return calendar.Moment{DateTime: time.Unix(seconds, 0).In(location).Format(time.RFC3339), TimeZone: zone}, true
}

func (p *provider) calendarEvents(ctx context.Context, request connector.TypedRequest[calendar.EventsRequest]) (connector.TypedResult[calendar.EventsPage], error) {
	var output calendar.EventsPage
	if err := request.Input.Validate(); err != nil {
		return readResult(connector.TypedResult[Response]{}, output, permanent("calendar.invalid_request", err.Error()))
	}
	start, end, err := request.Input.Window.Instants()
	if err != nil {
		return readResult(connector.TypedResult[Response]{}, output, permanent("calendar.invalid_request", err.Error()))
	}
	pageSize := (calendar.PageRequest{Limit: request.Input.Limit}).PageSize()
	providerPageSize := pageSize
	if providerPageSize < 10 {
		providerPageSize = 10
	}
	query := url.Values{"page_size": {strconv.Itoa(providerPageSize)}, "user_id_type": {"open_id"}}
	if request.Input.Cursor != "" {
		query.Set("page_token", request.Input.Cursor)
	} else {
		query.Set("start_time", strconv.FormatInt(start.Unix(), 10))
		query.Set("end_time", strconv.FormatInt(end.Unix(), 10))
	}
	session := p.newAPISession(request.Connection, request.Secrets)
	raw, err := session.call(ctx, http.MethodGet, "/open-apis/calendar/v4/calendars/"+url.PathEscape(request.Input.CalendarID)+"/events", query, nil, false)
	if err != nil {
		return readResult(raw, output, err)
	}
	data := nested(raw.Output, "data")
	events, decodeErr := decodeFeishuEvents(data["items"])
	hasMore, hasMoreOK := data["has_more"].(bool)
	next := mapString(data, "page_token")
	if decodeErr != nil || !hasMoreOK || len(events) > providerPageSize || len(next) > 8192 || hasMore && next == "" || !hasMore && next != "" {
		return readResult(raw, output, permanent("calendar.invalid_response", "invalid Feishu event page"))
	}
	output = calendar.EventsPage{Items: []calendar.Event{}, NextCursor: next, Complete: !hasMore, TimeZone: request.Input.TimeZone}
	for _, event := range events {
		normalized, normalizeErr := event.normalized(request.Input.CalendarID, request.Input.TimeZone)
		if normalizeErr != nil {
			return readResult(raw, calendar.EventsPage{}, normalizeErr)
		}
		output.Items = append(output.Items, normalized)
	}
	return readResult(raw, output, nil)
}

func (p *provider) calendarEvent(ctx context.Context, request connector.TypedRequest[calendar.EventRequest]) (connector.TypedResult[calendar.Event], error) {
	var output calendar.Event
	if err := request.Input.Validate(); err != nil {
		return readResult(connector.TypedResult[Response]{}, output, permanent("calendar.invalid_request", err.Error()))
	}
	query := url.Values{"user_id_type": {"open_id"}}
	session := p.newAPISession(request.Connection, request.Secrets)
	raw, err := session.call(ctx, http.MethodGet, "/open-apis/calendar/v4/calendars/"+url.PathEscape(request.Input.CalendarID)+"/events/"+url.PathEscape(request.Input.EventID), query, nil, false)
	if err != nil {
		return readResult(raw, output, err)
	}
	event, decodeErr := decodeFeishuEvent(nested(raw.Output, "data")["event"])
	if decodeErr != nil || event.ID != request.Input.EventID {
		return readResult(raw, output, permanent("calendar.invalid_response", "Feishu event identity is invalid"))
	}
	event.Attendees, err = p.calendarEventAttendees(ctx, session, request.Input.CalendarID, request.Input.EventID)
	if err != nil {
		raw.SecretUpdates = cloneStringMap(session.updates)
		return readResult(raw, output, err)
	}
	output, err = event.normalized(request.Input.CalendarID, request.Input.TimeZone)
	raw.SecretUpdates = cloneStringMap(session.updates)
	return readResult(raw, output, err)
}

func (p *provider) calendarEventAttendees(ctx context.Context, session *apiSession, calendarID, eventID string) ([]feishuAttendee, error) {
	const pageSize = 100
	attendees := make([]feishuAttendee, 0)
	pageToken := ""
	for page := 0; page < calendar.MaximumParticipants/pageSize; page++ {
		query := url.Values{"page_size": {strconv.Itoa(pageSize)}, "user_id_type": {"open_id"}}
		if pageToken != "" {
			query.Set("page_token", pageToken)
		}
		raw, err := session.call(ctx, http.MethodGet, "/open-apis/calendar/v4/calendars/"+url.PathEscape(calendarID)+"/events/"+url.PathEscape(eventID)+"/attendees", query, nil, false)
		if err != nil {
			return nil, err
		}
		data := nested(raw.Output, "data")
		items, decodeErr := decodeFeishuAttendees(data["items"])
		hasMore, hasMoreOK := data["has_more"].(bool)
		next := mapString(data, "page_token")
		if decodeErr != nil || !hasMoreOK || len(items) > pageSize || len(next) > 8192 || hasMore && (next == "" || next == pageToken) || !hasMore && next != "" || len(attendees)+len(items) > calendar.MaximumParticipants {
			return nil, permanent("calendar.invalid_response", "invalid Feishu event attendee page")
		}
		attendees = append(attendees, items...)
		if !hasMore {
			return attendees, nil
		}
		pageToken = next
	}
	return nil, permanent("calendar.invalid_response", "Feishu event exceeds the supported participant limit")
}

func (p *provider) calendarAvailability(ctx context.Context, request connector.TypedRequest[calendar.AvailabilityRequest]) (connector.TypedResult[calendar.Availability], error) {
	var output calendar.Availability
	if err := request.Input.Validate(); err != nil {
		return readResult(connector.TypedResult[Response]{}, output, permanent("calendar.invalid_request", err.Error()))
	}
	start, end, _ := request.Input.Window.Instants()
	session := p.newAPISession(request.Connection, request.Secrets)
	values := make([]calendar.CalendarBusy, 0, len(request.Input.CalendarIDs))
	lastRaw := connector.TypedResult[Response]{}
	for _, calendarID := range request.Input.CalendarIDs {
		busy := calendar.CalendarBusy{CalendarID: calendarID, Busy: []calendar.Window{}, Complete: true}
		for cursor := start; cursor.Before(end); {
			chunkEnd := cursor.Add(39 * 24 * time.Hour)
			if chunkEnd.After(end) {
				chunkEnd = end
			}
			query := url.Values{
				"start_time": {strconv.FormatInt(cursor.Unix(), 10)}, "end_time": {strconv.FormatInt(chunkEnd.Unix(), 10)}, "user_id_type": {"open_id"},
			}
			raw, err := session.call(ctx, http.MethodGet, "/open-apis/calendar/v4/calendars/"+url.PathEscape(calendarID)+"/events/instance_view", query, nil, false)
			lastRaw = raw
			if err != nil {
				return readResult(raw, output, err)
			}
			events, decodeErr := decodeFeishuEvents(nested(raw.Output, "data")["items"])
			if decodeErr != nil {
				return readResult(raw, output, permanent("calendar.invalid_response", "invalid Feishu instance view"))
			}
			for _, event := range events {
				if strings.EqualFold(event.Status, "cancelled") || strings.EqualFold(event.FreeBusyStatus, "free") {
					continue
				}
				normalized, normalizeErr := event.normalized(calendarID, request.Input.TimeZone)
				if normalizeErr != nil {
					return readResult(raw, output, normalizeErr)
				}
				window, windowErr := eventWindow(normalized, request.Input.TimeZone)
				if windowErr != nil {
					return readResult(raw, output, permanent("calendar.invalid_response", "invalid Feishu availability interval"))
				}
				busy.Busy = append(busy.Busy, window)
			}
			cursor = chunkEnd
		}
		values = append(values, busy)
	}
	var err error
	output, err = calendar.ResolveAvailability(request.Input, values)
	if err != nil {
		err = permanent("calendar.invalid_response", "invalid Feishu availability response")
	}
	lastRaw.SecretUpdates = cloneStringMap(session.updates)
	return readResult(lastRaw, output, err)
}

func eventWindow(event calendar.Event, zone string) (calendar.Window, error) {
	if event.Start.DateTime != "" {
		return calendar.Window{Start: event.Start.DateTime, End: event.End.DateTime}, nil
	}
	location, err := time.LoadLocation(zone)
	if err != nil {
		return calendar.Window{}, err
	}
	startDate, startErr := time.Parse(time.DateOnly, event.Start.Date)
	endDate, endErr := time.Parse(time.DateOnly, event.End.Date)
	if startErr != nil || endErr != nil {
		return calendar.Window{}, errors.New("invalid all-day interval")
	}
	start := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, location)
	end := time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 0, 0, 0, 0, location)
	return calendar.Window{Start: start.Format(time.RFC3339), End: end.Format(time.RFC3339)}, nil
}

func responseMaps(value any) ([]map[string]any, error) {
	items, ok := value.([]any)
	if !ok && value != nil {
		return nil, errors.New("expected response array")
	}
	output := make([]map[string]any, 0, len(items))
	for _, item := range items {
		value, ok := item.(map[string]any)
		if !ok {
			return nil, errors.New("expected response object")
		}
		output = append(output, value)
	}
	return output, nil
}
