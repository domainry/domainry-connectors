package google

import (
	"context"
	"errors"
	"html"
	"net/http"
	"net/url"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/calendar"
	"github.com/domainry/domainry-connector-sdk/calendarwrite"
	"github.com/domainry/domainry-connectors/internal/textcontent"
)

var (
	CalendarEventInspect = connector.CallOperation[calendarwrite.InspectRequest, calendarwrite.Snapshot]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: calendarwrite.InspectOperationKey, ContractSHA256: calendarwrite.OperationSHA256(calendarwrite.InspectOperationKey), Reliability: readReliability()}
	CalendarEventCreate  = connector.CallOperation[calendarwrite.CreateRequest, calendarwrite.Result]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: calendarwrite.CreateOperationKey, ContractSHA256: calendarwrite.OperationSHA256(calendarwrite.CreateOperationKey), Reliability: writeReliability()}
	CalendarEventUpdate  = connector.CallOperation[calendarwrite.UpdateRequest, calendarwrite.Result]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: calendarwrite.UpdateOperationKey, ContractSHA256: calendarwrite.OperationSHA256(calendarwrite.UpdateOperationKey), Reliability: writeReliability()}
)

type googleWriteAttendee struct {
	Email            string `json:"email"`
	Name             string `json:"displayName"`
	Optional         bool   `json:"optional"`
	Resource         bool   `json:"resource"`
	AdditionalGuests int    `json:"additionalGuests"`
	ResponseStatus   string `json:"responseStatus"`
	Comment          string `json:"comment"`
}

type googleWriteEvent struct {
	googleCalendarEvent
	ETag                    string                `json:"etag"`
	EventType               string                `json:"eventType"`
	Recurrence              []string              `json:"recurrence"`
	Attendees               []googleWriteAttendee `json:"attendees"`
	AttendeesOmitted        bool                  `json:"attendeesOmitted"`
	GuestsCanSeeOtherGuests *bool                 `json:"guestsCanSeeOtherGuests"`
}

func (e googleWriteEvent) snapshot(r calendarwrite.InspectRequest) (calendarwrite.Snapshot, error) {
	var out calendarwrite.Snapshot
	if e.Kind != "calendar#event" || e.ID != r.EventID || len(e.Attendees) > calendarwrite.MaximumAttendees || e.AttendeesOmitted || e.GuestsCanSeeOtherGuests != nil && !*e.GuestsCanSeeOtherGuests || e.EventType != "" && e.EventType != "default" {
		return out, permanent("calendar.write_target_unsupported", "calendar target does not expose a complete supported event")
	}
	event, err := e.event(r.CalendarID)
	if err != nil {
		return out, err
	}
	event.Description, err = textcontent.HTMLText(e.Description)
	if err != nil {
		return out, permanent("calendar.invalid_response", "calendar description cannot be converted to text")
	}
	out = calendarwrite.Snapshot{Event: event, Version: e.ETag, Kind: "single", Attendees: []calendarwrite.Attendee{}}
	if len(e.Recurrence) > 0 {
		out.Kind = "series"
	}
	if e.SeriesID != "" {
		if len(e.Recurrence) > 0 {
			return calendarwrite.Snapshot{}, permanent("calendar.invalid_response", "conflicting recurrence metadata")
		}
		// Google exposes instances and modified instances with the same fields;
		// both remain exact instance updates, never implicit series updates.
		out.Kind = "occurrence"
	}
	for _, a := range e.Attendees {
		if a.AdditionalGuests != 0 || a.Resource && a.Optional {
			return calendarwrite.Snapshot{}, permanent("calendar.write_target_unsupported", "calendar attendees cannot be represented completely")
		}
		kind := "required"
		if a.Optional {
			kind = "optional"
		}
		if a.Resource {
			kind = "resource"
		}
		out.Attendees = append(out.Attendees, calendarwrite.Attendee{Address: a.Email, Name: a.Name, Kind: kind})
	}
	if err = out.Validate(r); err != nil {
		return calendarwrite.Snapshot{}, permanent("calendar.invalid_response", err.Error())
	}
	return out, nil
}

func calendarWritePath(c connector.Connection, calendarID, eventID string) string {
	path := apiBase(c) + "/calendar/v3/calendars/" + url.PathEscape(calendarID) + "/events"
	if eventID != "" {
		path += "/" + url.PathEscape(eventID)
	}
	return path
}

func (p *provider) inspectCalendarWrite(ctx context.Context, c connector.Connection, secrets map[string]string, in calendarwrite.InspectRequest) (connector.TypedResult[Response], googleWriteEvent, calendarwrite.Snapshot, error) {
	var event googleWriteEvent
	raw, err := p.executeWithRefresh(ctx, c, secrets, http.MethodGet, calendarWritePath(c, in.CalendarID, in.EventID), url.Values{"timeZone": {in.TimeZone}}, nil, false)
	if err != nil {
		return raw, event, calendarwrite.Snapshot{}, err
	}
	if err = decodeCalendarResponse(raw.Output, &event); err != nil {
		return raw, event, calendarwrite.Snapshot{}, err
	}
	out, err := event.snapshot(in)
	return raw, event, out, err
}

func (p *provider) calendarEventInspect(ctx context.Context, r connector.TypedRequest[calendarwrite.InspectRequest]) (connector.TypedResult[calendarwrite.Snapshot], error) {
	if err := r.Input.Validate(); err != nil {
		return providerResult(empty(), calendarwrite.Snapshot{}, permanent("calendar.invalid_request", err.Error()))
	}
	raw, _, out, err := p.inspectCalendarWrite(ctx, r.Connection, r.Secrets, r.Input)
	return providerResult(raw, out, err)
}

func calendarWriteMoment(m calendar.Moment, patch bool) map[string]any {
	out := map[string]any{"timeZone": m.TimeZone}
	if m.Date != "" {
		out["date"] = m.Date
		if patch {
			out["dateTime"] = nil
		}
	} else {
		out["dateTime"] = m.DateTime
		if patch {
			out["date"] = nil
		}
	}
	return out
}

// Description is a plain-text input; Google interprets description as HTML.
func calendarWriteDescription(s string) string {
	return strings.ReplaceAll(html.EscapeString(strings.ReplaceAll(s, "\r\n", "\n")), "\n", "<br>")
}

func calendarWriteAttendees(values []calendarwrite.Attendee, previous []googleWriteAttendee) ([]map[string]any, error) {
	out := make([]map[string]any, 0, len(values))
	for _, a := range values {
		item := map[string]any{"email": a.Address, "displayName": a.Name, "optional": a.Kind == "optional", "resource": a.Kind == "resource"}
		for _, old := range previous {
			if !strings.EqualFold(old.Email, a.Address) {
				continue
			}
			if old.Resource != (a.Kind == "resource") {
				return nil, permanent("calendar.resource_kind_immutable", "Google cannot change an existing attendee's resource kind")
			}
			// Replacing an array must not reset existing RSVP state/comments.
			if old.ResponseStatus != "" {
				item["responseStatus"] = old.ResponseStatus
			}
			if old.Comment != "" {
				item["comment"] = old.Comment
			}
		}
		out = append(out, item)
	}
	return out, nil
}

func (p *provider) calendarEventCreate(ctx context.Context, r connector.TypedRequest[calendarwrite.CreateRequest]) (connector.TypedResult[calendarwrite.Result], error) {
	if err := r.Input.Validate(); err != nil {
		return providerResult(empty(), calendarwrite.Result{}, permanent("calendar.invalid_request", err.Error()))
	}
	if err := validateWriteEnvelope(r.Connection, r.RequestRef); err != nil {
		return providerResult(empty(), calendarwrite.Result{}, err)
	}
	id := writeCorrelation(r.Connection, r.RequestRef, calendarwrite.CreateOperationKey, r.Input.CalendarID)
	e := r.Input.Event
	attendees, err := calendarWriteAttendees(e.Attendees, nil)
	if err != nil {
		return providerResult(empty(), calendarwrite.Result{}, err)
	}
	body := map[string]any{"id": id, "summary": e.Title, "description": calendarWriteDescription(e.Description), "location": e.Location, "start": calendarWriteMoment(e.Start, false), "end": calendarWriteMoment(e.End, false), "attendees": attendees}
	raw, err := p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodPost, calendarWritePath(r.Connection, r.Input.CalendarID, ""), url.Values{"sendUpdates": {"all"}}, body, true)
	return calendarMutationResult(raw, err, calendarwrite.CreateOperationKey, r.RequestRef, r.Input.CalendarID, id)
}

func (p *provider) calendarEventUpdate(ctx context.Context, r connector.TypedRequest[calendarwrite.UpdateRequest]) (connector.TypedResult[calendarwrite.Result], error) {
	if err := r.Input.Validate(); err != nil {
		return providerResult(empty(), calendarwrite.Result{}, permanent("calendar.invalid_request", err.Error()))
	}
	if err := validateWriteEnvelope(r.Connection, r.RequestRef); err != nil {
		return providerResult(empty(), calendarwrite.Result{}, err)
	}
	read, event, snapshot, err := p.inspectCalendarWrite(ctx, r.Connection, r.Secrets, calendarwrite.InspectRequest{CalendarID: r.Input.CalendarID, EventID: r.Input.EventID, TimeZone: "UTC"})
	if err != nil {
		return providerResult(read, calendarwrite.Result{}, err)
	}
	if err = r.Input.ValidateAgainst(snapshot); err != nil {
		return providerResult(read, calendarwrite.Result{}, permanent("calendar.version_or_scope_conflict", err.Error()))
	}
	change := r.Input.Changes
	body := map[string]any{}
	if change.Title != nil {
		body["summary"] = *change.Title
	}
	if change.Description != nil {
		body["description"] = calendarWriteDescription(*change.Description)
	}
	if change.Location != nil {
		body["location"] = *change.Location
	}
	if change.Start != nil {
		body["start"], body["end"] = calendarWriteMoment(*change.Start, true), calendarWriteMoment(*change.End, true)
	}
	if change.Attendees != nil {
		body["attendees"], err = calendarWriteAttendees(*change.Attendees, event.Attendees)
		if err != nil {
			return providerResult(read, calendarwrite.Result{}, err)
		}
	}
	raw, err := p.executeWithPrecondition(ctx, r.Connection, writeSecrets(r.Secrets, read), http.MethodPatch, calendarWritePath(r.Connection, r.Input.CalendarID, r.Input.EventID), url.Values{"sendUpdates": {"all"}, "conferenceDataVersion": {"1"}}, body, true, r.Input.ExpectedVersion)
	return calendarMutationResult(mergeWriteState(read, raw), err, calendarwrite.UpdateOperationKey, r.RequestRef, r.Input.CalendarID, r.Input.EventID)
}

func calendarMutationResult(raw connector.TypedResult[Response], err error, operation, ref, calendarID, eventID string) (connector.TypedResult[calendarwrite.Result], error) {
	if err != nil {
		return providerResult(raw, calendarwrite.Result{}, err)
	}
	var event googleWriteEvent
	if err = decodeCalendarResponse(raw.Output, &event); err == nil {
		outcome := "created"
		if operation == calendarwrite.UpdateOperationKey {
			outcome = "updated"
		}
		out := calendarwrite.Result{RequestRef: ref, Outcome: outcome, CalendarID: calendarID, EventID: event.ID, Version: event.ETag, URL: event.URL, Notifications: "requested"}
		if event.Kind == "calendar#event" && out.Validate(operation, ref, calendarID, eventID) == nil {
			return providerResult(raw, out, nil)
		}
	}
	return providerResult(raw, calendarwrite.Result{}, connector.UncertainError("google.calendar.write_response_invalid", errors.New("Google may have applied the event mutation but returned no valid receipt")))
}
