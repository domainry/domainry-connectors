package microsoft

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/calendarwrite"
	"github.com/domainry/domainry-connectors/internal/textcontent"
)

var (
	CalendarEventInspect = connector.CallOperation[calendarwrite.InspectRequest, calendarwrite.Snapshot]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: calendarwrite.InspectOperationKey, ContractSHA256: calendarwrite.OperationSHA256(calendarwrite.InspectOperationKey), Reliability: readReliability()}
	CalendarEventCreate  = connector.CallOperation[calendarwrite.CreateRequest, calendarwrite.Result]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: calendarwrite.CreateOperationKey, ContractSHA256: calendarwrite.OperationSHA256(calendarwrite.CreateOperationKey), Reliability: writeReliability()}
	CalendarEventUpdate  = connector.CallOperation[calendarwrite.UpdateRequest, calendarwrite.Result]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: calendarwrite.UpdateOperationKey, ContractSHA256: calendarwrite.OperationSHA256(calendarwrite.UpdateOperationKey), Reliability: writeReliability()}
)

type graphWriteAttendee struct {
	Type  string `json:"type"`
	Email struct {
		Address string `json:"address"`
		Name    string `json:"name"`
	} `json:"emailAddress"`
	Status json.RawMessage `json:"status"`
}

type graphWriteEvent struct {
	graphCalendarEvent
	ETag            string                `json:"@odata.etag"`
	Attendees       *[]graphWriteAttendee `json:"attendees"`
	HideAttendees   *bool                 `json:"hideAttendees"`
	IsOrganizer     *bool                 `json:"isOrganizer"`
	IsOnlineMeeting *bool                 `json:"isOnlineMeeting"`
}

const calendarWriteFields = calendarEventFields + ",body,attendees,hideAttendees,isOrganizer,isOnlineMeeting"

func (s *calendarSession) inspectWrite(ctx context.Context, in calendarwrite.InspectRequest) (graphWriteEvent, calendarwrite.Snapshot, error) {
	var item graphWriteEvent
	var out calendarwrite.Snapshot
	if err := s.get(ctx, calendarPath(s.connection, in.CalendarID)+"/events/"+url.PathEscape(in.EventID), url.Values{"$select": {calendarWriteFields}}, "UTC", &item); err != nil {
		return item, out, err
	}
	if item.ID != in.EventID || item.Attendees == nil || len(*item.Attendees) > calendarwrite.MaximumAttendees || item.HideAttendees == nil || *item.HideAttendees || item.IsOrganizer == nil || !*item.IsOrganizer || item.Body == nil {
		return item, out, permanent("calendar.write_target_unsupported", "calendar target does not expose complete organizer-owned event data")
	}
	kind := map[string]string{"singleInstance": "single", "seriesMaster": "series", "occurrence": "occurrence", "exception": "exception"}[item.Type]
	if kind == "" {
		return item, out, calendarInvalidResponse("unknown event kind")
	}
	normalized := item.graphCalendarEvent
	body := *item.Body
	switch strings.ToLower(body.Type) {
	case "text":
	case "html":
		text, err := textcontent.HTMLText(body.Content)
		if err != nil {
			return item, out, calendarInvalidResponse("invalid event body")
		}
		body.Content = text
	default:
		return item, out, calendarInvalidResponse("unsupported event body type")
	}
	body.Type = "text"
	normalized.Body = &body
	event, err := s.event(ctx, normalized, in.CalendarID, in.TimeZone, true)
	if err != nil {
		return item, out, err
	}
	out = calendarwrite.Snapshot{Event: event, Version: item.ETag, Kind: kind, Attendees: []calendarwrite.Attendee{}}
	for _, a := range *item.Attendees {
		out.Attendees = append(out.Attendees, calendarwrite.Attendee{Address: a.Email.Address, Name: a.Email.Name, Kind: a.Type})
	}
	if err = out.Validate(in); err != nil {
		return item, calendarwrite.Snapshot{}, calendarInvalidResponse("invalid event write snapshot")
	}
	return item, out, nil
}

func (p *provider) calendarEventInspect(ctx context.Context, r connector.TypedRequest[calendarwrite.InspectRequest]) (connector.TypedResult[calendarwrite.Snapshot], error) {
	s := newCalendarSession(p, r.Connection, r.Secrets)
	if err := r.Input.Validate(); err != nil {
		return calendarResult(s, calendarwrite.Snapshot{}, permanent("calendar.invalid_request", err.Error()))
	}
	_, out, err := s.inspectWrite(ctx, r.Input)
	return calendarResult(s, out, err)
}

func calendarWriteAttendees(values []calendarwrite.Attendee, previous *[]graphWriteAttendee) []map[string]any {
	out := make([]map[string]any, 0, len(values))
	for _, a := range values {
		item := map[string]any{"type": a.Kind, "emailAddress": map[string]string{"address": a.Address, "name": a.Name}}
		if previous != nil {
			for _, old := range *previous {
				if strings.EqualFold(old.Email.Address, a.Address) && len(old.Status) > 0 {
					item["status"] = old.Status
				}
			}
		}
		out = append(out, item)
	}
	return out
}

func (p *provider) calendarEventCreate(ctx context.Context, r connector.TypedRequest[calendarwrite.CreateRequest]) (connector.TypedResult[calendarwrite.Result], error) {
	s := newCalendarSession(p, r.Connection, r.Secrets)
	if err := r.Input.Validate(); err != nil {
		return calendarResult(s, calendarwrite.Result{}, permanent("calendar.invalid_request", err.Error()))
	}
	if err := validateWriteEnvelope(r.Connection, r.RequestRef); err != nil {
		return calendarResult(s, calendarwrite.Result{}, err)
	}
	e := r.Input.Event
	start, end, err := s.writeMoments(ctx, e.Start, e.End, false)
	if err != nil {
		return calendarResult(s, calendarwrite.Result{}, err)
	}
	body := map[string]any{"subject": e.Title, "body": map[string]string{"contentType": "text", "content": e.Description}, "location": map[string]string{"displayName": e.Location}, "start": start, "end": end, "isAllDay": e.Start.Date != "", "attendees": calendarWriteAttendees(e.Attendees, nil), "transactionId": writeCorrelation(r.Connection, r.RequestRef, calendarwrite.CreateOperationKey, r.Input.CalendarID)}
	raw, err := p.executeGraphWithRefresh(ctx, r.Connection, s.secrets, http.MethodPost, calendarPath(r.Connection, r.Input.CalendarID)+"/events", nil, body, "", `IdType="ImmutableId"`, `outlook.timezone="UTC"`)
	s.state = mergeWriteState(s.state, raw)
	return calendarMutationResult(s, err, calendarwrite.CreateOperationKey, r.RequestRef, r.Input.CalendarID, "")
}

func (p *provider) calendarEventUpdate(ctx context.Context, r connector.TypedRequest[calendarwrite.UpdateRequest]) (connector.TypedResult[calendarwrite.Result], error) {
	s := newCalendarSession(p, r.Connection, r.Secrets)
	if err := r.Input.Validate(); err != nil {
		return calendarResult(s, calendarwrite.Result{}, permanent("calendar.invalid_request", err.Error()))
	}
	if err := validateWriteEnvelope(r.Connection, r.RequestRef); err != nil {
		return calendarResult(s, calendarwrite.Result{}, err)
	}
	item, snapshot, err := s.inspectWrite(ctx, calendarwrite.InspectRequest{CalendarID: r.Input.CalendarID, EventID: r.Input.EventID, TimeZone: "UTC"})
	if err != nil {
		return calendarResult(s, calendarwrite.Result{}, err)
	}
	if err = r.Input.ValidateAgainst(snapshot); err != nil {
		return calendarResult(s, calendarwrite.Result{}, permanent("calendar.version_or_scope_conflict", err.Error()))
	}
	change := r.Input.Changes
	body := map[string]any{}
	if change.Title != nil {
		body["subject"] = *change.Title
	}
	if change.Description != nil {
		// Graph's online-meeting blob is not a declared plain-text field. Its
		// format has no stable extraction contract; replacing it disables joining.
		if item.IsOnlineMeeting == nil || *item.IsOnlineMeeting {
			return calendarResult(s, calendarwrite.Result{}, permanent("calendar.online_body_unsupported", "online meeting descriptions require a native editor that preserves the meeting blob"))
		}
		body["body"] = map[string]string{"contentType": "text", "content": *change.Description}
	}
	if change.Location != nil {
		body["location"] = map[string]string{"displayName": *change.Location}
	}
	if change.Start != nil {
		body["start"], body["end"], err = s.writeMoments(ctx, *change.Start, *change.End, r.Input.Scope == calendarwrite.ScopeSeries)
		if err != nil {
			return calendarResult(s, calendarwrite.Result{}, err)
		}
		body["isAllDay"] = change.Start.Date != ""
	}
	if change.Attendees != nil {
		body["attendees"] = calendarWriteAttendees(*change.Attendees, item.Attendees)
	}
	raw, err := p.executeGraphWithRefresh(ctx, r.Connection, s.secrets, http.MethodPatch, calendarPath(r.Connection, r.Input.CalendarID)+"/events/"+url.PathEscape(r.Input.EventID), nil, body, r.Input.ExpectedVersion, `IdType="ImmutableId"`, `outlook.timezone="UTC"`)
	s.state = mergeWriteState(s.state, raw)
	return calendarMutationResult(s, err, calendarwrite.UpdateOperationKey, r.RequestRef, r.Input.CalendarID, r.Input.EventID)
}

func calendarMutationResult(s *calendarSession, err error, operation, ref, calendarID, eventID string) (connector.TypedResult[calendarwrite.Result], error) {
	if err != nil {
		return calendarResult(s, calendarwrite.Result{}, err)
	}
	expectedStatus, outcome := "http:201", "created"
	if operation == calendarwrite.UpdateOperationKey {
		expectedStatus, outcome = "http:200", "updated"
	}
	var native graphWriteEvent
	b, marshalErr := json.Marshal(s.state.Output)
	if marshalErr == nil && json.Unmarshal(b, &native) == nil && s.state.ResponseRef == expectedStatus {
		out := calendarwrite.Result{RequestRef: ref, Outcome: outcome, CalendarID: calendarID, EventID: native.ID, Version: native.ETag, URL: native.URL, Notifications: "requested"}
		if out.Validate(operation, ref, calendarID, eventID) == nil {
			return calendarResult(s, out, nil)
		}
	}
	return calendarResult(s, calendarwrite.Result{}, connector.UncertainError("microsoft.calendar.write_response_invalid", errors.New("Graph may have applied the event mutation but returned no valid receipt")))
}
