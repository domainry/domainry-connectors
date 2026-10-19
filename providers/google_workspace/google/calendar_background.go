package google

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

const googleCalendarSyncTaskKey = "google_calendar_sync"

// Integration commits each page cursor atomically with its events. No page is
// skipped on restart and no token is advanced before the final page is accepted.
type calendarSyncState struct {
	CalendarIndex int                           `json:"calendar_index"`
	Calendars     map[string]calendarSyncCursor `json:"calendars"`
	Completed     int64                         `json:"completed"`
}
type calendarSyncCursor struct {
	SyncToken   string `json:"sync_token,omitempty"`
	PageToken   string `json:"page_token,omitempty"`
	Initialized bool   `json:"initialized"`
	WindowStart string `json:"window_start,omitempty"`
	WindowEnd   string `json:"window_end,omitempty"`
}

func calendarSyncIDs(config map[string]any) []string {
	ids := []string{}
	seen := map[string]bool{}
	for _, id := range strings.Split(backgroundConfig(config, "calendar_ids", "primary"), ",") {
		id = strings.TrimSpace(id)
		if id != "" && !seen[id] {
			ids = append(ids, id)
			seen[id] = true
		}
	}
	if len(ids) == 0 {
		return []string{"primary"}
	}
	return ids
}

func (p *provider) processCalendarSync(ctx context.Context, request connector.BackgroundRequest) (connector.BackgroundResult, error) {
	if request.StateVersion != 1 {
		return connector.BackgroundResult{}, permanent("calendar.state_version_invalid", "unsupported calendar sync state")
	}
	state := calendarSyncState{}
	if len(request.State) != 0 && string(request.State) != "null" {
		if err := strictBackgroundJSON(request.State, &state); err != nil {
			return connector.BackgroundResult{}, permanent("calendar.state_invalid", "invalid calendar sync state")
		}
	}
	ids := calendarSyncIDs(request.Connection.Config)
	if len(ids) > 16 {
		return connector.BackgroundResult{}, permanent("calendar.config_invalid", "at most 16 calendars can be synchronized")
	}
	if state.Calendars == nil {
		state.Calendars = map[string]calendarSyncCursor{}
	}
	if state.CalendarIndex < 0 || state.CalendarIndex >= len(ids) {
		state.CalendarIndex = 0
	}
	id := ids[state.CalendarIndex]
	cursor := state.Calendars[id]
	if since, parseErr := time.Parse(time.RFC3339, cursor.WindowStart); parseErr == nil && cursor.PageToken == "" && !request.Now.Before(since.AddDate(0, 4, 0)) {
		// Refresh the bounded recurring-instance window monthly, without
		// deleting older business history or restarting an in-flight page scan.
		cursor = calendarSyncCursor{Initialized: cursor.Initialized}
	}
	if cursor.SyncToken == "" && cursor.WindowStart == "" {
		cursor.WindowStart = request.Now.AddDate(0, -3, 0).Format(time.RFC3339)
		cursor.WindowEnd = request.Now.AddDate(1, 0, 0).Format(time.RFC3339)
	}
	q := url.Values{"maxResults": {"100"}, "showDeleted": {"true"}, "singleEvents": {"true"}}
	if cursor.SyncToken == "" {
		set(q, "timeMin", cursor.WindowStart)
		set(q, "timeMax", cursor.WindowEnd)
	}
	set(q, "syncToken", cursor.SyncToken)
	set(q, "pageToken", cursor.PageToken)
	raw, err := p.executeWithRefresh(ctx, request.Connection, request.Secrets, http.MethodGet, apiBase(request.Connection)+"/calendar/v3/calendars/"+url.PathEscape(id)+"/events", q, nil, false)
	if err != nil {
		if backgroundErrorCodeIs(err, "google.http_410") {
			// Reset only the expired cursor, never CRM records or original evidence.
			state.Calendars[id] = calendarSyncCursor{}
			encoded, _ := json.Marshal(state)
			return connector.BackgroundResult{State: encoded, NextDueAt: request.Now.Add(5 * time.Second), SecretUpdates: raw.SecretUpdates}, nil
		}
		return connector.BackgroundResult{SecretUpdates: raw.SecretUpdates}, err
	}
	var page struct {
		Kind     string `json:"kind"`
		NextPage string `json:"nextPageToken"`
		NextSync string `json:"nextSyncToken"`
		Items    []struct {
			googleCalendarEvent
			UID        string `json:"iCalUID"`
			Updated    string `json:"updated"`
			Visibility string `json:"visibility"`
		} `json:"items"`
	}
	encoded, _ := json.Marshal(raw.Output)
	if json.Unmarshal(encoded, &page) != nil || page.Kind != "calendar#events" || len(page.Items) > 100 || len(page.NextPage) > 8192 || len(page.NextSync) > 8192 || (page.NextPage == "" && page.NextSync == "") || (page.NextPage != "" && page.NextPage == cursor.PageToken) {
		return connector.BackgroundResult{SecretUpdates: raw.SecretUpdates}, permanent("calendar.sync_response_invalid", "invalid Google calendar sync page")
	}
	events := []connector.BackgroundEvent{}
	for _, item := range page.Items {
		if strings.TrimSpace(item.ID) == "" {
			return connector.BackgroundResult{SecretUpdates: raw.SecretUpdates}, permanent("calendar.sync_response_invalid", "event identifier is missing")
		}
		payload := map[string]any{"provider": "google", "calendar_id": id, "event_id": item.ID, "source": "reconcile", "change_type": "update", "ical_uid": item.UID, "private": item.Visibility == "private" || item.Visibility == "confidential"}
		if item.Status == "cancelled" {
			payload["change_type"] = "cancel"
		} else {
			event, parseErr := item.event(id)
			if parseErr != nil {
				return connector.BackgroundResult{SecretUpdates: raw.SecretUpdates}, parseErr
			}
			payload["event"] = event
		}
		body, _ := json.Marshal(payload)
		hash := sha256.Sum256(append(body, []byte(item.Updated)...))
		events = append(events, connector.BackgroundEvent{ExternalID: "google:calendar:" + id + ":" + item.ID + ":" + hex.EncodeToString(hash[:]), EventType: "google.calendar.event.changed", Payload: body})
	}
	state.Completed += int64(len(page.Items))
	cursor.PageToken = page.NextPage
	next := request.Now.Add(5 * time.Second)
	if page.NextPage == "" {
		cursor.SyncToken = page.NextSync
		cursor.Initialized = true
		state.CalendarIndex = (state.CalendarIndex + 1) % len(ids)
		if state.CalendarIndex == 0 {
			next = request.Now.Add(5 * time.Minute)
		}
	}
	state.Calendars[id] = cursor
	complete := true
	for _, calendarID := range ids {
		if !state.Calendars[calendarID].Initialized {
			complete = false
		}
	}
	encoded, _ = json.Marshal(state)
	return connector.BackgroundResult{State: encoded, Events: events, SecretUpdates: raw.SecretUpdates, NextDueAt: next, Progress: &connector.BackgroundProgress{Phase: "calendar_sync", Unit: "events", CompletedUnits: state.Completed, InitializationComplete: complete}}, nil
}
