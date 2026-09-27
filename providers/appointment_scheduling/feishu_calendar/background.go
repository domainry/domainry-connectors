package feishucalendar

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	calendar "github.com/domainry/domainry-connector-sdk/calendar"
)

const (
	feishuCalendarSyncTaskKey         = "feishu_calendar_sync"
	feishuCalendarSubscriptionTaskKey = "feishu_calendar_subscription"
	feishuCalendarStateVersion        = 1
	feishuCalendarMaxScanPages        = 20
	feishuCalendarPageSize            = 100
)

type feishuCalendarSyncState struct {
	CalendarID       string            `json:"calendar_id,omitempty"`
	Cursor           string            `json:"cursor,omitempty"`
	Page             int               `json:"page,omitempty"`
	WindowStart      string            `json:"window_start,omitempty"`
	WindowEnd        string            `json:"window_end,omitempty"`
	Fingerprints     map[string]string `json:"fingerprints,omitempty"`
	ScanFingerprints map[string]string `json:"scan_fingerprints,omitempty"`
	InitialComplete  bool              `json:"initial_complete,omitempty"`
}

type feishuCalendarSubscriptionState struct {
	CalendarID    string `json:"calendar_id,omitempty"`
	Subscribed    bool   `json:"subscribed,omitempty"`
	LastCheckedAt string `json:"last_checked_at,omitempty"`
}

func (p *provider) BackgroundTasks(connection connector.Connection) []connector.BackgroundTaskDescriptor {
	if connection.Status != "active" || !feishuConfigBool(connection.Config, "calendar_sync_enabled", false) {
		return nil
	}
	tasks := []connector.BackgroundTaskDescriptor{{Key: feishuCalendarSyncTaskKey, StateVersion: feishuCalendarStateVersion}}
	if feishuConfigBool(connection.Config, "calendar_subscription_enabled", true) {
		tasks = append(tasks, connector.BackgroundTaskDescriptor{Key: feishuCalendarSubscriptionTaskKey, StateVersion: feishuCalendarStateVersion})
	}
	return tasks
}

func (p *provider) ProcessBackground(ctx context.Context, request connector.BackgroundRequest) (connector.BackgroundResult, error) {
	if err := request.Validate(); err != nil {
		return connector.BackgroundResult{}, err
	}
	if request.Connection.ConnectorKey != ConnectorKey || request.Connection.ProviderKey != ProviderKey {
		return connector.BackgroundResult{}, permanent("background.connection_mismatch", "background connection does not match Feishu Calendar")
	}
	if request.StateVersion != feishuCalendarStateVersion {
		return connector.BackgroundResult{}, permanent("background.state_version_unsupported", "unsupported Feishu Calendar background state version")
	}
	switch request.TaskKey {
	case feishuCalendarSyncTaskKey:
		return p.processCalendarSync(ctx, request)
	case feishuCalendarSubscriptionTaskKey:
		return p.processCalendarSubscription(ctx, request)
	default:
		return connector.BackgroundResult{}, permanent("background.task_unknown", "unknown Feishu Calendar background task")
	}
}

func (p *provider) processCalendarSync(ctx context.Context, request connector.BackgroundRequest) (connector.BackgroundResult, error) {
	state := feishuCalendarSyncState{}
	if err := decodeFeishuBackgroundState(request.State, &state); err != nil {
		return connector.BackgroundResult{}, permanent("background.sync_state_invalid", "Feishu Calendar sync state is invalid")
	}
	if len(state.Fingerprints) > feishuCalendarMaxScanPages*feishuCalendarPageSize || len(state.ScanFingerprints) > feishuCalendarMaxScanPages*feishuCalendarPageSize || state.Page < 0 || state.Page >= feishuCalendarMaxScanPages {
		return connector.BackgroundResult{}, permanent("background.sync_state_invalid", "Feishu Calendar sync state exceeds its bounded scan")
	}
	secrets := cloneStringMap(request.Secrets)
	updates := map[string]string{}
	session := p.newAPISession(request.Connection, secrets)
	calendarID, err := p.calendarID(ctx, session)
	mergeSecretUpdates(secrets, session.updates)
	mergeSecretUpdates(updates, session.updates)
	if err != nil {
		return connector.BackgroundResult{SecretUpdates: updates}, err
	}
	if state.CalendarID != "" && state.CalendarID != calendarID {
		state = feishuCalendarSyncState{}
	}
	state.CalendarID = calendarID
	if state.Cursor == "" {
		historyDays := feishuConfigInt(request.Connection.Config, "calendar_history_days", 30)
		futureDays := feishuConfigInt(request.Connection.Config, "calendar_future_days", 62)
		state.WindowStart = request.Now.Add(-time.Duration(historyDays) * 24 * time.Hour).UTC().Format(time.RFC3339)
		state.WindowEnd = request.Now.Add(time.Duration(futureDays) * 24 * time.Hour).UTC().Format(time.RFC3339)
		state.ScanFingerprints = map[string]string{}
		state.Page = 0
	}
	if state.Fingerprints == nil {
		state.Fingerprints = map[string]string{}
	}
	input := calendar.EventsRequest{
		CalendarID: calendarID, Cursor: state.Cursor, Limit: feishuCalendarPageSize,
		Window: calendar.Window{Start: state.WindowStart, End: state.WindowEnd}, TimeZone: config(request.Connection, "default_timezone", "Asia/Shanghai"),
	}
	page, currentUpdates, err := p.backgroundCalendarEvents(ctx, request, input, secrets)
	mergeSecretUpdates(secrets, currentUpdates)
	mergeSecretUpdates(updates, currentUpdates)
	if err != nil {
		return connector.BackgroundResult{SecretUpdates: updates}, err
	}
	events := make([]connector.BackgroundEvent, 0, len(page.Items))
	for _, event := range page.Items {
		rawEvent, marshalErr := json.Marshal(event)
		if marshalErr != nil {
			return connector.BackgroundResult{SecretUpdates: updates}, permanent("background.event_invalid", "Feishu Calendar event cannot be normalized")
		}
		digest := sha256.Sum256(rawEvent)
		fingerprint := hex.EncodeToString(digest[:])
		state.ScanFingerprints[event.ID] = fingerprint
		previous := state.Fingerprints[event.ID]
		if previous == fingerprint {
			continue
		}
		eventType := "feishu.calendar.event.updated"
		if previous == "" {
			eventType = "feishu.calendar.event.created"
			if !state.InitialComplete {
				eventType = "feishu.calendar.event.discovered"
			}
		}
		if strings.EqualFold(event.Status, "cancelled") {
			eventType = "feishu.calendar.event.cancelled"
		}
		payload, _ := json.Marshal(map[string]any{
			"provider": ProviderKey, "calendar_id": calendarID, "event_id": event.ID, "event": event,
			"content_hash": fingerprint, "source": "reconcile",
		})
		events = append(events, connector.BackgroundEvent{
			ExternalID: "feishu-calendar:" + request.Connection.Key + ":event:" + event.ID + ":" + fingerprint,
			EventType:  eventType, Payload: payload,
		})
	}
	nextDueAt := request.Now.Add(time.Duration(feishuConfigInt(request.Connection.Config, "calendar_reconcile_seconds", 900)) * time.Second)
	if page.NextCursor != "" {
		if page.NextCursor == state.Cursor || state.Page+1 >= feishuCalendarMaxScanPages {
			return connector.BackgroundResult{SecretUpdates: updates}, permanent("background.page_limit_exceeded", "Feishu Calendar scan exceeded its bounded page limit")
		}
		state.Cursor = page.NextCursor
		state.Page++
		nextDueAt = request.Now.Add(time.Second)
	} else {
		state.Fingerprints = state.ScanFingerprints
		state.ScanFingerprints = nil
		state.Cursor, state.WindowStart, state.WindowEnd = "", "", ""
		state.Page = 0
		state.InitialComplete = true
	}
	rawState, _ := json.Marshal(state)
	return connector.BackgroundResult{State: rawState, NextDueAt: nextDueAt, Events: events, SecretUpdates: updates}, nil
}

func (p *provider) backgroundCalendarEvents(ctx context.Context, request connector.BackgroundRequest, input calendar.EventsRequest, secrets map[string]string) (calendar.EventsPage, map[string]string, error) {
	payload, _ := json.Marshal(input)
	result, err := p.Adapter.Call(ctx, connector.CallRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CalendarEvents.Key, ContractSHA256: CalendarEvents.ContractSHA256,
		Mode: connector.ModeCall, Connection: request.Connection, Payload: payload, Secrets: cloneStringMap(secrets), Principal: request.Principal,
	})
	var page calendar.EventsPage
	if err == nil && json.Unmarshal(result.Payload, &page) != nil {
		err = permanent("background.response_invalid", "Feishu Calendar event page is invalid")
	}
	return page, result.SecretUpdates, err
}

func (p *provider) processCalendarSubscription(ctx context.Context, request connector.BackgroundRequest) (connector.BackgroundResult, error) {
	state := feishuCalendarSubscriptionState{}
	if err := decodeFeishuBackgroundState(request.State, &state); err != nil {
		return connector.BackgroundResult{}, permanent("background.subscription_state_invalid", "Feishu Calendar subscription state is invalid")
	}
	session := p.newAPISession(request.Connection, request.Secrets)
	calendarID, err := p.calendarID(ctx, session)
	if err != nil {
		return connector.BackgroundResult{SecretUpdates: cloneStringMap(session.updates)}, err
	}
	if state.Subscribed && state.CalendarID != "" && state.CalendarID != calendarID {
		if _, err = session.call(ctx, http.MethodPost, calendarSubscriptionPath(state.CalendarID, false), url.Values{"user_id_type": {"open_id"}}, nil, true); err != nil {
			return connector.BackgroundResult{SecretUpdates: cloneStringMap(session.updates)}, err
		}
	}
	if _, err = session.call(ctx, http.MethodPost, calendarSubscriptionPath(calendarID, true), url.Values{"user_id_type": {"open_id"}}, nil, true); err != nil {
		return connector.BackgroundResult{SecretUpdates: cloneStringMap(session.updates)}, err
	}
	state = feishuCalendarSubscriptionState{CalendarID: calendarID, Subscribed: true, LastCheckedAt: request.Now.UTC().Format(time.RFC3339)}
	rawState, _ := json.Marshal(state)
	return connector.BackgroundResult{State: rawState, NextDueAt: request.Now.Add(24 * time.Hour), SecretUpdates: cloneStringMap(session.updates)}, nil
}

func (p *provider) CleanupBackground(ctx context.Context, connection connector.Connection, secrets map[string]string, now time.Time, principal connector.Principal) (map[string]string, error) {
	if !feishuConfigBool(connection.Config, "calendar_subscription_enabled", true) {
		return nil, nil
	}
	session := p.newAPISession(connection, secrets)
	calendarID, err := p.calendarID(ctx, session)
	if err != nil {
		return cloneStringMap(session.updates), err
	}
	_, err = session.call(ctx, http.MethodPost, calendarSubscriptionPath(calendarID, false), url.Values{"user_id_type": {"open_id"}}, nil, true)
	return cloneStringMap(session.updates), err
}

func calendarSubscriptionPath(calendarID string, subscribe bool) string {
	action := "unsubscription"
	if subscribe {
		action = "subscription"
	}
	return "/open-apis/calendar/v4/calendars/" + url.PathEscape(calendarID) + "/events/" + action
}

func decodeFeishuBackgroundState(raw json.RawMessage, target any) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("Feishu Calendar background state contains multiple values")
	}
	return nil
}

func feishuConfigBool(values map[string]any, key string, fallback bool) bool {
	value, ok := values[key]
	if !ok {
		return fallback
	}
	parsed, ok := value.(bool)
	if !ok {
		return false
	}
	return parsed
}

func feishuConfigInt(values map[string]any, key string, fallback int) int {
	if _, ok := values[key]; !ok {
		return fallback
	}
	return intValue(values, key)
}

func mergeSecretUpdates(target, values map[string]string) {
	for key, value := range values {
		target[key] = value
	}
}

var _ connector.BackgroundProcessor = (*provider)(nil)
var _ connector.BackgroundCleanupProcessor = (*provider)(nil)
