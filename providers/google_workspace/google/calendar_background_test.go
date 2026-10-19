package google

import (
	"encoding/json"
	"net/url"
	"strings"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

func TestCalendarBackgroundResumesPagesAndRetainsIncrementalToken(t *testing.T) {
	var queries []url.Values
	transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		u, _ := url.Parse(r.URL)
		queries = append(queries, u.Query())
		body := `{"kind":"calendar#events","nextPageToken":"page-2","items":[{"id":"event-a","summary":"测试","iCalUID":"meeting-uid","start":{"dateTime":"2026-10-18T10:00:00Z"},"end":{"dateTime":"2026-10-18T11:00:00Z"},"hangoutLink":"https://meet.google.com/abc-defg-hij","organizer":{"email":"sales@example.test","self":true}}]}`
		if len(queries) == 2 {
			body = `{"kind":"calendar#events","nextSyncToken":"sync-2","items":[{"id":"deleted-a","status":"cancelled"}]}`
		}
		if len(queries) == 3 {
			body = `{"kind":"calendar#events","nextSyncToken":"sync-3","items":[]}`
		}
		return connector.HTTPResponse{StatusCode: 200, Body: []byte(body)}, nil
	}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	connection := validConnection()
	connection.Key = "calendar"
	connection.ConnectorKey, connection.ProviderKey = ConnectorKey, ProviderKey
	connection.Status = "active"
	connection.Config["calendar_sync_enabled"] = true
	connection.Config["gmail_ingest_enabled"] = false
	processor := adapter.(connector.BackgroundProcessor)
	tasks := processor.BackgroundTasks(connection)
	if len(tasks) != 1 || tasks[0].Key != googleCalendarSyncTaskKey {
		t.Fatalf("wrong tasks: %+v", tasks)
	}
	request := connector.BackgroundRequest{TaskKey: googleCalendarSyncTaskKey, StateVersion: 1, Connection: connection, Now: time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC), Secrets: map[string]string{"access_token": "test-token"}}
	first, err := processor.ProcessBackground(t.Context(), request)
	if err != nil || first.Validate() != nil || len(first.Events) != 1 || first.Progress.InitializationComplete {
		t.Fatalf("first page failed: %v", err)
	}
	var payload map[string]any
	_ = json.Unmarshal(first.Events[0].Payload, &payload)
	if payload["ical_uid"] != "meeting-uid" || payload["provider"] != "google" {
		t.Fatalf("source identity lost")
	}
	request.State = first.State
	second, err := processor.ProcessBackground(t.Context(), request)
	if err != nil || !second.Progress.InitializationComplete || queries[1].Get("pageToken") != "page-2" || queries[1].Get("syncToken") != "" {
		t.Fatalf("page resume failed: %v", err)
	}
	if !strings.Contains(string(second.Events[0].Payload), `"change_type":"cancel"`) {
		t.Fatal("cancel tombstone lost")
	}
	request.State = second.State
	third, err := processor.ProcessBackground(t.Context(), request)
	if err != nil || third.Validate() != nil || queries[2].Get("syncToken") != "sync-2" || queries[2].Get("timeMin") != "" || queries[2].Get("pageToken") != "" {
		t.Fatalf("incremental query failed: %v", err)
	}
}

func TestCalendarBackgroundExpiredTokenRescansWithoutDeletion(t *testing.T) {
	transport := &recordingTransport{respond: func(connector.HTTPRequest) (connector.HTTPResponse, error) {
		return connector.HTTPResponse{StatusCode: 410, Body: []byte(`{"error":{"code":410}}`)}, nil
	}}
	adapter, _ := New(transport)
	connection := validConnection()
	connection.Key = "calendar"
	connection.ConnectorKey, connection.ProviderKey = ConnectorKey, ProviderKey
	result, err := adapter.(connector.BackgroundProcessor).ProcessBackground(t.Context(), connector.BackgroundRequest{TaskKey: googleCalendarSyncTaskKey, StateVersion: 1, Connection: connection, Now: time.Now(), Secrets: map[string]string{"access_token": "test-token"}, State: json.RawMessage(`{"calendar_index":0,"calendars":{"primary":{"sync_token":"expired","initialized":true}},"completed":10}`)})
	if err != nil || len(result.Events) != 0 || strings.Contains(string(result.State), "expired") {
		t.Fatalf("expired cursor was not safely reset: %v", err)
	}
}

func TestCalendarBackgroundInvalidPageDoesNotAdvanceState(t *testing.T) {
	transport := &recordingTransport{respond: func(connector.HTTPRequest) (connector.HTTPResponse, error) {
		return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"kind":"calendar#events","items":[]}`)}, nil
	}}
	adapter, _ := New(transport)
	connection := validConnection()
	connection.Key = "calendar"
	connection.ConnectorKey, connection.ProviderKey = ConnectorKey, ProviderKey
	result, err := adapter.(connector.BackgroundProcessor).ProcessBackground(t.Context(), connector.BackgroundRequest{TaskKey: googleCalendarSyncTaskKey, StateVersion: 1, Connection: connection, Now: time.Now(), Secrets: map[string]string{"access_token": "test-token"}})
	if err == nil || len(result.State) != 0 || len(result.Events) != 0 {
		t.Fatal("invalid page committed a cursor")
	}
}
