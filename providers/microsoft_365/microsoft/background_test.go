package microsoft

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

func TestOutlookBackgroundTraversesOpaqueDeltaPagesAndAlternatesFolders(t *testing.T) {
	now := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)
	transport := &recordingTransport{respond: func(request connector.HTTPRequest) (connector.HTTPResponse, error) {
		switch {
		case strings.Contains(request.URL, "/v1.0/me?"):
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"id":"subject-a","mail":"Sales@Example.Test"}`)}, nil
		case strings.Contains(request.URL, "/mailFolders/inbox/messages/delta") && strings.Contains(request.URL, "skiptoken=page-2"):
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"@odata.deltaLink":"http://127.0.0.1:8080/v1.0/me/mailFolders/inbox/messages/delta?$deltatoken=inbox-final","value":[]}`)}, nil
		case strings.Contains(request.URL, "/mailFolders/inbox/messages/delta"):
			if !strings.Contains(request.URL, "%24filter=receivedDateTime+ge+2026-06-28T00%3A00%3A00Z") || !strings.Contains(request.URL, "%24orderby=receivedDateTime+desc") {
				t.Fatalf("initial delta query lost its bounded history window: %s", request.URL)
			}
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{
				"@odata.nextLink":"http://127.0.0.1:8080/v1.0/me/mailFolders/inbox/messages/delta?$skiptoken=page-2",
				"value":[{
					"id":"message-a","conversationId":"thread-a","internetMessageId":"<message-a@example.test>",
					"subject":"Discovery","from":{"emailAddress":{"name":"Buyer","address":"buyer@example.test"}},
					"toRecipients":[{"emailAddress":{"name":"Sales","address":"sales@example.test"}}],"ccRecipients":[],
					"receivedDateTime":"2026-09-25T01:00:00Z","sentDateTime":"2026-09-25T00:59:00Z","isDraft":false,
					"body":{"contentType":"text","content":"Hello"},"bodyPreview":"Hello",
					"internetMessageHeaders":[{"name":"In-Reply-To","value":"<previous@example.test>"}],
					"attachments":[{"id":"attachment-a","name":"brief.pdf","contentType":"application/pdf","size":42,"isInline":false}]
				}]
			}`)}, nil
		case strings.Contains(request.URL, "/mailFolders/sentitems/messages/delta"):
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"@odata.deltaLink":"http://127.0.0.1:8080/v1.0/me/mailFolders/sentitems/messages/delta?$deltatoken=sent-final","value":[]}`)}, nil
		default:
			t.Fatalf("unexpected request URL: %s", request.URL)
			return connector.HTTPResponse{}, nil
		}
	}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	processor := adapter.(connector.BackgroundProcessor)
	connection := outlookBackgroundConnection()
	request := connector.BackgroundRequest{
		TaskKey: outlookSyncTaskKey, StateVersion: 1, Connection: connection, State: json.RawMessage(`{}`),
		Secrets: map[string]string{"access_token": "token"}, Now: now,
	}

	first, err := processor.ProcessBackground(t.Context(), request)
	if err != nil || first.Validate() != nil || len(first.Events) != 1 || !first.NextDueAt.Equal(now.Add(5*time.Second)) {
		t.Fatalf("first=%+v err=%v validation=%v", first, err, first.Validate())
	}
	var firstState outlookSyncState
	if err = json.Unmarshal(first.State, &firstState); err != nil || firstState.AccountEmail != "sales@example.test" || firstState.Folder != "inbox" || firstState.InboxReady || !strings.Contains(firstState.InboxCursor, "$skiptoken=page-2") {
		t.Fatalf("first state=%s err=%v", first.State, err)
	}
	var payload map[string]any
	if err = json.Unmarshal(first.Events[0].Payload, &payload); err != nil || payload["source"] != "bootstrap" || payload["account_email"] != "sales@example.test" || payload["body"] != "Hello" || payload["in_reply_to"] != "<previous@example.test>" {
		t.Fatalf("event=%s err=%v", first.Events[0].Payload, err)
	}

	request.State, request.Now = first.State, now.Add(5*time.Second)
	second, err := processor.ProcessBackground(t.Context(), request)
	if err != nil || second.Validate() != nil || len(second.Events) != 0 {
		t.Fatalf("second=%+v err=%v validation=%v", second, err, second.Validate())
	}
	var secondState outlookSyncState
	if err = json.Unmarshal(second.State, &secondState); err != nil || !secondState.InboxReady || secondState.Folder != "sentitems" || !strings.Contains(secondState.InboxCursor, "$deltatoken=inbox-final") {
		t.Fatalf("second state=%s err=%v", second.State, err)
	}

	request.State, request.Now = second.State, now.Add(10*time.Second)
	third, err := processor.ProcessBackground(t.Context(), request)
	if err != nil || third.Validate() != nil || !third.NextDueAt.Equal(request.Now.Add(15*time.Minute)) {
		t.Fatalf("third=%+v err=%v validation=%v", third, err, third.Validate())
	}
	var thirdState outlookSyncState
	if err = json.Unmarshal(third.State, &thirdState); err != nil || !thirdState.SentReady || thirdState.Folder != "inbox" || !strings.Contains(thirdState.SentCursor, "$deltatoken=sent-final") {
		t.Fatalf("third state=%s err=%v", third.State, err)
	}
}

func TestOutlookBackgroundResetsExpiredCursorAndSkipsRemovedOrDraftMessages(t *testing.T) {
	now := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)
	connection := outlookBackgroundConnection()
	t.Run("expired cursor", func(t *testing.T) {
		transport := &recordingTransport{respond: func(request connector.HTTPRequest) (connector.HTTPResponse, error) {
			if !strings.Contains(request.URL, "deltatoken=expired") {
				t.Fatalf("opaque delta cursor was not preserved: %s", request.URL)
			}
			return connector.HTTPResponse{StatusCode: 410, Body: []byte(`{}`)}, nil
		}}
		adapter, _ := New(transport)
		state := json.RawMessage(`{"account_email":"sales@example.test","folder":"inbox","inbox_cursor":"http://127.0.0.1:8080/v1.0/me/mailFolders/inbox/messages/delta?$deltatoken=expired","inbox_ready":true}`)
		result, err := adapter.(connector.BackgroundProcessor).ProcessBackground(t.Context(), connector.BackgroundRequest{
			TaskKey: outlookSyncTaskKey, StateVersion: 1, Connection: connection, State: state,
			Secrets: map[string]string{"access_token": "token"}, Now: now,
		})
		if err != nil || len(result.Events) != 0 || !result.NextDueAt.Equal(now.Add(5*time.Second)) {
			t.Fatalf("result=%+v err=%v", result, err)
		}
		var next outlookSyncState
		if json.Unmarshal(result.State, &next) != nil || next.InboxCursor != "" || next.InboxReady {
			t.Fatalf("state=%s", result.State)
		}
	})

	t.Run("removed and draft", func(t *testing.T) {
		transport := &recordingTransport{respond: func(connector.HTTPRequest) (connector.HTTPResponse, error) {
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{
				"@odata.deltaLink":"http://127.0.0.1:8080/v1.0/me/mailFolders/inbox/messages/delta?$deltatoken=next",
				"value":[
					{"id":"removed","conversationId":"thread","@removed":{"reason":"deleted"}},
					{"id":"draft","conversationId":"thread","isDraft":true}
				]
			}`)}, nil
		}}
		adapter, _ := New(transport)
		state := json.RawMessage(`{"account_email":"sales@example.test","folder":"inbox"}`)
		result, err := adapter.(connector.BackgroundProcessor).ProcessBackground(t.Context(), connector.BackgroundRequest{
			TaskKey: outlookSyncTaskKey, StateVersion: 1, Connection: connection, State: state,
			Secrets: map[string]string{"access_token": "token"}, Now: now,
		})
		if err != nil || len(result.Events) != 0 {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	})
}

func TestOutlookDeltaCursorRejectsForeignOriginAndFolder(t *testing.T) {
	connection := outlookBackgroundConnection()
	now := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)
	for _, cursor := range []string{
		"https://attacker.example/v1.0/me/mailFolders/inbox/messages/delta?$deltatoken=x",
		"http://127.0.0.1:8080/v1.0/me/mailFolders/sentitems/messages/delta?$deltatoken=x",
	} {
		if _, _, err := outlookDeltaEndpoint(connection, "inbox", cursor, now, 90); err == nil {
			t.Fatalf("accepted foreign cursor %q", cursor)
		}
	}
}

func outlookBackgroundConnection() connector.Connection {
	connection := validConnection()
	connection.Key = "outlook-a"
	connection.WorkspaceID = "workspace-a"
	connection.ConnectorKey = ConnectorKey
	connection.ProviderKey = ProviderKey
	connection.Status = "active"
	connection.Config["outlook_ingest_enabled"] = true
	connection.Config["outlook_history_days"] = 90
	connection.Config["outlook_reconcile_seconds"] = 900
	return connection
}
