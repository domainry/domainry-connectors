package google

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
	"strconv"
	"strings"
	"testing"
	"time"
)

type recordingTransport struct {
	requests []connector.HTTPRequest
	respond  func(connector.HTTPRequest) (connector.HTTPResponse, error)
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, r connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, r)
	if t.respond != nil {
		return t.respond(r)
	}
	return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"id":"message-1"}`)}, nil
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func TestAllContractsValidateAndRepresentativeOperationsUseRuntimeHTTP(t *testing.T) {
	transport := &recordingTransport{}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	p := adapter.(*provider)
	p.now = func() time.Time { return time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC) }
	tests := []struct {
		op, hash, path string
		input          any
	}{{SyncCalendar.Key, SyncCalendar.ContractSHA256, "/calendar/v3/calendars/primary/events", SyncCalendarInput{}}, {SyncDriveFileRefs.Key, SyncDriveFileRefs.ContractSHA256, "/drive/v3/files", SyncDriveFileRefsInput{}}, {SyncEmailHistory.Key, SyncEmailHistory.ContractSHA256, "/gmail/v1/users/me/history", SyncEmailHistoryInput{StartHistoryID: "100"}}, {GmailGetMessage.Key, GmailGetMessage.ContractSHA256, "/messages/m1", GmailGetMessageInput{MessageID: "m1"}}, {GmailWatch.Key, GmailWatch.ContractSHA256, "/users/me/watch", GmailWatchInput{TopicName: "projects/p/topics/t"}}, {TestConnection.Key, TestConnection.ContractSHA256, "/oauth2/v2/userinfo", struct{}{}}}
	for _, test := range tests {
		payload, _ := json.Marshal(test.input)
		_, callErr := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: test.op, ContractSHA256: test.hash, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_token": "runtime-token"}, Payload: payload})
		if callErr != nil {
			t.Fatalf("operation=%s err=%v", test.op, callErr)
		}
		request := transport.requests[len(transport.requests)-1]
		if !strings.Contains(request.URL, test.path) || request.SecretHeaders["Authorization"][0] != "Bearer runtime-token" || request.MaxResponseBytes != responseLimit {
			t.Fatalf("request=%+v", request)
		}
		raw, _ := json.Marshal(request)
		if strings.Contains(string(raw), "runtime-token") {
			t.Fatalf("leak=%s", raw)
		}
	}
}
func TestGmailSendRemainsEnqueueAndBuildsSafeRFCMessage(t *testing.T) {
	transport := &recordingTransport{}
	adapter, _ := New(transport)
	payload, _ := json.Marshal(GmailSendMessageInput{To: []any{"Person <person@example.test>"}, Subject: "你好", Text: "body", RFCMessageID: "<m1@example.test>"})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: GmailSendMessage.Key, ContractSHA256: GmailSendMessage.ContractSHA256, Mode: connector.ModeEnqueue, Delivery: true, Connection: validConnection(), Secrets: map[string]string{"access_token": "token"}, Payload: payload})
	if err != nil || result.ResponseRef != "gmail:message-1" || len(result.Payload) != 0 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	body := map[string]string{}
	_ = json.Unmarshal(transport.requests[0].Body, &body)
	decoded, decodeErr := base64.RawURLEncoding.DecodeString(body["raw"])
	if decodeErr != nil || !strings.Contains(string(decoded), "Message-ID: <m1@example.test>") {
		t.Fatalf("message=%q err=%v", decoded, decodeErr)
	}
	bad, _ := json.Marshal(GmailSendMessageInput{To: "victim@example.test\r\nBcc: attacker@example.test", Subject: "x", Text: "x", RFCMessageID: "<m@example.test>"})
	if _, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: GmailSendMessage.Key, ContractSHA256: GmailSendMessage.ContractSHA256, Mode: connector.ModeEnqueue, Delivery: true, Connection: validConnection(), Secrets: map[string]string{"access_token": "token"}, Payload: bad}); err == nil {
		t.Fatal("header injection accepted")
	}
}
func TestRefreshAndFailureClassification(t *testing.T) {
	calls := 0
	transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		if strings.HasSuffix(r.URL, "/token") {
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"access_token":"fresh","refresh_token":"rotated"}`)}, nil
		}
		calls++
		if r.SecretHeaders["Authorization"][0] == "Bearer stale" {
			return connector.HTTPResponse{StatusCode: 401, Body: []byte(`{}`)}, nil
		}
		return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"email":"person@example.test"}`)}, nil
	}}
	adapter, _ := New(transport)
	payload, _ := json.Marshal(struct{}{})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: TestConnection.Key, ContractSHA256: TestConnection.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_token": "stale", "refresh_token": "refresh", "client_id": "client", "client_secret": "secret"}, Payload: payload})
	if err != nil || calls != 2 || result.SecretUpdates["access_token"] != "fresh" || result.SecretUpdates["refresh_token"] != "rotated" {
		t.Fatalf("result=%+v calls=%d err=%v", result, calls, err)
	}
	transport.respond = func(connector.HTTPRequest) (connector.HTTPResponse, error) {
		return connector.HTTPResponse{}, errors.New("reset")
	}
	send, _ := json.Marshal(GmailWatchInput{TopicName: "projects/p/topics/t"})
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: GmailWatch.Key, ContractSHA256: GmailWatch.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"access_token": "token"}, Payload: send})
	classification, ok := connector.ErrorClassificationOf(err)
	if !ok || classification != connector.ErrorUncertain {
		t.Fatalf("classification=%q err=%v", classification, err)
	}
}

func TestConnectionPublishesVerifiedProviderAccountRoute(t *testing.T) {
	transport := &recordingTransport{respond: func(connector.HTTPRequest) (connector.HTTPResponse, error) {
		return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"id":"google-subject-1","email":"Person@Example.Test","verified_email":true}`)}, nil
	}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: validConnection(), Secrets: map[string]string{"access_token": "token"}})
	if err != nil || !result.Connected {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	var details struct {
		ProviderAccount struct {
			Subject string `json:"subject"`
			Routes  []struct {
				Kind  string `json:"kind"`
				Value string `json:"value"`
			} `json:"routes"`
		} `json:"provider_account"`
	}
	if err = json.Unmarshal(result.Details, &details); err != nil {
		t.Fatal(err)
	}
	if details.ProviderAccount.Subject != "google-subject-1" || len(details.ProviderAccount.Routes) != 1 || details.ProviderAccount.Routes[0].Kind != "email" || details.ProviderAccount.Routes[0].Value != "person@example.test" {
		t.Fatalf("provider account details=%s", result.Details)
	}

	transport.respond = func(connector.HTTPRequest) (connector.HTTPResponse, error) {
		return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"id":"google-subject-1","email":"person@example.test","verified_email":false}`)}, nil
	}
	if _, err = adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: validConnection(), Secrets: map[string]string{"access_token": "token"}}); err == nil {
		t.Fatal("unverified Google email was accepted as an account route")
	}
}

func validConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"api_base_url": "http://127.0.0.1:8080", "gmail_base_url": "http://127.0.0.1:8080", "token_url": "http://127.0.0.1:8080/token", "timeout_seconds": 30}}
}

func TestGmailBackgroundSyncOwnsCursorAndProjectsEvents(t *testing.T) {
	transport := &recordingTransport{respond: func(request connector.HTTPRequest) (connector.HTTPResponse, error) {
		switch {
		case strings.HasSuffix(request.URL, "/gmail/v1/users/me/profile"):
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"emailAddress":"person@example.test","historyId":"12"}`)}, nil
		case strings.Contains(request.URL, "/gmail/v1/users/me/history"):
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"historyId":"12","history":[{"messagesAdded":[{"message":{"id":"m1"}}]}]}`)}, nil
		case strings.Contains(request.URL, "/gmail/v1/users/me/messages/m1"):
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"id":"m1","threadId":"t1","internalDate":"1787529600000","labelIds":["INBOX"],"payload":{"mimeType":"text/plain","headers":[{"name":"From","value":"Buyer <buyer@example.test>"},{"name":"To","value":"person@example.test"},{"name":"Subject","value":"hello"}],"body":{"size":4,"data":"Ym9keQ"}}}`)}, nil
		default:
			return connector.HTTPResponse{StatusCode: 500, Body: []byte(`{}`)}, nil
		}
	}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	processor := adapter.(connector.BackgroundProcessor)
	connection := validConnection()
	connection.Key, connection.WorkspaceID, connection.ConnectorKey, connection.ProviderKey, connection.Status = "gmail", "workspace", ConnectorKey, ProviderKey, "active"
	connection.Config["gmail_ingest_enabled"] = true
	tasks := processor.BackgroundTasks(connection)
	if len(tasks) != 1 || tasks[0].Key != gmailSyncTaskKey {
		t.Fatalf("tasks=%#v", tasks)
	}
	result, err := processor.ProcessBackground(t.Context(), connector.BackgroundRequest{TaskKey: gmailSyncTaskKey, StateVersion: 2, Connection: connection, State: json.RawMessage(`{"account_email":"person@example.test","history_id":"10"}`), Secrets: map[string]string{"access_token": "token"}, Now: time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC), Principal: connector.Principal{IsAuthenticated: true, WorkspaceID: "workspace"}})
	if err != nil || result.Validate() != nil || len(result.Events) != 1 || result.Events[0].EventType != "gmail.message.received" || !strings.Contains(string(result.State), `"history_id":"12"`) || !result.NextDueAt.Equal(time.Date(2026, 8, 24, 1, 0, 0, 0, time.UTC)) {
		t.Fatalf("result=%+v err=%v validation=%v", result, err, result.Validate())
	}
}

func TestGmailBootstrapTraversesHistoricalPagesBeforeAdvancingHistoryCursor(t *testing.T) {
	transport := &recordingTransport{respond: func(request connector.HTTPRequest) (connector.HTTPResponse, error) {
		switch {
		case strings.HasSuffix(request.URL, "/gmail/v1/users/me/profile"):
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"emailAddress":"person@example.test","historyId":"20"}`)}, nil
		case strings.Contains(request.URL, "/gmail/v1/users/me/messages?"):
			if strings.Contains(request.URL, "pageToken=p2") {
				return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"messages":[{"id":"m2"}]}`)}, nil
			}
			if !strings.Contains(request.URL, "q=newer_than%3A90d") {
				t.Fatalf("bootstrap query did not apply the bounded history window: %s", request.URL)
			}
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"messages":[{"id":"m1"}],"nextPageToken":"p2"}`)}, nil
		case strings.Contains(request.URL, "/gmail/v1/users/me/messages/m1"):
			return gmailBackgroundMessage("m1"), nil
		case strings.Contains(request.URL, "/gmail/v1/users/me/messages/m2"):
			return gmailBackgroundMessage("m2"), nil
		default:
			return connector.HTTPResponse{StatusCode: 500, Body: []byte(`{}`)}, nil
		}
	}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	connection := validConnection()
	connection.Key, connection.WorkspaceID, connection.ConnectorKey, connection.ProviderKey, connection.Status = "gmail", "workspace", ConnectorKey, ProviderKey, "active"
	connection.Config["gmail_ingest_enabled"], connection.Config["gmail_ingest_bootstrap"] = true, true
	request := connector.BackgroundRequest{
		TaskKey: gmailSyncTaskKey, StateVersion: 2, Connection: connection, State: json.RawMessage(`{}`),
		Secrets: map[string]string{"access_token": "token"}, Now: time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC), Principal: connector.Principal{IsAuthenticated: true, WorkspaceID: "workspace"},
	}
	first, err := adapter.(connector.BackgroundProcessor).ProcessBackground(t.Context(), request)
	if err != nil || len(first.Events) != 1 || !strings.Contains(string(first.State), `"bootstrap_history_id":"20"`) || !strings.Contains(string(first.State), `"bootstrap_page_token":"p2"`) || strings.Contains(string(first.State), `"history_id":"20"`) {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	var firstPayload map[string]any
	if err = json.Unmarshal(first.Events[0].Payload, &firstPayload); err != nil || firstPayload["source"] != "bootstrap" {
		t.Fatalf("first payload=%s err=%v", first.Events[0].Payload, err)
	}
	request.State, request.Now = first.State, request.Now.Add(5*time.Second)
	second, err := adapter.(connector.BackgroundProcessor).ProcessBackground(t.Context(), request)
	if err != nil || len(second.Events) != 1 || !strings.Contains(string(second.State), `"history_id":"20"`) || strings.Contains(string(second.State), "bootstrap_page_token") {
		t.Fatalf("second=%+v err=%v", second, err)
	}
}

func gmailBackgroundMessage(id string) connector.HTTPResponse {
	body := `{"id":"` + id + `","threadId":"thread-a","internalDate":"1790298000000","labelIds":["INBOX"],"payload":{"mimeType":"text/html","headers":[{"name":"From","value":"Buyer <buyer@example.test>"},{"name":"To","value":"person@example.test"},{"name":"Subject","value":"hello"},{"name":"Auto-Submitted","value":"no"}],"body":{"size":12,"data":"PHA-aGVsbG88L3A-"}}}`
	return connector.HTTPResponse{StatusCode: 200, Body: []byte(body)}
}

func TestGmailWatchUsesOneSharedTopicWithoutPullingSubscription(t *testing.T) {
	now := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)
	expires := now.Add(7 * 24 * time.Hour)
	transport := &recordingTransport{respond: func(request connector.HTTPRequest) (connector.HTTPResponse, error) {
		if strings.HasSuffix(request.URL, "/gmail/v1/users/me/stop") {
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{}`)}, nil
		}
		if !strings.HasSuffix(request.URL, "/gmail/v1/users/me/watch") {
			t.Fatalf("per-account watch called Pub/Sub management or pull endpoint: %s", request.URL)
		}
		return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"historyId":"42","expiration":"` + strconv.FormatInt(expires.UnixMilli(), 10) + `"}`)}, nil
	}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	connection := validConnection()
	connection.Key, connection.WorkspaceID, connection.ConnectorKey, connection.ProviderKey, connection.Status = "gmail", "workspace", ConnectorKey, ProviderKey, "active"
	connection.Config["gmail_ingest_enabled"] = true
	connection.Config["gmail_pubsub_project_id"] = "project-a"
	connection.Config["gmail_pubsub_topic_id"] = "domainry-gmail-events"
	result, err := adapter.(connector.BackgroundProcessor).ProcessBackground(t.Context(), connector.BackgroundRequest{
		TaskKey: gmailWatchTaskKey, StateVersion: 2, Connection: connection, State: json.RawMessage(`{}`),
		RelatedStates: map[string]json.RawMessage{gmailSyncTaskKey: json.RawMessage(`{"account_email":"person@example.test","history_id":"41"}`)},
		Secrets:       map[string]string{"access_token": "token"}, Now: now, Principal: connector.Principal{IsAuthenticated: true, WorkspaceID: "workspace"},
	})
	if err != nil || result.Validate() != nil || len(transport.requests) != 1 {
		t.Fatalf("result=%+v requests=%d err=%v", result, len(transport.requests), err)
	}
	var requestBody struct {
		TopicName string `json:"topicName"`
	}
	if err = json.Unmarshal(transport.requests[0].Body, &requestBody); err != nil {
		t.Fatal(err)
	}
	topic := gmailPushTopic("domainry-gmail-events")
	if requestBody.TopicName != "projects/project-a/topics/"+topic || !result.NextDueAt.Before(expires.Add(-12*time.Hour)) || result.NextDueAt.Before(expires.Add(-18*time.Hour)) {
		t.Fatalf("topic=%q due=%s expiry=%s", requestBody.TopicName, result.NextDueAt, expires)
	}
	var state gmailWatchState
	if err = json.Unmarshal(result.State, &state); err != nil || state.TopicID != topic {
		t.Fatalf("state=%s err=%v", result.State, err)
	}

	transport.requests = nil
	updates, err := adapter.(connector.BackgroundCleanupProcessor).CleanupBackground(t.Context(), connection, map[string]string{"access_token": "token"}, now, connector.Principal{IsAuthenticated: true, WorkspaceID: "workspace"})
	if err != nil || len(updates) != 0 || len(transport.requests) != 1 || !strings.HasSuffix(transport.requests[0].URL, "/gmail/v1/users/me/stop") {
		t.Fatalf("cleanup requests=%#v updates=%v err=%v", transport.requests, updates, err)
	}
}

func TestBackgroundHelpersRejectUnsafeStateAndAbsentLabels(t *testing.T) {
	if strictBackgroundJSON([]byte(`{} {}`), &gmailSyncState{}) == nil {
		t.Fatal("multiple state values accepted")
	}
	if backgroundHasLabel(map[string]any{}, "INBOX") {
		t.Fatal("message without labels matched")
	}
}
