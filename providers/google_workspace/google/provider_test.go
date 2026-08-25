package google

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
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
	}{{SyncCalendar.Key, SyncCalendar.ContractSHA256, "/calendar/v3/calendars/primary/events", SyncCalendarInput{}}, {SyncDriveFileRefs.Key, SyncDriveFileRefs.ContractSHA256, "/drive/v3/files", SyncDriveFileRefsInput{}}, {SyncEmailHistory.Key, SyncEmailHistory.ContractSHA256, "/gmail/v1/users/me/history", SyncEmailHistoryInput{StartHistoryID: "100"}}, {GmailGetMessage.Key, GmailGetMessage.ContractSHA256, "/messages/m1", GmailGetMessageInput{MessageID: "m1"}}, {GmailWatch.Key, GmailWatch.ContractSHA256, "/users/me/watch", GmailWatchInput{TopicName: "projects/p/topics/t"}}, {PubSubEnsureSubscription.Key, PubSubEnsureSubscription.ContractSHA256, "/projects/p/subscriptions/s", PubSubEnsureSubscriptionInput{ProjectID: "p", TopicID: "t", SubscriptionID: "s"}}, {PubSubPull.Key, PubSubPull.ContractSHA256, "/projects/p/subscriptions/s:pull", PubSubPullInput{ProjectID: "p", SubscriptionID: "s"}}, {TestConnection.Key, TestConnection.ContractSHA256, "/oauth2/v2/userinfo", struct{}{}}}
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
func validConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"api_base_url": "http://127.0.0.1:8080", "gmail_base_url": "http://127.0.0.1:8080", "pubsub_base_url": "http://127.0.0.1:8080", "token_url": "http://127.0.0.1:8080/token", "timeout_seconds": 30}}
}

func TestGmailBackgroundSyncOwnsCursorAndProjectsEvents(t *testing.T) {
	transport := &recordingTransport{respond: func(request connector.HTTPRequest) (connector.HTTPResponse, error) {
		switch {
		case strings.HasSuffix(request.URL, "/gmail/v1/users/me/profile"):
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"emailAddress":"person@example.test","historyId":"12"}`)}, nil
		case strings.Contains(request.URL, "/gmail/v1/users/me/history"):
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"historyId":"12","history":[{"messagesAdded":[{"message":{"id":"m1"}}]}]}`)}, nil
		case strings.Contains(request.URL, "/gmail/v1/users/me/messages/m1"):
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"id":"m1","threadId":"t1","labelIds":["INBOX"],"payload":{"headers":[{"name":"Subject","value":"hello"}],"body":{"data":"Ym9keQ"}}}`)}, nil
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
	if len(tasks) != 2 || tasks[0].Key != gmailSyncTaskKey || tasks[1].Key != gmailWatchTaskKey {
		t.Fatalf("tasks=%#v", tasks)
	}
	result, err := processor.ProcessBackground(t.Context(), connector.BackgroundRequest{TaskKey: gmailSyncTaskKey, StateVersion: 1, Connection: connection, State: json.RawMessage(`{"account_email":"person@example.test","history_id":"10"}`), Secrets: map[string]string{"access_token": "token"}, Now: time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC), Principal: connector.Principal{IsAuthenticated: true, WorkspaceID: "workspace"}})
	if err != nil || result.Validate() != nil || len(result.Events) != 1 || result.Events[0].EventType != "gmail.message.received" || !strings.Contains(string(result.State), `"history_id":"12"`) {
		t.Fatalf("result=%+v err=%v validation=%v", result, err, result.Validate())
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
