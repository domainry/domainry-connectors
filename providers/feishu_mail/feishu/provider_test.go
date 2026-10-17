package feishu

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
	"github.com/domainry/domainry-connector-sdk/mail"
	"github.com/domainry/domainry-connector-sdk/mailwrite"
	"github.com/domainry/domainry-connectors/mailattachment"
)

type recordingTransport struct {
	requests  []connector.HTTPRequest
	responses []connector.HTTPResponse
	errors    []error
}

func (transport *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	transport.requests = append(transport.requests, request)
	index := len(transport.requests) - 1
	var response connector.HTTPResponse
	if index < len(transport.responses) {
		response = transport.responses[index]
	}
	var err error
	if index < len(transport.errors) {
		err = transport.errors[index]
	}
	return response, err
}

func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func connection() connector.Connection {
	return connector.Connection{
		Key: "feishu-mail-user", WorkspaceID: "workspace-a", ConnectorKey: ConnectorKey, ProviderKey: ProviderKey,
		Status: "active",
		Config: map[string]any{"mailbox_id": "me", "base_url": "http://localhost:8080", "oauth_base_url": "http://localhost:8080"},
	}
}

func response(value any) connector.HTTPResponse {
	raw, _ := json.Marshal(value)
	return connector.HTTPResponse{StatusCode: http.StatusOK, Body: raw}
}

func messageFixture(id, subject string) map[string]any {
	raw := base64.RawURLEncoding.EncodeToString([]byte("From: Sender <sender@example.test>\r\nTo: me@example.test\r\nReply-To: reply@example.test\r\nSubject: " + subject + "\r\nMessage-ID: <smtp@example.test>\r\n\r\nBody"))
	return map[string]any{"code": 0, "data": map[string]any{"message": map[string]any{
		"message_id": id, "thread_id": "thread-1", "smtp_message_id": "smtp@example.test", "subject": subject,
		"to": []any{map[string]any{"mail_address": "me@example.test", "name": "Me"}}, "cc": []any{}, "bcc": []any{},
		"head_from":     map[string]any{"mail_address": "sender@example.test", "name": "Sender"},
		"internal_date": "1791943200000", "message_state": 1, "raw": raw,
		"body_plain_text": base64.RawURLEncoding.EncodeToString([]byte("正文内容")),
	}}}
}

func call[I, O any](t *testing.T, adapter connector.Adapter, operation connector.CallOperation[I, O], input I, requestRef string) (O, connector.CallResult, error) {
	t.Helper()
	payload, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Call(t.Context(), connector.CallRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: operation.Key, ContractSHA256: operation.ContractSHA256,
		Mode: connector.ModeCall, Connection: connection(), Secrets: map[string]string{"access_token": "private-token"}, Payload: payload, RequestRef: requestRef,
	})
	var output O
	if err == nil && json.Unmarshal(result.Payload, &output) != nil {
		t.Fatalf("invalid typed result: %s", result.Payload)
	}
	return output, result, err
}

func TestProviderDescriptorIsGenericMailContract(t *testing.T) {
	adapter, err := New(&recordingTransport{})
	if err != nil {
		t.Fatal(err)
	}
	if err = contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	if _, ok := adapter.(connector.Reconciler); !ok {
		t.Fatal("mail send/reply declare provider lookup but provider exposes no Reconciler")
	}
	descriptor := adapter.Descriptor()
	if descriptor.ConnectorKey != ConnectorKey || descriptor.ProviderKey != ProviderKey || len(descriptor.Operations) != 7 {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	for _, key := range []string{mail.ListOperationKey, mail.SearchOperationKey, mail.ReadOperationKey, mailwrite.SendOperationKey, mailwrite.ReplyOperationKey, mailattachment.DownloadOperationKey, TestConnection.Key} {
		found := false
		for _, operation := range descriptor.Operations {
			found = found || operation.Key == key
		}
		if !found {
			t.Fatalf("missing operation %s", key)
		}
	}
}

func TestMailListSearchAndReadUseFeishuPaginationAndNormalizeMessages(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{
		response(map[string]any{"code": 0, "data": map[string]any{"items": []string{"mail-1"}, "has_more": true, "page_token": "next"}}),
		response(messageFixture("mail-1", "报价确认")),
		response(messageFixture("mail-1", "报价确认")),
	}}
	adapter, _ := New(transport)
	page, _, err := call(t, adapter, MailList, mail.PageRequest{Limit: 1}, "")
	if err != nil || len(page.Items) != 1 || page.Items[0].Subject != "报价确认" || page.NextCursor != "next" || page.Complete || page.Items[0].ReplyTo[0].Address != "reply@example.test" || !page.Items[0].MetadataComplete {
		t.Fatalf("page=%+v err=%v", page, err)
	}
	listed, _ := url.Parse(transport.requests[0].URL)
	if listed.Path != "/open-apis/mail/v1/user_mailboxes/me/messages" || listed.Query().Get("page_size") != "1" || transport.requests[0].SecretHeaders["Authorization"][0] != "Bearer private-token" {
		t.Fatalf("list request=%+v", transport.requests[0])
	}
	message, _, err := call(t, adapter, MailRead, mail.ReadRequest{MessageID: "mail-1", MaxBodyBytes: 1024}, "")
	if err != nil || message.Body.Text != "正文内容" || !message.Body.Complete || message.Summary.ThreadID != "thread-1" {
		t.Fatalf("message=%+v err=%v", message, err)
	}
}

func TestMailListCapsProviderPageSizeAtOfficialMaximum(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{
		response(map[string]any{"code": 0, "data": map[string]any{"items": []string{}, "has_more": false}}),
	}}
	adapter, _ := New(transport)
	page, _, err := call(t, adapter, MailList, mail.PageRequest{Limit: 25}, "")
	if err != nil || !page.Complete {
		t.Fatalf("page=%+v err=%v", page, err)
	}
	listed, _ := url.Parse(transport.requests[0].URL)
	if listed.Query().Get("page_size") != "20" {
		t.Fatalf("Feishu Mail list exceeded the official page-size maximum: %s", listed.RawQuery)
	}
}

func TestMailPermissionDenialRequiresUserReauthorization(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{
		response(map[string]any{"code": 99991679, "msg": "Unauthorized"}),
	}}
	adapter, _ := New(transport)
	_, _, err := call(t, adapter, MailList, mail.PageRequest{Limit: 1}, "")
	code, ok := connector.ProviderErrorCodeOf(err)
	if !ok || code != "feishu_mail.oauth.user_authorization_required" {
		t.Fatalf("permission denial code=%q ok=%v err=%v", code, ok, err)
	}
}

func TestMailSendAndReplyUseBoundedReceiptsAndNativeDedupe(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{
		response(map[string]any{"code": 0, "data": map[string]any{"primary_email_address": "me@example.test"}}),
		response(map[string]any{"code": 0, "data": map[string]any{"message_id": "sent-1", "thread_id": "sent-thread"}}),
		response(messageFixture("mail-1", "报价确认")),
		response(map[string]any{"code": 0, "data": map[string]any{"primary_email_address": "me@example.test"}}),
		response(map[string]any{"code": 0, "data": map[string]any{"message_id": "sent-2", "thread_id": "thread-1"}}),
	}}
	adapter, _ := New(transport)
	message := mailwrite.Message{To: []mail.Address{{Address: "buyer@example.test"}}, CC: []mail.Address{}, BCC: []mail.Address{}, Subject: "新邮件", Text: "正文"}
	sent, _, err := call(t, adapter, MailSend, mailwrite.SendRequest{Message: message}, "request-send")
	if err != nil || sent.MessageID != "sent-1" || sent.Delivery != "unknown" {
		t.Fatalf("sent=%+v err=%v", sent, err)
	}
	var sendBody map[string]any
	_ = json.Unmarshal(transport.requests[1].Body, &sendBody)
	decoded, _ := base64.RawURLEncoding.DecodeString(sendBody["raw"].(string))
	if !strings.Contains(string(decoded), "Message-ID: <") || !strings.Contains(string(decoded), "@domainry.invalid>") || !strings.Contains(string(decoded), "Content-Transfer-Encoding: quoted-printable") || sendBody["dedupe_key"] == "" {
		t.Fatalf("send body=%v", sendBody)
	}
	replyMessage := mailwrite.Message{To: []mail.Address{{Address: "reply@example.test"}}, CC: []mail.Address{}, BCC: []mail.Address{}, Subject: "Re: 报价确认", Text: "收到"}
	replied, _, err := call(t, adapter, MailReply, mailwrite.ReplyRequest{MessageID: "mail-1", Message: replyMessage}, "request-reply")
	if err != nil || replied.MessageID != "sent-2" || replied.InReplyToMessageID != "mail-1" {
		t.Fatalf("replied=%+v err=%v", replied, err)
	}
	var replyBody map[string]any
	_ = json.Unmarshal(transport.requests[4].Body, &replyBody)
	raw, _ := base64.RawURLEncoding.DecodeString(replyBody["raw"].(string))
	if !strings.Contains(string(raw), "In-Reply-To: <smtp@example.test>") || !strings.Contains(string(raw), "From: <me@example.test>") {
		t.Fatalf("reply MIME=%s", raw)
	}
}

func TestMailWriteReconciliationReadsSentFolderByStableMessageID(t *testing.T) {
	requestRef := "request-send"
	fixture := messageFixture("sent-1", "新邮件")
	message := fixture["data"].(map[string]any)["message"].(map[string]any)
	message["smtp_message_id"] = writeCorrelation(connection(), requestRef, mailwrite.SendOperationKey, "") + "@domainry.invalid"
	message["message_state"] = 2
	message["thread_id"] = "sent-thread"
	transport := &recordingTransport{responses: []connector.HTTPResponse{
		response(map[string]any{"code": 0, "data": map[string]any{"items": []string{"sent-1"}, "has_more": false}}),
		response(fixture),
	}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(mailwrite.SendRequest{Message: mailwrite.Message{To: []mail.Address{{Address: "buyer@example.test"}}, CC: []mail.Address{}, BCC: []mail.Address{}, Subject: "新邮件", Text: "正文"}})
	result, err := adapter.(connector.Reconciler).Reconcile(t.Context(), connector.ReconcileRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: mailwrite.SendOperationKey,
		ContractSHA256: mailwrite.OperationSHA256(mailwrite.SendOperationKey), Connection: connection(), RequestRef: requestRef,
		Payload: payload, Secrets: map[string]string{"access_token": "private-token"}, Principal: connector.Principal{IsAuthenticated: true, WorkspaceID: "workspace-a"},
	})
	if err != nil || result.Validate() != nil || result.Outcome != connector.ReconciliationSucceeded || result.Result == nil {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	var receipt mailwrite.Result
	if json.Unmarshal(result.Result.Payload, &receipt) != nil || receipt.MessageID != "sent-1" || receipt.ThreadID != "sent-thread" || receipt.RequestRef != requestRef {
		t.Fatalf("receipt=%+v", receipt)
	}
	listed, _ := url.Parse(transport.requests[0].URL)
	if listed.Query().Get("folder_id") != "SENT" || listed.Query().Get("page_size") != "20" || len(transport.requests) != 2 {
		t.Fatalf("requests=%+v", transport.requests)
	}
}

func TestAttachmentUsesOneTimeURLWithoutExposingItsQuery(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{
		response(map[string]any{"code": 0, "data": map[string]any{"download_urls": []any{map[string]any{"attachment_id": "attachment-1", "download_url": "https://api-drive-stream.feishu.cn/file?code=private-code"}}, "failed_ids": []any{}}}),
		{StatusCode: http.StatusOK, Body: []byte("attachment")},
	}}
	adapter, _ := New(transport)
	output, _, err := call(t, adapter, MailAttachmentDownload, mailattachment.DownloadRequest{MessageID: "mail-1", AttachmentID: "attachment-1"}, "")
	if err != nil || output.Size != 10 {
		t.Fatalf("output=%+v err=%v", output, err)
	}
	if transport.requests[1].URL != "https://api-drive-stream.feishu.cn/file" || transport.requests[1].SecretQuery["code"] != "private-code" || strings.Contains(transport.requests[1].URL, "private-code") {
		t.Fatalf("download request=%+v", transport.requests[1])
	}
}

func TestOAuthDeclaresExactMailScopesAndPKCE(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	authorizer, ok := connector.ResolveOAuthAuthorizer(adapter)
	if !ok {
		t.Fatal("OAuth authorizer missing")
	}
	verifier := strings.Repeat("v", 43)
	digest := sha256.Sum256([]byte(verifier))
	rawURL, err := authorizer.AuthorizationURL(connector.OAuthAuthorizationRequest{
		Connection: connection(), ClientID: "client", RedirectURI: "https://aurora.example.test/oauth/callback",
		Scopes: append(mailDetailScopes(), mailSendScope), State: strings.Repeat("s", 43), CodeChallenge: base64.RawURLEncoding.EncodeToString(digest[:]),
	})
	parsed, _ := url.Parse(rawURL)
	if err != nil || parsed.Path != "/open-apis/authen/v1/authorize" || parsed.Query().Get("code_challenge_method") != "S256" {
		t.Fatalf("url=%s err=%v", rawURL, err)
	}
	scopes, declared := connector.ResolveOAuthOperationScopes(adapter, mailwrite.ReplyOperationKey)
	if !declared || len(scopes) != 1 || strings.Join(scopes[0], " ") != "mail:user_mailbox:readonly mail:user_mailbox.message:readonly mail:user_mailbox.message.subject:read mail:user_mailbox.message.address:read mail:user_mailbox.message.body:read mail:user_mailbox.message:send" {
		t.Fatalf("reply scopes=%v declared=%v", scopes, declared)
	}
	readScopes, declared := connector.ResolveOAuthOperationScopes(adapter, mail.ReadOperationKey)
	if !declared || len(readScopes) != 1 || strings.Join(readScopes[0], " ") != "mail:user_mailbox.message:readonly mail:user_mailbox.message.subject:read mail:user_mailbox.message.address:read mail:user_mailbox.message.body:read" {
		t.Fatalf("read scopes=%v declared=%v", readScopes, declared)
	}
}

func TestBackgroundSyncPagesInboxAndEmitsNormalizedIdempotentEvent(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{
		response(map[string]any{"code": 0, "data": map[string]any{"primary_email_address": "me@example.test"}}),
		response(map[string]any{"code": 0, "data": map[string]any{"items": []string{"mail-1"}, "has_more": false}}),
		response(messageFixture("mail-1", "报价确认")),
	}}
	adapter, _ := New(transport)
	processor, ok := adapter.(connector.BackgroundProcessor)
	if !ok {
		t.Fatal("background processor missing")
	}
	tasks := processor.BackgroundTasks(connection())
	if len(tasks) != 2 || tasks[0].Key != mailSyncTaskKey || tasks[1].Key != mailSubscriptionTaskKey {
		t.Fatalf("tasks=%+v", tasks)
	}
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	result, err := processor.ProcessBackground(t.Context(), connector.BackgroundRequest{
		TaskKey: mailSyncTaskKey, StateVersion: mailSyncVersion, Connection: connection(), State: json.RawMessage(`{}`),
		Secrets: map[string]string{"access_token": "token"}, Now: now, Principal: connector.Principal{IsAuthenticated: true, WorkspaceID: "workspace-a"},
	})
	if err != nil || result.Validate() != nil || len(result.Events) != 1 || result.Events[0].ExternalID != "feishu_mail:feishu-mail-user:message:mail-1" || result.Events[0].EventType != "feishu_mail.message.received" || !strings.Contains(string(result.State), `"folder":"SENT"`) || !result.NextDueAt.Equal(now.Add(5*time.Second)) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	profile, _ := url.Parse(transport.requests[0].URL)
	if profile.Path != "/open-apis/mail/v1/user_mailboxes/me/profile" {
		t.Fatalf("mailbox identity must come from Feishu Mail profile, got %s", profile.Path)
	}
	if result.Progress == nil || result.Progress.Phase != "bootstrap.sent" || result.Progress.Unit != "message" || result.Progress.CompletedUnits != 1 || result.Progress.InitializationComplete {
		t.Fatalf("progress=%+v", result.Progress)
	}
	var payload map[string]any
	if json.Unmarshal(result.Events[0].Payload, &payload) != nil || payload["thread_id"] != "thread-1" || payload["body"] != "正文内容" || payload["connection_key"] != "feishu-mail-user" {
		t.Fatalf("payload=%s", result.Events[0].Payload)
	}
}

func TestBackgroundBootstrapCompletionPersistsCountAndBecomesReady(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{
		response(map[string]any{"code": 0, "data": map[string]any{"items": []string{"mail-2"}, "has_more": false}}),
		response(messageFixture("mail-2", "已发送")),
	}}
	adapter, _ := New(transport)
	processor := adapter.(connector.BackgroundProcessor)
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	result, err := processor.ProcessBackground(t.Context(), connector.BackgroundRequest{
		TaskKey: mailSyncTaskKey, StateVersion: mailSyncVersion, Connection: connection(),
		State:   json.RawMessage(`{"account_email":"me@example.test","folder":"SENT","bootstrap_processed":20}`),
		Secrets: map[string]string{"access_token": "token"}, Now: now, Principal: connector.Principal{IsAuthenticated: true, WorkspaceID: "workspace-a"},
	})
	if err != nil || result.Validate() != nil || result.Progress == nil || result.Progress.Phase != "ready" || result.Progress.CompletedUnits != 21 || !result.Progress.InitializationComplete || !strings.Contains(string(result.State), `"ready":true`) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestBackgroundBootstrapDoesNotStopAtAnArbitraryHistoryCutoff(t *testing.T) {
	old := messageFixture("mail-old", "历史邮件")
	message := old["data"].(map[string]any)["message"].(map[string]any)
	message["internal_date"] = "946684800000"
	transport := &recordingTransport{responses: []connector.HTTPResponse{
		response(map[string]any{"code": 0, "data": map[string]any{"primary_email_address": "me@example.test"}}),
		response(map[string]any{"code": 0, "data": map[string]any{"items": []string{"mail-old"}, "has_more": true, "page_token": "next"}}),
		response(old),
	}}
	adapter, _ := New(transport)
	processor := adapter.(connector.BackgroundProcessor)
	result, err := processor.ProcessBackground(t.Context(), connector.BackgroundRequest{
		TaskKey: mailSyncTaskKey, StateVersion: mailSyncVersion, Connection: connection(), State: json.RawMessage(`{}`),
		Secrets: map[string]string{"access_token": "token"}, Now: time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC), Principal: connector.Principal{IsAuthenticated: true, WorkspaceID: "workspace-a"},
	})
	if err != nil || len(result.Events) != 1 || !strings.Contains(string(result.State), `"page_token":"next"`) || !strings.Contains(string(result.State), `"folder":"INBOX"`) {
		t.Fatalf("old history was not emitted and paged: result=%+v err=%v", result, err)
	}
	listed, _ := url.Parse(transport.requests[1].URL)
	if listed.Query().Get("page_size") != "20" {
		t.Fatalf("background sync exceeded the official page-size maximum: %s", listed.RawQuery)
	}
}

func TestBackgroundSubscriptionChecksThenSubscribes(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{
		response(map[string]any{"code": 0, "data": map[string]any{"event_types": []int{}}}),
		response(map[string]any{"code": 0, "data": map[string]any{}}),
	}}
	adapter, _ := New(transport)
	processor := adapter.(connector.BackgroundProcessor)
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	result, err := processor.ProcessBackground(t.Context(), connector.BackgroundRequest{
		TaskKey: mailSubscriptionTaskKey, StateVersion: mailSyncVersion, Connection: connection(), State: json.RawMessage(`{}`),
		Secrets: map[string]string{"access_token": "token"}, Now: now, Principal: connector.Principal{IsAuthenticated: true, WorkspaceID: "workspace-a"},
	})
	if err != nil || result.Validate() != nil || !result.NextDueAt.Equal(now.Add(24*time.Hour)) || len(transport.requests) != 2 || !strings.HasSuffix(transport.requests[1].URL, "/event/subscribe") {
		t.Fatalf("result=%+v requests=%+v err=%v", result, transport.requests, err)
	}
}

func TestWebhookOnlyWakesSyncAndCarriesStableRoutes(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	payload := []byte(`{"schema":"2.0","header":{"event_id":"delivery-1","event_type":"mail.user_mailbox.event.message_received_v1","token":"verify-token"},"event":{"mail_address":"Me@Example.Test","message_id":"mail-1","mailbox_type":1,"subscriber":{"user_ids":[{"open_id":"ou_a","union_id":"on_a","user_id":"u_a"}]}}}`)
	verified, err := adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Connection: connection(), Secrets: map[string]string{"verification_token": "verify-token"}, Body: payload, ReceivedAt: time.Now().UTC(),
	})
	if err != nil || verified.ExternalID != "delivery-1" || verified.EventType != "feishu_mail.message.notification" {
		t.Fatalf("verified=%+v err=%v", verified, err)
	}
	var envelope struct {
		Routes []map[string]string `json:"routes"`
		Wake   []string            `json:"wake_tasks"`
		Events []any               `json:"events"`
	}
	if json.Unmarshal(verified.Payload, &envelope) != nil || len(envelope.Routes) != 4 || len(envelope.Wake) != 1 || envelope.Wake[0] != mailSyncTaskKey || len(envelope.Events) != 0 {
		t.Fatalf("envelope=%s", verified.Payload)
	}
}
