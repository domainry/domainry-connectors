package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	stdmail "net/mail"
	"net/url"
	"strconv"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/mail"
)

const (
	mailSyncTaskKey         = "feishu_mail_sync"
	mailSubscriptionTaskKey = "feishu_mail_subscription"
	mailSyncVersion         = 2
	mailSyncPageSize        = 20
	mailSyncBodyLimit       = 1 << 20
)

type mailSyncState struct {
	AccountEmail       string `json:"account_email,omitempty"`
	Folder             string `json:"folder,omitempty"`
	PageToken          string `json:"page_token,omitempty"`
	BootstrapProcessed int64  `json:"bootstrap_processed,omitempty"`
	Ready              bool   `json:"ready,omitempty"`
}

func (*provider) BackgroundTasks(connection connector.Connection) []connector.BackgroundTaskDescriptor {
	if connection.Status != "active" || !configBool(connection.Config, "mail_ingest_enabled", true) {
		return nil
	}
	tasks := []connector.BackgroundTaskDescriptor{{Key: mailSyncTaskKey, StateVersion: mailSyncVersion}}
	if configBool(connection.Config, "mail_subscription_enabled", true) {
		tasks = append(tasks, connector.BackgroundTaskDescriptor{Key: mailSubscriptionTaskKey, StateVersion: mailSyncVersion})
	}
	return tasks
}

func (p *provider) ProcessBackground(ctx context.Context, request connector.BackgroundRequest) (connector.BackgroundResult, error) {
	if err := request.Validate(); err != nil {
		return connector.BackgroundResult{}, err
	}
	if request.Connection.ConnectorKey != ConnectorKey || request.Connection.ProviderKey != ProviderKey || request.StateVersion != mailSyncVersion {
		return connector.BackgroundResult{}, permanent("background.request_invalid", "Feishu Mail background request is invalid")
	}
	if request.TaskKey == mailSubscriptionTaskKey {
		return p.processMailSubscription(ctx, request)
	}
	if request.TaskKey != mailSyncTaskKey {
		return connector.BackgroundResult{}, permanent("background.task_unknown", "Feishu Mail background task is unknown")
	}
	state := mailSyncState{}
	if len(request.State) != 0 && string(request.State) != "null" {
		decoder := json.NewDecoder(strings.NewReader(string(request.State)))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&state) != nil || decoder.Decode(&struct{}{}) != io.EOF {
			return connector.BackgroundResult{}, permanent("background.state_invalid", "Feishu Mail background state is invalid")
		}
	}
	session := p.newAPISession(request.Connection, request.Secrets)
	if state.AccountEmail == "" {
		accountEmail, err := session.authenticatedMailbox(ctx)
		if err != nil {
			return connector.BackgroundResult{SecretUpdates: cloneStrings(session.updates)}, err
		}
		state.AccountEmail = strings.ToLower(accountEmail)
	}
	folder := strings.ToUpper(strings.TrimSpace(state.Folder))
	if folder != "SENT" {
		folder = "INBOX"
	}
	query := url.Values{"page_size": {strconv.Itoa(mailSyncPageSize)}, "folder_id": {folder}}
	if state.PageToken != "" {
		query.Set("page_token", state.PageToken)
	}
	pageResult, err := session.call(ctx, http.MethodGet, messagePath(request.Connection), query, nil, false)
	if err != nil {
		return connector.BackgroundResult{SecretUpdates: cloneStrings(session.updates)}, err
	}
	var page feishuMessagePage
	if decodeData(pageResult.Output, &page) != nil || len(page.Items) > mailSyncPageSize || page.HasMore && page.PageToken == "" || !page.HasMore && page.PageToken != "" {
		return connector.BackgroundResult{SecretUpdates: cloneStrings(session.updates)}, mailInvalidResponse()
	}
	bootstrap := !state.Ready
	events := make([]connector.BackgroundEvent, 0, len(page.Items))
	for _, messageID := range page.Items {
		_, message, fetchErr := session.getMessage(ctx, messageID)
		if fetchErr != nil {
			return connector.BackgroundResult{SecretUpdates: cloneStrings(session.updates)}, fetchErr
		}
		event, _, projectionErr := feishuMailEvent(request.Connection.Key, state.AccountEmail, folder, backgroundSource(state.Ready), messageID, message)
		if projectionErr != nil {
			payload, _ := json.Marshal(map[string]any{"source": backgroundSource(state.Ready), "connection_key": request.Connection.Key, "account_email": state.AccountEmail, "message_id": messageID, "failure_code": "feishu_mail.message_malformed"})
			events = append(events, connector.BackgroundEvent{ExternalID: "feishu_mail:" + request.Connection.Key + ":message:" + messageID, EventType: "feishu_mail.message.malformed", Payload: payload})
			continue
		}
		events = append(events, event)
	}
	if bootstrap {
		state.BootstrapProcessed += int64(len(page.Items))
	}
	completed := !page.HasMore
	nextDue := request.Now.Add(5 * time.Second)
	if !completed {
		state.Folder, state.PageToken = folder, page.PageToken
	} else if folder == "INBOX" {
		state.Folder, state.PageToken = "SENT", ""
	} else {
		state.Folder, state.PageToken, state.Ready = "INBOX", "", true
		reconcileSeconds := configInt(request.Connection.Config, "mail_reconcile_seconds", 900)
		if reconcileSeconds < 300 {
			reconcileSeconds = 300
		}
		if reconcileSeconds > 86400 {
			reconcileSeconds = 86400
		}
		nextDue = request.Now.Add(time.Duration(reconcileSeconds) * time.Second)
	}
	rawState, _ := json.Marshal(state)
	phase := "reconcile." + strings.ToLower(state.Folder)
	if !state.Ready {
		phase = "bootstrap." + strings.ToLower(state.Folder)
	} else if bootstrap {
		phase = "ready"
	} else if completed && folder == "SENT" {
		phase = "reconcile.waiting"
	}
	progress := &connector.BackgroundProgress{Phase: phase, Unit: "message", CompletedUnits: state.BootstrapProcessed, InitializationComplete: state.Ready}
	return connector.BackgroundResult{State: rawState, NextDueAt: nextDue, Progress: progress, Events: events, SecretUpdates: cloneStrings(session.updates)}, nil
}

func (p *provider) processMailSubscription(ctx context.Context, request connector.BackgroundRequest) (connector.BackgroundResult, error) {
	session := p.newAPISession(request.Connection, request.Secrets)
	path := "/open-apis/mail/v1/user_mailboxes/" + url.PathEscape(mailboxID(request.Connection)) + "/event"
	current, err := session.call(ctx, http.MethodGet, path+"/subscription", nil, nil, false)
	if err != nil {
		return connector.BackgroundResult{SecretUpdates: cloneStrings(session.updates)}, err
	}
	var subscription struct {
		EventTypes []int `json:"event_types"`
	}
	if decodeData(current.Output, &subscription) != nil {
		return connector.BackgroundResult{SecretUpdates: cloneStrings(session.updates)}, mailInvalidResponse()
	}
	subscribed := false
	for _, eventType := range subscription.EventTypes {
		subscribed = subscribed || eventType == 1
	}
	if !subscribed {
		if _, err = session.call(ctx, http.MethodPost, path+"/subscribe", nil, map[string]any{"event_type": 1}, true); err != nil {
			return connector.BackgroundResult{SecretUpdates: cloneStrings(session.updates)}, err
		}
	}
	return connector.BackgroundResult{State: json.RawMessage(`{"subscribed":true}`), NextDueAt: request.Now.Add(24 * time.Hour), SecretUpdates: cloneStrings(session.updates)}, nil
}

func (p *provider) CleanupBackground(ctx context.Context, connection connector.Connection, secrets map[string]string, _ time.Time, _ connector.Principal) (map[string]string, error) {
	if !configBool(connection.Config, "mail_subscription_enabled", true) {
		return nil, nil
	}
	session := p.newAPISession(connection, secrets)
	path := "/open-apis/mail/v1/user_mailboxes/" + url.PathEscape(mailboxID(connection)) + "/event/unsubscribe"
	_, err := session.call(ctx, http.MethodPost, path, nil, map[string]any{"event_type": 1}, true)
	return cloneStrings(session.updates), err
}

func backgroundSource(ready bool) string {
	if ready {
		return "reconcile"
	}
	return "bootstrap"
}

func feishuMailEvent(connectionKey, accountEmail, folder, source, expectedID string, message feishuMessage) (connector.BackgroundEvent, time.Time, error) {
	summary, err := message.summary(expectedID)
	if err != nil || summary.ThreadID == "" || len(summary.From) == 0 {
		return connector.BackgroundEvent{}, time.Time{}, errors.New("message identity or sender is missing")
	}
	date := summary.ReceivedAt
	if date == "" {
		date = summary.SentAt
	}
	occurredAt, err := time.Parse(time.RFC3339Nano, date)
	if err != nil {
		return connector.BackgroundEvent{}, time.Time{}, errors.New("message timestamp is invalid")
	}
	body, err := message.body(mailSyncBodyLimit)
	if err != nil {
		return connector.BackgroundEvent{}, time.Time{}, err
	}
	headers := message.rawHeaders()
	attachments := make([]map[string]any, 0, len(message.Attachments))
	for _, attachment := range message.Attachments {
		if strings.TrimSpace(attachment.ID) == "" {
			continue
		}
		attachments = append(attachments, map[string]any{
			"filename": attachment.Filename, "mime_type": "", "attachment_id": attachment.ID, "size": nil, "attachment_type": attachment.AttachmentType,
		})
	}
	labels := []string{folder}
	payload := map[string]any{
		"source": source, "connection_key": connectionKey, "account_email": accountEmail, "message_id": summary.ID, "thread_id": summary.ThreadID,
		"rfc_message_id": summary.InternetMessageID, "in_reply_to": headers["in-reply-to"], "references": headers["references"],
		"from": addressHeader(summary.From), "to": addressHeader(summary.To), "cc": addressHeader(summary.CC), "subject": summary.Subject,
		"date": date, "internal_date": stringValue(message.InternalDate), "label_ids": labels, "snippet": truncateUTF8(body.Text, 200),
		"body": body.Text, "attachments": attachments, "auto_submitted": headers["auto-submitted"], "precedence": headers["precedence"],
		"list_id": headers["list-id"], "list_unsubscribe": headers["list-unsubscribe"], "x_auto_response_suppress": headers["x-auto-response-suppress"],
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return connector.BackgroundEvent{}, time.Time{}, err
	}
	return connector.BackgroundEvent{ExternalID: "feishu_mail:" + connectionKey + ":message:" + expectedID, EventType: "feishu_mail.message.received", Payload: raw}, occurredAt, nil
}

func (message feishuMessage) rawHeaders() map[string]string {
	result := map[string]string{}
	if message.Raw == nil {
		return result
	}
	raw, err := decodeBase64URL(*message.Raw)
	if err != nil {
		return result
	}
	parsed, err := stdmail.ReadMessage(strings.NewReader(string(raw)))
	if err != nil {
		return result
	}
	for _, key := range []string{"In-Reply-To", "References", "Auto-Submitted", "Precedence", "List-ID", "List-Unsubscribe", "X-Auto-Response-Suppress"} {
		result[strings.ToLower(key)] = strings.TrimSpace(parsed.Header.Get(key))
	}
	return result
}

func addressHeader(addresses []mail.Address) string {
	values := make([]string, 0, len(addresses))
	for _, address := range addresses {
		values = append(values, (&stdmail.Address{Name: address.Name, Address: address.Address}).String())
	}
	return strings.Join(values, ", ")
}

var _ connector.BackgroundProcessor = (*provider)(nil)
var _ connector.BackgroundCleanupProcessor = (*provider)(nil)
