package microsoft

import (
	"context"
	"encoding/json"
	"fmt"
	mailparser "net/mail"
	"net/url"
	"strconv"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/internal/mailcontent"
)

const outlookSyncTaskKey = "outlook_mail_sync"

type outlookSyncState struct {
	AccountEmail string `json:"account_email,omitempty"`
	Folder       string `json:"folder,omitempty"`
	InboxCursor  string `json:"inbox_cursor,omitempty"`
	SentCursor   string `json:"sent_cursor,omitempty"`
	InboxReady   bool   `json:"inbox_ready,omitempty"`
	SentReady    bool   `json:"sent_ready,omitempty"`
}

type outlookDeltaPage struct {
	Items []graphMailMessage `json:"value"`
	Next  string             `json:"@odata.nextLink"`
	Delta string             `json:"@odata.deltaLink"`
}

func (p *provider) BackgroundTasks(connection connector.Connection) []connector.BackgroundTaskDescriptor {
	if connection.Status != "active" || !microsoftBackgroundBool(connection.Config, "outlook_ingest_enabled", true) {
		return nil
	}
	return []connector.BackgroundTaskDescriptor{{Key: outlookSyncTaskKey, StateVersion: 1}}
}

func (p *provider) ProcessBackground(ctx context.Context, request connector.BackgroundRequest) (connector.BackgroundResult, error) {
	if err := request.Validate(); err != nil {
		return connector.BackgroundResult{}, err
	}
	if request.Connection.ConnectorKey != ConnectorKey || request.Connection.ProviderKey != ProviderKey || request.TaskKey != outlookSyncTaskKey || request.StateVersion != 1 {
		return connector.BackgroundResult{}, permanent("background.request_invalid", "Microsoft background request is invalid")
	}
	state := outlookSyncState{}
	if len(request.State) != 0 && string(request.State) != "null" {
		if err := json.Unmarshal(request.State, &state); err != nil {
			return connector.BackgroundResult{}, permanent("outlook.sync_state_invalid", "Outlook sync state is invalid")
		}
	}
	updates := map[string]string{}
	if state.AccountEmail == "" {
		profile, profileErr := p.executeWithRefresh(ctx, request.Connection, request.Secrets, graphBase(request.Connection)+"/me", url.Values{"$select": {"id,mail,userPrincipalName"}})
		mergeMicrosoftSecretUpdates(updates, profile.SecretUpdates)
		if profileErr != nil {
			return connector.BackgroundResult{SecretUpdates: updates}, profileErr
		}
		state.AccountEmail = strings.ToLower(strings.TrimSpace(graphString(profile.Output, "mail")))
		if state.AccountEmail == "" {
			state.AccountEmail = strings.ToLower(strings.TrimSpace(graphString(profile.Output, "userPrincipalName")))
		}
		if !validMicrosoftAccountEmail(state.AccountEmail) {
			return connector.BackgroundResult{SecretUpdates: updates}, permanent("outlook.account_identity_invalid", "Microsoft profile lacks a mailbox email")
		}
	}
	folder := strings.ToLower(strings.TrimSpace(state.Folder))
	if folder != "sentitems" {
		folder = "inbox"
	}
	cursor, ready := state.InboxCursor, state.InboxReady
	if folder == "sentitems" {
		cursor, ready = state.SentCursor, state.SentReady
	}
	endpoint, query, endpointErr := outlookDeltaEndpoint(request.Connection, folder, cursor, request.Now, microsoftBackgroundInt(request.Connection.Config, "outlook_history_days", 90))
	if endpointErr != nil {
		return connector.BackgroundResult{}, endpointErr
	}
	pageResult, pageErr := p.executeGraphWithRefresh(ctx, request.Connection, request.Secrets, "GET", endpoint, query, nil, "", `IdType="ImmutableId"`, `outlook.body-content-type="text"`, "odata.maxpagesize=100")
	mergeMicrosoftSecretUpdates(updates, pageResult.SecretUpdates)
	if pageErr != nil {
		if code, ok := connector.ProviderErrorCodeOf(pageErr); ok && code == "microsoft.http_410" {
			if folder == "inbox" {
				state.InboxCursor, state.InboxReady = "", false
			} else {
				state.SentCursor, state.SentReady = "", false
			}
			raw, _ := json.Marshal(state)
			return connector.BackgroundResult{State: raw, NextDueAt: request.Now.Add(5 * time.Second), SecretUpdates: updates}, nil
		}
		return connector.BackgroundResult{SecretUpdates: updates}, pageErr
	}
	var page outlookDeltaPage
	rawPage, marshalErr := json.Marshal(pageResult.Output)
	if marshalErr != nil || json.Unmarshal(rawPage, &page) != nil || page.Next != "" && page.Delta != "" || page.Next == "" && page.Delta == "" {
		return connector.BackgroundResult{SecretUpdates: updates}, permanent("outlook.delta_response_invalid", "Outlook delta response is invalid")
	}
	events := make([]connector.BackgroundEvent, 0, len(page.Items))
	source := "bootstrap"
	if ready {
		source = "incremental"
	}
	for _, message := range page.Items {
		if message.Removed != nil || message.IsDraft != nil && *message.IsDraft {
			continue
		}
		event, projectionErr := outlookMessageEvent(request.Connection.Key, state.AccountEmail, folder, source, message)
		if projectionErr != nil {
			payload, _ := json.Marshal(map[string]any{"source": source, "account_email": state.AccountEmail, "message_id": message.ID, "failure_code": "outlook.message_malformed"})
			events = append(events, connector.BackgroundEvent{ExternalID: "outlook:" + request.Connection.Key + ":message:" + message.ID, EventType: "outlook.message.malformed", Payload: payload})
			continue
		}
		events = append(events, event)
	}
	next := strings.TrimSpace(page.Next)
	completed := next == ""
	if completed {
		next = strings.TrimSpace(page.Delta)
	}
	if _, _, err := outlookDeltaEndpoint(request.Connection, folder, next, request.Now, 90); err != nil {
		return connector.BackgroundResult{SecretUpdates: updates}, err
	}
	if folder == "inbox" {
		state.InboxCursor, state.InboxReady = next, completed || state.InboxReady
	} else {
		state.SentCursor, state.SentReady = next, completed || state.SentReady
	}
	nextDue := request.Now.Add(5 * time.Second)
	if completed {
		if folder == "inbox" {
			state.Folder = "sentitems"
		} else {
			state.Folder = "inbox"
			reconcileSeconds := microsoftBackgroundInt(request.Connection.Config, "outlook_reconcile_seconds", 900)
			if reconcileSeconds < 300 {
				reconcileSeconds = 300
			}
			if reconcileSeconds > 86400 {
				reconcileSeconds = 86400
			}
			nextDue = request.Now.Add(time.Duration(reconcileSeconds) * time.Second)
		}
	} else {
		state.Folder = folder
	}
	rawState, _ := json.Marshal(state)
	return connector.BackgroundResult{State: rawState, NextDueAt: nextDue, Events: events, SecretUpdates: updates}, nil
}

func outlookDeltaEndpoint(connection connector.Connection, folder, cursor string, now time.Time, historyDays int) (string, url.Values, error) {
	base, err := url.Parse(graphBase(connection))
	if err != nil {
		return "", nil, permanent("outlook.delta_cursor_invalid", "Microsoft Graph base URL is invalid")
	}
	expectedPath := strings.TrimRight(base.Path, "/") + "/me/mailFolders/" + url.PathEscape(folder) + "/messages/delta"
	if strings.TrimSpace(cursor) != "" {
		parsed, parseErr := url.Parse(strings.TrimSpace(cursor))
		if parseErr != nil || parsed.Scheme != base.Scheme || parsed.Host != base.Host || parsed.User != nil || parsed.Fragment != "" || parsed.Path != expectedPath {
			return "", nil, permanent("outlook.delta_cursor_invalid", "Outlook delta cursor is outside the configured Microsoft Graph endpoint")
		}
		return parsed.String(), nil, nil
	}
	if historyDays < 1 {
		historyDays = 1
	}
	if historyDays > 3650 {
		historyDays = 3650
	}
	query := url.Values{
		"$top":     {"100"},
		"$select":  {"id,conversationId,internetMessageId,subject,from,toRecipients,ccRecipients,receivedDateTime,sentDateTime,isDraft,body,bodyPreview,internetMessageHeaders"},
		"$expand":  {"attachments($select=id,name,contentType,size,isInline)"},
		"$filter":  {"receivedDateTime ge " + now.AddDate(0, 0, -historyDays).UTC().Format(time.RFC3339)},
		"$orderby": {"receivedDateTime desc"},
	}
	base.Path, base.RawQuery = expectedPath, ""
	return base.String(), query, nil
}

func outlookMessageEvent(connectionKey, accountEmail, folder, source string, message graphMailMessage) (connector.BackgroundEvent, error) {
	if strings.TrimSpace(message.ID) == "" || strings.TrimSpace(message.ThreadID) == "" || message.From == nil || message.From.EmailAddress == nil {
		return connector.BackgroundEvent{}, fmt.Errorf("message identity or sender is missing")
	}
	date := strings.TrimSpace(message.SentAt)
	if date == "" {
		date = strings.TrimSpace(message.ReceivedAt)
	}
	if _, err := time.Parse(time.RFC3339Nano, date); err != nil {
		return connector.BackgroundEvent{}, fmt.Errorf("message timestamp is invalid")
	}
	from := graphAddressHeader([]graphMailRecipient{*message.From})
	if from == "" {
		return connector.BackgroundEvent{}, fmt.Errorf("message sender is invalid")
	}
	body := ""
	if message.Body != nil && message.Body.Content != nil {
		body = *message.Body.Content
	}
	if strings.TrimSpace(body) == "" && message.BodyPreview != nil {
		body = *message.BodyPreview
	}
	body, _ = mailcontent.Text(body, 1<<20, true)
	headers := map[string]string{}
	for _, header := range message.InternetMessageHeaders {
		key := strings.ToLower(strings.TrimSpace(header.Name))
		if key != "" && headers[key] == "" {
			headers[key] = strings.TrimSpace(header.Value)
		}
	}
	attachments := make([]map[string]any, 0, len(message.Attachments))
	for _, attachment := range message.Attachments {
		if strings.TrimSpace(attachment.ID) == "" {
			continue
		}
		attachments = append(attachments, map[string]any{
			"filename": attachment.Name, "mime_type": attachment.ContentType, "attachment_id": attachment.ID,
			"size": attachment.Size, "inline": attachment.IsInline,
		})
	}
	labels := []string{"INBOX"}
	if folder == "sentitems" {
		labels = []string{"SENT"}
	}
	subject := ""
	if message.Subject != nil {
		subject = *message.Subject
	}
	internetMessageID := ""
	if message.InternetMessageID != nil {
		internetMessageID = *message.InternetMessageID
	}
	payload := map[string]any{
		"source": source, "account_email": accountEmail, "message_id": message.ID, "thread_id": message.ThreadID,
		"rfc_message_id": internetMessageID, "in_reply_to": headers["in-reply-to"], "references": headers["references"],
		"from": from, "to": graphAddressHeader(graphRecipients(message.To)), "cc": graphAddressHeader(graphRecipients(message.CC)),
		"subject": subject, "date": date, "internal_date": "", "label_ids": labels, "snippet": microsoftString(message.BodyPreview),
		"body": body, "attachments": attachments, "auto_submitted": headers["auto-submitted"], "precedence": headers["precedence"],
		"list_id": headers["list-id"], "list_unsubscribe": headers["list-unsubscribe"], "x_auto_response_suppress": headers["x-auto-response-suppress"],
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return connector.BackgroundEvent{}, err
	}
	return connector.BackgroundEvent{ExternalID: "outlook:" + connectionKey + ":message:" + message.ID, EventType: "outlook.message.received", Payload: raw}, nil
}

func graphRecipients(values *[]graphMailRecipient) []graphMailRecipient {
	if values == nil {
		return nil
	}
	return *values
}

func graphAddressHeader(values []graphMailRecipient) string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value.EmailAddress == nil {
			continue
		}
		address := strings.ToLower(strings.TrimSpace(value.EmailAddress.Address))
		if !validMicrosoftAccountEmail(address) {
			continue
		}
		result = append(result, (&mailparser.Address{Name: strings.TrimSpace(value.EmailAddress.Name), Address: address}).String())
	}
	return strings.Join(result, ", ")
}

func microsoftString(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func microsoftBackgroundBool(config map[string]any, key string, fallback bool) bool {
	value, exists := config[key]
	if !exists {
		return fallback
	}
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(typed))
		return err == nil && parsed
	default:
		return fallback
	}
}

func microsoftBackgroundInt(config map[string]any, key string, fallback int) int {
	return integer(config[key], fallback)
}

func mergeMicrosoftSecretUpdates(target, values map[string]string) {
	for key, value := range values {
		target[key] = value
	}
}

var _ connector.BackgroundProcessor = (*provider)(nil)
