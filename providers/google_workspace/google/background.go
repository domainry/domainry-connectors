package google

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"strconv"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/internal/mailcontent"
)

const (
	gmailSyncTaskKey                        = "gmail_sync"
	gmailWatchTaskKey                       = "gmail_watch"
	gmailMaxPages                           = 20
	gmailMaxBodyBytes                       = 1 << 20
	gmailHistoryMessagesAddedObservationKey = "google.gmail.history.messages_added"
	gmailHistoryCursorExpiredObservationKey = "google.gmail.history.cursor_expired"
)

type gmailSyncState struct {
	AccountEmail       string `json:"account_email,omitempty"`
	HistoryID          string `json:"history_id,omitempty"`
	BootstrapHistoryID string `json:"bootstrap_history_id,omitempty"`
	BootstrapPageToken string `json:"bootstrap_page_token,omitempty"`
	BootstrapSource    string `json:"bootstrap_source,omitempty"`
}

type gmailWatchState struct {
	TopicID     string `json:"topic_id,omitempty"`
	ExpiresAt   string `json:"expires_at,omitempty"`
	NextRenewAt string `json:"next_renew_at,omitempty"`
}

func (p *provider) BackgroundTasks(connection connector.Connection) []connector.BackgroundTaskDescriptor {
	if !backgroundBool(connection.Config, "gmail_ingest_enabled", true) || connection.Status != "active" {
		return nil
	}
	tasks := []connector.BackgroundTaskDescriptor{{Key: gmailSyncTaskKey, StateVersion: 2}}
	if backgroundBool(connection.Config, "gmail_watch_enabled", true) && backgroundConfig(connection.Config, "gmail_pubsub_project_id", "") != "" {
		tasks = append(tasks, connector.BackgroundTaskDescriptor{Key: gmailWatchTaskKey, StateVersion: 2})
	}
	return tasks
}

func (p *provider) ProcessBackground(ctx context.Context, request connector.BackgroundRequest) (connector.BackgroundResult, error) {
	if err := request.Validate(); err != nil {
		return connector.BackgroundResult{}, err
	}
	if request.Connection.ConnectorKey != ConnectorKey || request.Connection.ProviderKey != ProviderKey {
		return connector.BackgroundResult{}, permanent("background.connection_mismatch", "background connection does not match Google Workspace")
	}
	if request.StateVersion != 2 {
		return connector.BackgroundResult{}, permanent("background.state_version_unsupported", "unsupported Google Workspace background state version")
	}
	switch request.TaskKey {
	case gmailSyncTaskKey:
		return p.processGmailSync(ctx, request)
	case gmailWatchTaskKey:
		return p.processGmailWatch(ctx, request)
	default:
		return connector.BackgroundResult{}, permanent("background.task_unknown", "unknown Google Workspace background task")
	}
}

func (p *provider) CleanupBackground(ctx context.Context, connection connector.Connection, secrets map[string]string, now time.Time, principal connector.Principal) (map[string]string, error) {
	if !backgroundBool(connection.Config, "gmail_watch_enabled", true) || backgroundConfig(connection.Config, "gmail_pubsub_project_id", "") == "" {
		return nil, nil
	}
	request := connector.BackgroundRequest{TaskKey: gmailWatchTaskKey, StateVersion: 2, Connection: connection, State: json.RawMessage(`{}`), Secrets: secrets, Now: now, Principal: principal}
	resolved, updates := cloneStrings(secrets), map[string]string{}
	_, current, err := p.backgroundCall(ctx, request, GmailStop.Key, GmailStop.ContractSHA256, map[string]any{}, resolved)
	mergeStrings(updates, current)
	if err != nil && !backgroundErrorCodeIs(err, "google.http_404") {
		return updates, err
	}
	return updates, nil
}

func (p *provider) processGmailSync(ctx context.Context, request connector.BackgroundRequest) (connector.BackgroundResult, error) {
	state := gmailSyncState{}
	if len(request.State) != 0 && string(request.State) != "null" {
		if err := strictBackgroundJSON(request.State, &state); err != nil {
			return connector.BackgroundResult{}, permanent("gmail.sync_state_invalid", "Gmail sync state is invalid")
		}
	}
	secrets := cloneStrings(request.Secrets)
	profile, updates, err := p.backgroundCall(ctx, request, GmailGetProfile.Key, GmailGetProfile.ContractSHA256, map[string]any{}, secrets)
	updates = cloneStrings(updates)
	mergeStrings(secrets, updates)
	if err != nil {
		return connector.BackgroundResult{SecretUpdates: updates}, err
	}
	account, currentHistoryID := backgroundString(profile, "emailAddress"), backgroundString(profile, "historyId")
	if account == "" || currentHistoryID == "" {
		return connector.BackgroundResult{}, permanent("gmail.profile_invalid", "Gmail profile lacks emailAddress or historyId")
	}
	events := []connector.BackgroundEvent{}
	observations := []connector.BackgroundObservation{}
	if state.HistoryID == "" {
		if state.BootstrapHistoryID == "" && backgroundBool(request.Connection.Config, "gmail_ingest_bootstrap", true) {
			state.BootstrapHistoryID, state.BootstrapSource = currentHistoryID, "bootstrap"
		}
		if state.BootstrapHistoryID != "" {
			var bootstrapUpdates map[string]string
			var nextPage string
			events, nextPage, bootstrapUpdates, err = p.currentGmailMessagesPage(ctx, request, account, state.BootstrapPageToken, state.BootstrapSource, secrets)
			mergeStrings(secrets, bootstrapUpdates)
			mergeStrings(updates, bootstrapUpdates)
			if err == nil && nextPage != "" {
				state.AccountEmail, state.BootstrapPageToken = account, nextPage
				raw, _ := json.Marshal(state)
				return connector.BackgroundResult{State: raw, NextDueAt: request.Now.Add(5 * time.Second), Events: events, SecretUpdates: updates}, nil
			}
			currentHistoryID = state.BootstrapHistoryID
			state.BootstrapHistoryID, state.BootstrapPageToken, state.BootstrapSource = "", "", ""
		}
	} else {
		var historyUpdates map[string]string
		var historyMessagesAdded int64
		currentHistoryID, events, historyMessagesAdded, historyUpdates, err = p.gmailHistory(ctx, request, account, state.HistoryID, secrets)
		mergeStrings(updates, historyUpdates)
		if err == nil && historyMessagesAdded > 0 && gmailPushMonitoringEnabled(request.Connection) {
			observations = append(observations, connector.BackgroundObservation{Key: gmailHistoryMessagesAddedObservationKey, Value: historyMessagesAdded})
		}
		if err != nil && backgroundErrorCodeIs(err, "google.http_404") {
			if gmailPushMonitoringEnabled(request.Connection) {
				observations = append(observations, connector.BackgroundObservation{Key: gmailHistoryCursorExpiredObservationKey, Value: 1})
			}
			var reconcileUpdates map[string]string
			var nextPage string
			state.HistoryID, state.BootstrapHistoryID, state.BootstrapSource = "", backgroundString(profile, "historyId"), "reconcile"
			events, nextPage, reconcileUpdates, err = p.currentGmailMessagesPage(ctx, request, account, "", state.BootstrapSource, secrets)
			mergeStrings(updates, reconcileUpdates)
			if err == nil && nextPage != "" {
				state.AccountEmail, state.BootstrapPageToken = account, nextPage
				raw, _ := json.Marshal(state)
				return connector.BackgroundResult{State: raw, NextDueAt: request.Now.Add(5 * time.Second), Events: events, Observations: observations, SecretUpdates: updates}, nil
			}
			currentHistoryID = state.BootstrapHistoryID
			state.BootstrapHistoryID, state.BootstrapPageToken, state.BootstrapSource = "", "", ""
		}
	}
	if err != nil {
		return connector.BackgroundResult{SecretUpdates: updates}, err
	}
	state.AccountEmail, state.HistoryID = account, currentHistoryID
	raw, _ := json.Marshal(state)
	reconcileSeconds := backgroundInt(request.Connection.Config, "gmail_reconcile_seconds", 3600)
	if reconcileSeconds < 300 {
		reconcileSeconds = 300
	}
	if reconcileSeconds > 86400 {
		reconcileSeconds = 86400
	}
	return connector.BackgroundResult{State: raw, NextDueAt: request.Now.Add(time.Duration(reconcileSeconds) * time.Second), Events: events, Observations: observations, SecretUpdates: updates}, nil
}

func gmailPushMonitoringEnabled(connection connector.Connection) bool {
	return backgroundBool(connection.Config, "gmail_watch_enabled", true) && backgroundConfig(connection.Config, "gmail_pubsub_project_id", "") != ""
}

func (p *provider) gmailHistory(ctx context.Context, request connector.BackgroundRequest, account, start string, secrets map[string]string) (string, []connector.BackgroundEvent, int64, map[string]string, error) {
	pageToken, finalID := "", start
	seen, events, updates := map[string]struct{}{}, []connector.BackgroundEvent{}, map[string]string{}
	for page := 0; page < gmailMaxPages; page++ {
		input := map[string]any{"start_history_id": start, "limit": 100, "label_id": backgroundConfig(request.Connection.Config, "gmail_ingest_label", "")}
		if pageToken != "" {
			input["page_token"] = pageToken
		}
		response, currentUpdates, err := p.backgroundCall(ctx, request, SyncEmailHistory.Key, SyncEmailHistory.ContractSHA256, input, secrets)
		mergeStrings(secrets, currentUpdates)
		mergeStrings(updates, currentUpdates)
		if err != nil {
			return "", nil, 0, updates, err
		}
		for _, history := range backgroundMapSlice(response["history"]) {
			for _, added := range backgroundMapSlice(history["messagesAdded"]) {
				message, _ := added["message"].(map[string]any)
				id := backgroundString(message, "id")
				if id == "" {
					continue
				}
				if _, exists := seen[id]; exists {
					continue
				}
				seen[id] = struct{}{}
				event, eventUpdates, err := p.gmailMessageEvent(ctx, request, account, id, "incremental", secrets)
				mergeStrings(secrets, eventUpdates)
				mergeStrings(updates, eventUpdates)
				if err != nil {
					return "", nil, 0, updates, err
				}
				if event.ExternalID != "" {
					events = append(events, event)
				}
			}
		}
		if value := backgroundString(response, "historyId"); value != "" {
			finalID = value
		}
		pageToken = backgroundString(response, "nextPageToken")
		if pageToken == "" {
			return finalID, events, int64(len(seen)), updates, nil
		}
	}
	return "", nil, 0, updates, permanent("gmail.page_limit_exceeded", "Gmail history page limit exceeded")
}

func (p *provider) currentGmailMessagesPage(ctx context.Context, request connector.BackgroundRequest, account, pageToken, source string, secrets map[string]string) ([]connector.BackgroundEvent, string, map[string]string, error) {
	input := map[string]any{"limit": 100, "label_id": backgroundConfig(request.Connection.Config, "gmail_ingest_label", "")}
	query := backgroundConfig(request.Connection.Config, "gmail_ingest_query", "")
	if query == "" {
		days := backgroundInt(request.Connection.Config, "gmail_history_days", 90)
		if days < 1 {
			days = 1
		}
		if days > 3650 {
			days = 3650
		}
		query = fmt.Sprintf("newer_than:%dd", days)
	}
	if query != "" {
		input["query"] = query
	}
	if strings.TrimSpace(pageToken) != "" {
		input["page_token"] = strings.TrimSpace(pageToken)
	}
	response, updates, err := p.backgroundCall(ctx, request, GmailListMessages.Key, GmailListMessages.ContractSHA256, input, secrets)
	updates = cloneStrings(updates)
	mergeStrings(secrets, updates)
	if err != nil {
		return nil, "", updates, err
	}
	events := []connector.BackgroundEvent{}
	for _, message := range backgroundMapSlice(response["messages"]) {
		id := backgroundString(message, "id")
		if id == "" {
			continue
		}
		event, currentUpdates, err := p.gmailMessageEvent(ctx, request, account, id, source, secrets)
		mergeStrings(secrets, currentUpdates)
		mergeStrings(updates, currentUpdates)
		if err != nil {
			return nil, "", updates, err
		}
		if event.ExternalID != "" {
			events = append(events, event)
		}
	}
	return events, backgroundString(response, "nextPageToken"), updates, nil
}

func (p *provider) gmailMessageEvent(ctx context.Context, request connector.BackgroundRequest, account, messageID, source string, secrets map[string]string) (connector.BackgroundEvent, map[string]string, error) {
	message, updates, err := p.backgroundCall(ctx, request, GmailGetMessage.Key, GmailGetMessage.ContractSHA256, map[string]any{"message_id": messageID, "format": "full"}, secrets)
	if err != nil {
		return connector.BackgroundEvent{}, updates, err
	}
	if !backgroundHasLabel(message, backgroundConfig(request.Connection.Config, "gmail_ingest_label", "")) {
		return connector.BackgroundEvent{}, updates, nil
	}
	payload, projectionErr := projectBackgroundGmailMessage(message, account, request.Connection.Key, source)
	eventType := "gmail.message.received"
	if projectionErr != nil {
		eventType = "gmail.message.malformed"
		payload = map[string]any{"connection_key": request.Connection.Key, "account_email": account, "message_id": messageID, "failure_code": "gmail.message_malformed"}
	}
	raw, _ := json.Marshal(payload)
	return connector.BackgroundEvent{ExternalID: "gmail:" + request.Connection.Key + ":message:" + messageID, EventType: eventType, Payload: raw}, updates, nil
}

func (p *provider) processGmailWatch(ctx context.Context, request connector.BackgroundRequest) (connector.BackgroundResult, error) {
	state := gmailWatchState{}
	if len(request.State) != 0 && string(request.State) != "null" {
		if err := strictBackgroundJSON(request.State, &state); err != nil {
			return connector.BackgroundResult{}, permanent("gmail.watch_state_invalid", "Gmail watch state is invalid")
		}
	}
	syncState := gmailSyncState{}
	if raw := request.RelatedStates[gmailSyncTaskKey]; len(raw) != 0 {
		_ = strictBackgroundJSON(raw, &syncState)
	}
	projectID := backgroundConfig(request.Connection.Config, "gmail_pubsub_project_id", "")
	if projectID == "" {
		return connector.BackgroundResult{}, permanent("gmail.pubsub_project_required", "Gmail Pub/Sub project is required")
	}
	if syncState.AccountEmail == "" {
		raw, _ := json.Marshal(state)
		return connector.BackgroundResult{State: raw, NextDueAt: request.Now.Add(5 * time.Minute), WakeTasks: []string{gmailSyncTaskKey}}, nil
	}
	topicID := gmailPushTopic(backgroundConfig(request.Connection.Config, "gmail_pubsub_topic_id", "domainry-gmail-events"))
	secrets, updates := cloneStrings(request.Secrets), map[string]string{}
	if watchRenewalDue(state, request.Now) {
		watch, currentUpdates, err := p.backgroundCall(ctx, request, GmailWatch.Key, GmailWatch.ContractSHA256, map[string]any{"topic_name": "projects/" + projectID + "/topics/" + topicID, "label_id": backgroundConfig(request.Connection.Config, "gmail_ingest_label", "")}, secrets)
		mergeStrings(updates, currentUpdates)
		if err != nil {
			return connector.BackgroundResult{SecretUpdates: updates}, err
		}
		expiresAt, nextRenewAt, scheduleErr := gmailWatchSchedule(backgroundString(watch, "expiration"), request.Now, request.Connection.WorkspaceID, request.Connection.Key)
		if scheduleErr != nil {
			return connector.BackgroundResult{SecretUpdates: updates}, scheduleErr
		}
		state.ExpiresAt, state.NextRenewAt = expiresAt.Format(time.RFC3339), nextRenewAt.Format(time.RFC3339)
	}
	state.TopicID = topicID
	nextRenewAt, err := time.Parse(time.RFC3339, state.NextRenewAt)
	if err != nil || !nextRenewAt.After(request.Now) {
		return connector.BackgroundResult{}, permanent("gmail.watch_schedule_invalid", "Gmail watch renewal schedule is invalid")
	}
	raw, _ := json.Marshal(state)
	return connector.BackgroundResult{State: raw, NextDueAt: nextRenewAt, SecretUpdates: updates}, nil
}

func (p *provider) backgroundCall(ctx context.Context, request connector.BackgroundRequest, key, hash string, input map[string]any, secrets map[string]string) (map[string]any, map[string]string, error) {
	payload, _ := json.Marshal(input)
	result, err := p.Adapter.Call(ctx, connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: key, ContractSHA256: hash, Mode: connector.ModeCall, Connection: request.Connection, Payload: payload, Secrets: cloneStrings(secrets), Principal: request.Principal})
	if err != nil {
		return nil, result.SecretUpdates, err
	}
	out := map[string]any{}
	if len(result.Payload) != 0 {
		if err := json.Unmarshal(result.Payload, &out); err != nil {
			return nil, result.SecretUpdates, permanent("background.response_invalid", "provider response is invalid")
		}
	}
	return out, result.SecretUpdates, nil
}

func strictBackgroundJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}
func cloneStrings(input map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range input {
		out[k] = v
	}
	return out
}
func mergeStrings(target, values map[string]string) {
	for k, v := range values {
		target[k] = v
	}
}
func backgroundConfig(config map[string]any, key, fallback string) string {
	if v, ok := config[key].(string); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return fallback
}
func backgroundBool(config map[string]any, key string, fallback bool) bool {
	v, ok := config[key]
	if !ok {
		return fallback
	}
	switch x := v.(type) {
	case bool:
		return x
	case string:
		return strings.EqualFold(strings.TrimSpace(x), "true")
	}
	return fallback
}
func backgroundInt(config map[string]any, key string, fallback int) int {
	switch v := config[key].(type) {
	case int:
		return v
	case float64:
		return int(v)
	case string:
		if n, e := strconv.Atoi(v); e == nil {
			return n
		}
	}
	return fallback
}
func backgroundString(values map[string]any, key string) string {
	if values == nil {
		return ""
	}
	switch v := values[key].(type) {
	case string:
		return strings.TrimSpace(v)
	case float64:
		return strconv.FormatInt(int64(v), 10)
	}
	return ""
}
func backgroundMapSlice(value any) []map[string]any {
	raw, ok := value.([]any)
	if !ok {
		return nil
	}
	out := []map[string]any{}
	for _, item := range raw {
		if mapped, ok := item.(map[string]any); ok {
			out = append(out, mapped)
		}
	}
	return out
}
func backgroundHasLabel(message map[string]any, required string) bool {
	if required == "" {
		return true
	}
	labels, ok := message["labelIds"].([]any)
	if !ok {
		if values, stringOK := message["labelIds"].([]string); stringOK {
			for _, value := range values {
				if strings.TrimSpace(value) == required {
					return true
				}
			}
		}
		return false
	}
	for _, v := range labels {
		if strings.TrimSpace(fmt.Sprint(v)) == required {
			return true
		}
	}
	return false
}
func projectBackgroundGmailMessage(message map[string]any, account, connection, source string) (map[string]any, error) {
	id, thread := backgroundString(message, "id"), backgroundString(message, "threadId")
	root, ok := message["payload"].(map[string]any)
	if id == "" || thread == "" || !ok {
		return nil, fmt.Errorf("invalid message")
	}
	headers := map[string]string{}
	for _, h := range backgroundMapSlice(root["headers"]) {
		headers[strings.ToLower(backgroundString(h, "name"))] = backgroundString(h, "value")
	}
	if strings.TrimSpace(headers["from"]) == "" || strings.TrimSpace(headers["date"]) == "" && backgroundString(message, "internalDate") == "" {
		return nil, fmt.Errorf("message lacks sender or timestamp")
	}
	body, attachments := backgroundParts(root)
	if strings.TrimSpace(body) == "" {
		body = backgroundString(message, "snippet")
		if len(body) > gmailMaxBodyBytes {
			body = body[:gmailMaxBodyBytes]
		}
	}
	return map[string]any{
		"source": source, "connection_key": connection, "account_email": account, "message_id": id, "thread_id": thread,
		"rfc_message_id": headers["message-id"], "in_reply_to": headers["in-reply-to"], "references": headers["references"],
		"from": headers["from"], "to": headers["to"], "cc": headers["cc"], "subject": headers["subject"], "date": headers["date"],
		"internal_date": backgroundString(message, "internalDate"), "label_ids": message["labelIds"], "snippet": backgroundString(message, "snippet"),
		"body": body, "attachments": attachments, "auto_submitted": headers["auto-submitted"], "precedence": headers["precedence"],
		"list_id": headers["list-id"], "list_unsubscribe": headers["list-unsubscribe"], "x_auto_response_suppress": headers["x-auto-response-suppress"],
	}, nil
}
func backgroundParts(part map[string]any) (string, []map[string]any) {
	text, textScore := "", 0
	attachments := []map[string]any{}
	var walk func(map[string]any)
	walk = func(current map[string]any) {
		mimeType, filename := backgroundString(current, "mimeType"), backgroundString(current, "filename")
		body, _ := current["body"].(map[string]any)
		attachment := backgroundString(body, "attachmentId")
		if filename != "" || attachment != "" {
			attachments = append(attachments, map[string]any{"filename": filename, "mime_type": mimeType, "attachment_id": attachment, "size": body["size"]})
		} else if mimeType == "text/plain" || mimeType == "text/html" {
			score := 1
			if mimeType == "text/plain" {
				score = 2
			}
			if decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(backgroundString(body, "data"), "=")); err == nil && score > textScore {
				if len(decoded) > gmailMaxBodyBytes {
					decoded = decoded[:gmailMaxBodyBytes]
				}
				charset := ""
				for _, header := range backgroundMapSlice(current["headers"]) {
					if strings.EqualFold(backgroundString(header, "name"), "Content-Type") {
						_, parameters, parseErr := mime.ParseMediaType(backgroundString(header, "value"))
						if parseErr == nil {
							charset = parameters["charset"]
						}
					}
				}
				decodedText, decodeErr := mailcontent.DecodeText(decoded, charset)
				if decodeErr == nil && mimeType == "text/html" {
					decodedText, decodeErr = mailcontent.HTMLText(decodedText)
				}
				if decodeErr == nil {
					text, textScore = strings.ReplaceAll(decodedText, "\x00", ""), score
				}
			}
		}
		for _, child := range backgroundMapSlice(current["parts"]) {
			walk(child)
		}
	}
	walk(part)
	return text, attachments
}
func watchRenewalDue(state gmailWatchState, now time.Time) bool {
	if state.ExpiresAt == "" || state.NextRenewAt == "" {
		return true
	}
	next, err := time.Parse(time.RFC3339, state.NextRenewAt)
	return err != nil || !next.After(now)
}

func gmailPushTopic(prefix string) string {
	return strings.TrimSpace(prefix)
}

func gmailWatchSchedule(expirationMilliseconds string, now time.Time, workspaceID, connectionKey string) (time.Time, time.Time, error) {
	milliseconds, err := strconv.ParseInt(strings.TrimSpace(expirationMilliseconds), 10, 64)
	if err != nil {
		return time.Time{}, time.Time{}, permanent("gmail.watch_expiration_invalid", "Gmail watch expiration is invalid")
	}
	expiresAt := time.UnixMilli(milliseconds).UTC()
	if !expiresAt.After(now.Add(30 * time.Minute)) {
		return time.Time{}, time.Time{}, permanent("gmail.watch_expiration_invalid", "Gmail watch expiration is too soon")
	}
	digest := sha256.Sum256([]byte(workspaceID + "\x00" + connectionKey + "\x00" + expiresAt.Format(time.RFC3339Nano)))
	jitterMinutes := int(digest[0])<<8 | int(digest[1])
	jitter := time.Duration(jitterMinutes%360) * time.Minute
	next := expiresAt.Add(-12*time.Hour - jitter)
	minimum := now.Add(5 * time.Minute)
	if next.Before(minimum) {
		next = minimum
	}
	return expiresAt, next.UTC(), nil
}
func backgroundErrorCodeIs(err error, expected string) bool {
	code, ok := connector.ProviderErrorCodeOf(err)
	return ok && code == expected
}

var _ connector.BackgroundProcessor = (*provider)(nil)
var _ connector.BackgroundCleanupProcessor = (*provider)(nil)
