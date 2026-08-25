package google

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	gmailSyncTaskKey   = "gmail_sync"
	gmailWatchTaskKey  = "gmail_watch"
	gmailMaxPages      = 20
	gmailMaxBodyBytes  = 1 << 20
	gmailPushPublisher = "serviceAccount:gmail-api-push@system.gserviceaccount.com"
)

type gmailSyncState struct {
	AccountEmail string `json:"account_email,omitempty"`
	HistoryID    string `json:"history_id,omitempty"`
}

type gmailWatchState struct {
	ProjectID      string `json:"project_id,omitempty"`
	TopicID        string `json:"topic_id,omitempty"`
	SubscriptionID string `json:"subscription_id,omitempty"`
	ExpiresAt      string `json:"expires_at,omitempty"`
	NextRenewAt    string `json:"next_renew_at,omitempty"`
}

func (p *provider) BackgroundTasks(connection connector.Connection) []connector.BackgroundTaskDescriptor {
	if !backgroundBool(connection.Config, "gmail_ingest_enabled", false) || connection.Status != "active" {
		return nil
	}
	tasks := []connector.BackgroundTaskDescriptor{{Key: gmailSyncTaskKey, StateVersion: 1}}
	if backgroundBool(connection.Config, "gmail_watch_enabled", true) {
		tasks = append(tasks, connector.BackgroundTaskDescriptor{Key: gmailWatchTaskKey, StateVersion: 1})
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
	if request.StateVersion != 1 {
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
	if !backgroundBool(connection.Config, "gmail_watch_enabled", true) {
		return nil, nil
	}
	projectID := backgroundConfig(connection.Config, "gmail_pubsub_project_id", "")
	if projectID == "" {
		return nil, nil
	}
	request := connector.BackgroundRequest{TaskKey: gmailWatchTaskKey, StateVersion: 1, Connection: connection, State: json.RawMessage(`{}`), Secrets: secrets, Now: now, Principal: principal}
	resolved, updates := cloneStrings(secrets), map[string]string{}
	_, current, err := p.backgroundCall(ctx, request, GmailStop.Key, GmailStop.ContractSHA256, map[string]any{}, resolved)
	mergeStrings(resolved, current)
	mergeStrings(updates, current)
	if err != nil && !backgroundErrorCodeIs(err, "http_status_404") {
		return updates, err
	}
	_, current, err = p.backgroundCall(ctx, request, PubSubDeleteSubscription.Key, PubSubDeleteSubscription.ContractSHA256, map[string]any{"project_id": projectID, "subscription_id": backgroundConfig(connection.Config, "gmail_pubsub_subscription", managedGmailSubscription(connection.WorkspaceID, connection.Key))}, resolved)
	mergeStrings(updates, current)
	if err != nil && !backgroundErrorCodeIs(err, "http_status_404") {
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
	mergeStrings(secrets, updates)
	if err != nil {
		return connector.BackgroundResult{}, err
	}
	account, currentHistoryID := backgroundString(profile, "emailAddress"), backgroundString(profile, "historyId")
	if account == "" || currentHistoryID == "" {
		return connector.BackgroundResult{}, permanent("gmail.profile_invalid", "Gmail profile lacks emailAddress or historyId")
	}
	events := []connector.BackgroundEvent{}
	if state.HistoryID == "" {
		if backgroundBool(request.Connection.Config, "gmail_ingest_bootstrap", false) {
			var bootstrapUpdates map[string]string
			events, bootstrapUpdates, err = p.currentGmailMessages(ctx, request, account, secrets)
			mergeStrings(secrets, bootstrapUpdates)
			mergeStrings(updates, bootstrapUpdates)
		}
	} else {
		var historyUpdates map[string]string
		currentHistoryID, events, historyUpdates, err = p.gmailHistory(ctx, request, account, state.HistoryID, secrets)
		mergeStrings(updates, historyUpdates)
		if err != nil && backgroundErrorCodeIs(err, "http_status_404") {
			var reconcileUpdates map[string]string
			events, reconcileUpdates, err = p.currentGmailMessages(ctx, request, account, secrets)
			mergeStrings(updates, reconcileUpdates)
			currentHistoryID = backgroundString(profile, "historyId")
		}
	}
	if err != nil {
		return connector.BackgroundResult{}, err
	}
	state.AccountEmail, state.HistoryID = account, currentHistoryID
	raw, _ := json.Marshal(state)
	pollSeconds := backgroundInt(request.Connection.Config, "gmail_poll_seconds", 60)
	if pollSeconds < 5 {
		pollSeconds = 5
	}
	if pollSeconds > 3600 {
		pollSeconds = 3600
	}
	return connector.BackgroundResult{State: raw, NextDueAt: request.Now.Add(time.Duration(pollSeconds) * time.Second), Events: events, SecretUpdates: updates}, nil
}

func (p *provider) gmailHistory(ctx context.Context, request connector.BackgroundRequest, account, start string, secrets map[string]string) (string, []connector.BackgroundEvent, map[string]string, error) {
	pageToken, finalID := "", start
	seen, events, updates := map[string]struct{}{}, []connector.BackgroundEvent{}, map[string]string{}
	for page := 0; page < gmailMaxPages; page++ {
		input := map[string]any{"start_history_id": start, "limit": 100, "label_id": backgroundConfig(request.Connection.Config, "gmail_ingest_label", "INBOX")}
		if pageToken != "" {
			input["page_token"] = pageToken
		}
		response, currentUpdates, err := p.backgroundCall(ctx, request, SyncEmailHistory.Key, SyncEmailHistory.ContractSHA256, input, secrets)
		mergeStrings(secrets, currentUpdates)
		mergeStrings(updates, currentUpdates)
		if err != nil {
			return "", nil, updates, err
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
				event, eventUpdates, err := p.gmailMessageEvent(ctx, request, account, id, secrets)
				mergeStrings(secrets, eventUpdates)
				mergeStrings(updates, eventUpdates)
				if err != nil {
					return "", nil, updates, err
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
			return finalID, events, updates, nil
		}
	}
	return "", nil, updates, permanent("gmail.page_limit_exceeded", "Gmail history page limit exceeded")
}

func (p *provider) currentGmailMessages(ctx context.Context, request connector.BackgroundRequest, account string, secrets map[string]string) ([]connector.BackgroundEvent, map[string]string, error) {
	input := map[string]any{"limit": 100, "label_id": backgroundConfig(request.Connection.Config, "gmail_ingest_label", "INBOX")}
	if query := backgroundConfig(request.Connection.Config, "gmail_ingest_query", ""); query != "" {
		input["query"] = query
	}
	response, updates, err := p.backgroundCall(ctx, request, GmailListMessages.Key, GmailListMessages.ContractSHA256, input, secrets)
	mergeStrings(secrets, updates)
	if err != nil {
		return nil, updates, err
	}
	events := []connector.BackgroundEvent{}
	for _, message := range backgroundMapSlice(response["messages"]) {
		id := backgroundString(message, "id")
		if id == "" {
			continue
		}
		event, currentUpdates, err := p.gmailMessageEvent(ctx, request, account, id, secrets)
		mergeStrings(secrets, currentUpdates)
		mergeStrings(updates, currentUpdates)
		if err != nil {
			return nil, updates, err
		}
		if event.ExternalID != "" {
			events = append(events, event)
		}
	}
	return events, updates, nil
}

func (p *provider) gmailMessageEvent(ctx context.Context, request connector.BackgroundRequest, account, messageID string, secrets map[string]string) (connector.BackgroundEvent, map[string]string, error) {
	message, updates, err := p.backgroundCall(ctx, request, GmailGetMessage.Key, GmailGetMessage.ContractSHA256, map[string]any{"message_id": messageID, "format": "full"}, secrets)
	if err != nil {
		return connector.BackgroundEvent{}, updates, err
	}
	if !backgroundHasLabel(message, backgroundConfig(request.Connection.Config, "gmail_ingest_label", "INBOX")) {
		return connector.BackgroundEvent{}, updates, nil
	}
	payload, projectionErr := projectBackgroundGmailMessage(message, account, request.Connection.Key)
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
	projectID := backgroundConfig(request.Connection.Config, "gmail_pubsub_project_id", state.ProjectID)
	topicID := backgroundConfig(request.Connection.Config, "gmail_pubsub_topic", "domainry-gmail-events")
	subscriptionID := backgroundConfig(request.Connection.Config, "gmail_pubsub_subscription", managedGmailSubscription(request.Connection.WorkspaceID, request.Connection.Key))
	if projectID == "" {
		return connector.BackgroundResult{}, permanent("gmail.pubsub_project_required", "Gmail Pub/Sub project is required")
	}
	secrets, updates := cloneStrings(request.Secrets), map[string]string{}
	wake := []string{}
	if watchRenewalDue(state, request.Now) {
		currentUpdates, err := p.ensureGmailPubSub(ctx, request, projectID, topicID, subscriptionID, secrets)
		mergeStrings(secrets, currentUpdates)
		mergeStrings(updates, currentUpdates)
		if err != nil {
			return connector.BackgroundResult{}, err
		}
		watch, currentUpdates, err := p.backgroundCall(ctx, request, GmailWatch.Key, GmailWatch.ContractSHA256, map[string]any{"topic_name": "projects/" + projectID + "/topics/" + topicID, "label_id": backgroundConfig(request.Connection.Config, "gmail_ingest_label", "INBOX")}, secrets)
		mergeStrings(updates, currentUpdates)
		if err != nil {
			return connector.BackgroundResult{}, err
		}
		state.ExpiresAt = expirationRFC3339(backgroundString(watch, "expiration"))
		state.NextRenewAt = request.Now.Add(24 * time.Hour).Format(time.RFC3339)
		if syncState.HistoryID == "" {
			wake = append(wake, gmailSyncTaskKey)
		}
	}
	state.ProjectID, state.TopicID, state.SubscriptionID = projectID, topicID, subscriptionID
	pull, currentUpdates, err := p.backgroundCall(ctx, request, PubSubPull.Key, PubSubPull.ContractSHA256, map[string]any{"project_id": projectID, "subscription_id": subscriptionID, "max_messages": 25}, secrets)
	mergeStrings(updates, currentUpdates)
	if err != nil {
		return connector.BackgroundResult{}, err
	}
	events, ackIDs := []connector.BackgroundEvent{}, []string{}
	for _, received := range backgroundMapSlice(pull["receivedMessages"]) {
		ackID := backgroundString(received, "ackId")
		message, _ := received["message"].(map[string]any)
		messageID := backgroundString(message, "messageId")
		encodedData := backgroundString(message, "data")
		externalID := gmailPushExternalID(messageID, encodedData)
		data, decodeErr := decodeBackgroundNotification(encodedData)
		if decodeErr != nil || backgroundString(data, "historyId") == "" {
			raw, _ := json.Marshal(map[string]any{"connection_key": request.Connection.Key, "failure_code": "gmail.notification_malformed"})
			events = append(events, connector.BackgroundEvent{ExternalID: externalID, EventType: "gmail.notification.malformed", Payload: raw})
			if ackID != "" {
				ackIDs = append(ackIDs, ackID)
			}
			continue
		}
		email, historyID := backgroundString(data, "emailAddress"), backgroundString(data, "historyId")
		if syncState.AccountEmail != "" && !strings.EqualFold(syncState.AccountEmail, email) {
			raw, _ := json.Marshal(map[string]any{"connection_key": request.Connection.Key, "failure_code": "gmail.account_mismatch"})
			events = append(events, connector.BackgroundEvent{ExternalID: externalID, EventType: "gmail.notification.account_mismatch", Payload: raw})
			if ackID != "" {
				ackIDs = append(ackIDs, ackID)
			}
			continue
		}
		if historyAtLeast(syncState.HistoryID, historyID) {
			if ackID != "" {
				ackIDs = append(ackIDs, ackID)
			}
		} else {
			wake = append(wake, gmailSyncTaskKey)
		}
	}
	commits := []connector.BackgroundCommit{}
	if len(ackIDs) > 0 {
		raw, _ := json.Marshal(map[string]any{"project_id": projectID, "subscription_id": subscriptionID, "ack_ids": ackIDs})
		commits = append(commits, connector.BackgroundCommit{OperationKey: PubSubAcknowledge.Key, ContractSHA256: PubSubAcknowledge.ContractSHA256, Payload: raw})
	}
	raw, _ := json.Marshal(state)
	return connector.BackgroundResult{State: raw, NextDueAt: request.Now.Add(5 * time.Second), Events: events, Commit: commits, WakeTasks: uniqueStrings(wake), SecretUpdates: updates}, nil
}

func (p *provider) ensureGmailPubSub(ctx context.Context, request connector.BackgroundRequest, projectID, topicID, subscriptionID string, secrets map[string]string) (map[string]string, error) {
	updates := map[string]string{}
	_, current, err := p.backgroundCall(ctx, request, PubSubEnsureTopic.Key, PubSubEnsureTopic.ContractSHA256, map[string]any{"project_id": projectID, "topic_id": topicID}, secrets)
	mergeStrings(secrets, current)
	mergeStrings(updates, current)
	if err != nil && !backgroundErrorCodeIs(err, "http_status_409") {
		return updates, err
	}
	policy, current, err := p.backgroundCall(ctx, request, PubSubGetTopicPolicy.Key, PubSubGetTopicPolicy.ContractSHA256, map[string]any{"project_id": projectID, "topic_id": topicID}, secrets)
	mergeStrings(secrets, current)
	mergeStrings(updates, current)
	if err != nil {
		return updates, err
	}
	_, current, err = p.backgroundCall(ctx, request, PubSubSetTopicPolicy.Key, PubSubSetTopicPolicy.ContractSHA256, map[string]any{"project_id": projectID, "topic_id": topicID, "policy": backgroundPolicyWithPublisher(policy, gmailPushPublisher)}, secrets)
	mergeStrings(secrets, current)
	mergeStrings(updates, current)
	if err != nil {
		return updates, err
	}
	_, current, err = p.backgroundCall(ctx, request, PubSubEnsureSubscription.Key, PubSubEnsureSubscription.ContractSHA256, map[string]any{"project_id": projectID, "topic_id": topicID, "subscription_id": subscriptionID, "ack_deadline_seconds": 60}, secrets)
	mergeStrings(updates, current)
	if err != nil && !backgroundErrorCodeIs(err, "http_status_409") {
		return updates, err
	}
	return updates, nil
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
func projectBackgroundGmailMessage(message map[string]any, account, connection string) (map[string]any, error) {
	id, thread := backgroundString(message, "id"), backgroundString(message, "threadId")
	root, ok := message["payload"].(map[string]any)
	if id == "" || thread == "" || !ok {
		return nil, fmt.Errorf("invalid message")
	}
	headers := map[string]string{}
	for _, h := range backgroundMapSlice(root["headers"]) {
		headers[strings.ToLower(backgroundString(h, "name"))] = backgroundString(h, "value")
	}
	body, attachments := backgroundParts(root)
	if strings.TrimSpace(body) == "" {
		body = backgroundString(message, "snippet")
		if len(body) > gmailMaxBodyBytes {
			body = body[:gmailMaxBodyBytes]
		}
	}
	return map[string]any{"connection_key": connection, "account_email": account, "message_id": id, "thread_id": thread, "rfc_message_id": headers["message-id"], "in_reply_to": headers["in-reply-to"], "references": headers["references"], "from": headers["from"], "to": headers["to"], "cc": headers["cc"], "subject": headers["subject"], "date": headers["date"], "internal_date": backgroundString(message, "internalDate"), "label_ids": message["labelIds"], "snippet": backgroundString(message, "snippet"), "body": body, "attachments": attachments}, nil
}
func backgroundParts(part map[string]any) (string, []map[string]any) {
	text := ""
	attachments := []map[string]any{}
	var walk func(map[string]any)
	walk = func(current map[string]any) {
		mime, filename := backgroundString(current, "mimeType"), backgroundString(current, "filename")
		body, _ := current["body"].(map[string]any)
		attachment := backgroundString(body, "attachmentId")
		if filename != "" || attachment != "" {
			attachments = append(attachments, map[string]any{"filename": filename, "mime_type": mime, "attachment_id": attachment, "size": body["size"]})
		} else if text == "" && (mime == "text/plain" || mime == "text/html") {
			if decoded, err := base64.RawURLEncoding.DecodeString(backgroundString(body, "data")); err == nil {
				if len(decoded) > gmailMaxBodyBytes {
					decoded = decoded[:gmailMaxBodyBytes]
				}
				text = strings.ReplaceAll(string(decoded), "\x00", "")
			}
		}
		for _, child := range backgroundMapSlice(current["parts"]) {
			walk(child)
		}
	}
	walk(part)
	return text, attachments
}
func managedGmailSubscription(workspace, connection string) string {
	sum := sha256.Sum256([]byte(workspace + "\x00" + connection))
	return "domainry-gmail-" + hex.EncodeToString(sum[:])[:20]
}
func gmailPushExternalID(messageID, encodedData string) string {
	if messageID != "" {
		return "gmail-pubsub:" + messageID
	}
	sum := sha256.Sum256([]byte(encodedData))
	return "gmail-pubsub:malformed:" + hex.EncodeToString(sum[:])
}
func watchRenewalDue(state gmailWatchState, now time.Time) bool {
	if state.ExpiresAt == "" || state.NextRenewAt == "" {
		return true
	}
	next, err := time.Parse(time.RFC3339, state.NextRenewAt)
	return err != nil || !next.After(now)
}
func expirationRFC3339(milliseconds string) string {
	value := new(big.Int)
	if _, ok := value.SetString(milliseconds, 10); !ok {
		return ""
	}
	return time.UnixMilli(value.Int64()).UTC().Format(time.RFC3339)
}
func historyAtLeast(current, notified string) bool {
	left, right := new(big.Int), new(big.Int)
	if _, ok := left.SetString(current, 10); !ok {
		return false
	}
	if _, ok := right.SetString(notified, 10); !ok {
		return false
	}
	return left.Cmp(right) >= 0
}
func decodeBackgroundNotification(encoded string) (map[string]any, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		raw, err = base64.RawStdEncoding.DecodeString(encoded)
	}
	if err != nil || len(raw) > 64<<10 {
		return nil, fmt.Errorf("invalid notification")
	}
	out := map[string]any{}
	if json.Unmarshal(raw, &out) != nil {
		return nil, fmt.Errorf("invalid notification")
	}
	return out, nil
}
func backgroundPolicyWithPublisher(policy map[string]any, member string) map[string]any {
	bindings := backgroundMapSlice(policy["bindings"])
	found := false
	for i := range bindings {
		if backgroundString(bindings[i], "role") != "roles/pubsub.publisher" {
			continue
		}
		members, _ := bindings[i]["members"].([]any)
		for _, v := range members {
			if v == member {
				found = true
			}
		}
		if !found {
			bindings[i]["members"] = append(members, member)
			found = true
		}
	}
	if !found {
		bindings = append(bindings, map[string]any{"role": "roles/pubsub.publisher", "members": []any{member}})
	}
	out := map[string]any{"bindings": bindings}
	if etag := backgroundString(policy, "etag"); etag != "" {
		out["etag"] = etag
	}
	if version, ok := policy["version"]; ok {
		out["version"] = version
	}
	return out
}
func uniqueStrings(values []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, v := range values {
		if v != "" && !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}
func backgroundErrorCodeIs(err error, expected string) bool {
	code, ok := connector.ProviderErrorCodeOf(err)
	return ok && code == expected
}

var _ connector.BackgroundProcessor = (*provider)(nil)
var _ connector.BackgroundCleanupProcessor = (*provider)(nil)
