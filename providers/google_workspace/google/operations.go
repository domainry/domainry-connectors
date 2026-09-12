package google

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/internal/oauth2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

func (p *provider) test(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	return p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodGet, apiBase(r.Connection)+"/oauth2/v2/userinfo", nil, nil, false)
}
func (p *provider) TestConnection(ctx context.Context, r connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.test(ctx, connector.TypedRequest[struct{}]{Connection: r.Connection, Secrets: r.Secrets})
	if err != nil {
		return connector.TestConnectionResult{SecretUpdates: result.SecretUpdates}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details, SecretUpdates: result.SecretUpdates}, nil
}
func (p *provider) syncCalendar(ctx context.Context, r connector.TypedRequest[SyncCalendarInput]) (connector.TypedResult[Response], error) {
	limit, err := pageLimit(r.Input.Limit)
	if err != nil {
		return empty(), err
	}
	query := url.Values{"maxResults": {strconv.Itoa(limit)}}
	set(query, "pageToken", r.Input.PageToken)
	set(query, "syncToken", r.Input.SyncToken)
	set(query, "updatedMin", r.Input.UpdatedMin)
	id := strings.TrimSpace(r.Input.CalendarID)
	if id == "" {
		id = "primary"
	}
	return p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodGet, apiBase(r.Connection)+"/calendar/v3/calendars/"+url.PathEscape(id)+"/events", query, nil, false)
}
func (p *provider) syncDrive(ctx context.Context, r connector.TypedRequest[SyncDriveFileRefsInput]) (connector.TypedResult[Response], error) {
	limit, err := pageLimit(r.Input.Limit)
	if err != nil {
		return empty(), err
	}
	query := url.Values{"pageSize": {strconv.Itoa(limit)}, "fields": {"nextPageToken,files(id,name,mimeType,modifiedTime,webViewLink)"}}
	set(query, "pageToken", r.Input.PageToken)
	set(query, "q", r.Input.Query)
	return p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodGet, apiBase(r.Connection)+"/drive/v3/files", query, nil, false)
}
func (p *provider) syncEmail(ctx context.Context, r connector.TypedRequest[SyncEmailHistoryInput]) (connector.TypedResult[Response], error) {
	if strings.TrimSpace(r.Input.StartHistoryID) == "" {
		return empty(), permanent("gmail.start_history_id_required", "start_history_id is required")
	}
	limit, err := pageLimit(r.Input.Limit)
	if err != nil {
		return empty(), err
	}
	query := url.Values{"maxResults": {strconv.Itoa(limit)}, "startHistoryId": {r.Input.StartHistoryID}, "historyTypes": {"messageAdded"}}
	set(query, "pageToken", r.Input.PageToken)
	set(query, "labelId", r.Input.LabelID)
	return p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodGet, gmailBase(r.Connection)+"/gmail/v1/users/me/history", query, nil, false)
}
func (p *provider) gmailGetProfile(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	return p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodGet, gmailBase(r.Connection)+"/gmail/v1/users/me/profile", nil, nil, false)
}
func (p *provider) gmailListMessages(ctx context.Context, r connector.TypedRequest[GmailListMessagesInput]) (connector.TypedResult[Response], error) {
	limit, err := pageLimit(r.Input.Limit)
	if err != nil {
		return empty(), err
	}
	query := url.Values{"maxResults": {strconv.Itoa(limit)}}
	set(query, "pageToken", r.Input.PageToken)
	set(query, "labelIds", r.Input.LabelID)
	set(query, "q", r.Input.Query)
	return p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodGet, gmailBase(r.Connection)+"/gmail/v1/users/me/messages", query, nil, false)
}
func (p *provider) gmailGetMessage(ctx context.Context, r connector.TypedRequest[GmailGetMessageInput]) (connector.TypedResult[Response], error) {
	id := strings.TrimSpace(r.Input.MessageID)
	if id == "" {
		return empty(), permanent("gmail.message_id_required", "message_id is required")
	}
	format := strings.TrimSpace(r.Input.Format)
	if format == "" {
		format = "full"
	}
	return p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodGet, gmailBase(r.Connection)+"/gmail/v1/users/me/messages/"+url.PathEscape(id), url.Values{"format": {format}}, nil, false)
}
func (p *provider) gmailWatch(ctx context.Context, r connector.TypedRequest[GmailWatchInput]) (connector.TypedResult[Response], error) {
	topic := strings.TrimSpace(r.Input.TopicName)
	if topic == "" {
		return empty(), permanent("gmail.topic_name_required", "topic_name is required")
	}
	label := strings.TrimSpace(r.Input.LabelID)
	if label == "" {
		label = "INBOX"
	}
	return p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodPost, gmailBase(r.Connection)+"/gmail/v1/users/me/watch", nil, map[string]any{"topicName": topic, "labelIds": []string{label}, "labelFilterBehavior": "INCLUDE"}, true)
}
func (p *provider) gmailStop(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	return p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodPost, gmailBase(r.Connection)+"/gmail/v1/users/me/stop", nil, map[string]any{}, true)
}
func (p *provider) gmailSendMessage(ctx context.Context, r connector.TypedRequest[GmailSendMessageInput]) (connector.DeliveryResult, error) {
	body, err := p.gmailMessage(r.Input)
	if err != nil {
		return connector.DeliveryResult{}, err
	}
	result, err := p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodPost, gmailBase(r.Connection)+"/gmail/v1/users/me/messages/send", nil, body, true)
	return connector.DeliveryResult{ResponseRef: result.ResponseRef, SecretUpdates: result.SecretUpdates, ResourceHealth: result.ResourceHealth}, err
}
func (p *provider) pubsubEnsureTopic(ctx context.Context, r connector.TypedRequest[PubSubResourceInput]) (connector.TypedResult[Response], error) {
	resource, err := topicResource(r.Input.ProjectID, r.Input.TopicID)
	if err != nil {
		return empty(), err
	}
	return p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodPut, pubsubBase(r.Connection)+"/v1/"+resource, nil, map[string]any{}, true)
}
func (p *provider) pubsubEnsureSubscription(ctx context.Context, r connector.TypedRequest[PubSubEnsureSubscriptionInput]) (connector.TypedResult[Response], error) {
	subscription, err := subscriptionResource(r.Input.ProjectID, r.Input.SubscriptionID)
	if err != nil {
		return empty(), err
	}
	topic, err := topicResource(r.Input.ProjectID, r.Input.TopicID)
	if err != nil {
		return empty(), err
	}
	deadline := r.Input.AckDeadlineSeconds
	if deadline == 0 {
		deadline = 60
	}
	if deadline < 10 || deadline > 600 {
		return empty(), permanent("pubsub.ack_deadline_invalid", "ack_deadline_seconds is invalid")
	}
	return p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodPut, pubsubBase(r.Connection)+"/v1/"+subscription, nil, map[string]any{"topic": topic, "ackDeadlineSeconds": deadline}, true)
}
func (p *provider) pubsubGetTopicPolicy(ctx context.Context, r connector.TypedRequest[PubSubResourceInput]) (connector.TypedResult[Response], error) {
	resource, err := topicResource(r.Input.ProjectID, r.Input.TopicID)
	if err != nil {
		return empty(), err
	}
	return p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodPost, pubsubBase(r.Connection)+"/v1/"+resource+":getIamPolicy", nil, map[string]any{}, false)
}
func (p *provider) pubsubSetTopicPolicy(ctx context.Context, r connector.TypedRequest[PubSubSetTopicPolicyInput]) (connector.TypedResult[Response], error) {
	resource, err := topicResource(r.Input.ProjectID, r.Input.TopicID)
	if err != nil {
		return empty(), err
	}
	if len(r.Input.Policy) == 0 {
		return empty(), permanent("pubsub.policy_required", "policy is required")
	}
	return p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodPost, pubsubBase(r.Connection)+"/v1/"+resource+":setIamPolicy", nil, map[string]any{"policy": r.Input.Policy}, true)
}
func (p *provider) pubsubPull(ctx context.Context, r connector.TypedRequest[PubSubPullInput]) (connector.TypedResult[Response], error) {
	resource, err := subscriptionResource(r.Input.ProjectID, r.Input.SubscriptionID)
	if err != nil {
		return empty(), err
	}
	max := r.Input.MaxMessages
	if max == 0 {
		max = 25
	}
	if max < 1 || max > 1000 {
		return empty(), permanent("pubsub.max_messages_invalid", "max_messages is invalid")
	}
	return p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodPost, pubsubBase(r.Connection)+"/v1/"+resource+":pull", nil, map[string]any{"maxMessages": max}, false)
}
func (p *provider) pubsubAcknowledge(ctx context.Context, r connector.TypedRequest[PubSubAcknowledgeInput]) (connector.TypedResult[Response], error) {
	resource, err := subscriptionResource(r.Input.ProjectID, r.Input.SubscriptionID)
	if err != nil {
		return empty(), err
	}
	ids := cleanIDs(r.Input.AckIDs)
	if len(ids) == 0 {
		return empty(), permanent("pubsub.ack_ids_required", "ack_ids are required")
	}
	return p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodPost, pubsubBase(r.Connection)+"/v1/"+resource+":acknowledge", nil, map[string]any{"ackIds": ids}, true)
}
func (p *provider) pubsubDeleteSubscription(ctx context.Context, r connector.TypedRequest[PubSubResourceInput]) (connector.TypedResult[Response], error) {
	resource, err := subscriptionResource(r.Input.ProjectID, r.Input.SubscriptionID)
	if err != nil {
		return empty(), err
	}
	return p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodDelete, pubsubBase(r.Connection)+"/v1/"+resource, nil, nil, true)
}

func (p *provider) executeWithRefresh(ctx context.Context, connection connector.Connection, secrets map[string]string, method, endpoint string, query url.Values, body map[string]any, write bool) (connector.TypedResult[Response], error) {
	return p.executeWithPrecondition(ctx, connection, secrets, method, endpoint, query, body, write, "")
}

// Only a provider-selected, already validated ETag enters the conditional
// request. A 401 refresh preserves the same body and precondition; a 412 never
// refreshes the event version or retries the mutation.
func (p *provider) executeWithPrecondition(ctx context.Context, connection connector.Connection, secrets map[string]string, method, endpoint string, query url.Values, body map[string]any, write bool, version string) (connector.TypedResult[Response], error) {
	token := strings.TrimSpace(secrets["access_token"])
	if token == "" {
		return empty(), permanent("access_token_required", "resolved access token is required")
	}
	result, status, err := p.execute(ctx, connection, method, endpoint, query, body, token, write, version)
	if status != http.StatusUnauthorized {
		return result, err
	}
	refresh, clientID := strings.TrimSpace(secrets["refresh_token"]), strings.TrimSpace(secrets["client_id"])
	if refresh == "" || clientID == "" {
		return result, err
	}
	updated, refreshErr := oauth2.Refresh(ctx, p.transport, oauth2.RefreshRequest{Endpoint: tokenURL(connection), RefreshToken: refresh, ClientID: clientID, ClientSecret: strings.TrimSpace(secrets["client_secret"]), ClientSecretOptional: true, ClientAuthentication: oauth2.ClientAuthenticationForm, ErrorPrefix: "google.oauth"})
	if refreshErr != nil {
		return connector.TypedResult[Response]{ResponseRef: "oauth:refresh_failed"}, refreshErr
	}
	result, _, err = p.execute(ctx, connection, method, endpoint, query, body, updated.AccessToken, write, version)
	result.SecretUpdates = map[string]string{"access_token": updated.AccessToken}
	if updated.RefreshToken != "" {
		result.SecretUpdates["refresh_token"] = updated.RefreshToken
	}
	return result, err
}
func (p *provider) execute(ctx context.Context, connection connector.Connection, method, endpoint string, query url.Values, body map[string]any, token string, write bool, version string) (connector.TypedResult[Response], int, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return empty(), 0, err
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return empty(), 0, permanent("endpoint_invalid", "endpoint is invalid")
	}
	parsed.RawQuery = query.Encode()
	var raw []byte
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return empty(), 0, permanent("request_invalid", "request is invalid")
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	if version != "" {
		headers["If-Match"] = []string{version}
	}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: parsed.String(), Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, Body: raw, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		return empty(), 0, transportFailure(write, "network_error", transportErr)
	}
	status, ref := response.StatusCode, "http:"+strconv.Itoa(response.StatusCode)
	payload := Response{}
	valid := status == http.StatusNoContent || len(response.Body) == 0 || json.Unmarshal(response.Body, &payload) == nil
	if status < 200 || status >= 300 {
		cause := fmt.Errorf("Google API returned HTTP %d", status)
		code := "google.http_" + strconv.Itoa(status)
		if status == http.StatusUnauthorized {
			return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, status, connector.PermanentError(code, cause)
		}
		if status == http.StatusTooManyRequests {
			return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, status, connector.RetryableError(code, cause)
		}
		if status == http.StatusRequestTimeout || status >= 500 {
			return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, status, transportFailure(write, "http_"+strconv.Itoa(status), cause)
		}
		return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, status, connector.PermanentError(code, cause)
	}
	if !valid {
		return connector.TypedResult[Response]{ResponseRef: ref}, status, transportFailure(write, "response_invalid", errors.New("Google response is invalid JSON"))
	}
	if id := mapString(payload, "id"); id != "" && strings.Contains(endpoint, "/messages/send") {
		ref = "gmail:" + id
	}
	return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, status, nil
}
