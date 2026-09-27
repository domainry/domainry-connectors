package feishucalendar

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

func backgroundConnection() connector.Connection {
	value := oauthConnection()
	value.Key = "feishu-sales-a"
	value.WorkspaceID = "workspace-a"
	value.Status = "active"
	value.Config["calendar_sync_enabled"] = true
	value.Config["calendar_subscription_enabled"] = true
	value.Config["calendar_history_days"] = 30
	value.Config["calendar_future_days"] = 62
	value.Config["calendar_reconcile_seconds"] = 900
	return value
}

func TestCalendarBackgroundHistoryDeltaSubscriptionAndCleanup(t *testing.T) {
	first := `{"code":0,"data":{"items":[{"event_id":"meeting-42","summary":"Discovery","description":"Customer needs","start_time":{"date_time":"2026-09-26T09:00:00+08:00","timezone":"Asia/Shanghai"},"end_time":{"date_time":"2026-09-26T10:00:00+08:00","timezone":"Asia/Shanghai"},"status":"confirmed","free_busy_status":"busy","attendees":[{"user_id":"ou_customer","display_name":"Customer","rsvp_status":"accept"}],"vchat":{"meeting_url":"https://vc.feishu.cn/j/42"}}]}}`
	updated := strings.Replace(first, `"summary":"Discovery"`, `"summary":"Discovery updated"`, 1)
	transport := &recordingTransport{responses: []connector.HTTPResponse{
		{StatusCode: 200, Body: []byte(first)},
		{StatusCode: 200, Body: []byte(first)},
		{StatusCode: 200, Body: []byte(updated)},
		{StatusCode: 200, Body: []byte(`{"code":0}`)},
		{StatusCode: 200, Body: []byte(`{"code":0}`)},
	}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	processor := adapter.(connector.BackgroundProcessor)
	connection := backgroundConnection()
	tasks := processor.BackgroundTasks(connection)
	if len(tasks) != 2 || tasks[0].Key != feishuCalendarSyncTaskKey || tasks[1].Key != feishuCalendarSubscriptionTaskKey {
		t.Fatalf("tasks=%+v", tasks)
	}
	now := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)
	request := connector.BackgroundRequest{
		TaskKey: feishuCalendarSyncTaskKey, StateVersion: feishuCalendarStateVersion, Connection: connection,
		State: json.RawMessage(`{}`), Secrets: map[string]string{"access_token": "token"}, Now: now,
		Principal: connector.Principal{IsAuthenticated: true, WorkspaceID: connection.WorkspaceID},
	}
	result, err := processor.ProcessBackground(t.Context(), request)
	if err != nil || len(result.Events) != 1 || result.Events[0].EventType != "feishu.calendar.event.discovered" || result.NextDueAt != now.Add(900*time.Second) {
		t.Fatalf("initial result=%+v err=%v", result, err)
	}
	if !strings.Contains(string(result.Events[0].Payload), `"meeting_url":"https://vc.feishu.cn/j/42"`) || !strings.Contains(string(result.Events[0].Payload), `"content_hash"`) {
		t.Fatalf("initial event=%s", result.Events[0].Payload)
	}
	request.State, request.Now = result.State, now.Add(15*time.Minute)
	result, err = processor.ProcessBackground(t.Context(), request)
	if err != nil || len(result.Events) != 0 {
		t.Fatalf("unchanged result=%+v err=%v", result, err)
	}
	request.State, request.Now = result.State, now.Add(30*time.Minute)
	result, err = processor.ProcessBackground(t.Context(), request)
	if err != nil || len(result.Events) != 1 || result.Events[0].EventType != "feishu.calendar.event.updated" {
		t.Fatalf("updated result=%+v err=%v", result, err)
	}
	subscription, err := processor.ProcessBackground(t.Context(), connector.BackgroundRequest{
		TaskKey: feishuCalendarSubscriptionTaskKey, StateVersion: feishuCalendarStateVersion, Connection: connection,
		State: json.RawMessage(`{}`), Secrets: map[string]string{"access_token": "token"}, Now: now,
		Principal: connector.Principal{IsAuthenticated: true, WorkspaceID: connection.WorkspaceID},
	})
	if err != nil || subscription.NextDueAt != now.Add(24*time.Hour) || !strings.Contains(string(subscription.State), `"subscribed":true`) || !strings.Contains(transport.requests[3].URL, "/events/subscription") {
		t.Fatalf("subscription=%+v request=%+v err=%v", subscription, transport.requests[3], err)
	}
	if _, err = adapter.(connector.BackgroundCleanupProcessor).CleanupBackground(t.Context(), connection, map[string]string{"access_token": "token"}, now, connector.Principal{}); err != nil || !strings.Contains(transport.requests[4].URL, "/events/unsubscription") {
		t.Fatalf("cleanup request=%+v err=%v", transport.requests[4], err)
	}
}

func TestCalendarWebhookVerifiesEncryptedEventAndBuildsBoundedRoutes(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	plain := []byte(`{"schema":"2.0","header":{"event_id":"delivery-42","event_type":"calendar.calendar.event.changed_v4","token":"verify-token"},"event":{"calendar_id":"calendar-primary","calendar_event_id":"event-42","change_type":"delete","user_id_list":[{"open_id":"ou_a","union_id":"on_a","user_id":"u_a"},{"open_id":"ou_b"}]}}`)
	encryptKey := "calendar-encrypt-key"
	body := encryptedFeishuBody(t, plain, encryptKey)
	timestamp, nonce := "1800000000", "nonce-42"
	digest := sha256.Sum256(append([]byte(timestamp+nonce+encryptKey), body...))
	adapter, _ := New(&recordingTransport{})
	verified, err := adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Connection: backgroundConnection(),
		Secrets: map[string]string{"encrypt_key": encryptKey, "verification_token": "verify-token"}, Body: body, ReceivedAt: now,
		Headers: map[string][]string{"X-Lark-Request-Timestamp": {timestamp}, "X-Lark-Request-Nonce": {nonce}, "X-Lark-Signature": {strings.ToUpper(hex.EncodeToString(digest[:]))}},
	})
	if err != nil || verified.EventType != "feishu.calendar.event.cancelled" || verified.ExternalID != "delivery-42" || verified.Security == nil || !verified.Security.SignatureVerified {
		t.Fatalf("verified=%+v err=%v", verified, err)
	}
	var envelope struct {
		Routes []struct {
			Kind  string `json:"kind"`
			Value string `json:"value"`
		} `json:"routes"`
		WakeTasks []string                    `json:"wake_tasks"`
		Events    []connector.BackgroundEvent `json:"events"`
		Data      map[string]any              `json:"data"`
	}
	if json.Unmarshal(verified.Payload, &envelope) != nil || len(envelope.Routes) != 4 || len(envelope.WakeTasks) != 1 || envelope.WakeTasks[0] != feishuCalendarSyncTaskKey || len(envelope.Events) != 1 || envelope.Events[0].EventType != "feishu.calendar.event.cancelled" || envelope.Data["event_id"] != "event-42" {
		t.Fatalf("envelope=%s", verified.Payload)
	}
	invalid := connector.VerifyWebhookRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Connection: backgroundConnection(), Secrets: map[string]string{"verification_token": "wrong"}, Body: plain, ReceivedAt: now}
	if _, err = adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), invalid); err == nil {
		t.Fatal("invalid verification token was accepted")
	}
}

func TestCalendarWebhookReturnsJSONChallenge(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	verified, err := adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Connection: backgroundConnection(),
		Secrets: map[string]string{"verification_token": "verify-token"}, Body: []byte(`{"type":"url_verification","token":"verify-token","challenge":"challenge-42"}`), ReceivedAt: time.Now().UTC(),
	})
	if err != nil || verified.Challenge != "challenge-42" || verified.ChallengeFormat != "json" {
		t.Fatalf("verified=%+v err=%v", verified, err)
	}
}

func encryptedFeishuBody(t *testing.T, plain []byte, encryptKey string) []byte {
	t.Helper()
	key := sha256.Sum256([]byte(encryptKey))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		t.Fatal(err)
	}
	padding := aes.BlockSize - len(plain)%aes.BlockSize
	padded := append(append([]byte(nil), plain...), []byte(strings.Repeat(string(rune(byte(padding))), padding))...)
	iv := []byte("0123456789abcdef")
	ciphertext := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(ciphertext, padded)
	envelope, _ := json.Marshal(map[string]string{"encrypt": base64.StdEncoding.EncodeToString(append(iv, ciphertext...))})
	return envelope
}
