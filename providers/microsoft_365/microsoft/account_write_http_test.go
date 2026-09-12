package microsoft

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/calendar"
	"github.com/domainry/domainry-connector-sdk/calendarwrite"
	maildto "github.com/domainry/domainry-connector-sdk/mail"
	"github.com/domainry/domainry-connector-sdk/mailwrite"
	"github.com/domainry/domainry-connectors/internal/releaseverify"
)

func TestAccountWriteActualHTTPConditionalRefreshAndLostAcceptance(t *testing.T) {
	var mu sync.Mutex
	var event map[string]any
	var creates, patches, sends, refreshes, zones atomic.Int32
	var conflict, loseSend atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/token" {
			refreshes.Add(1)
			if err := r.ParseForm(); err != nil || r.PostForm.Get("refresh_token") != "refresh" || r.PostForm.Get("client_id") != "client" {
				t.Error("bad OAuth form")
				w.WriteHeader(400)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"access_token": "rotated", "refresh_token": "rotated-refresh", "token_type": "Bearer"})
			return
		}
		if r.Header.Get("Authorization") != "Bearer token" && r.Header.Get("Authorization") != "Bearer old" && r.Header.Get("Authorization") != "Bearer rotated" {
			t.Error("missing account credential")
			w.WriteHeader(403)
			return
		}
		if !strings.Contains(r.Header.Get("Prefer"), `IdType="ImmutableId"`) {
			t.Error("missing stable ID preference")
		}
		if strings.HasSuffix(r.URL.Path, supportedIanaZonesPath) {
			zones.Add(1)
			_, _ = w.Write(writeZones().Body)
			return
		}
		if strings.Contains(r.URL.Path, "/calendars/") {
			mu.Lock()
			defer mu.Unlock()
			switch r.Method {
			case "POST":
				creates.Add(1)
				var body map[string]any
				if json.NewDecoder(r.Body).Decode(&body) != nil {
					w.WriteHeader(400)
					return
				}
				event = writeEvent("event-http", `W/"created"`)
				event["subject"], event["body"], event["start"], event["end"], event["attendees"] = body["subject"], body["body"], body["start"], body["end"], body["attendees"]
				w.WriteHeader(201)
			case "GET":
				if event == nil || !strings.HasSuffix(r.URL.Path, "/events/event-http") {
					w.WriteHeader(404)
					return
				}
			case "PATCH":
				patches.Add(1)
				if r.Header.Get("Authorization") == "Bearer old" {
					w.WriteHeader(401)
					return
				}
				if r.Header.Get("If-Match") != event["@odata.etag"] || conflict.Load() {
					w.WriteHeader(412)
					return
				}
				var body map[string]any
				if json.NewDecoder(r.Body).Decode(&body) != nil || len(body) != 1 {
					t.Error("patch shape changed")
					w.WriteHeader(400)
					return
				}
				event["subject"] = body["subject"]
				event["@odata.etag"] = `W/"updated"`
			default:
				w.WriteHeader(405)
				return
			}
			_ = json.NewEncoder(w).Encode(event)
			return
		}
		if r.URL.Path == "/v1.0/me/messages/original" && r.Method == "GET" {
			v := graphMail("original", "text", "")
			delete(v, "body")
			_ = json.NewEncoder(w).Encode(v)
			return
		}
		if r.Method == "POST" && (r.URL.Path == "/v1.0/me/sendMail" || r.URL.Path == "/v1.0/me/messages/original/reply") {
			sends.Add(1)
			var body map[string]any
			if json.NewDecoder(r.Body).Decode(&body) != nil {
				t.Error("bad mail JSON")
				w.WriteHeader(400)
				return
			}
			message, ok := body["message"].(map[string]any)
			if !ok || len(message) != 5 || len(message["bccRecipients"].([]any)) != 1 {
				t.Error("mail targets changed")
			}
			if loseSend.Load() {
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				_ = conn.Close()
				return
			}
			w.WriteHeader(202)
			return
		}
		w.WriteHeader(404)
	}))
	defer server.Close()
	a := mailAdapter(t, releaseverify.HTTPTransport{Client: server.Client()})
	c := writeConnection()
	c.Config["graph_base_url"], c.Config["token_url"] = server.URL+"/v1.0", server.URL+"/token"
	call := func(key, hash string, input any, secrets map[string]string) (connector.CallResult, error) {
		b, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		return a.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: key, ContractSHA256: hash, Mode: connector.ModeCall, Connection: c, RequestRef: "http-" + key, Payload: b, Secrets: secrets})
	}
	draft := writeDraft()
	draft.Event.Start = calendar.Moment{DateTime: "2026-09-12T01:00:00Z", TimeZone: "UTC"}
	draft.Event.End = calendar.Moment{DateTime: "2026-09-12T02:00:00Z", TimeZone: "UTC"}
	raw, err := call(CalendarEventCreate.Key, CalendarEventCreate.ContractSHA256, draft, map[string]string{"access_token": "token"})
	if err != nil {
		t.Fatal(err)
	}
	var created calendarwrite.Result
	_ = json.Unmarshal(raw.Payload, &created)
	if created.EventID != "event-http" || created.Version != `W/"created"` {
		t.Fatal(created)
	}
	patch := writePatch()
	patch.EventID = created.EventID
	patch.ExpectedVersion = created.Version
	raw, err = call(CalendarEventUpdate.Key, CalendarEventUpdate.ContractSHA256, patch, map[string]string{"access_token": "old", "refresh_token": "refresh", "client_id": "client"})
	if err != nil || raw.SecretUpdates["refresh_token"] != "rotated-refresh" {
		t.Fatal(err, raw.SecretUpdates)
	}
	mu.Lock()
	title, etag := event["subject"], event["@odata.etag"].(string)
	mu.Unlock()
	if title != *patch.Changes.Title || etag != `W/"updated"` {
		t.Fatal("native event not updated")
	}
	conflict.Store(true)
	patch.ExpectedVersion = etag
	_, err = call(CalendarEventUpdate.Key, CalendarEventUpdate.ContractSHA256, patch, map[string]string{"access_token": "token"})
	writeClass(t, err, connector.ErrorPermanent)
	m := writeMessage()
	m.To = []maildto.Address{{Address: "reply@example.test"}}
	m.Subject = "Re: 评审安排"
	raw, err = call(MailReply.Key, MailReply.ContractSHA256, mailwrite.ReplyRequest{MessageID: "original", Message: m}, map[string]string{"access_token": "token"})
	if err != nil {
		t.Fatal(err)
	}
	var accepted mailwrite.Result
	_ = json.Unmarshal(raw.Payload, &accepted)
	if accepted.Status != "accepted" || accepted.Delivery != "unknown" || accepted.MessageID != "" || accepted.ThreadID != "" {
		t.Fatal("invented delivery receipt", accepted)
	}
	loseSend.Store(true)
	_, err = call(MailSend.Key, MailSend.ContractSHA256, mailwrite.SendRequest{Message: writeMessage()}, map[string]string{"access_token": "token"})
	writeClass(t, err, connector.ErrorUncertain)
	if creates.Load() != 1 || patches.Load() != 3 || sends.Load() != 2 || refreshes.Load() != 1 || zones.Load() != 1 {
		t.Fatal("unexpected replay", creates.Load(), patches.Load(), sends.Load(), refreshes.Load(), zones.Load())
	}
	t.Logf("actual HTTP: creates=%d patch attempts=%d (401, success, 412) sends=%d (202, accepted then disconnected) refreshes=%d supported-zones=%d; no uncertain write replay", creates.Load(), patches.Load(), sends.Load(), refreshes.Load(), zones.Load())
}

func TestAccountWriteActualHTTPCancelReplyBeforeSend(t *testing.T) {
	entered := make(chan struct{})
	var writes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			writes.Add(1)
			w.WriteHeader(202)
			return
		}
		close(entered)
		<-r.Context().Done()
	}))
	defer server.Close()
	a := mailAdapter(t, releaseverify.HTTPTransport{Client: server.Client()})
	c := writeConnection()
	c.Config["graph_base_url"] = server.URL
	m := writeMessage()
	m.To = []maildto.Address{{Address: "reply@example.test"}}
	b, _ := json.Marshal(mailwrite.ReplyRequest{MessageID: "original", Message: m})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := a.Call(ctx, connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: MailReply.Key, ContractSHA256: MailReply.ContractSHA256, Mode: connector.ModeCall, Connection: c, RequestRef: "cancelled", Payload: b, Secrets: map[string]string{"access_token": "token"}})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("metadata did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled reply succeeded")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled read did not stop")
	}
	if writes.Load() != 0 {
		t.Fatal("cancelled reply sent")
	}
}
