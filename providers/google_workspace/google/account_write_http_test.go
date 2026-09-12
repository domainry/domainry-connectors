package google

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/calendarwrite"
	maildto "github.com/domainry/domainry-connector-sdk/mail"
	"github.com/domainry/domainry-connector-sdk/mailwrite"
	"github.com/domainry/domainry-connectors/internal/releaseverify"
)

func TestAccountWriteActualHTTPRefreshCASAndLostSend(t *testing.T) {
	var mu sync.Mutex
	var event googleWriteEvent
	var creates, patches, sends, refreshes atomic.Int32
	var loseSend, conflict atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/token" {
			refreshes.Add(1)
			if err := r.ParseForm(); err != nil || r.PostForm.Get("refresh_token") != "old-refresh" || r.PostForm.Get("client_id") != "client" {
				t.Error("invalid token exchange")
				w.WriteHeader(400)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"access_token": "fresh-token", "refresh_token": "fresh-refresh", "token_type": "Bearer"})
			return
		}
		if r.Header.Get("Authorization") == "Bearer expired-token" {
			w.WriteHeader(401)
			return
		}
		if r.Header.Get("Authorization") != "Bearer fresh-token" {
			t.Error("missing governed credentials")
			w.WriteHeader(403)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/calendar/") {
			mu.Lock()
			defer mu.Unlock()
			switch r.Method {
			case "POST":
				creates.Add(1)
				var body map[string]json.RawMessage
				if json.NewDecoder(r.Body).Decode(&body) != nil {
					w.WriteHeader(400)
					return
				}
				var id string
				_ = json.Unmarshal(body["id"], &id)
				event = writeEvent(id, `"created"`)
				event.Start, event.End = googleCalendarMoment{}, googleCalendarMoment{}
				_ = json.Unmarshal(body["summary"], &event.Summary)
				_ = json.Unmarshal(body["start"], &event.Start)
				_ = json.Unmarshal(body["end"], &event.End)
				if r.URL.Query().Get("sendUpdates") != "all" {
					t.Error("create notification policy")
				}
			case "PATCH":
				patches.Add(1)
				if r.Header.Get("If-Match") != event.ETag || conflict.Load() {
					w.WriteHeader(412)
					return
				}
				var body map[string]string
				if json.NewDecoder(r.Body).Decode(&body) != nil {
					w.WriteHeader(400)
					return
				}
				if r.URL.Query().Get("sendUpdates") != "all" || len(body) != 1 {
					t.Error("patch scope changed")
				}
				event.Summary = body["summary"]
				event.ETag = `"updated"`
			case "GET":
				if !strings.HasSuffix(r.URL.Path, "/events/"+event.ID) {
					w.WriteHeader(404)
					return
				}
			default:
				w.WriteHeader(405)
				return
			}
			_ = json.NewEncoder(w).Encode(event)
			return
		}
		switch r.URL.Path {
		case "/gmail/v1/users/me/profile":
			_ = json.NewEncoder(w).Encode(map[string]string{"emailAddress": "account@example.test"})
		case "/gmail/v1/users/me/messages/original":
			if r.URL.Query().Get("format") != "metadata" {
				t.Error("reply requested unnecessary body")
			}
			_ = json.NewEncoder(w).Encode(mailMessage("original", googleMailPart{}))
		case "/gmail/v1/users/me/messages/send":
			if r.Method != "POST" {
				w.WriteHeader(405)
				return
			}
			sends.Add(1)
			var body map[string]string
			if json.NewDecoder(r.Body).Decode(&body) != nil || body["raw"] == "" {
				t.Error("invalid MIME envelope")
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
			_ = json.NewEncoder(w).Encode(map[string]string{"id": "sent-message", "threadId": "thread"})
		default:
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	// An isolated test transport, not a second persistent product service.
	a := mailAdapter(t, releaseverify.HTTPTransport{Client: server.Client()})
	c := accountWriteConnection()
	c.Config["api_base_url"], c.Config["gmail_base_url"], c.Config["token_url"] = server.URL, server.URL, server.URL+"/token"
	call := func(key, hash string, in any, secrets map[string]string) (connector.CallResult, error) {
		b, err := json.Marshal(in)
		if err != nil {
			t.Fatal(err)
		}
		return a.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: key, ContractSHA256: hash, Mode: connector.ModeCall, Connection: c, Payload: b, RequestRef: "http-" + key, Secrets: secrets})
	}
	fresh := map[string]string{"access_token": "fresh-token"}
	raw, err := call(CalendarEventCreate.Key, CalendarEventCreate.ContractSHA256, writeDraft(), fresh)
	if err != nil {
		t.Fatal(err)
	}
	var created calendarwrite.Result
	if json.Unmarshal(raw.Payload, &created) != nil {
		t.Fatal("invalid receipt")
	}
	patch := writePatch()
	patch.EventID = created.EventID
	patch.ExpectedVersion = created.Version
	raw, err = call(CalendarEventUpdate.Key, CalendarEventUpdate.ContractSHA256, patch, map[string]string{"access_token": "expired-token", "refresh_token": "old-refresh", "client_id": "client"})
	if err != nil || raw.SecretUpdates["access_token"] != "fresh-token" {
		t.Fatal(err, raw.SecretUpdates)
	}
	mu.Lock()
	title, version := event.Summary, event.ETag
	mu.Unlock()
	if title != *patch.Changes.Title || version != `"updated"` {
		t.Fatal("actual event not patched")
	}
	patch.ExpectedVersion = version
	conflict.Store(true)
	_, err = call(CalendarEventUpdate.Key, CalendarEventUpdate.ContractSHA256, patch, fresh)
	writeClass(t, err, connector.ErrorPermanent)
	m := writeMail()
	m.Subject = "Re: café"
	m.To = []maildto.Address{{Address: "reply@example.test"}}
	raw, err = call(MailReply.Key, MailReply.ContractSHA256, mailwrite.ReplyRequest{MessageID: "original", Message: m}, fresh)
	if err != nil {
		t.Fatal(err)
	}
	var accepted mailwrite.Result
	_ = json.Unmarshal(raw.Payload, &accepted)
	if accepted.MessageID != "sent-message" || accepted.Delivery != "unknown" {
		t.Fatal(accepted)
	}
	loseSend.Store(true)
	_, err = call(MailSend.Key, MailSend.ContractSHA256, mailwrite.SendRequest{Message: writeMail()}, fresh)
	writeClass(t, err, connector.ErrorUncertain)
	if creates.Load() != 1 || patches.Load() != 2 || sends.Load() != 2 || refreshes.Load() != 1 {
		t.Fatal("unexpected replay", creates.Load(), patches.Load(), sends.Load(), refreshes.Load())
	}
	t.Logf("actual HTTP: creates=%d patches=%d (one 412), sends=%d (one response lost), refreshes=%d; no mutation replay", creates.Load(), patches.Load(), sends.Load(), refreshes.Load())
}

func TestAccountWriteActualHTTPCancellationBeforeSend(t *testing.T) {
	entered := make(chan struct{})
	var writes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			writes.Add(1)
			_, _ = io.WriteString(w, `{}`)
			return
		}
		close(entered)
		<-r.Context().Done()
	}))
	defer server.Close()
	a := mailAdapter(t, releaseverify.HTTPTransport{Client: server.Client()})
	c := accountWriteConnection()
	c.Config["gmail_base_url"] = server.URL
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	b, _ := json.Marshal(mailwrite.SendRequest{Message: writeMail()})
	go func() {
		_, err := a.Call(ctx, connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: MailSend.Key, ContractSHA256: MailSend.ContractSHA256, Mode: connector.ModeCall, Connection: c, Payload: b, RequestRef: "cancel", Secrets: map[string]string{"access_token": "token"}})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("profile read did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled mail succeeded")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation did not stop I/O")
	}
	if writes.Load() != 0 {
		t.Fatal("cancelled mail was sent")
	}
}
