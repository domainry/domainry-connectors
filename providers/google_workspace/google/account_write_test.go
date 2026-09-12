package google

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/mail"
	"net/url"
	"reflect"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/calendar"
	"github.com/domainry/domainry-connector-sdk/calendarwrite"
	maildto "github.com/domainry/domainry-connector-sdk/mail"
	"github.com/domainry/domainry-connector-sdk/mailwrite"
)

func accountWriteConnection() connector.Connection {
	c := validConnection()
	c.WorkspaceID = "workspace"
	c.Key = "google-account"
	return c
}
func accountWriteCall[I, O any](t *testing.T, a connector.Adapter, op connector.CallOperation[I, O], in I, secrets map[string]string) (O, connector.CallResult, error) {
	t.Helper()
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	r, err := a.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: op.Key, ContractSHA256: op.ContractSHA256, Mode: connector.ModeCall, Connection: accountWriteConnection(), Payload: b, RequestRef: "request-1", Secrets: secrets})
	var out O
	if err == nil {
		if e := json.Unmarshal(r.Payload, &out); e != nil {
			t.Fatal(e)
		}
	}
	return out, r, err
}
func writeEvent(id, version string) googleWriteEvent {
	return googleWriteEvent{googleCalendarEvent: googleCalendarEvent{Kind: "calendar#event", ID: id, Summary: "Original", Status: "confirmed", Start: googleCalendarMoment{DateTime: "2026-09-12T09:00:00+08:00", TimeZone: "Asia/Shanghai"}, End: googleCalendarMoment{DateTime: "2026-09-12T10:00:00+08:00", TimeZone: "Asia/Shanghai"}}, ETag: version, EventType: "default", Attendees: []googleWriteAttendee{{Email: "guest@example.test", ResponseStatus: "accepted", Comment: "Keep RSVP"}}}
}
func writeDraft() calendarwrite.CreateRequest {
	return calendarwrite.CreateRequest{CalendarID: "team/calendar", Notifications: calendarwrite.NotifyAttendees, Event: calendarwrite.Draft{Title: "确认日程", Description: "<b>纯文本</b>\n第二行", Start: calendar.Moment{Date: "2026-09-12", TimeZone: "Asia/Shanghai"}, End: calendar.Moment{Date: "2026-09-14", TimeZone: "Asia/Shanghai"}, Attendees: []calendarwrite.Attendee{{Address: "guest@example.test", Kind: "required"}, {Address: "room@example.test", Kind: "resource"}}}}
}
func writePatch() calendarwrite.UpdateRequest {
	title := "New title"
	return calendarwrite.UpdateRequest{CalendarID: "team/calendar", EventID: "event/one", ExpectedVersion: `"v1"`, Scope: calendarwrite.ScopeEvent, Notifications: calendarwrite.NotifyAttendees, Changes: calendarwrite.Patch{Title: &title}}
}
func writeMail() mailwrite.Message {
	return mailwrite.Message{To: []maildto.Address{{Address: "recipient@example.test", Name: "收件人"}}, CC: []maildto.Address{{Address: "copy@example.test"}}, BCC: []maildto.Address{{Address: "hidden@example.test"}}, Subject: "中文 subject", Text: "确认正文\nline 2\r\nthird"}
}
func writeClass(t *testing.T, err error, want connector.ErrorClassification) {
	t.Helper()
	got, ok := connector.ErrorClassificationOf(err)
	if !ok || got != want {
		t.Fatalf("classification=%s error=%v want=%s", got, err, want)
	}
}

func TestAccountWriteRegistrationScopesAndReadIsolation(t *testing.T) {
	a := mailAdapter(t, &recordingTransport{})
	count := 0
	for _, op := range a.Descriptor().Operations {
		hash := ""
		effect := connector.EffectWrite
		switch op.Key {
		case calendarwrite.InspectOperationKey:
			hash = calendarwrite.OperationSHA256(op.Key)
			effect = connector.EffectRead
		case calendarwrite.CreateOperationKey, calendarwrite.UpdateOperationKey:
			hash = calendarwrite.OperationSHA256(op.Key)
		case mailwrite.SendOperationKey, mailwrite.ReplyOperationKey:
			hash = mailwrite.OperationSHA256(op.Key)
		default:
			continue
		}
		count++
		if op.Mode != connector.ModeCall || op.ContractSHA256 != hash || op.Reliability.Effect != effect {
			t.Fatalf("unexpected operation %+v", op)
		}
		if effect == connector.EffectWrite && (op.Reliability.Idempotency.Strategy != connector.IdempotencyNone || op.Reliability.Reconciliation != connector.ReconciliationNone) {
			t.Fatal("writes must not advertise replay")
		}
	}
	if count != 5 {
		t.Fatal("missing write family operations", count)
	}
	allows := func(key string, grants ...string) bool {
		sets, ok := connector.ResolveOAuthOperationScopes(a, key)
		if !ok {
			t.Fatal("missing scopes", key)
		}
		for _, set := range sets {
			all := true
			for _, scope := range set {
				found := false
				for _, grant := range grants {
					found = found || scope == grant
				}
				all = all && found
			}
			if all {
				return true
			}
		}
		return false
	}
	p := "https://www.googleapis.com/auth/"
	if allows(CalendarEventCreate.Key, p+"calendar.readonly") || !allows(CalendarEventUpdate.Key, p+"calendar.events") || !allows(CalendarEventInspect.Key, p+"calendar.events.readonly") {
		t.Fatal("calendar scope boundary")
	}
	if allows(MailSend.Key, p+"gmail.send") || allows(MailReply.Key, p+"gmail.compose") || allows(MailSend.Key, p+"gmail.readonly") {
		t.Fatal("insufficient mail scope accepted")
	}
	if !allows(MailSend.Key, p+"gmail.compose") || !allows(MailReply.Key, p+"gmail.send", p+"gmail.metadata") || !allows(MailReply.Key, p+"gmail.modify") {
		t.Fatal("valid mail scope rejected")
	}
	if allows(MailRead.Key, p+"gmail.send", p+"gmail.metadata") {
		t.Fatal("write grants enabled body reads")
	}
}

func TestAccountWriteCalendarInspectRejectsIncompleteTargets(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*googleWriteEvent)
	}{
		{"omitted", func(e *googleWriteEvent) { e.AttendeesOmitted = true }},
		{"hidden", func(e *googleWriteEvent) { v := false; e.GuestsCanSeeOtherGuests = &v }},
		{"extra-guests", func(e *googleWriteEvent) { e.Attendees[0].AdditionalGuests = 1 }},
		{"cancelled", func(e *googleWriteEvent) { e.Status = "cancelled" }},
		{"no-etag", func(e *googleWriteEvent) { e.ETag = "" }},
		{"wrong-target", func(e *googleWriteEvent) { e.ID = "other" }},
		{"special-event", func(e *googleWriteEvent) { e.EventType = "outOfOffice" }},
		{"too-many", func(e *googleWriteEvent) { e.Attendees = make([]googleWriteAttendee, 101) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := writeEvent("one", `"v1"`)
			tc.change(&e)
			tr := &recordingTransport{respond: func(connector.HTTPRequest) (connector.HTTPResponse, error) { return mailHTTP(e), nil }}
			a := mailAdapter(t, tr)
			_, _, err := accountWriteCall(t, a, CalendarEventInspect, calendarwrite.InspectRequest{CalendarID: "team", EventID: "one", TimeZone: "UTC"}, map[string]string{"access_token": "token"})
			writeClass(t, err, connector.ErrorPermanent)
			if len(tr.requests) != 1 {
				t.Fatal("unexpected replay")
			}
		})
	}
}

func TestAccountWriteCalendarCreateAndConditionalPatch(t *testing.T) {
	var postedID string
	in := writeDraft()
	tr := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		u, _ := url.Parse(r.URL)
		if r.SecretHeaders["Authorization"][0] != "Bearer token" || r.MaxResponseBytes != responseLimit {
			t.Fatal("ungoverned transport")
		}
		if r.Method == "GET" {
			if !strings.HasSuffix(u.EscapedPath(), "/team%2Fcalendar/events/event%2Fone") {
				t.Fatal(u)
			}
			return mailHTTP(writeEvent("event/one", `"v1"`)), nil
		}
		var body map[string]any
		if err := json.Unmarshal(r.Body, &body); err != nil {
			t.Fatal(err)
		}
		if u.Query().Get("sendUpdates") != "all" {
			t.Fatal("missing notification policy")
		}
		if r.Method == "POST" {
			postedID, _ = body["id"].(string)
			if len(postedID) != 64 || body["description"] != "&lt;b&gt;纯文本&lt;/b&gt;<br>第二行" {
				t.Fatal(body)
			}
			if body["start"].(map[string]any)["date"] != "2026-09-12" || body["end"].(map[string]any)["date"] != "2026-09-14" {
				t.Fatal("all day changed")
			}
			return mailHTTP(writeEvent(postedID, `"created"`)), nil
		}
		if r.Method != "PATCH" || r.Headers["If-Match"][0] != `"v1"` || len(body) != 5 {
			t.Fatal("wrong patch", r.Method, body)
		}
		if body["description"] != "" || body["location"] != "" {
			t.Fatal("clear semantics lost")
		}
		start := body["start"].(map[string]any)
		if _, ok := start["date"]; !ok || start["date"] != nil {
			t.Fatal("opposite time type must be cleared")
		}
		attendee := body["attendees"].([]any)[0].(map[string]any)
		if attendee["responseStatus"] != "accepted" || attendee["comment"] != "Keep RSVP" {
			t.Fatal("RSVP overwritten")
		}
		return mailHTTP(writeEvent("event/one", `"updated"`)), nil
	}}
	a := mailAdapter(t, tr)
	secrets := map[string]string{"access_token": "token"}
	out, _, err := accountWriteCall(t, a, CalendarEventCreate, in, secrets)
	if err != nil || out.EventID != postedID || out.Outcome != "created" || out.Notifications != "requested" {
		t.Fatal(out, err)
	}
	patch := writePatch()
	clear := ""
	start := calendar.Moment{DateTime: "2026-11-01T01:30:00-07:00", TimeZone: "America/Los_Angeles"}
	end := calendar.Moment{DateTime: "2026-11-01T01:30:00-08:00", TimeZone: "America/Los_Angeles"}
	attendees := []calendarwrite.Attendee{{Address: "guest@example.test", Kind: "optional"}}
	patch.Changes = calendarwrite.Patch{Description: &clear, Location: &clear, Start: &start, End: &end, Attendees: &attendees}
	out, _, err = accountWriteCall(t, a, CalendarEventUpdate, patch, secrets)
	if err != nil || out.Version != `"updated"` || out.Outcome != "updated" {
		t.Fatal(out, err)
	}
	if len(tr.requests) != 3 {
		t.Fatal("unexpected requests", len(tr.requests))
	}
}

func TestAccountWriteCalendarConflictsNeverOverwrite(t *testing.T) {
	for _, scenario := range []string{"stale-before", "stale-during", "series-scope", "resource-change", "uncertain", "duplicate-create"} {
		t.Run(scenario, func(t *testing.T) {
			writes := 0
			tr := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
				if r.Method == "GET" {
					e := writeEvent("event/one", `"v1"`)
					if scenario == "stale-before" {
						e.ETag = `"v2"`
					}
					if scenario == "series-scope" {
						e.Recurrence = []string{"RRULE:FREQ=WEEKLY"}
					}
					return mailHTTP(e), nil
				}
				writes++
				if scenario == "uncertain" {
					return connector.HTTPResponse{}, errors.New("lost response after commit")
				}
				status := 412
				if scenario == "duplicate-create" {
					status = 409
				}
				return connector.HTTPResponse{StatusCode: status}, nil
			}}
			a := mailAdapter(t, tr)
			patch := writePatch()
			if scenario == "resource-change" {
				v := []calendarwrite.Attendee{{Address: "guest@example.test", Kind: "resource"}}
				patch.Changes.Attendees = &v
			}
			var err error
			if scenario == "duplicate-create" {
				_, _, err = accountWriteCall(t, a, CalendarEventCreate, writeDraft(), map[string]string{"access_token": "token"})
			} else {
				_, _, err = accountWriteCall(t, a, CalendarEventUpdate, patch, map[string]string{"access_token": "token"})
			}
			want := connector.ErrorPermanent
			if scenario == "uncertain" {
				want = connector.ErrorUncertain
			}
			writeClass(t, err, want)
			wantWrites := 1
			if scenario == "stale-before" || scenario == "series-scope" || scenario == "resource-change" {
				wantWrites = 0
			}
			if writes != wantWrites {
				t.Fatal("mutation replayed or bypassed guard", writes)
			}
		})
	}
}

func decodeSentMIME(t *testing.T, r connector.HTTPRequest) (*mail.Message, map[string]any) {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(r.Body, &body); err != nil {
		t.Fatal(err)
	}
	raw, err := base64.RawURLEncoding.DecodeString(body["raw"].(string))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\r\n") {
		if len(line) > 998 {
			t.Fatal("RFC line too long", len(line))
		}
	}
	m, err := mail.ReadMessage(strings.NewReader(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	return m, body
}

func TestAccountWriteMailExplicitTargetsMIMEAndNativeReply(t *testing.T) {
	message := writeMail()
	message.Subject = strings.Repeat("界longword", 170)
	message.To[0].Name = strings.Repeat("界", 170)
	posted := 0
	tr := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		u, _ := url.Parse(r.URL)
		if strings.HasSuffix(u.Path, "/profile") {
			return mailHTTP(map[string]string{"emailAddress": "account@example.test"}), nil
		}
		if r.Method == "GET" {
			source := mailMessage("original/one", googleMailPart{})
			for i := range source.Payload.Headers {
				if source.Payload.Headers[i].Name == "Subject" {
					source.Payload.Headers[i].Value = message.Subject
				}
			}
			source.Payload.Headers = append(source.Payload.Headers, googleMailHeader{"References", "<earlier@example.test>"})
			return mailHTTP(source), nil
		}
		posted++
		m, body := decodeSentMIME(t, r)
		if r.Method != "POST" || u.Path != "/gmail/v1/users/me/messages/send" {
			t.Fatal(u)
		}
		subject, err := new(mime.WordDecoder).DecodeHeader(m.Header.Get("Subject"))
		if err != nil || subject != message.Subject {
			t.Fatal("subject changed", err)
		}
		b, err := io.ReadAll(base64.NewDecoder(base64.StdEncoding, m.Body))
		if err != nil || string(b) != message.Text {
			t.Fatal("body changed", err)
		}
		if m.Header.Get("From") != "<account@example.test>" || m.Header.Get("Bcc") != "<hidden@example.test>" || m.Header.Get("Cc") != "<copy@example.test>" {
			t.Fatal("targets changed")
		}
		to, err := m.Header.AddressList("To")
		if err != nil || len(to) != 1 || to[0].Address != message.To[0].Address {
			t.Fatal("recipient changed", err)
		}
		if !writeMessageID(m.Header.Get("Message-ID")) {
			t.Fatal("missing native correlation")
		}
		if posted == 1 {
			if _, ok := body["threadId"]; ok || m.Header.Get("In-Reply-To") != "" {
				t.Fatal("new message threaded")
			}
		} else {
			if body["threadId"] != "thread" || m.Header.Get("In-Reply-To") != "<original@example.test>" || m.Header.Get("References") != "<earlier@example.test> <original@example.test>" {
				t.Fatal("thread lost", m.Header)
			}
		}
		return mailHTTP(map[string]string{"id": "sent-id", "threadId": "thread"}), nil
	}}
	a := mailAdapter(t, tr)
	secrets := map[string]string{"access_token": "token"}
	out, _, err := accountWriteCall(t, a, MailSend, mailwrite.SendRequest{Message: message}, secrets)
	if err != nil || out.Status != "accepted" || out.Delivery != "unknown" {
		t.Fatal(out, err)
	}
	message.To = []maildto.Address{{Address: "reply@example.test"}}
	out, _, err = accountWriteCall(t, a, MailReply, mailwrite.ReplyRequest{MessageID: "original/one", Message: message}, secrets)
	if err != nil || out.InReplyToMessageID != "original/one" {
		t.Fatal(out, err)
	}
	if posted != 2 {
		t.Fatal(posted)
	}
}

func TestAccountWriteReplyRejectsChangedOrUnsafeOriginal(t *testing.T) {
	for _, scenario := range []string{"target", "draft", "reply-to", "subject", "missing-id", "injected-id", "unsafe-references", "incomplete"} {
		t.Run(scenario, func(t *testing.T) {
			writes := 0
			tr := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
				u, _ := url.Parse(r.URL)
				if strings.HasSuffix(u.Path, "/profile") {
					return mailHTTP(map[string]string{"emailAddress": "account@example.test"}), nil
				}
				if r.Method != "GET" {
					writes++
					return mailHTTP(map[string]string{"id": "sent", "threadId": "thread"}), nil
				}
				source := mailMessage("original", googleMailPart{})
				switch scenario {
				case "target":
					source.ID = "other"
				case "draft":
					*source.Labels = append(*source.Labels, "DRAFT")
				case "incomplete":
					source.Labels = nil
				case "unsafe-references":
					source.Payload.Headers = append(source.Payload.Headers, googleMailHeader{"References", "<ok@example.test>\r\nBcc: injected@example.test"})
				}
				for i := range source.Payload.Headers {
					h := &source.Payload.Headers[i]
					switch {
					case h.Name == "Subject" && scenario == "subject":
						h.Value = "changed"
					case h.Name == "Reply-To" && scenario == "reply-to":
						h.Value = "changed@example.test"
					case h.Name == "Message-ID" && scenario == "missing-id":
						h.Value = ""
					case h.Name == "Message-ID" && scenario == "injected-id":
						h.Value = "<a@example.test><injected@example.test>"
					}
				}
				return mailHTTP(source), nil
			}}
			a := mailAdapter(t, tr)
			m := writeMail()
			m.Subject = "Re: café"
			m.To = []maildto.Address{{Address: "reply@example.test"}}
			_, _, err := accountWriteCall(t, a, MailReply, mailwrite.ReplyRequest{MessageID: "original", Message: m}, map[string]string{"access_token": "token"})
			writeClass(t, err, connector.ErrorPermanent)
			if writes != 0 {
				t.Fatal("sent despite invalid source")
			}
		})
	}
}

func TestAccountWriteMailUncertainAndRefreshPreservesSecrets(t *testing.T) {
	for _, scenario := range []string{"network", "http500", "invalid-json", "missing-id", "wrong-thread", "http403"} {
		t.Run(scenario, func(t *testing.T) {
			writes := 0
			refreshes := 0
			tr := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
				u, _ := url.Parse(r.URL)
				if u.Path == "/token" {
					refreshes++
					return mailHTTP(map[string]string{"access_token": "new-token", "refresh_token": "new-refresh", "token_type": "Bearer"}), nil
				}
				if r.SecretHeaders["Authorization"][0] == "Bearer old-token" {
					return connector.HTTPResponse{StatusCode: 401}, nil
				}
				if strings.HasSuffix(u.Path, "/profile") {
					return mailHTTP(map[string]string{"emailAddress": "account@example.test"}), nil
				}
				if r.Method == "GET" {
					return mailHTTP(mailMessage("original", googleMailPart{})), nil
				}
				writes++
				switch scenario {
				case "network":
					return connector.HTTPResponse{}, errors.New("lost response")
				case "http500":
					return connector.HTTPResponse{StatusCode: 500}, nil
				case "invalid-json":
					return connector.HTTPResponse{StatusCode: 200, Body: []byte("{")}, nil
				case "wrong-thread":
					return mailHTTP(map[string]string{"id": "sent", "threadId": "wrong"}), nil
				case "http403":
					return connector.HTTPResponse{StatusCode: 403}, nil
				default:
					return mailHTTP(map[string]string{}), nil
				}
			}}
			a := mailAdapter(t, tr)
			secrets := map[string]string{"access_token": "old-token", "refresh_token": "old-refresh", "client_id": "client"}
			originalSecrets := cloneStrings(secrets)
			m := writeMail()
			m.Subject = "Re: café"
			m.To = []maildto.Address{{Address: "reply@example.test"}}
			_, result, err := accountWriteCall(t, a, MailReply, mailwrite.ReplyRequest{MessageID: "original", Message: m}, secrets)
			want := connector.ErrorUncertain
			if scenario == "http403" {
				want = connector.ErrorPermanent
			}
			writeClass(t, err, want)
			if writes != 1 || refreshes != 1 || result.SecretUpdates["access_token"] != "new-token" || result.SecretUpdates["refresh_token"] != "new-refresh" || !reflect.DeepEqual(secrets, originalSecrets) {
				t.Fatal("lost rotation, mutated host secrets or replayed", writes, refreshes, result.SecretUpdates)
			}
		})
	}
}

func TestAccountWriteInvalidEnvelopeAndPayloadHaveNoIO(t *testing.T) {
	tr := &recordingTransport{}
	a := mailAdapter(t, tr)
	valid, _ := json.Marshal(mailwrite.SendRequest{Message: writeMail()})
	for _, tc := range []struct {
		payload []byte
		ref     string
	}{{valid, ""}, {valid, "bad\r\nref"}, {[]byte(`{"message":{"to":[],"cc":[],"bcc":[],"subject":"x","text":"x"},"raw":"injected"}`), "request"}} {
		_, err := a.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: MailSend.Key, ContractSHA256: MailSend.ContractSHA256, Mode: connector.ModeCall, Connection: accountWriteConnection(), Payload: tc.payload, RequestRef: tc.ref, Secrets: map[string]string{"access_token": "token"}})
		if err == nil {
			t.Fatal("invalid write accepted")
		}
	}
	if len(tr.requests) != 0 {
		t.Fatal("invalid input made I/O")
	}
	// Different account/workspace/operation/target cannot reuse native identity.
	c := accountWriteConnection()
	base := writeCorrelation(c, "request", "op", "target")
	c.Key = "other"
	if base == writeCorrelation(c, "request", "op", "target") {
		t.Fatal("correlation crosses account")
	}
}

func TestAccountWriteCalendarRefreshPreservesPatchAndCondition(t *testing.T) {
	var bodies [][]byte
	tr := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		if strings.HasSuffix(r.URL, "/token") {
			return mailHTTP(map[string]string{"access_token": "rotated", "refresh_token": "rotated-refresh", "token_type": "Bearer"}), nil
		}
		if r.Method == "GET" {
			return mailHTTP(writeEvent("event/one", `"v1"`)), nil
		}
		if r.Headers["If-Match"][0] != `"v1"` {
			t.Fatal("refresh replaced condition")
		}
		bodies = append(bodies, append([]byte(nil), r.Body...))
		if r.SecretHeaders["Authorization"][0] == "Bearer expired" {
			return connector.HTTPResponse{StatusCode: 401}, nil
		}
		return connector.HTTPResponse{StatusCode: 500}, nil
	}}
	a := mailAdapter(t, tr)
	_, result, err := accountWriteCall(t, a, CalendarEventUpdate, writePatch(), map[string]string{"access_token": "expired", "refresh_token": "refresh", "client_id": "client"})
	writeClass(t, err, connector.ErrorUncertain)
	if len(bodies) != 2 || string(bodies[0]) != string(bodies[1]) || result.SecretUpdates["refresh_token"] != "rotated-refresh" || len(tr.requests) != 4 {
		t.Fatal("refresh changed or replayed write", result.SecretUpdates, len(tr.requests))
	}
}

func TestAccountWriteCalendarSeriesAndInstanceStayOnExactTarget(t *testing.T) {
	for _, series := range []bool{false, true} {
		t.Run(map[bool]string{false: "instance", true: "series"}[series], func(t *testing.T) {
			patch := writePatch()
			if series {
				patch.Scope = calendarwrite.ScopeSeries
			}
			tr := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
				u, _ := url.Parse(r.URL)
				if !strings.HasSuffix(u.EscapedPath(), "/events/event%2Fone") {
					t.Fatal("silently redirected target", u)
				}
				e := writeEvent("event/one", `"v1"`)
				if series {
					e.Recurrence = []string{"RRULE:FREQ=WEEKLY"}
				} else {
					e.SeriesID = "master"
					e.OriginalStart = &googleCalendarMoment{DateTime: "2026-09-12T08:00:00+08:00", TimeZone: "Asia/Shanghai"}
				}
				if r.Method == "PATCH" {
					e.ETag = `"v2"`
				}
				return mailHTTP(e), nil
			}}
			_, _, err := accountWriteCall(t, mailAdapter(t, tr), CalendarEventUpdate, patch, map[string]string{"access_token": "token"})
			if err != nil || len(tr.requests) != 2 {
				t.Fatal(err, len(tr.requests))
			}
		})
	}
}

func TestAccountWriteMailBCCOnlyAndMaximumText(t *testing.T) {
	m := writeMail()
	m.To = []maildto.Address{}
	m.CC = []maildto.Address{}
	m.Subject = ""
	m.Text = strings.Repeat("x", mailwrite.MaximumTextBytes)
	tr := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		if r.Method == "GET" {
			return mailHTTP(map[string]string{"emailAddress": "account@example.test"}), nil
		}
		mime, body := decodeSentMIME(t, r)
		if mime.Header.Get("To") != "" || mime.Header.Get("Cc") != "" || mime.Header.Get("Bcc") != "<hidden@example.test>" || len(body) != 1 {
			t.Fatal("BCC-only mail changed targets")
		}
		text, err := io.ReadAll(base64.NewDecoder(base64.StdEncoding, mime.Body))
		if err != nil || string(text) != m.Text {
			t.Fatal("maximum body corrupted", err)
		}
		return mailHTTP(map[string]string{"id": "sent", "threadId": "thread"}), nil
	}}
	_, _, err := accountWriteCall(t, mailAdapter(t, tr), MailSend, mailwrite.SendRequest{Message: m}, map[string]string{"access_token": "token"})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAccountWriteCalendarDescriptionRoundTripAtLimit(t *testing.T) {
	text := strings.Repeat("<", calendarwrite.MaximumDescriptionBytes-12) + "\n<b>literal"
	e := writeEvent("one", `"v1"`)
	e.Description = calendarWriteDescription(text)
	tr := &recordingTransport{respond: func(connector.HTTPRequest) (connector.HTTPResponse, error) { return mailHTTP(e), nil }}
	out, _, err := accountWriteCall(t, mailAdapter(t, tr), CalendarEventInspect, calendarwrite.InspectRequest{CalendarID: "team", EventID: "one", TimeZone: "UTC"}, map[string]string{"access_token": "token"})
	if err != nil || out.Event.Description != text {
		t.Fatalf("created plain text cannot be inspected again: %v; bytes=%d", err, len(out.Event.Description))
	}
}
