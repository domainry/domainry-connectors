package microsoft

import (
	"encoding/json"
	"errors"
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

func writeConnection() connector.Connection {
	c := validConnection()
	c.WorkspaceID = "workspace"
	c.Key = "account"
	return c
}
func writeCall[I, O any](t *testing.T, a connector.Adapter, op connector.CallOperation[I, O], input I, secrets map[string]string) (O, connector.CallResult, error) {
	t.Helper()
	b, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	r, err := a.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: op.Key, ContractSHA256: op.ContractSHA256, Mode: connector.ModeCall, Connection: writeConnection(), Payload: b, RequestRef: "request", Secrets: secrets})
	var out O
	if err == nil {
		if e := json.Unmarshal(r.Payload, &out); e != nil {
			t.Fatal(e)
		}
	}
	return out, r, err
}
func writeClass(t *testing.T, err error, want connector.ErrorClassification) {
	t.Helper()
	got, ok := connector.ErrorClassificationOf(err)
	if !ok || got != want {
		t.Fatalf("classification=%s want=%s err=%v", got, want, err)
	}
}
func writeEvent(id, etag string) map[string]any {
	e := graphEvent(id, "2026-09-12T01:00:00", "2026-09-12T02:00:00", "busy")
	e["@odata.etag"], e["changeKey"] = etag, "change-1"
	e["isOrganizer"], e["hideAttendees"], e["isOnlineMeeting"] = true, false, false
	e["body"] = map[string]string{"contentType": "text", "content": "Original"}
	e["attendees"] = []map[string]any{{"type": "required", "emailAddress": map[string]string{"address": "guest@example.test", "name": "Guest"}, "status": map[string]string{"response": "accepted", "time": "2026-09-11T00:00:00Z"}}}
	return e
}
func writeZones() connector.HTTPResponse {
	return mailHTTP(map[string]any{"value": []map[string]string{{"alias": "UTC"}, {"alias": "Asia/Shanghai"}, {"alias": "America/Los_Angeles"}}})
}
func writeDraft() calendarwrite.CreateRequest {
	return calendarwrite.CreateRequest{CalendarID: "calendar/team", Notifications: calendarwrite.NotifyAttendees, Event: calendarwrite.Draft{Title: "会议", Description: "<b>literal</b>\n正文", Location: "Room", Start: calendar.Moment{Date: "2026-09-12", TimeZone: "Asia/Shanghai"}, End: calendar.Moment{Date: "2026-09-14", TimeZone: "Asia/Shanghai"}, Attendees: []calendarwrite.Attendee{{Address: "guest@example.test", Kind: "required"}, {Address: "room@example.test", Kind: "resource"}}}}
}
func writePatch() calendarwrite.UpdateRequest {
	title := "New"
	return calendarwrite.UpdateRequest{CalendarID: "calendar/team", EventID: "event/one", ExpectedVersion: `W/"v1"`, Scope: calendarwrite.ScopeEvent, Notifications: calendarwrite.NotifyAttendees, Changes: calendarwrite.Patch{Title: &title}}
}
func writeMessage() mailwrite.Message {
	return mailwrite.Message{To: []maildto.Address{{Address: "recipient@example.test", Name: "接收者"}}, CC: []maildto.Address{{Address: "cc@example.test"}}, BCC: []maildto.Address{{Address: "bcc@example.test"}}, Subject: "评审安排", Text: "纯文本\n<script>literal</script>"}
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
			t.Fatal(op)
		}
		if effect == connector.EffectWrite && op.Reliability.Idempotency.Strategy != connector.IdempotencyNone {
			t.Fatal("write advertised replay")
		}
	}
	if count != 5 {
		t.Fatal("missing operations", count)
	}
	allows := func(key string, grants ...string) bool {
		sets, ok := connector.ResolveOAuthOperationScopes(a, key)
		if !ok {
			t.Fatal("scopes missing", key)
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
	if allows(CalendarEventCreate.Key, "Calendars.ReadWrite") || allows(CalendarEventUpdate.Key, "Calendars.Read", "User.Read") || !allows(CalendarEventCreate.Key, "Calendars.ReadWrite", "https://graph.microsoft.com/User.Read") {
		t.Fatal("calendar scope intersection incorrect")
	}
	if allows(MailSend.Key, "Mail.ReadWrite") || allows(MailReply.Key, "Mail.Send") || !allows(MailSend.Key, "Mail.Send") || !allows(MailReply.Key, "Mail.Send", "https://graph.microsoft.com/Mail.ReadBasic") {
		t.Fatal("mail scope intersection incorrect")
	}
	if allows(MailRead.Key, "Mail.Send", "Mail.ReadBasic") {
		t.Fatal("basic write account gained body read")
	}
	values, _ := connector.ResolveOAuthOperationScopes(a, MailReply.Key)
	values[0][0] = "mutated"
	again, _ := connector.ResolveOAuthOperationScopes(a, MailReply.Key)
	if again[0][0] == "mutated" {
		t.Fatal("mutable grant registry")
	}
}

func TestAccountWriteCalendarCreatePreservesAllDayAndUsesNativeTransaction(t *testing.T) {
	var body map[string]any
	tr := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		u, _ := url.Parse(r.URL)
		if strings.HasSuffix(u.Path, supportedIanaZonesPath) {
			return writeZones(), nil
		}
		if r.Method != "POST" || !strings.HasSuffix(u.EscapedPath(), "/calendars/calendar%2Fteam/events") || r.SecretHeaders["Authorization"][0] != "Bearer token" || !strings.Contains(r.Headers["Prefer"][0], `IdType="ImmutableId"`) {
			t.Fatal("incorrect governed write", u, r.Method)
		}
		if json.Unmarshal(r.Body, &body) != nil {
			t.Fatal("invalid JSON")
		}
		out := mailHTTP(writeEvent("created", `W/"created"`))
		out.StatusCode = 201
		return out, nil
	}}
	out, _, err := writeCall(t, mailAdapter(t, tr), CalendarEventCreate, writeDraft(), map[string]string{"access_token": "token"})
	if err != nil || out.Outcome != "created" || out.EventID != "created" || out.Notifications != "requested" {
		t.Fatal(out, err)
	}
	if body["isAllDay"] != true || len(body["transactionId"].(string)) != 64 || body["start"].(map[string]any)["dateTime"] != "2026-09-12T00:00:00" || body["end"].(map[string]any)["dateTime"] != "2026-09-14T00:00:00" || body["body"].(map[string]any)["content"] != writeDraft().Event.Description {
		t.Fatal("draft changed", body)
	}
	if len(tr.requests) != 2 {
		t.Fatal("unexpected replay")
	}
}

func TestAccountWriteCalendarConditionalPatchAndNativeScope(t *testing.T) {
	for _, kind := range []string{"singleInstance", "occurrence", "exception", "seriesMaster"} {
		t.Run(kind, func(t *testing.T) {
			patch := writePatch()
			if kind == "seriesMaster" {
				patch.Scope = calendarwrite.ScopeSeries
			}
			clear := ""
			attendees := []calendarwrite.Attendee{{Address: "guest@example.test", Kind: "optional"}}
			patch.Changes = calendarwrite.Patch{Description: &clear, Location: &clear, Attendees: &attendees}
			tr := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
				u, _ := url.Parse(r.URL)
				if !strings.HasSuffix(u.EscapedPath(), "/calendars/calendar%2Fteam/events/event%2Fone") {
					t.Fatal("target redirected", u)
				}
				e := writeEvent("event/one", `W/"v1"`)
				e["type"] = kind
				if kind == "occurrence" || kind == "exception" {
					e["seriesMasterId"] = "master"
					e["originalStart"] = "2026-09-12T00:00:00Z"
				}
				if r.Method == "GET" {
					return mailHTTP(e), nil
				}
				var body map[string]any
				_ = json.Unmarshal(r.Body, &body)
				if r.Method != "PATCH" || r.Headers["If-Match"][0] != `W/"v1"` || len(body) != 3 || body["body"].(map[string]any)["content"] != "" || body["location"].(map[string]any)["displayName"] != "" {
					t.Fatal("condition or patch changed", body)
				}
				if body["attendees"].([]any)[0].(map[string]any)["status"].(map[string]any)["response"] != "accepted" {
					t.Fatal("RSVP lost")
				}
				e["@odata.etag"] = `W/"v2"`
				return mailHTTP(e), nil
			}}
			out, _, err := writeCall(t, mailAdapter(t, tr), CalendarEventUpdate, patch, map[string]string{"access_token": "token"})
			if err != nil || out.Version != `W/"v2"` || len(tr.requests) != 2 {
				t.Fatal(out, err)
			}
		})
	}
}

func TestAccountWriteCalendarRejectsIncompleteConflictAndOnlineBody(t *testing.T) {
	for _, scenario := range []string{"stale", "series-scope", "hidden", "missing-attendees", "not-organizer", "online-body", "unknown-online", "cancelled", "no-etag", "duplicate-attendee", "412"} {
		t.Run(scenario, func(t *testing.T) {
			writes := 0
			tr := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
				if r.Method != "GET" {
					writes++
					return connector.HTTPResponse{StatusCode: 412}, nil
				}
				e := writeEvent("event/one", `W/"v1"`)
				switch scenario {
				case "stale":
					e["@odata.etag"] = `W/"v2"`
				case "series-scope":
					e["type"] = "seriesMaster"
				case "hidden":
					e["hideAttendees"] = true
				case "missing-attendees":
					delete(e, "attendees")
				case "not-organizer":
					e["isOrganizer"] = false
				case "online-body":
					e["isOnlineMeeting"] = true
				case "unknown-online":
					delete(e, "isOnlineMeeting")
				case "cancelled":
					e["isCancelled"] = true
				case "no-etag":
					delete(e, "@odata.etag")
				case "duplicate-attendee":
					v := e["attendees"].([]map[string]any)
					e["attendees"] = append(v, v[0])
				}
				return mailHTTP(e), nil
			}}
			patch := writePatch()
			if strings.Contains(scenario, "online") {
				v := "changed"
				patch.Changes.Description = &v
			}
			_, _, err := writeCall(t, mailAdapter(t, tr), CalendarEventUpdate, patch, map[string]string{"access_token": "token"})
			writeClass(t, err, connector.ErrorPermanent)
			want := 0
			if scenario == "412" {
				want = 1
			}
			if writes != want {
				t.Fatal("unsafe write or replay", writes)
			}
		})
	}
}

func TestAccountWriteCalendarDSTAndSupportedZones(t *testing.T) {
	for _, scenario := range []string{"fold-single", "fold-series", "unsupported", "truncated-zones"} {
		t.Run(scenario, func(t *testing.T) {
			writes := 0
			tr := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
				u, _ := url.Parse(r.URL)
				if strings.HasSuffix(u.Path, supportedIanaZonesPath) {
					if scenario == "truncated-zones" {
						return mailHTTP(map[string]any{"value": []any{}, "@odata.nextLink": "https://other.test/continue"}), nil
					}
					return writeZones(), nil
				}
				if r.Method == "GET" {
					e := writeEvent("event/one", `W/"v1"`)
					e["type"] = "seriesMaster"
					return mailHTTP(e), nil
				}
				writes++
				var body map[string]any
				_ = json.Unmarshal(r.Body, &body)
				if body["start"].(map[string]any)["dateTime"] != "2026-11-01T08:30:00" || body["end"].(map[string]any)["dateTime"] != "2026-11-01T09:30:00" || body["start"].(map[string]any)["timeZone"] != "UTC" {
					t.Fatal("fold changed instant", body)
				}
				v := mailHTTP(writeEvent("created", `W/"v2"`))
				v.StatusCode = 201
				return v, nil
			}}
			d := writeDraft()
			d.Event.Start = calendar.Moment{DateTime: "2026-11-01T01:30:00-07:00", TimeZone: "America/Los_Angeles"}
			d.Event.End = calendar.Moment{DateTime: "2026-11-01T01:30:00-08:00", TimeZone: "America/Los_Angeles"}
			if scenario == "unsupported" {
				d.Event.Start = calendar.Moment{Date: "2026-09-12", TimeZone: "Europe/Paris"}
				d.Event.End = calendar.Moment{Date: "2026-09-13", TimeZone: "Europe/Paris"}
			}
			a := mailAdapter(t, tr)
			var err error
			if scenario == "fold-series" {
				p := writePatch()
				p.Scope = calendarwrite.ScopeSeries
				p.Changes.Start = &d.Event.Start
				p.Changes.End = &d.Event.End
				_, _, err = writeCall(t, a, CalendarEventUpdate, p, map[string]string{"access_token": "token"})
			} else {
				_, _, err = writeCall(t, a, CalendarEventCreate, d, map[string]string{"access_token": "token"})
			}
			if scenario == "fold-single" {
				if err != nil || writes != 1 {
					t.Fatal(err, writes)
				}
			} else {
				writeClass(t, err, connector.ErrorPermanent)
				if writes != 0 {
					t.Fatal("unrepresentable time written")
				}
			}
		})
	}
}

func TestAccountWriteMailExplicitRecipientsAnd202(t *testing.T) {
	phase := 0
	m := writeMessage()
	tr := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		u, _ := url.Parse(r.URL)
		if r.Method == "GET" {
			if !strings.HasSuffix(u.EscapedPath(), "/messages/original%2Fone") || strings.Contains(u.Query().Get("$select"), "body") {
				t.Fatal("reply source escaped", u)
			}
			return mailHTTP(graphMail("original/one", "text", "not-requested")), nil
		}
		var body map[string]any
		_ = json.Unmarshal(r.Body, &body)
		message := body["message"].(map[string]any)
		if r.Method != "POST" || len(message) != 5 || message["subject"] != m.Subject || message["body"].(map[string]any)["contentType"] != "text" || message["body"].(map[string]any)["content"] != m.Text {
			t.Fatal("outgoing content changed")
		}
		for _, group := range []struct {
			key    string
			values []maildto.Address
		}{{"toRecipients", m.To}, {"ccRecipients", m.CC}, {"bccRecipients", m.BCC}} {
			actual := message[group.key].([]any)
			if len(actual) != len(group.values) {
				t.Fatal("recipient count changed")
			}
			for i, a := range group.values {
				address := actual[i].(map[string]any)["emailAddress"].(map[string]any)
				if address["address"] != a.Address || address["name"] != a.Name {
					t.Fatal("recipient identity changed")
				}
			}
		}
		if phase == 0 {
			if !strings.HasSuffix(u.Path, "/me/sendMail") || body["saveToSentItems"] != true {
				t.Fatal(body, u)
			}
		} else {
			if !strings.HasSuffix(u.EscapedPath(), "/messages/original%2Fone/reply") || len(body) != 1 {
				t.Fatal("reply expanded", body, u)
			}
		}
		return connector.HTTPResponse{StatusCode: 202}, nil
	}}
	a := mailAdapter(t, tr)
	out, _, err := writeCall(t, a, MailSend, mailwrite.SendRequest{Message: m}, map[string]string{"access_token": "token"})
	if err != nil || out.Status != "accepted" || out.Delivery != "unknown" || out.MessageID != "" || out.ThreadID != "" {
		t.Fatal(out, err)
	}
	phase = 1
	m.To = []maildto.Address{{Address: "reply@example.test"}}
	m.Subject = "Re: 评审安排"
	out, _, err = writeCall(t, a, MailReply, mailwrite.ReplyRequest{MessageID: "original/one", Message: m}, map[string]string{"access_token": "token"})
	if err != nil || out.InReplyToMessageID != "original/one" {
		t.Fatal(out, err)
	}
}

func TestAccountWriteReplyRejectsChangedOriginal(t *testing.T) {
	for _, scenario := range []string{"id", "reply-to", "subject", "draft", "missing-recipients", "incomplete"} {
		t.Run(scenario, func(t *testing.T) {
			writes := 0
			tr := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
				if r.Method != "GET" {
					writes++
					return connector.HTTPResponse{StatusCode: 202}, nil
				}
				v := graphMail("original", "text", "")
				switch scenario {
				case "id":
					v["id"] = "other"
				case "reply-to":
					v["replyTo"] = []any{map[string]any{"emailAddress": map[string]string{"address": "changed@example.test"}}}
				case "subject":
					v["subject"] = "changed"
				case "draft":
					v["isDraft"] = true
				case "missing-recipients":
					delete(v, "replyTo")
				case "incomplete":
					delete(v, "isDraft")
				}
				return mailHTTP(v), nil
			}}
			m := writeMessage()
			m.To = []maildto.Address{{Address: "reply@example.test"}}
			_, _, err := writeCall(t, mailAdapter(t, tr), MailReply, mailwrite.ReplyRequest{MessageID: "original", Message: m}, map[string]string{"access_token": "token"})
			writeClass(t, err, connector.ErrorPermanent)
			if writes != 0 {
				t.Fatal("invalid reply sent")
			}
		})
	}
}

func TestAccountWriteMailRefreshAndUncertainNeverReplay(t *testing.T) {
	for _, scenario := range []string{"network", "500", "malformed", "wrong-status", "invented-id", "403"} {
		t.Run(scenario, func(t *testing.T) {
			writes, rotations := 0, 0
			tr := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
				if strings.HasSuffix(r.URL, "/token") {
					rotations++
					return mailHTTP(map[string]string{"access_token": "rotated", "refresh_token": "new-refresh", "token_type": "Bearer"}), nil
				}
				if r.SecretHeaders["Authorization"][0] == "Bearer old" {
					return connector.HTTPResponse{StatusCode: 401}, nil
				}
				if r.Method == "GET" {
					return mailHTTP(graphMail("original", "text", "")), nil
				}
				writes++
				switch scenario {
				case "network":
					return connector.HTTPResponse{}, errors.New("accepted then disconnected")
				case "500":
					return connector.HTTPResponse{StatusCode: 500}, nil
				case "malformed":
					return connector.HTTPResponse{StatusCode: 202, Body: []byte("{")}, nil
				case "wrong-status":
					return connector.HTTPResponse{StatusCode: 200}, nil
				case "403":
					return connector.HTTPResponse{StatusCode: 403}, nil
				default:
					return connector.HTTPResponse{StatusCode: 202, Body: []byte(`{"id":"untrusted"}`)}, nil
				}
			}}
			secrets := map[string]string{"access_token": "old", "refresh_token": "refresh", "client_id": "client"}
			before := writeSecrets(secrets, empty())
			m := writeMessage()
			m.To = []maildto.Address{{Address: "reply@example.test"}}
			_, r, err := writeCall(t, mailAdapter(t, tr), MailReply, mailwrite.ReplyRequest{MessageID: "original", Message: m}, secrets)
			want := connector.ErrorUncertain
			if scenario == "403" {
				want = connector.ErrorPermanent
			}
			writeClass(t, err, want)
			if writes != 1 || rotations != 1 || r.SecretUpdates["access_token"] != "rotated" || r.SecretUpdates["refresh_token"] != "new-refresh" || !reflect.DeepEqual(secrets, before) {
				t.Fatal("lost rotation or replayed", writes, rotations, r.SecretUpdates)
			}
		})
	}
}

func TestAccountWriteCalendarMutationRefreshKeepsOriginalCondition(t *testing.T) {
	var bodies [][]byte
	tr := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		if strings.HasSuffix(r.URL, "/token") {
			return mailHTTP(map[string]string{"access_token": "rotated", "refresh_token": "next-refresh", "token_type": "Bearer"}), nil
		}
		if r.Method == "GET" {
			return mailHTTP(writeEvent("event/one", `W/"v1"`)), nil
		}
		if r.Headers["If-Match"][0] != `W/"v1"` {
			t.Fatal("version silently refreshed")
		}
		bodies = append(bodies, append([]byte(nil), r.Body...))
		if r.SecretHeaders["Authorization"][0] == "Bearer old" {
			return connector.HTTPResponse{StatusCode: 401}, nil
		}
		return connector.HTTPResponse{StatusCode: 500}, nil
	}}
	_, r, err := writeCall(t, mailAdapter(t, tr), CalendarEventUpdate, writePatch(), map[string]string{"access_token": "old", "refresh_token": "refresh", "client_id": "client"})
	writeClass(t, err, connector.ErrorUncertain)
	if len(bodies) != 2 || string(bodies[0]) != string(bodies[1]) || len(tr.requests) != 4 || r.SecretUpdates["refresh_token"] != "next-refresh" {
		t.Fatal("mutation replay or lost refresh", len(tr.requests), r.SecretUpdates)
	}
}

func TestAccountWriteAllDayInspectionRechecksSourceRevision(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "stable", true: "changed"}[changed], func(t *testing.T) {
			tr := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
				e := writeEvent("event/one", `W/"v1"`)
				e["isAllDay"] = true
				e["originalStartTimeZone"] = "Asia/Shanghai"
				e["originalEndTimeZone"] = "Asia/Shanghai"
				if strings.Contains(r.Headers["Prefer"][0], `outlook.timezone="Asia/Shanghai"`) {
					e["start"] = map[string]string{"dateTime": "2026-09-12T00:00:00.0000000", "timeZone": "Asia/Shanghai"}
					e["end"] = map[string]string{"dateTime": "2026-09-14T00:00:00.0000000", "timeZone": "Asia/Shanghai"}
					if changed {
						e["changeKey"] = "other-version"
					}
				}
				return mailHTTP(e), nil
			}}
			out, _, err := writeCall(t, mailAdapter(t, tr), CalendarEventInspect, calendarwrite.InspectRequest{CalendarID: "calendar/team", EventID: "event/one", TimeZone: "America/Los_Angeles"}, map[string]string{"access_token": "token"})
			if changed {
				writeClass(t, err, connector.ErrorRetryable)
			} else if err != nil || out.Event.Start.Date != "2026-09-12" || out.Event.End.Date != "2026-09-14" || out.Version != `W/"v1"` {
				t.Fatal(out, err)
			}
			if len(tr.requests) != 2 {
				t.Fatal("missing exact source-zone refetch")
			}
		})
	}
}

func TestAccountWriteMailBCCOnlyMaximumTextAndInvalidInput(t *testing.T) {
	m := writeMessage()
	m.To = []maildto.Address{}
	m.CC = []maildto.Address{}
	m.Subject = ""
	m.Text = strings.Repeat("x", mailwrite.MaximumTextBytes)
	tr := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		var body map[string]any
		_ = json.Unmarshal(r.Body, &body)
		mail := body["message"].(map[string]any)
		if len(mail["toRecipients"].([]any)) != 0 || len(mail["ccRecipients"].([]any)) != 0 || len(mail["bccRecipients"].([]any)) != 1 || mail["body"].(map[string]any)["content"] != m.Text {
			t.Fatal("BCC-only or bounded body changed")
		}
		return connector.HTTPResponse{StatusCode: 202}, nil
	}}
	a := mailAdapter(t, tr)
	_, _, err := writeCall(t, a, MailSend, mailwrite.SendRequest{Message: m}, map[string]string{"access_token": "token"})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(mailwrite.SendRequest{Message: m})
	for _, tc := range []struct {
		ref     string
		payload []byte
	}{{"", raw}, {"bad\r\nref", raw}, {"valid", []byte(`{"message":{"to":[],"cc":[],"bcc":[],"subject":"x","text":"x"},"from":"spoofed"}`)}} {
		_, err := a.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: MailSend.Key, ContractSHA256: MailSend.ContractSHA256, Mode: connector.ModeCall, Connection: writeConnection(), RequestRef: tc.ref, Payload: tc.payload, Secrets: map[string]string{"access_token": "token"}})
		if err == nil {
			t.Fatal("invalid input accepted")
		}
	}
	if len(tr.requests) != 1 {
		t.Fatal("invalid input made I/O")
	}
}

func TestAccountWriteCalendarHTMLInspectionHasBoundedVisibleText(t *testing.T) {
	for _, oversized := range []bool{false, true} {
		t.Run(map[bool]string{false: "literal", true: "over-limit"}[oversized], func(t *testing.T) {
			text := strings.Repeat("<", calendarwrite.MaximumDescriptionBytes-24) + "\n<b>literal"
			body := strings.ReplaceAll(strings.ReplaceAll(text, "<", "&lt;"), "\n", "<br>")
			if oversized {
				body = strings.Repeat("x", calendarwrite.MaximumDescriptionBytes+1)
			}
			tr := &recordingTransport{respond: func(connector.HTTPRequest) (connector.HTTPResponse, error) {
				e := writeEvent("event/one", `W/"v1"`)
				e["body"] = map[string]string{"contentType": "html", "content": body}
				return mailHTTP(e), nil
			}}
			out, _, err := writeCall(t, mailAdapter(t, tr), CalendarEventInspect, calendarwrite.InspectRequest{CalendarID: "calendar/team", EventID: "event/one", TimeZone: "UTC"}, map[string]string{"access_token": "token"})
			if oversized {
				writeClass(t, err, connector.ErrorPermanent)
			} else if err != nil || out.Event.Description != text {
				t.Fatal("HTML changed visible text", err)
			}
		})
	}
}

func TestAccountWriteCalendarCreateNeverInventsReceipt(t *testing.T) {
	for _, scenario := range []string{"missing-etag", "missing-id", "wrong-status", "duplicate", "network", "500"} {
		t.Run(scenario, func(t *testing.T) {
			writes := 0
			tr := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
				if r.Method == "GET" {
					return writeZones(), nil
				}
				writes++
				e := writeEvent("created", `W/"v1"`)
				status := 201
				switch scenario {
				case "missing-etag":
					delete(e, "@odata.etag")
				case "missing-id":
					delete(e, "id")
				case "wrong-status":
					status = 200
				case "duplicate":
					status = 409
				case "network":
					return connector.HTTPResponse{}, errors.New("created then lost response")
				case "500":
					status = 500
				}
				v := mailHTTP(e)
				v.StatusCode = status
				return v, nil
			}}
			_, _, err := writeCall(t, mailAdapter(t, tr), CalendarEventCreate, writeDraft(), map[string]string{"access_token": "token"})
			want := connector.ErrorUncertain
			if scenario == "duplicate" {
				want = connector.ErrorPermanent
			}
			writeClass(t, err, want)
			if writes != 1 {
				t.Fatal("creation replayed")
			}
		})
	}
}
