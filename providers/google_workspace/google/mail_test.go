package google

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/url"
	"reflect"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	mail "github.com/domainry/domainry-connector-sdk/mail"
)

func mailCall[I, O any](t *testing.T, a connector.Adapter, op connector.CallOperation[I, O], in I, secrets map[string]string) (O, connector.CallResult, error) {
	t.Helper()
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	r, err := a.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: op.Key, ContractSHA256: op.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Payload: raw, Secrets: secrets})
	var out O
	if err == nil {
		if err := json.Unmarshal(r.Payload, &out); err != nil {
			t.Fatal(err)
		}
	}
	return out, r, err
}
func mailAdapter(t *testing.T, transport connector.Transport) connector.Adapter {
	t.Helper()
	a, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	r := connector.NewRegistry()
	if err = r.Register(a); err != nil {
		t.Fatal(err)
	}
	r.Freeze()
	a, _ = r.Provider(ConnectorKey, ProviderKey)
	return a
}
func mailHTTP(value any) connector.HTTPResponse {
	b, _ := json.Marshal(value)
	return connector.HTTPResponse{StatusCode: 200, Body: b}
}
func mailPart(kind, text string) googleMailPart {
	n := len(text)
	return googleMailPart{MIMEType: kind, Body: &googleMailBody{Size: &n, Data: base64.RawURLEncoding.EncodeToString([]byte(text))}}
}
func mailMessage(id string, payload googleMailPart) googleMailMessage {
	labels := []string{"INBOX", "UNREAD"}
	payload.Headers = append(payload.Headers, []googleMailHeader{{"From", "=?UTF-8?B?5byg5LiJ?= <sender@example.test>"}, {"To", "recipient@example.test"}, {"Cc", "colleague@example.test"}, {"Reply-To", "reply@example.test"}, {"Subject", "=?ISO-8859-1?Q?caf=E9?="}, {"Date", "Fri, 11 Sep 2026 09:15:00 +0800"}, {"Message-ID", "<original@example.test>"}}...)
	return googleMailMessage{ID: id, ThreadID: "thread", Labels: &labels, InternalDate: "1789089300000", Payload: &payload}
}

func TestMailListSearchPaginationUsesMetadataAndExactAccountQuery(t *testing.T) {
	phase := 0
	transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		u, _ := url.Parse(r.URL)
		q := u.Query()
		if r.Method != "GET" || len(r.Body) != 0 || r.SecretHeaders["Authorization"][0] != "Bearer account-token" {
			t.Fatal("mail did not use read credential transport", r.Method)
		}
		if strings.HasSuffix(u.Path, "/messages") {
			if q.Get("maxResults") != "2" || q.Get("includeSpamTrash") != "false" {
				t.Fatal(q)
			}
			switch phase {
			case 0:
				if q.Has("q") || q.Has("pageToken") {
					t.Fatal(q)
				}
			case 1:
				if q.Get("q") != `from:sender@example.test subject:report` || q.Has("pageToken") {
					t.Fatal(q)
				}
			case 2:
				if q.Get("pageToken") != "opaque/+==" {
					t.Fatal(q)
				}
				return mailHTTP(map[string]any{"resultSizeEstimate": 0}), nil
			}
			return mailHTTP(map[string]any{"messages": []map[string]string{{"id": "mail/a", "threadId": "thread"}}, "nextPageToken": "opaque/+==", "resultSizeEstimate": 2}), nil
		}
		if !strings.HasSuffix(u.EscapedPath(), "/messages/mail%2Fa") || q.Get("format") != "metadata" || len(q["metadataHeaders"]) != 7 || strings.Contains(q.Get("fields"), "snippet") {
			t.Fatal(u, q)
		}
		return mailHTTP(mailMessage("mail/a", mailPart("text/plain", "MUST-NOT-LEAK-IN-LIST"))), nil
	}}
	a := mailAdapter(t, transport)
	secrets := map[string]string{"access_token": "account-token"}
	list, raw, err := mailCall(t, a, MailList, mail.PageRequest{Limit: 2}, secrets)
	if err != nil || list.Complete || len(list.Items) != 1 || list.Items[0].Subject != "café" || list.Items[0].From[0].Name != "张三" || list.Items[0].IsRead == nil || *list.Items[0].IsRead || !list.Items[0].MetadataComplete || strings.Contains(string(raw.Payload), "MUST-NOT-LEAK") {
		t.Fatal(list, err)
	}
	if list.Items[0].SentAt != "2026-09-11T09:15:00+08:00" || list.Items[0].InternetMessageID != "<original@example.test>" {
		t.Fatal(list.Items[0])
	}
	phase = 1
	request := mail.SearchRequest{Query: `from:sender@example.test subject:report`, QuerySyntax: mail.GmailSyntax, Limit: 2}
	search, _, err := mailCall(t, a, MailSearch, request, secrets)
	if err != nil || search.Complete || search.NextCursor == list.NextCursor {
		t.Fatal(search, err)
	}
	before := len(transport.requests)
	bad := request
	bad.Cursor = search.NextCursor
	bad.Query = "different"
	if _, _, err = mailCall(t, a, MailSearch, bad, secrets); err == nil || len(transport.requests) != before {
		t.Fatal("changed query dispatched")
	}
	bad = request
	bad.QuerySyntax = mail.GraphSyntax
	if _, _, err = mailCall(t, a, MailSearch, bad, secrets); err == nil || len(transport.requests) != before {
		t.Fatal("wrong query dialect dispatched")
	}
	bad = request
	bad.Cursor = list.NextCursor
	if _, _, err = mailCall(t, a, MailSearch, bad, secrets); err == nil || len(transport.requests) != before {
		t.Fatal("list cursor used as search")
	}
	phase = 2
	request.Cursor = search.NextCursor
	last, _, err := mailCall(t, a, MailSearch, request, secrets)
	if err != nil || !last.Complete || last.Items == nil || len(last.Items) != 0 {
		t.Fatal(last, err)
	}
}

func TestMailBodyRepresentationsEncodingTruncationAndAttachments(t *testing.T) {
	plain := mailPart("text/plain", "实际内容\n请周五回复")
	html := mailPart("text/html", `<p>实际 &amp; 内容</p><script>SECRET SCRIPT</script><img src="https://tracker.test"/><a href="javascript:evil()">回复</a>`)
	attachment := mailPart("text/plain", "ATTACHMENT SECRET")
	attachment.Filename = "secret.txt"
	encoded := mailPart("text/plain", string([]byte{0x63, 0x61, 0x66, 0xe9}))
	encoded.Headers = []googleMailHeader{{"Content-Type", "text/plain; charset=iso-8859-1"}}
	unknown := plain
	unknown.Headers = []googleMailHeader{{"Content-Type", "text/plain; charset=unknown"}}
	bad := plain
	size := 2
	bad.Body = &googleMailBody{Size: &size, Data: "%%%"}
	for _, tc := range []struct {
		name         string
		part         googleMailPart
		want, reason string
		complete     bool
	}{
		{"plain", plain, "实际内容\n请周五回复", "", true},
		{"html", html, "实际 & 内容", "", true},
		{"alternative", googleMailPart{MIMEType: "multipart/alternative", Parts: []googleMailPart{plain, html}}, "实际内容\n请周五回复", "", true},
		{"mixed", googleMailPart{MIMEType: "multipart/mixed", Parts: []googleMailPart{plain, attachment}}, "实际内容\n请周五回复", "", true},
		{"charset", encoded, "café", "", true},
		{"unknown charset", unknown, "", "body_encoding_unsupported", false},
		{"invalid base64", bad, "", "body_invalid", false},
		{"missing body", googleMailPart{MIMEType: "text/plain"}, "", "body_missing", false},
		{"only attachment", attachment, "", "body_unavailable", false},
		{"oversized", mailPart("text/plain", strings.Repeat("你", 400)), strings.Repeat("你", 341), "body_truncated", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
				u, _ := url.Parse(r.URL)
				if u.Query().Get("format") != "full" || !strings.HasSuffix(u.EscapedPath(), "/mail%2Fa") {
					t.Fatal(u)
				}
				return mailHTTP(mailMessage("mail/a", tc.part)), nil
			}}
			a := mailAdapter(t, transport)
			out, result, err := mailCall(t, a, MailRead, mail.ReadRequest{MessageID: "mail/a", MaxBodyBytes: 1024}, map[string]string{"access_token": "t"})
			if err != nil || out.Body.Complete != tc.complete || !strings.Contains(out.Body.Text, tc.want) || tc.reason != "" && !strings.Contains(strings.Join(out.Body.OmittedReasons, ","), tc.reason) {
				t.Fatal(out, err)
			}
			for _, hidden := range []string{"SECRET SCRIPT", "tracker.test", "javascript:", "ATTACHMENT SECRET"} {
				if strings.Contains(string(result.Payload), hidden) {
					t.Fatal("nonbody disclosed", hidden)
				}
			}
			if len(transport.requests) != 1 {
				t.Fatal("unexpected attachment/link access")
			}
			if tc.name == "alternative" && strings.Contains(out.Body.Text, "&") {
				t.Fatal("duplicate alternative body")
			}
		})
	}
}

func TestMailExternalTextBodyFetchIsBoundedAndNeverFetchesNamedAttachments(t *testing.T) {
	for _, count := range []int{1, 5} {
		t.Run(string(rune('0'+count)), func(t *testing.T) {
			text := "External text 正文"
			size := len(text)
			parts := []googleMailPart{}
			for i := 0; i < count; i++ {
				parts = append(parts, googleMailPart{MIMEType: "text/plain", Body: &googleMailBody{Size: &size, AttachmentID: "body/part"}})
			}
			parts = append(parts, googleMailPart{MIMEType: "text/plain", Filename: "private.txt", Body: &googleMailBody{Size: &size, AttachmentID: "file"}})
			transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
				u, _ := url.Parse(r.URL)
				if strings.Contains(u.Path, "/attachments/") {
					if !strings.HasSuffix(u.EscapedPath(), "/attachments/body%2Fpart") {
						t.Fatal("named attachment fetched", u)
					}
					return mailHTTP(googleMailBody{Size: &size, Data: base64.RawURLEncoding.EncodeToString([]byte(text))}), nil
				}
				return mailHTTP(mailMessage("m", googleMailPart{MIMEType: "multipart/mixed", Parts: parts})), nil
			}}
			a := mailAdapter(t, transport)
			out, _, err := mailCall(t, a, MailRead, mail.ReadRequest{MessageID: "m"}, map[string]string{"access_token": "t"})
			if err != nil || out.Body.Complete != (count == 1) || !strings.Contains(out.Body.Text, text) || len(transport.requests) > 5 {
				t.Fatal(out, len(transport.requests), err)
			}
		})
	}
}

func TestMailReadRotationSurvivesSubsequentMetadataFailure(t *testing.T) {
	transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		if strings.HasSuffix(r.URL, "/token") {
			return mailHTTP(map[string]any{"access_token": "fresh", "refresh_token": "rotated"}), nil
		}
		if r.SecretHeaders["Authorization"][0] == "Bearer expired" {
			return connector.HTTPResponse{StatusCode: 401, Body: []byte(`{}`)}, nil
		}
		u, _ := url.Parse(r.URL)
		if strings.HasSuffix(u.Path, "/messages") {
			return mailHTTP(map[string]any{"messages": []map[string]string{{"id": "m", "threadId": "thread"}}, "resultSizeEstimate": 1}), nil
		}
		if r.SecretHeaders["Authorization"][0] != "Bearer fresh" {
			t.Fatal("rotation not used on next read")
		}
		return mailHTTP(mailMessage("WRONG-ID", mailPart("text/plain", "PRIVATE"))), nil
	}}
	a := mailAdapter(t, transport)
	secrets := map[string]string{"access_token": "expired", "refresh_token": "old", "client_id": "client"}
	_, result, err := mailCall(t, a, MailList, mail.PageRequest{}, secrets)
	if err == nil || len(result.Payload) != 0 || !reflect.DeepEqual(result.SecretUpdates, map[string]string{"access_token": "fresh", "refresh_token": "rotated"}) || secrets["access_token"] != "expired" || len(transport.requests) != 4 {
		t.Fatal(result, err, len(transport.requests))
	}
}

func TestMailMalformedPagesFailWithoutBodyDisclosure(t *testing.T) {
	for _, page := range []any{map[string]any{}, map[string]any{"messages": []map[string]string{{"id": "m", "threadId": "thread"}, {"id": "m", "threadId": "thread"}}, "resultSizeEstimate": 2}, map[string]any{"messages": []map[string]string{{"id": "", "threadId": "thread"}}, "resultSizeEstimate": 1}} {
		transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
			u, _ := url.Parse(r.URL)
			if strings.HasSuffix(u.Path, "/messages") {
				return mailHTTP(page), nil
			}
			return mailHTTP(mailMessage("m", mailPart("text/plain", "PRIVATE"))), nil
		}}
		a := mailAdapter(t, transport)
		_, result, err := mailCall(t, a, MailList, mail.PageRequest{}, map[string]string{"access_token": "t"})
		if err == nil || len(result.Payload) != 0 || strings.Contains(err.Error(), "PRIVATE") {
			t.Fatal(result, err)
		}
	}
	transport := &recordingTransport{}
	p := &provider{transport: transport}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := newMailSession(p, validConnection(), map[string]string{"access_token": "t"}).get(ctx, "messages", nil, new(any)); err == nil || len(transport.requests) != 0 {
		t.Fatal("cancelled read dispatched")
	}
}

func TestMailRegisteredOAuthScopesSeparateHeadersFromSearchAndBody(t *testing.T) {
	a := mailAdapter(t, &recordingTransport{})
	for _, key := range []string{mail.ListOperationKey, mail.SearchOperationKey, mail.ReadOperationKey} {
		scopes, declared := connector.ResolveOAuthOperationScopes(a, key)
		if !declared {
			t.Fatal(key)
		}
		metadata := false
		for _, alternative := range scopes {
			if len(alternative) != 1 {
				t.Fatal(alternative)
			}
			switch alternative[0] {
			case "https://www.googleapis.com/auth/gmail.metadata":
				metadata = true
			case "https://www.googleapis.com/auth/gmail.readonly", "https://www.googleapis.com/auth/gmail.modify", "https://mail.google.com/":
			default:
				t.Fatal(alternative)
			}
		}
		if metadata != (key == mail.ListOperationKey) {
			t.Fatal("metadata can read body/search", key, scopes)
		}
		scopes[0][0] = "mutated"
		fresh, _ := connector.ResolveOAuthOperationScopes(a, key)
		if fresh[0][0] == "mutated" {
			t.Fatal("scope declaration mutated")
		}
	}
	for _, key := range []string{GmailSendMessage.Key, GmailGetMessage.Key, SyncEmailHistory.Key} {
		if _, declared := connector.ResolveOAuthOperationScopes(a, key); declared {
			t.Fatal("legacy/background operation silently acquired account read access", key)
		}
	}
}
