package microsoft

import (
	"encoding/json"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	mail "github.com/domainry/domainry-connector-sdk/mail"
)

func mailCall[I, O any](t *testing.T, a connector.Adapter, op connector.CallOperation[I, O], in I, secrets map[string]string) (O, connector.CallResult, error) {
	t.Helper()
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	r, err := a.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: op.Key, ContractSHA256: op.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Payload: b, Secrets: secrets})
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
func mailHTTP(v any) connector.HTTPResponse {
	b, _ := json.Marshal(v)
	return connector.HTTPResponse{StatusCode: 200, Body: b}
}
func graphMail(id, contentType, content string) map[string]any {
	recipient := func(name, address string) map[string]any {
		return map[string]any{"emailAddress": map[string]string{"name": name, "address": address}}
	}
	return map[string]any{"id": id, "conversationId": "thread", "internetMessageId": "<original@example.test>", "subject": "评审安排", "from": recipient("张三", "sender@example.test"), "toRecipients": []any{recipient("李四", "recipient@example.test")}, "ccRecipients": []any{}, "replyTo": []any{recipient("", "reply@example.test")}, "receivedDateTime": "2026-09-11T01:15:00Z", "sentDateTime": "2026-09-11T09:15:00+08:00", "isRead": false, "isDraft": false, "body": map[string]string{"contentType": contentType, "content": content}}
}
func nextMailURL(u *url.URL, token string) string {
	copy := *u
	q := copy.Query()
	q.Del("$skip")
	q.Set("$skiptoken", token)
	copy.RawQuery = q.Encode()
	return copy.String()
}

func TestMailListSearchAndReadUseHeaderBoundedFieldsAndImmutableIDs(t *testing.T) {
	phase := 0
	transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		u, _ := url.Parse(r.URL)
		q := u.Query()
		if r.Method != "GET" || len(r.Body) != 0 || r.SecretHeaders["Authorization"][0] != "Bearer token" || !strings.Contains(r.Headers["Prefer"][0], `IdType="ImmutableId"`) {
			t.Fatal("mail read transport identity changed", r.Method)
		}
		if strings.HasSuffix(u.Path, "/messages") {
			if q.Get("$top") != "2" || strings.Contains(q.Get("$select"), "body") || strings.Contains(q.Get("$select"), "attachments") {
				t.Fatal(q)
			}
			switch phase {
			case 0:
				if q.Get("$orderby") != "receivedDateTime desc" || q.Has("$search") {
					t.Fatal(q)
				}
			case 1:
				if q.Get("$search") != `"subject:review"` || q.Has("$orderby") {
					t.Fatal(q)
				}
			case 2:
				if q.Get("$skiptoken") != "opaque/+==" || q.Get("$search") != `"subject:review"` {
					t.Fatal(q)
				}
				return mailHTTP(map[string]any{"value": []any{}}), nil
			}
			return mailHTTP(map[string]any{"value": []any{graphMail("mail/a", "html", "BODY MUST NOT ENTER LIST")}, "@odata.nextLink": nextMailURL(u, "opaque/+==")}), nil
		}
		if !strings.HasSuffix(u.EscapedPath(), "/messages/mail%2Fa") || q.Get("$select") != mailMetadataFields+",body" || !strings.Contains(r.Headers["Prefer"][0], `outlook.body-content-type="text"`) {
			t.Fatal(u, q)
		}
		return mailHTTP(graphMail("mail/a", "text", "请周五前回复评审意见。")), nil
	}}
	a := mailAdapter(t, transport)
	secrets := map[string]string{"access_token": "token"}
	list, result, err := mailCall(t, a, MailList, mail.PageRequest{Limit: 2}, secrets)
	if err != nil || list.Complete || len(list.Items) != 1 || list.Items[0].ID != "mail/a" || !list.Items[0].MetadataComplete || list.Items[0].IsRead == nil || *list.Items[0].IsRead || strings.Contains(string(result.Payload), "BODY MUST") {
		t.Fatal(list, err)
	}
	phase = 1
	request := mail.SearchRequest{Query: "subject:review", QuerySyntax: mail.GraphSyntax, Limit: 2}
	search, _, err := mailCall(t, a, MailSearch, request, secrets)
	if err != nil || search.Complete || search.NextCursor == list.NextCursor {
		t.Fatal(search, err)
	}
	before := len(transport.requests)
	bad := request
	bad.QuerySyntax = mail.GmailSyntax
	if _, _, err = mailCall(t, a, MailSearch, bad, secrets); err == nil || len(transport.requests) != before {
		t.Fatal("wrong syntax dispatched")
	}
	bad = request
	bad.Cursor = search.NextCursor
	bad.Query = "changed"
	if _, _, err = mailCall(t, a, MailSearch, bad, secrets); err == nil || len(transport.requests) != before {
		t.Fatal("changed search dispatched")
	}
	bad = request
	bad.Cursor = list.NextCursor
	if _, _, err = mailCall(t, a, MailSearch, bad, secrets); err == nil || len(transport.requests) != before {
		t.Fatal("list cursor used in search")
	}
	phase = 2
	request.Cursor = search.NextCursor
	last, _, err := mailCall(t, a, MailSearch, request, secrets)
	if err != nil || !last.Complete || last.Items == nil || len(last.Items) != 0 {
		t.Fatal(last, err)
	}
	read, _, err := mailCall(t, a, MailRead, mail.ReadRequest{MessageID: "mail/a"}, secrets)
	if err != nil || !read.Body.Complete || read.Body.Text != "请周五前回复评审意见。" || read.Summary.SentAt != "2026-09-11T09:15:00+08:00" || read.Summary.ReplyTo[0].Address != "reply@example.test" {
		t.Fatal(read, err)
	}
}

func TestMailBodyFallbackAndOmissionsAreExplicit(t *testing.T) {
	for _, tc := range []struct {
		name, kind, content, want, reason string
		missing                           bool
	}{
		{"HTML fallback", "html", `<p>实际 &amp; 内容</p><script>SECRET</script><img src="https://tracker.test"/>`, "实际 & 内容", "", false},
		{"empty complete", "text", "", "", "", false},
		{"missing body", "", "", "", "body_missing", true},
		{"unsupported type", "rtf", "HIDDEN", "", "body_format_unsupported", false},
		{"truncated UTF8", "text", strings.Repeat("你", 400), strings.Repeat("你", 341), "body_truncated", false},
		{"malformed HTML", "html", "<head>HIDDEN", "", "body_html_invalid", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transport := &recordingTransport{respond: func(connector.HTTPRequest) (connector.HTTPResponse, error) {
				m := graphMail("m", tc.kind, tc.content)
				if tc.missing {
					delete(m, "body")
				}
				return mailHTTP(m), nil
			}}
			a := mailAdapter(t, transport)
			out, result, err := mailCall(t, a, MailRead, mail.ReadRequest{MessageID: "m", MaxBodyBytes: 1024}, map[string]string{"access_token": "t"})
			if err != nil || out.Body.Complete != (tc.reason == "") || !strings.Contains(out.Body.Text, tc.want) || tc.reason != "" && !reflect.DeepEqual(out.Body.OmittedReasons, []string{tc.reason}) || strings.Contains(string(result.Payload), "SECRET") || strings.Contains(string(result.Payload), "tracker.test") {
				t.Fatal(out, err)
			}
			if len(transport.requests) != 1 {
				t.Fatal("body fallback fetched other resources")
			}
		})
	}
}

func TestMailSearchCapIsNotACompleteMatchingMailbox(t *testing.T) {
	pages := 0
	transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		u, _ := url.Parse(r.URL)
		q := u.Query()
		if pages > 0 && q.Get("$skiptoken") != strconv.Itoa(pages) {
			t.Fatal("continuation query changed", q)
		}
		items := []any{}
		for i := 0; i < 25; i++ {
			items = append(items, graphMail("mail-"+strconv.Itoa(pages*25+i), "text", "ignored"))
		}
		pages++
		out := map[string]any{"value": items}
		if pages < 40 {
			out["@odata.nextLink"] = nextMailURL(u, strconv.Itoa(pages))
		}
		return mailHTTP(out), nil
	}}
	a := mailAdapter(t, transport)
	r := mail.SearchRequest{Query: "report", QuerySyntax: mail.GraphSyntax, Limit: 25}
	for i := 0; i < 40; i++ {
		out, _, err := mailCall(t, a, MailSearch, r, map[string]string{"access_token": "t"})
		if err != nil {
			t.Fatal(i, err)
		}
		if out.Complete || len(out.Items) != 25 {
			t.Fatal(i, out)
		}
		if i == 39 {
			if out.NextCursor != "" || out.LimitReason != "provider_search_limit" {
				t.Fatal(out)
			}
		} else if out.NextCursor == "" || out.LimitReason != "" {
			t.Fatal(i, out)
		}
		r.Cursor = out.NextCursor
	}
	if pages != 40 {
		t.Fatal(pages)
	}
}

func TestMailReadFailurePreservesRotationAndSuppressesBody(t *testing.T) {
	transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		if strings.HasSuffix(r.URL, "/token") {
			return mailHTTP(map[string]string{"access_token": "fresh", "refresh_token": "rotated"}), nil
		}
		if r.SecretHeaders["Authorization"][0] == "Bearer expired" {
			return connector.HTTPResponse{StatusCode: 401, Body: []byte(`{}`)}, nil
		}
		if !strings.Contains(r.Headers["Prefer"][0], `IdType="ImmutableId"`) {
			t.Fatal("refresh lost stable ID preference")
		}
		return mailHTTP(graphMail("wrong-id", "text", "PRIVATE MAIL")), nil
	}}
	a := mailAdapter(t, transport)
	secrets := map[string]string{"access_token": "expired", "refresh_token": "old", "client_id": "client"}
	_, result, err := mailCall(t, a, MailRead, mail.ReadRequest{MessageID: "expected"}, secrets)
	if err == nil || len(result.Payload) != 0 || !reflect.DeepEqual(result.SecretUpdates, map[string]string{"access_token": "fresh", "refresh_token": "rotated"}) || secrets["access_token"] != "expired" || len(transport.requests) != 3 {
		t.Fatal(result, err)
	}
}

func TestMailOAuthGrantsDoNotPromoteBasicOrSendToContentRead(t *testing.T) {
	a := mailAdapter(t, &recordingTransport{})
	for _, key := range []string{mail.ListOperationKey, mail.SearchOperationKey, mail.ReadOperationKey} {
		values, declared := connector.ResolveOAuthOperationScopes(a, key)
		if !declared {
			t.Fatal(key)
		}
		for _, scope := range []string{"Mail.ReadBasic", "Mail.ReadBasic.Shared", "Mail.Read", "Mail.ReadWrite", "Mail.Read.Shared", "Mail.ReadWrite.Shared", "Mail.ReadBasic.All", "Mail.Send", "User.Read"} {
			want := (strings.Contains(scope, "Read") && scope != "User.Read" && scope != "Mail.ReadBasic.All" && (!strings.Contains(scope, "Basic") || key == mail.ListOperationKey))
			for _, prefix := range []string{"", "https://graph.microsoft.com/"} {
				found := false
				for _, v := range values {
					found = found || reflect.DeepEqual(v, []string{prefix + scope})
				}
				if found != want {
					t.Fatal(key, prefix+scope, found, want)
				}
			}
		}
		values[0][0] = "changed"
		fresh, _ := connector.ResolveOAuthOperationScopes(a, key)
		if fresh[0][0] == "changed" {
			t.Fatal("scope mutated")
		}
	}
	if _, declared := connector.ResolveOAuthOperationScopes(a, SyncOutlookMail.Key); declared {
		t.Fatal("legacy sync promoted to authorized mail reads")
	}
}
