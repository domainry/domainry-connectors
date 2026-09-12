package microsoft

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	mail "github.com/domainry/domainry-connector-sdk/mail"
)

func TestMailNextLinkCannotChangeResourceOrExpandFields(t *testing.T) {
	for _, tc := range []string{"host", "scheme", "path", "userinfo", "fragment", "extra field", "duplicate query", "changed size", "missing base", "negative skip", "zero skip", "both paging"} {
		t.Run(tc, func(t *testing.T) {
			transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
				u, _ := url.Parse(r.URL)
				next, _ := url.Parse(nextMailURL(u, "opaque"))
				q := next.Query()
				switch tc {
				case "host":
					next.Host = "evil.example.test"
				case "scheme":
					next.Scheme = "https"
				case "path":
					next.Path = "/v1.0/users/other/messages"
				case "userinfo":
					next.User = url.User("other")
				case "fragment":
					next.Fragment = "private"
				case "extra field":
					q.Set("$select", q.Get("$select")+",body")
				case "duplicate query":
					q.Add("$select", q.Get("$select"))
				case "changed size":
					q.Set("$top", "1000")
				case "missing base":
					q.Del("$orderby")
				case "negative skip":
					q.Del("$skiptoken")
					q.Set("$skip", "-1")
				case "zero skip":
					q.Del("$skiptoken")
					q.Set("$skip", "0")
				case "both paging":
					q.Set("$skip", "10")
				}
				next.RawQuery = q.Encode()
				return mailHTTP(map[string]any{"value": []any{graphMail("m", "text", "PRIVATE")}, "@odata.nextLink": next.String()}), nil
			}}
			a := mailAdapter(t, transport)
			_, result, err := mailCall(t, a, MailList, mail.PageRequest{}, map[string]string{"access_token": "t"})
			if err == nil || len(result.Payload) != 0 || len(transport.requests) != 1 {
				t.Fatal("invalid next link disclosed or followed", result, err)
			}
		})
	}
}

func TestMailPagingPreservesGraphSkipAndRejectsRepeatedOrCrossAccountCursors(t *testing.T) {
	phase := 0
	transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		u, _ := url.Parse(r.URL)
		q := u.Query()
		if phase > 0 && q.Get("$skip") != "73" {
			t.Fatal("Graph skip manipulated", q)
		}
		q.Set("$skip", "73")
		u.RawQuery = q.Encode()
		return mailHTTP(map[string]any{"value": []any{graphMail("m", "text", "PRIVATE")}, "@odata.nextLink": u.String()}), nil
	}}
	a := mailAdapter(t, transport)
	secrets := map[string]string{"access_token": "t"}
	page, _, err := mailCall(t, a, MailList, mail.PageRequest{Limit: 2}, secrets)
	if err != nil {
		t.Fatal(err)
	}
	c := validConnection()
	c.Key = "different-account"
	b, _ := json.Marshal(mail.PageRequest{Limit: 2, Cursor: page.NextCursor})
	before := len(transport.requests)
	_, err = a.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: MailList.Key, ContractSHA256: MailList.ContractSHA256, Mode: connector.ModeCall, Connection: c, Secrets: secrets, Payload: b})
	if err == nil || len(transport.requests) != before {
		t.Fatal("account changed before continuation")
	}
	phase = 1
	_, result, err := mailCall(t, a, MailList, mail.PageRequest{Limit: 2, Cursor: page.NextCursor}, secrets)
	if err == nil || len(result.Payload) != 0 || len(transport.requests) != before+1 {
		t.Fatal("repeated continuation accepted", result, err)
	}
}

func TestMailInvalidPagesAndUnknownMetadataAreNotCompleteData(t *testing.T) {
	for _, value := range []any{nil, []any{graphMail("same", "text", "PRIVATE"), graphMail("same", "text", "PRIVATE")}, []any{map[string]any{"id": ".."}}} {
		transport := &recordingTransport{respond: func(connector.HTTPRequest) (connector.HTTPResponse, error) {
			return mailHTTP(map[string]any{"value": value}), nil
		}}
		a := mailAdapter(t, transport)
		_, result, err := mailCall(t, a, MailList, mail.PageRequest{}, map[string]string{"access_token": "t"})
		if err == nil || len(result.Payload) != 0 {
			t.Fatal(result, err)
		}
	}
	transport := &recordingTransport{respond: func(connector.HTTPRequest) (connector.HTTPResponse, error) {
		return mailHTTP(map[string]any{"id": "m"}), nil
	}}
	a := mailAdapter(t, transport)
	out, _, err := mailCall(t, a, MailRead, mail.ReadRequest{MessageID: "m"}, map[string]string{"access_token": "t"})
	if err != nil || out.Summary.MetadataComplete || out.Summary.IsRead != nil || out.Summary.From == nil || out.Body.Complete || out.Body.OmittedReasons[0] != "body_missing" {
		t.Fatal(out, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	before := len(transport.requests)
	p := &provider{transport: transport}
	if _, err = p.mailRead(ctx, connector.TypedRequest[mail.ReadRequest]{Connection: validConnection(), Input: mail.ReadRequest{MessageID: "m"}, Secrets: map[string]string{"access_token": "t"}}); err == nil || len(transport.requests) != before {
		t.Fatal("cancelled mail dispatched")
	}
}

func TestMailMaximumMetadataPageAndReadOnlyDescriptor(t *testing.T) {
	transport := &recordingTransport{respond: func(connector.HTTPRequest) (connector.HTTPResponse, error) {
		items := []any{}
		for i := 0; i < 25; i++ {
			m := graphMail(strconv.Itoa(i), "text", "PRIVATE BODY")
			m["subject"] = strings.Repeat("你", 4000)
			recipients := []any{}
			for j := 0; j < 100; j++ {
				recipients = append(recipients, map[string]any{"emailAddress": map[string]string{"address": "person@example.test", "name": strings.Repeat("长", 300)}})
			}
			m["toRecipients"] = recipients
			items = append(items, m)
		}
		return mailHTTP(map[string]any{"value": items}), nil
	}}
	a := mailAdapter(t, transport)
	for _, op := range a.Descriptor().Operations {
		if op.Key == MailList.Key || op.Key == MailSearch.Key || op.Key == MailRead.Key {
			if op.Mode != connector.ModeCall || op.Reliability.Effect != connector.EffectRead || op.ContractSHA256 != mail.OperationSHA256(op.Key) {
				t.Fatal(op)
			}
		}
	}
	out, result, err := mailCall(t, a, MailList, mail.PageRequest{Limit: 25}, map[string]string{"access_token": "t"})
	if err != nil || len(out.Items) != 25 || !out.Complete || len(result.Payload) > 1<<20 || out.Items[0].MetadataComplete || len(out.Items[0].To) != 50 || strings.Contains(string(result.Payload), "PRIVATE BODY") {
		t.Fatal(len(result.Payload), out.Complete, err)
	}
}
