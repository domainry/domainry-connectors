package google

import (
	"encoding/json"
	"net/url"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	mail "github.com/domainry/domainry-connector-sdk/mail"
)

func TestMailRelatedBodySelectsItsRootAndOmitsAssociatedText(t *testing.T) {
	root := mailPart("text/html", "<p>Root body</p>")
	root.Headers = []googleMailHeader{{"Content-ID", "<root>"}}
	resource := mailPart("text/plain", "ASSOCIATED RESOURCE")
	resource.Headers = []googleMailHeader{{"Content-ID", "<resource>"}}
	for _, tc := range []struct {
		contentType string
		parts       []googleMailPart
		complete    bool
	}{
		{`multipart/related; type="text/html"; start="<root>"`, []googleMailPart{resource, root}, true},
		{`multipart/related; type="text/html"`, []googleMailPart{root, resource}, true},
		{`multipart/related; start="<missing>"`, []googleMailPart{root, resource}, false},
		{`multipart/related; type="text/plain"; start="<root>"`, []googleMailPart{resource, root}, false},
	} {
		part := googleMailPart{MIMEType: "multipart/related", Headers: []googleMailHeader{{"Content-Type", tc.contentType}}, Parts: tc.parts}
		transport := &recordingTransport{respond: func(connector.HTTPRequest) (connector.HTTPResponse, error) {
			return mailHTTP(mailMessage("m", part)), nil
		}}
		a := mailAdapter(t, transport)
		out, _, err := mailCall(t, a, MailRead, mail.ReadRequest{MessageID: "m"}, map[string]string{"access_token": "t"})
		if err != nil || out.Body.Complete != tc.complete || strings.Contains(out.Body.Text, "ASSOCIATED") || tc.complete && !strings.Contains(out.Body.Text, "Root body") {
			t.Fatal(out, err)
		}
	}
}

func TestMailBodyPartSizeAndTreeLimitsDoNotSilentlyClaimCompleteness(t *testing.T) {
	big := 256<<10 + 1
	deep := mailPart("text/plain", "DEEPLY NESTED")
	for i := 0; i < 18; i++ {
		deep = googleMailPart{MIMEType: "multipart/mixed", Parts: []googleMailPart{deep}}
	}
	for _, part := range []googleMailPart{
		{MIMEType: "text/plain", Body: &googleMailBody{Size: &big, AttachmentID: "huge"}},
		deep,
		{MIMEType: "multipart/mixed", Parts: make([]googleMailPart, 257)},
	} {
		transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
			if strings.Contains(r.URL, "/attachments/") {
				t.Fatal("oversized body fetched")
			}
			return mailHTTP(mailMessage("m", part)), nil
		}}
		a := mailAdapter(t, transport)
		out, _, err := mailCall(t, a, MailRead, mail.ReadRequest{MessageID: "m"}, map[string]string{"access_token": "t"})
		if err != nil || out.Body.Complete || len(out.Body.OmittedReasons) == 0 || out.Body.Text != "" || len(transport.requests) != 1 {
			t.Fatal(out, err)
		}
	}
}

func TestMailCursorCannotChangeAccountAndRepeatedProviderCursorFails(t *testing.T) {
	transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		u, _ := url.Parse(r.URL)
		if strings.HasSuffix(u.Path, "/messages") {
			return mailHTTP(map[string]any{"messages": []map[string]string{{"id": "m", "threadId": "thread"}}, "nextPageToken": "repeat", "resultSizeEstimate": 2}), nil
		}
		return mailHTTP(mailMessage("m", mailPart("text/plain", "body"))), nil
	}}
	a := mailAdapter(t, transport)
	secrets := map[string]string{"access_token": "t"}
	first, _, err := mailCall(t, a, MailList, mail.PageRequest{}, secrets)
	if err != nil {
		t.Fatal(err)
	}
	before := len(transport.requests)
	c := validConnection()
	c.Key = "other-account"
	raw, _ := json.Marshal(mail.PageRequest{Cursor: first.NextCursor})
	_, err = a.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: MailList.Key, ContractSHA256: MailList.ContractSHA256, Mode: connector.ModeCall, Connection: c, Payload: raw, Secrets: secrets})
	if err == nil || len(transport.requests) != before {
		t.Fatal("cross-account cursor dispatched")
	}
	_, result, err := mailCall(t, a, MailList, mail.PageRequest{Cursor: first.NextCursor}, secrets)
	if err == nil || len(result.Payload) != 0 || len(transport.requests) != before+1 {
		t.Fatal("repeated page disclosed", result, err)
	}
}

func TestMailReadOnlyDescriptorAndMaximumMetadataPageBound(t *testing.T) {
	long := strings.Repeat("你", 4096)
	transport := &recordingTransport{respond: func(r connector.HTTPRequest) (connector.HTTPResponse, error) {
		u, _ := url.Parse(r.URL)
		if strings.HasSuffix(u.Path, "/messages") {
			items := []map[string]string{}
			for i := 0; i < mail.MaximumPageSize; i++ {
				items = append(items, map[string]string{"id": string(rune('a' + i)), "threadId": "thread"})
			}
			return mailHTTP(map[string]any{"messages": items, "resultSizeEstimate": 25}), nil
		}
		id := u.Path[strings.LastIndex(u.Path, "/")+1:]
		m := mailMessage(id, mailPart("text/plain", "BODY MUST NOT ENTER LIST"))
		m.Payload.Headers = []googleMailHeader{{"Subject", long}, {"To", strings.Repeat("a@example.test,", 99) + "z@example.test"}}
		return mailHTTP(m), nil
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
	if err != nil || len(out.Items) != 25 || !out.Complete || len(result.Payload) > 1<<20 || out.Items[0].MetadataComplete || len(out.Items[0].To) != 50 || len(out.Items[0].Subject) > 2048 {
		t.Fatal(len(out.Items), len(result.Payload), err)
	}
}

func TestMailSenderDateDoesNotGuessObsoleteLocalTimezone(t *testing.T) {
	m := mailMessage("m", mailPart("text/plain", "body"))
	for i := range m.Payload.Headers {
		if m.Payload.Headers[i].Name == "Date" {
			m.Payload.Headers[i].Value = "Fri, 11 Sep 2026 09:15:00 PDT"
		}
	}
	out, err := m.summary()
	if err != nil || out.SentAt != "" || out.ReceivedAt == "" || out.MetadataComplete {
		t.Fatal(out, err)
	}
}
