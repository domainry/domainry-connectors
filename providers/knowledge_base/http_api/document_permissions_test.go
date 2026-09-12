package httpapi

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
)

func TestPrivateDocumentWriteUsesTrustedACLAndStableCommand(t *testing.T) {
	tr := &recordingTransport{response: connector.HTTPResponse{StatusCode: 202, Body: []byte(`{"err_code":0}`)}}
	a, err := New(tr)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		configured bool
		ids        any
		header     string
	}{
		{"omitted", false, nil, ""},
		{"explicit public", true, []string{}, "[]"},
		{"ASCII Unicode canonical", true, []string{"z", "资料:🧪", `a"\<&>`, "z"}, `["a\"\\<&>","z","\u8d44\u6599:\ud83e\uddea"]`},
		{"JSON configuration", true, []any{"z", "a", "z"}, `["a","z"]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := request(PutDocument.Descriptor(), PutDocumentInput{DocID: "doc-1", Filename: "report.md", Content: []byte("document")})
			r.RequestRef = "stable-command-1"
			if tc.configured {
				r.Connection.Config["document_permission_ids"] = tc.ids
			}
			// Untrusted headers do not supply or override document ACLs.
			r.Headers = map[string]string{"X-KB-Permission-Ids": "[]", "X-KB-Request-ID": "forged"}
			for range 2 {
				if _, err := a.Call(t.Context(), r); err != nil {
					t.Fatal(err)
				}
				sent := tr.requests[len(tr.requests)-1]
				// SDK maps retain spelling; net/http canonicalization is transport owned.
				got := strings.Join(sent.Headers["X-KB-Permission-Ids"], "")
				if got != tc.header {
					t.Fatalf("ACL command: %q", got)
				}
				wantID := ""
				if tc.configured {
					wantID = r.RequestRef
				}
				if strings.Join(sent.Headers["X-KB-Request-ID"], "") != wantID {
					t.Fatal("logical ID lost or supplied by untrusted header")
				}
				for _, c := range got {
					if c >= 128 {
						t.Fatal("non-ASCII header")
					}
				}
				if tc.configured {
					var ids []string
					if json.Unmarshal([]byte(got), &ids) != nil || !slices.IsSorted(ids) {
						t.Fatal("invalid canonical ACL")
					}
				}
			}
			r = request(DeleteDocument.Descriptor(), DocumentInput{DocID: "doc-1"})
			r.Connection.Config["document_permission_ids"] = []string{"private"}
			if _, err := a.Call(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			sent := tr.requests[len(tr.requests)-1]
			if len(sent.Headers["X-KB-Permission-Ids"]) != 0 || len(sent.Headers["X-KB-Request-ID"]) != 0 {
				t.Fatal("DELETE sent an ACL mutation")
			}
		})
	}
}

func TestDocumentACLRejectsInvalidPolicyAndCommandsBeforeIO(t *testing.T) {
	tr := &recordingTransport{}
	a, err := New(tr)
	if err != nil {
		t.Fatal(err)
	}
	tooMany := make([]string, 257)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("scope:%d", i)
	}
	tooLarge := make([]string, 80)
	for i := range tooLarge {
		tooLarge[i] = fmt.Sprintf("%d:%s", i, strings.Repeat("界", 40))
	}
	invalid := []any{nil, []string(nil), []any(nil), "[]", []any{1}, []string{""}, []string{" a"}, []string{"a "}, []string{"a\n"}, []string{"a\x7f"}, []string{string([]byte{0xff})}, []string{strings.Repeat("界", 43)}, tooMany, tooLarge}
	for _, ids := range invalid {
		r := request(PutDocument.Descriptor(), PutDocumentInput{DocID: "doc", Filename: "a.txt", Content: []byte("a")})
		r.RequestRef = "stable"
		r.Connection.Config["document_permission_ids"] = ids
		if a.(connector.ConfigValidator).ValidateConfig(r.Connection) == nil {
			t.Fatalf("invalid ACL configuration accepted (%T)", ids)
		}
		if _, err := a.Call(t.Context(), r); err == nil {
			t.Fatal("invalid ACL reached write")
		}
	}
	for _, id := range []string{"", " bad", "bad ", "bad\r\n", "bad\x7f", strings.Repeat("a", 513), string([]byte{0xff})} {
		r := request(PutDocument.Descriptor(), PutDocumentInput{DocID: "doc", Filename: "a.txt", Content: []byte("a")})
		r.RequestRef = id
		r.Connection.Config["document_permission_ids"] = []string{"private"}
		if _, err := a.Call(t.Context(), r); err == nil {
			t.Fatal("invalid request ID accepted")
		}
	}
	for _, in := range []PutDocumentInput{
		{DocID: "doc/other", Filename: "a.txt", Content: []byte("a")},
		{DocID: strings.Repeat("a", 129), Filename: "a.txt", Content: []byte("a")},
		{DocID: "doc", Filename: "中文.txt", Content: []byte("a")},
		{DocID: "doc", Filename: "a.txt", Content: make([]byte, 10<<20+1)},
	} {
		if _, err := a.Call(t.Context(), request(PutDocument.Descriptor(), in)); err == nil {
			t.Fatal("upstream contract violation accepted")
		}
	}
	if len(tr.requests) != 0 {
		t.Fatal("invalid private request reached transport")
	}
}
