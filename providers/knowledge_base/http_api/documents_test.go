package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
)

func TestDocumentWritesUseOriginalBytesAndValidatedScope(t *testing.T) {
	body := []byte{'%', 0, 0xff, 0x80, '\n'}
	tr := &recordingTransport{response: connector.HTTPResponse{StatusCode: 202, Body: []byte(`{"err_code":0,"data":{"status":"PENDING"}}`)}}
	a, err := New(tr)
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range []connector.OperationDescriptor{PutDocument.Descriptor(), DeleteDocument.Descriptor()} {
		var call connector.CallRequest
		if op.Key == "put_document" {
			call = putRequest("doc.safe-1", "expenses 2026.pdf", body)
		} else {
			call = request(op, DocumentInput{DocID: "doc.safe-1"})
		}
		out, err := a.Call(t.Context(), call)
		if err != nil {
			t.Fatal(err)
		}
		strategy := connector.IdempotencyNone
		if op.Key == DeleteDocument.Key {
			strategy = connector.IdempotencyNatural
		}
		if op.Reliability.Effect != connector.EffectWrite || op.Reliability.Idempotency.Strategy != strategy || op.Reliability.Reconciliation != connector.ReconciliationNone {
			t.Fatal("unverified write guarantees advertised")
		}
		r := tr.requests[len(tr.requests)-1]
		u, err := url.Parse(r.URL)
		if err != nil || u.Path != "/v1/kb/kbs/bcri/documents" || u.Query().Get("doc_id") != "doc.safe-1" || r.SecretHeaders["Authorization"][0] != "Bearer private-key" {
			t.Fatal("document scope escaped")
		}
		if op.Key == "put_document" {
			if r.Method != http.MethodPost || u.Query().Get("filename") != "expenses 2026.pdf" || !bytes.Equal(r.Body, body) || r.Headers["Content-Type"][0] != "application/octet-stream" {
				t.Fatal("original document bytes or filename changed")
			}
		} else if r.Method != http.MethodDelete || len(u.Query()) != 1 || len(r.Body) != 0 {
			t.Fatal("delete protocol mismatch")
		}
		var result Output
		if json.Unmarshal(out.Payload, &result) != nil || !bytes.Equal(result.Result, tr.response.Body) {
			t.Fatal("acknowledgement changed")
		}
		pub, _ := json.Marshal(r)
		if bytes.Contains(pub, []byte("private-key")) {
			t.Fatal("credential serialized")
		}
	}
}

func TestDocumentStatusPreservesStateAndHidesMetadataContent(t *testing.T) {
	for _, state := range []string{"PENDING", "CHUNKED", "INDEXED", "FUTURE_STATE"} {
		tr := &recordingTransport{response: connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"err_code":0,"data":{"doc_id":"doc","status":"` + state + `","chunks":[{"content":"PRIVATE-BODY"}]}}`)}}
		a, err := New(tr)
		if err != nil {
			t.Fatal(err)
		}
		r := request(DocumentStatus.Descriptor(), DocumentInput{DocID: "doc"})
		r.Connection.Config["permission_ids_by_user"] = map[string][]string{"user": {"reader"}}
		value, err := a.Call(t.Context(), r)
		if err != nil {
			t.Fatal(err)
		}
		var out DocumentStatusOutput
		if json.Unmarshal(value.Payload, &out) != nil || !out.Exists || out.IndexStatus != state || out.DocID != "doc" || strings.Contains(string(value.Payload), "PRIVATE-BODY") {
			t.Fatal("index state fabricated or content leaked")
		}
		var sent map[string]any
		if json.Unmarshal(tr.requests[0].Body, &sent) != nil || sent["include_content"] != false || sent["live"].(map[string]any)["enabled"] != false || sent["permission_ids"].([]any)[0] != "reader" {
			t.Fatal("metadata query lost permission policy")
		}
	}
	for _, tc := range []struct {
		body    string
		status  int
		code    string
		missing bool
	}{
		{`{"err_code":1004,"err_msg":"not found"}`, 200, "", true},
		{`{}`, 200, "response_invalid", false},
		{`{"err_code":0,"data":{"doc_id":"foreign","status":"INDEXED"}}`, 200, "response_invalid", false},
		{`{"err_code":0,"data":{"status":null}}`, 200, "response_invalid", false},
		{`{"err_code":0,"data":{"status":true}}`, 200, "response_invalid", false},
		{`{"err_code":1099}`, 200, "failed", false},
		{`private`, 403, "access_denied", false},
	} {
		tr := &recordingTransport{response: connector.HTTPResponse{StatusCode: tc.status, Body: []byte(tc.body)}}
		a, err := New(tr)
		if err != nil {
			t.Fatal(err)
		}
		got, err := a.Call(t.Context(), request(DocumentStatus.Descriptor(), DocumentInput{DocID: "doc"}))
		code, _ := connector.ProviderErrorCodeOf(err)
		if tc.code != "" {
			if code != "knowledge_api."+tc.code || len(got.Payload) != 0 {
				t.Fatal("invalid state treated as absent", err)
			}
			continue
		}
		var out DocumentStatusOutput
		if err != nil || json.Unmarshal(got.Payload, &out) != nil || out.Exists || out.IndexStatus != "" || out.DocID != "doc" || !tc.missing {
			t.Fatal("missing document not reported precisely", err)
		}
	}
}

func TestDocumentWritesRejectInvalidInputBeforeTransport(t *testing.T) {
	tr := &recordingTransport{}
	a, err := New(tr)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*connector.CallRequest){
		func(r *connector.CallRequest) { r.Principal.WorkspaceID = "other" },
		func(r *connector.CallRequest) { r.Principal.IsAuthenticated = false },
		func(r *connector.CallRequest) { r.Secrets = nil },
		func(r *connector.CallRequest) { r.Connection.Config["kb_id"] = "../foreign" },
		func(r *connector.CallRequest) { r.Connection.Config["kb_id"] = ".." },
		func(r *connector.CallRequest) {
			r.Payload = []byte(`{"doc_id":"d","filename":"f","bytes":4,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","permission_ids":["public"]}`)
		},
		func(r *connector.CallRequest) {
			r.Payload = []byte(`{"doc_id":"d","filename":"f","bytes":4,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","kb_id":"foreign"}`)
		},
		func(r *connector.CallRequest) {
			r.Payload = []byte(`{"doc_id":"d","filename":"../f","bytes":4,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`)
		},
		func(r *connector.CallRequest) {
			r.ContentSHA256 = "not-a-sha"
		},
		func(r *connector.CallRequest) { r.Content = nil },
		func(r *connector.CallRequest) {
			r.ContentLength = MaximumInlineDocumentBytes + 1
			r.Payload, _ = json.Marshal(PutDocumentInput{DocID: "d", Filename: "f", Bytes: r.ContentLength, SHA256: r.ContentSHA256})
		},
	} {
		r := putRequest("doc", "test.md", []byte("test"))
		mutate(&r)
		if _, err = a.Call(t.Context(), r); err == nil {
			t.Fatal("invalid document request accepted")
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err = a.Call(ctx, putRequest("doc", "test.md", []byte("test"))); !errors.Is(err, context.Canceled) {
		t.Fatal("cancel ignored", err)
	}
	if len(tr.requests) != 0 {
		t.Fatal("rejected write reached transport")
	}
	tr.err = errors.New("secret network uncertainty")
	_, err = a.Call(t.Context(), putRequest("doc", "test.md", []byte("test")))
	if code, _ := connector.ProviderErrorCodeOf(err); code != "knowledge_api.network" || strings.Contains(err.Error(), "secret") {
		t.Fatal("uncertain write misreported", err)
	}
	if len(tr.requests) != 1 {
		t.Fatal("connector automatically retried write")
	}
}
