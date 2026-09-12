package httpapi

import (
	"encoding/json"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
)

func TestIdempotentDeleteIsOneAttemptAndKeepsDocumentScopeOnRecovery(t *testing.T) {
	tr := &recordingTransport{response: connector.HTTPResponse{StatusCode: 503, Body: []byte(`{"err_code":1099}`)}}
	a, err := New(tr)
	if err != nil {
		t.Fatal(err)
	}
	r := request(DeleteDocument.Descriptor(), DocumentInput{DocID: "owned-immutable-generation"})
	if _, err = a.Call(t.Context(), r); err == nil || len(tr.requests) != 1 {
		t.Fatal("uncertain delete was hidden or automatically retried", err)
	}
	first := tr.requests[0]
	tr.response = connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"err_code":0,"data":{"ok":true,"stats":{"vectors_deleted":0}}}`)}
	out, err := a.Call(t.Context(), r)
	if err != nil || len(tr.requests) != 2 {
		t.Fatal(err)
	}
	if tr.requests[1].URL != first.URL || tr.requests[1].Method != first.Method || len(tr.requests[1].Body) != 0 {
		t.Fatal("retry changed the immutable document")
	}
	var value Output
	if json.Unmarshal(out.Payload, &value) != nil || string(value.Result) != string(tr.response.Body) {
		t.Fatal("zero-delete receipt lost")
	}
	if DeleteDocument.Reliability.Idempotency.Strategy != connector.IdempotencyNatural || PutDocument.Reliability.Idempotency.Strategy != connector.IdempotencyNone {
		t.Fatal("upload and deletion reliability conflated")
	}
	r.ContractSHA256 = "4729fefbfaf34173e5f8d651216f66c293091af7a4f72240004b1557e49054b6"
	if _, err = a.Call(t.Context(), r); err == nil || len(tr.requests) != 2 {
		t.Fatal("old deletion contract accepted or reached transport")
	}
}
