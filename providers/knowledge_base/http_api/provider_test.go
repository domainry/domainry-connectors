package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
	"github.com/domainry/domainry-connectors/catalog"
)

type recordingTransport struct {
	requests []connector.HTTPRequest
	response connector.HTTPResponse
	err      error
}

func (r *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	r.requests = append(r.requests, request)
	return r.response, r.err
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func request(op connector.OperationDescriptor, input any) connector.CallRequest {
	raw, _ := json.Marshal(input)
	return connector.CallRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: op.Key, ContractSHA256: op.ContractSHA256, Mode: op.Mode, Payload: raw,
		Connection: connector.Connection{WorkspaceID: "workspace", Config: map[string]any{"base_url": "https://kb.example.com", "team_id": "team", "kb_id": "bcri"}},
		Secrets:    map[string]string{"api_key": "private-key"},
		Principal:  connector.Principal{IsAuthenticated: true, UserID: "user", WorkspaceID: "workspace"},
	}
}

func TestContractAndCatalogIdentity(t *testing.T) {
	transport := &recordingTransport{}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	if err = contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	if len(transport.requests) != 0 {
		t.Fatal("constructor performed I/O")
	}
	definitions, err := catalog.Definitions()
	if err != nil {
		t.Fatal(err)
	}
	var definition catalog.ConnectorSchema
	for _, item := range definitions {
		if item.Key == ConnectorKey {
			if err = json.Unmarshal(item.Payload, &definition); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(definition.Operations) != 2 {
		t.Fatal("missing knowledge definition")
	}
	for _, operation := range definition.Operations {
		hash, err := catalog.OperationContractSHA256(ConnectorKey, ProviderKey, operation)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, descriptor := range adapter.Descriptor().Operations {
			if descriptor.Key == operation.Key {
				found = true
				if descriptor.ContractSHA256 != hash || descriptor.Reliability.Effect != connector.EffectRead {
					t.Fatal("catalog/Provider operation mismatch")
				}
			}
		}
		if !found {
			t.Fatal("catalog operation not implemented")
		}
	}
}

func TestSearchFetchPreserveContentAndKeepPermissionsInHostPolicy(t *testing.T) {
	fixture := json.RawMessage(`{"passages":[{"doc_id":"doc-1","content":"上传后的文档内容","url":"https://example.com/source"}],"new_field":1234567890123456789}`)
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: 200, Body: fixture}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		op    connector.OperationDescriptor
		input any
		path  string
		body  map[string]any
	}{
		{Search.Descriptor(), SearchInput{Query: "操作步骤", TopK: 3}, "/v1/kb/search", map[string]any{"query": "操作步骤", "top_k": float64(3)}},
		{Fetch.Descriptor(), FetchInput{DocID: "doc-1"}, "/v1/kb/fetch", map[string]any{"doc_id": "doc-1"}},
	} {
		r := request(tc.op, tc.input)
		r.Connection.Config["permission_ids_by_user"] = map[string][]string{"user": {"dept:finance"}, "another": {"dept:private"}}
		got, err := adapter.Call(t.Context(), r)
		if err != nil {
			t.Fatal(err)
		}
		var out Output
		if json.Unmarshal(got.Payload, &out) != nil || out.KBID != "bcri" || !reflect.DeepEqual(out.Result, fixture) {
			t.Fatalf("document content changed: %s", got.Payload)
		}
		upstream := transport.requests[len(transport.requests)-1]
		if upstream.Method != http.MethodPost || upstream.URL != "https://kb.example.com"+tc.path || upstream.SecretHeaders["Authorization"][0] != "Bearer private-key" || upstream.MaxResponseBytes != responseLimit {
			t.Fatal("protocol mismatch")
		}
		public, _ := json.Marshal(upstream)
		if strings.Contains(string(public), "private-key") || upstream.Headers["Authorization"] != nil {
			t.Fatal("secret escaped transport envelope")
		}
		var body map[string]any
		if json.Unmarshal(upstream.Body, &body) != nil {
			t.Fatal("invalid request JSON")
		}
		tc.body["team_id"], tc.body["kb_id"], tc.body["permission_ids"] = "team", "bcri", []any{"dept:finance"}
		if !reflect.DeepEqual(body, tc.body) {
			t.Fatalf("body %+v", body)
		}
	}
	for _, tc := range []struct {
		op    connector.OperationDescriptor
		input any
		body  map[string]any
	}{
		{Search.Descriptor(), SearchInput{Query: "query", ResultContent: "metadata"}, map[string]any{"team_id": "team", "kb_id": "bcri", "query": "query", "top_k": float64(5), "result_content": "metadata"}},
		{Fetch.Descriptor(), FetchInput{DocID: "doc-1", ResultContent: "metadata"}, map[string]any{"team_id": "team", "kb_id": "bcri", "doc_id": "doc-1", "include_content": false, "live": map[string]any{"enabled": false}}},
	} {
		r := request(tc.op, tc.input)
		if _, err = adapter.Call(t.Context(), r); err != nil {
			t.Fatal(err)
		}
		var body map[string]any
		if err = json.Unmarshal(transport.requests[len(transport.requests)-1].Body, &body); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(body, tc.body) {
			t.Fatalf("team-visible %s metadata request incorrect: %+v", tc.op.Key, body)
		}
	}
}

func TestRejectScopePermissionInjectionAndInvalidConfiguration(t *testing.T) {
	transport := &recordingTransport{}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*connector.CallRequest){
		func(r *connector.CallRequest) { r.Principal.WorkspaceID = "other" },
		func(r *connector.CallRequest) { r.Principal.IsAuthenticated = false },
		func(r *connector.CallRequest) {
			r.Payload = json.RawMessage(`{"query":"q","permission_ids":["admin"]}`)
		},
		func(r *connector.CallRequest) { r.Payload = json.RawMessage(`{"query":"q","kb_id":"foreign"}`) },
		func(r *connector.CallRequest) { r.Connection.Config["base_url"] = "http://remote.example" },
		func(r *connector.CallRequest) {
			r.Connection.Config["base_url"] = "https://kb.example.com/evil?key=secret"
		},
		func(r *connector.CallRequest) { r.Connection.Config["permission_ids_by_user"] = "malformed" },
		func(r *connector.CallRequest) { r.Secrets = nil },
		func(r *connector.CallRequest) { r.Payload = json.RawMessage(`{"query":"q","top_k":21}`) },
		func(r *connector.CallRequest) {
			r.Payload = json.RawMessage(`{"query":"q","result_content":"unknown"}`)
		},
	} {
		r := request(Search.Descriptor(), SearchInput{Query: "q"})
		mutate(&r)
		if _, err := adapter.Call(t.Context(), r); err == nil {
			t.Fatal("invalid or unauthorized request accepted")
		}
	}
	if len(transport.requests) != 0 {
		t.Fatal("rejected request reached transport")
	}
}

func TestErrorClassificationAndResponseBounds(t *testing.T) {
	for _, tc := range []struct {
		status     int
		body, code string
		retry      bool
	}{
		{403, "private-key", "access_denied", false}, {429, "private-key", "rate_limited", true}, {503, "private-key", "unavailable", true},
		{200, "not JSON private-key", "response_invalid", false}, {200, "null", "response_invalid", false}, {200, `{} {}`, "response_invalid", false},
		{200, `{"error":"private-key"}`, "failed", false}, {200, `{}` + strings.Repeat(" ", responseLimit), "response_invalid", false},
		{200, `{"err_code":1004,"err_msg":"document not found private-key"}`, "not_found", false},
		{200, `{"err_code":1099,"err_msg":"private-key","data":{"hits":[{"content":"must not escape"}]}}`, "failed", false},
		{200, `{"err_code":null}`, "response_invalid", false}, {200, `{"err_code":"0"}`, "response_invalid", false},
		{200, `{"err_code":0.5}`, "response_invalid", false}, {200, `{"err_code":true}`, "response_invalid", false},
		{200, `{"err_code":9223372036854775808}`, "response_invalid", false},
	} {
		transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: tc.status, Body: []byte(tc.body)}}
		adapter, err := New(transport)
		if err != nil {
			t.Fatal(err)
		}
		output, callErr := adapter.Call(t.Context(), request(Search.Descriptor(), SearchInput{Query: "q"}))
		err = callErr
		code, _ := connector.ProviderErrorCodeOf(err)
		class, _ := connector.ErrorClassificationOf(err)
		if code != "knowledge_api."+tc.code || (class == connector.ErrorRetryable) != tc.retry || strings.Contains(err.Error(), "private-key") {
			t.Fatalf("error classification: %v", err)
		}
		if len(output.Payload) != 0 {
			t.Fatal("failed upstream data escaped as a successful payload")
		}
	}
	transport := &recordingTransport{err: errors.New("private-key in transport error")}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Call(t.Context(), request(Search.Descriptor(), SearchInput{Query: "q"}))
	if code, _ := connector.ProviderErrorCodeOf(err); code != "knowledge_api.network" || errors.Unwrap(err) != nil {
		t.Fatal("network failure leaked")
	}
	transport.err = context.DeadlineExceeded
	_, err = adapter.Call(t.Context(), request(Search.Descriptor(), SearchInput{Query: "q"}))
	if code, _ := connector.ProviderErrorCodeOf(err); code != "knowledge_api.timeout" {
		t.Fatal("timeout not classified")
	}
}

func TestSuccessfulBusinessEnvelopePreservesBothOperationResponses(t *testing.T) {
	for _, op := range []connector.OperationDescriptor{Search.Descriptor(), Fetch.Descriptor()} {
		raw := json.RawMessage(`{"err_code":0,"data":{"hits":[],"exact":9007199254740993}}`)
		transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: 200, Body: raw}}
		adapter, err := New(transport)
		if err != nil {
			t.Fatal(err)
		}
		var input any = SearchInput{Query: "empty"}
		if op.Key == "fetch" {
			input = FetchInput{DocID: "doc"}
		}
		result, err := adapter.Call(t.Context(), request(op, input))
		if err != nil {
			t.Fatal(err)
		}
		var output Output
		if json.Unmarshal(result.Payload, &output) != nil || !reflect.DeepEqual(output.Result, raw) {
			t.Fatal("successful business envelope changed or lost precision")
		}
	}
}
