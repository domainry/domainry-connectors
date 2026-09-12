package llmproxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
	"github.com/domainry/domainry-connector-sdk/web"
)

type fakeTransport struct {
	request  connector.HTTPRequest
	response connector.HTTPResponse
	err      error
	calls    int
	deadline time.Time
}

func (f *fakeTransport) RoundTripHTTP(ctx context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	f.calls++
	f.request = request
	f.deadline, _ = ctx.Deadline()
	return f.response, f.err
}
func (*fakeTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func adapter(t *testing.T, transport connector.Transport) connector.Adapter {
	t.Helper()
	a, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func callRequest(t *testing.T, operation string, input any) connector.CallRequest {
	t.Helper()
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: operation,
		ContractSHA256: web.OperationSHA256(operation), Mode: connector.ModeCall, Payload: raw,
		Connection: connector.Connection{Config: map[string]any{"base_url": "https://proxy.example.test", "allowed_source_hosts": []string{"example.com", "docs.example.com"}}},
		Secrets:    map[string]string{"api_token": "private-passport-fixture"},
	}
}
func response(v any) connector.HTTPResponse {
	b, _ := json.Marshal(v)
	return connector.HTTPResponse{StatusCode: 200, Body: b}
}
func searchJSON() string {
	return `{"search_id":"search-2026","results":[{"url":"https://EXAMPLE.com/news#budget","title":"今日预算","excerpts":["2026-09-11 新消息"]}]}`
}
func fetchJSON() string {
	return `{"code":0,"msg":"success","data":{"url":"https://docs.example.com/news","title":"今日预算","description":"来源说明","content":"2026-09-11 正文\n未确认事项","warning":"upstream warning","usage":{"tokens":42}}}`
}
func assertError(t *testing.T, err error, class connector.ErrorClassification, code string) {
	t.Helper()
	got, ok := connector.ErrorClassificationOf(err)
	c, _ := connector.ProviderErrorCodeOf(err)
	if !ok || got != class || c != "llm_proxy_web."+code {
		t.Fatalf("error=%v class=%s code=%s", err, got, c)
	}
	for e := err; e != nil; e = errors.Unwrap(e) {
		if strings.Contains(e.Error(), "private-passport") || strings.Contains(e.Error(), "upstream-private") {
			t.Fatal("private error retained")
		}
	}
}

func TestAdapterIdentityAndNoImplicitProbe(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("nil transport")
	}
	f := &fakeTransport{}
	a := adapter(t, f)
	if err := contracttest.ValidateAdapter(a); err != nil {
		t.Fatal(err)
	}
	d := a.Descriptor()
	if len(d.Operations) != 2 || d.ConnectorKey != "web" {
		t.Fatalf("descriptor %+v", d)
	}
	for _, op := range d.Operations {
		if op.ContractSHA256 != web.OperationSHA256(op.Key) || op.Reliability.Effect != connector.EffectRead || op.Reliability.Idempotency.Strategy != connector.IdempotencyNatural {
			t.Fatalf("operation %+v", op)
		}
	}
	if _, ok := a.(connector.ConnectionTester); ok {
		t.Fatal("no free service probe exists")
	}
	scopes := a.(connector.OAuthOperationScopeProvider)
	for _, key := range []string{Search.Key, Fetch.Key} {
		rules, known := scopes.OAuthOperationScopes(key)
		if !known || len(rules) != 1 || len(rules[0]) != 0 {
			t.Fatal("wrong service scope")
		}
	}
	if _, known := scopes.OAuthOperationScopes("mail_read"); known {
		t.Fatal("unknown operation authorized")
	}
	if f.calls != 0 {
		t.Fatal("construction performed I/O")
	}
}

func TestSearchTranslationAndSourceNormalization(t *testing.T) {
	f := &fakeTransport{response: connector.HTTPResponse{StatusCode: 200, Body: []byte(searchJSON())}}
	r := callRequest(t, Search.Key, web.SearchRequest{Query: "今日预算"})
	r.Connection.Config["processor"] = "pro"
	got, err := adapter(t, f).Call(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	var out web.SearchResult
	if json.Unmarshal(got.Payload, &out) != nil || out.Validate(web.SearchRequest{Query: "今日预算"}) != nil {
		t.Fatalf("bad result %s", got.Payload)
	}
	if out.SearchID != "search-2026" || out.Scope != "ranked_results" || out.Truncated || out.Items[0].URL != "https://example.com/news" || out.Items[0].Excerpts[0] != "2026-09-11 新消息" {
		t.Fatalf("output %+v", out)
	}
	want := `{"objective":"今日预算","processor":"pro","max_results":5,"max_chars_per_result":1500,"source_policy":{"sources":["docs.example.com","example.com"]}}`
	if string(f.request.Body) != want || f.request.URL != "https://proxy.example.test/tool/web_search" || f.request.Method != "POST" || f.request.MaxResponseBytes != 4<<20 {
		t.Fatalf("request %s %s", f.request.URL, f.request.Body)
	}
	if len(f.request.Headers) != 2 || f.request.SecretHeaders["Authorization"][0] != "Bearer private-passport-fixture" || f.deadline.IsZero() || time.Until(f.deadline) > 40*time.Second {
		t.Fatal("private injection/deadline missing")
	}
	serialized, _ := json.Marshal(f.request)
	if strings.Contains(string(serialized), "private-passport") || strings.Contains(string(got.Payload), "private-passport") {
		t.Fatal("credential serialized")
	}
	if got.ResponseRef != "http:200" || f.calls != 1 {
		t.Fatal("response or retry changed")
	}
}

func TestFetchEnvelopeAndUnknownCompleteness(t *testing.T) {
	f := &fakeTransport{response: connector.HTTPResponse{StatusCode: 200, Body: []byte(fetchJSON())}}
	request := web.FetchRequest{URL: "https://EXAMPLE.com:443/news#part"}
	got, err := adapter(t, f).Call(t.Context(), callRequest(t, Fetch.Key, request))
	var out web.Page
	if err != nil || json.Unmarshal(got.Payload, &out) != nil || out.Validate(request) != nil {
		t.Fatalf("result %s %v", got.Payload, err)
	}
	if out.RequestedURL != "https://example.com/news" || out.URL != "https://docs.example.com/news" || out.SourceCompleteness != "unknown" || out.Truncated || out.Content != "2026-09-11 正文\n未确认事项" || len(out.Warnings) != 1 {
		t.Fatalf("output %+v", out)
	}
	if f.request.URL != "https://proxy.example.test/tool/web_fetch_jina" || string(f.request.Body) != `{"url":"https://example.com/news"}` {
		t.Fatal("wrong fetch protocol")
	}
	// Empty extracted content is an unknown page, not a fabricated successful
	// full document. Missing content is a malformed response.
	f.response.Body = []byte(`{"code":0,"data":{"url":"https://example.com/","content":""}}`)
	got, err = adapter(t, f).Call(t.Context(), callRequest(t, Fetch.Key, web.FetchRequest{URL: "https://example.com/"}))
	if err != nil || json.Unmarshal(got.Payload, &out) != nil || out.Content != "" || out.SourceCompleteness != "unknown" {
		t.Fatal("empty content semantics changed")
	}
}

func TestSourcePolicyAndUTF8Bounds(t *testing.T) {
	items := []map[string]any{
		{"url": "https://evil.com/", "excerpts": []string{"do not expose"}},
		{"url": "http://127.0.0.1/private", "excerpts": []string{"do not expose"}},
		{"url": "https://example.com/a", "title": strings.Repeat("标", 500), "excerpts": []string{strings.Repeat("文", 100), "more"}},
		{"url": "https://example.com/a#duplicate"},
		{"url": "https://example.com/b"},
	}
	f := &fakeTransport{response: response(map[string]any{"search_id": "ranked", "results": items})}
	input := web.SearchRequest{Query: "bounded", Limit: 1, MaxExcerptBytes: 100}
	got, err := adapter(t, f).Call(t.Context(), callRequest(t, Search.Key, input))
	var out web.SearchResult
	if err != nil || json.Unmarshal(got.Payload, &out) != nil || out.Validate(input) != nil {
		t.Fatal(err, string(got.Payload))
	}
	if !out.Truncated || len(out.Items) != 1 || !out.Items[0].Truncated || len(out.Items[0].Excerpts[0]) != 99 || len(out.Items[0].Title) != 1023 || strings.Contains(string(got.Payload), "do not expose") {
		t.Fatalf("bounds %+v", out)
	}
	f.response = response(map[string]any{"code": 0, "data": map[string]any{"url": "https://example.com/", "content": strings.Repeat("文", 1000), "title": "a\r\nb\u0000", "description": strings.Repeat("说", 1500), "warning": strings.Repeat("警", 200)}})
	pageInput := web.FetchRequest{URL: "https://example.com/", MaxContentBytes: 1024}
	got, err = adapter(t, f).Call(t.Context(), callRequest(t, Fetch.Key, pageInput))
	var page web.Page
	if err != nil || json.Unmarshal(got.Payload, &page) != nil || page.Validate(pageInput) != nil {
		t.Fatal(err, string(got.Payload))
	}
	if !page.Truncated || len(page.Content) != 1023 || !utf8.ValidString(page.Content) || page.Title != "ab" || len(page.Description) != 4095 || len(page.Warnings[0]) != 510 || page.SourceCompleteness != "unknown" {
		t.Fatalf("page bounds %+v", page)
	}
	for _, source := range []string{"https://evil.com/", "https://example.com.evil.com/", "https://sub.example.com/", "http://169.254.169.254/", "https://name:secret@example.com/"} {
		before := f.calls
		_, err = adapter(t, f).Call(t.Context(), callRequest(t, Fetch.Key, web.FetchRequest{URL: source}))
		if err == nil || f.calls != before {
			t.Fatal("forbidden request dispatched", source)
		}
	}
	f.response.Body = []byte(`{"code":0,"data":{"url":"https://evil.com/","content":"do not expose"}}`)
	got, err = adapter(t, f).Call(t.Context(), callRequest(t, Fetch.Key, pageInput))
	assertError(t, err, connector.ErrorPermanent, "source_denied")
	if len(got.Payload) != 0 {
		t.Fatal("unauthorized returned content leaked")
	}
}

func TestMalformedResponsesErrorsAndNoRetries(t *testing.T) {
	for _, tc := range []struct{ op, body string }{
		{Search.Key, `{}`}, {Search.Key, `{"search_id":"x","results":null}`}, {Search.Key, `{"search_id":"x\n","results":[]}`},
		{Fetch.Key, `{"data":{"url":"https://example.com/","content":"x"}}`}, {Fetch.Key, `{"code":0,"data":{"url":"https://example.com/"}}`},
		{Fetch.Key, `{"code":500,"msg":"upstream-private"}`}, {Fetch.Key, searchJSON()}, {Search.Key, fetchJSON()},
	} {
		f := &fakeTransport{response: connector.HTTPResponse{StatusCode: 200, Body: []byte(tc.body)}}
		var input any = web.SearchRequest{Query: "test"}
		if tc.op == Fetch.Key {
			input = web.FetchRequest{URL: "https://example.com/"}
		}
		got, err := adapter(t, f).Call(t.Context(), callRequest(t, tc.op, input))
		assertError(t, err, connector.ErrorUncertain, "response_invalid")
		if len(got.Payload) != 0 || f.calls != 1 {
			t.Fatal("bad response returned or retried")
		}
	}
	for _, tc := range []struct {
		status int
		class  connector.ErrorClassification
		code   string
	}{
		{401, connector.ErrorPermanent, "unauthorized"}, {403, connector.ErrorPermanent, "forbidden"}, {400, connector.ErrorPermanent, "request_rejected"},
		{413, connector.ErrorPermanent, "request_rejected"}, {404, connector.ErrorPermanent, "endpoint_unavailable"}, {429, connector.ErrorPermanent, "rate_limited"},
		{302, connector.ErrorUncertain, "upstream_error"}, {500, connector.ErrorUncertain, "upstream_error"},
	} {
		f := &fakeTransport{response: connector.HTTPResponse{StatusCode: tc.status, Body: []byte("upstream-private")}}
		_, err := adapter(t, f).Call(t.Context(), callRequest(t, Search.Key, web.SearchRequest{Query: "test"}))
		assertError(t, err, tc.class, tc.code)
		if f.calls != 1 {
			t.Fatal("retried")
		}
	}
	f := &fakeTransport{err: errors.New("private-passport-fixture upstream-private")}
	_, err := adapter(t, f).Call(t.Context(), callRequest(t, Search.Key, web.SearchRequest{Query: "test"}))
	assertError(t, err, connector.ErrorUncertain, "network_error")
	f.err = nil
	f.response = connector.HTTPResponse{StatusCode: 200, Body: []byte(strings.Repeat("x", maxResponseBytes+1))}
	_, err = adapter(t, f).Call(t.Context(), callRequest(t, Search.Key, web.SearchRequest{Query: "test"}))
	assertError(t, err, connector.ErrorUncertain, "response_invalid")
	f.response.Body = []byte{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'}
	_, err = adapter(t, f).Call(t.Context(), callRequest(t, Search.Key, web.SearchRequest{Query: "test"}))
	assertError(t, err, connector.ErrorUncertain, "response_invalid")
}

func TestInvalidConfigurationCredentialAndInputDoNotDispatch(t *testing.T) {
	for key, values := range map[string][]any{
		"base_url":             {"", "http://proxy.example.com", "https://u:p@proxy.example.com", "https://proxy.example.com/path", "https://proxy.example.com?", "https://proxy.example.com#", "https://proxy.example.com:", "https://proxy.example.com:65536", " https://proxy.example.com"},
		"allowed_source_hosts": {nil, []string{}, []string{"*"}, []string{"*.example.com"}, []string{"example.com/"}, []string{"example.com:443"}, []string{"localhost"}, []string{"127.0.0.1"}, []string{"8.8.8.8"}, []string{"example.com", "EXAMPLE.com"}, `["example.com"]`, []any{123}, []string{"x.internal"}},
		"processor":            {"", "huge", 1}, "timeout_seconds": {0, 61, 1.5, "1s"},
	} {
		for _, value := range values {
			r := callRequest(t, Search.Key, web.SearchRequest{Query: "test"})
			r.Connection.Config[key] = value
			f := &fakeTransport{}
			a := adapter(t, f)
			if a.(connector.ConfigValidator).ValidateConfig(r.Connection) == nil {
				t.Fatalf("invalid config %s=%v accepted", key, value)
			}
			if _, err := a.Call(t.Context(), r); err == nil || f.calls != 0 {
				t.Fatalf("invalid config %s dispatched", key)
			}
		}
	}
	for _, token := range []string{"", " secret", "secret\n", "secret\x00", "secret\u00a0"} {
		r := callRequest(t, Search.Key, web.SearchRequest{Query: "test"})
		r.Secrets["api_token"] = token
		f := &fakeTransport{}
		_, err := adapter(t, f).Call(t.Context(), r)
		assertError(t, err, connector.ErrorPermanent, "credential_invalid")
		if f.calls != 0 {
			t.Fatal("bad credential dispatched")
		}
	}
	for _, payload := range []string{`{"query":""}`, `{"query":"test","limit":11}`, `{"query":"test","base_url":"https://evil.com"}`, `{"query":"test","api_token":"model"}`} {
		r := callRequest(t, Search.Key, web.SearchRequest{})
		r.Payload = []byte(payload)
		f := &fakeTransport{}
		if _, err := adapter(t, f).Call(t.Context(), r); err == nil || f.calls != 0 {
			t.Fatal("invalid/model-controlled input dispatched")
		}
	}
	f := &fakeTransport{}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := adapter(t, f).Call(ctx, callRequest(t, Search.Key, web.SearchRequest{Query: "test"}))
	assertError(t, err, connector.ErrorPermanent, "request_cancelled")
	if f.calls != 0 {
		t.Fatal("cancelled request dispatched")
	}
}

// Only this test host owns an HTTP client. The production Provider still sees
// only the public Transport capability and fixed llm-proxy protocol.
type localHTTPTransport struct{ server *httptest.Server }

func (h localHTTPTransport) RoundTripHTTP(ctx context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	if request.Method != http.MethodPost || (request.URL != h.server.URL+searchPath && request.URL != h.server.URL+fetchPath) {
		return connector.HTTPResponse{}, errors.New("host denied destination")
	}
	r, err := http.NewRequestWithContext(ctx, request.Method, request.URL, strings.NewReader(string(request.Body)))
	if err != nil {
		return connector.HTTPResponse{}, err
	}
	for k, vs := range request.Headers {
		for _, v := range vs {
			r.Header.Add(k, v)
		}
	}
	for k, vs := range request.SecretHeaders {
		if r.Header.Get(k) != "" {
			return connector.HTTPResponse{}, errors.New("header collision")
		}
		for _, v := range vs {
			r.Header.Add(k, v)
		}
	}
	r.GetBody = nil
	resp, err := h.server.Client().Do(r)
	if err != nil {
		return connector.HTTPResponse{}, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, request.MaxResponseBytes+1))
	if err != nil || int64(len(b)) > request.MaxResponseBytes {
		return connector.HTTPResponse{}, errors.New("host response limit")
	}
	return connector.HTTPResponse{StatusCode: resp.StatusCode, Body: b}, nil
}
func (localHTTPTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("SQL unavailable")
}

func TestActualHTTPProtocolAndCancellation(t *testing.T) {
	var calls atomic.Int32
	var block atomic.Bool
	started, stopped := make(chan struct{}), make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != "POST" || r.Header.Get("Authorization") != "Bearer private-passport-fixture" || r.Header.Get("Content-Type") != "application/json" {
			t.Error("wrong HTTP protocol")
		}
		b, _ := io.ReadAll(r.Body)
		if strings.Contains(string(b), "private-passport") {
			t.Error("secret in body")
		}
		if block.Load() {
			close(started)
			<-r.Context().Done()
			close(stopped)
			return
		}
		switch r.URL.Path {
		case searchPath:
			fmt.Fprint(w, searchJSON())
		case fetchPath:
			fmt.Fprint(w, fetchJSON())
		default:
			t.Error("wrong route")
		}
	}))
	defer s.Close()
	s.Client().CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	a := adapter(t, localHTTPTransport{s})
	makeRequest := func(op string) connector.CallRequest {
		var input any = web.SearchRequest{Query: "current"}
		if op == Fetch.Key {
			input = web.FetchRequest{URL: "https://example.com/news"}
		}
		r := callRequest(t, op, input)
		r.Connection.Config["base_url"] = s.URL
		return r
	}
	for _, op := range []string{Search.Key, Fetch.Key} {
		got, err := a.Call(t.Context(), makeRequest(op))
		if err != nil || len(got.Payload) == 0 {
			t.Fatal(err)
		}
	}
	block.Store(true)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := a.Call(ctx, makeRequest(Fetch.Key)); done <- err }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	assertError(t, <-done, connector.ErrorUncertain, "network_error")
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("HTTP cancellation did not propagate")
	}
	if calls.Load() != 3 {
		t.Fatal("unexpected HTTP retry", calls.Load())
	}
}
