package llmproxy

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
	"github.com/domainry/domainry-connectors/catalog"
	"github.com/domainry/domainry-connectors/internal/releaseverify"
	"github.com/domainry/domainry-connectors/module"
)

const receiptResponse = `{"code":0,"msg":"success","data":{"text":"Coffee receipt","entities":[
	{"type":"total_amount","mention_text":"$12.34","confidence":0.98,"normalized_value":{"moneyValue":{"currencyCode":"USD","units":"12","nanos":340000000}},"page_refs":[{"page":0,"layout_type":"VISUAL_ELEMENT"}]},
	{"type":"line_item","mention_text":"Coffee","confidence":0.9,"properties":[{"type":"line_item/amount","mention_text":"12.34","confidence":0.8,"normalized_value":{"integerValue":9007199254740993,"dateValue":{"year":2026,"month":9,"day":2}}}]}
]}}`

type recordingTransport struct {
	request  connector.HTTPRequest
	response connector.HTTPResponse
	err      error
	calls    int
	deadline time.Time
}

func (r *recordingTransport) RoundTripHTTP(ctx context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	r.request = request
	r.calls++
	r.deadline, _ = ctx.Deadline()
	return r.response, r.err
}

func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func requestFor(t *testing.T, input ParseExpenseInput) connector.CallRequest {
	t.Helper()
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	return connector.CallRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: ParseExpense.Key,
		ContractSHA256: ParseExpense.ContractSHA256, Mode: connector.ModeCall,
		Connection: connector.Connection{Config: map[string]any{"base_url": "https://proxy.example.test"}},
		Secrets:    map[string]string{"api_token": "sk-runtime-secret"}, Payload: raw,
	}
}

func validInput() ParseExpenseInput {
	return ParseExpenseInput{Document: base64.StdEncoding.EncodeToString([]byte("%PDF-1.7\nreceipt")), MIMEType: "application/pdf"}
}

func adapterFor(t *testing.T, transport connector.Transport) connector.Adapter {
	t.Helper()
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func assertError(t *testing.T, err error, want connector.ErrorClassification, code string) {
	t.Helper()
	class, ok := connector.ErrorClassificationOf(err)
	gotCode, _ := connector.ProviderErrorCodeOf(err)
	if !ok || class != want || gotCode != "llm_proxy_expense."+code {
		t.Fatalf("error=%v classification=%q code=%q want=%q/%s", err, class, gotCode, want, code)
	}
}

func TestDescriptorCatalogAndSelectedComposition(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("nil transport accepted")
	}
	transport := &recordingTransport{}
	adapter := adapterFor(t, transport)
	if err := contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	descriptor := adapter.Descriptor()
	if len(descriptor.Operations) != 1 || descriptor.Operations[0].Reliability.Effect != connector.EffectWrite || descriptor.Operations[0].Reliability.Idempotency.Strategy != connector.IdempotencyNone {
		t.Fatalf("billable recognition must not advertise replay safety: %+v", descriptor)
	}
	if _, ok := adapter.(connector.ConnectionTester); ok {
		t.Fatal("no non-billable OCR readiness endpoint exists")
	}
	documents, err := catalog.DefinitionDocuments()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, document := range documents {
		if document.Key != ConnectorKey {
			continue
		}
		found = true
		hash, err := catalog.OperationContractSHA256(document.Key, ProviderKey, document.Operations[0])
		if err != nil || hash != ParseExpense.ContractSHA256 {
			t.Fatalf("wire contract mismatch: %s (%v)", hash, err)
		}
		if document.Operations[0].SideEffect != string(ParseExpense.Reliability.Effect) || document.Operations[0].IdempotencySupported {
			t.Fatal("catalog reliability differs from Provider")
		}
		if len(document.Providers) != 1 || document.Providers[0].SecretFields[0].Config["test_requirement"] != string(descriptor.SecretFields[0].TestRequirement) {
			t.Fatal("catalog credential contract differs from Provider")
		}
	}
	if !found {
		t.Fatal("expense_ocr definition is not discoverable")
	}
	registry, err := module.NewFactory(module.Options{Providers: connector.ProviderSet{Providers: []connector.Adapter{adapter}}}).Registry()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := registry.Provider(ConnectorKey, ProviderKey); !ok || !registry.Frozen() {
		t.Fatal("selected Provider is not registered")
	}
	empty, err := module.NewFactory(module.Options{}).Registry()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := empty.Provider(ConnectorKey, ProviderKey); ok {
		t.Fatal("unselected Provider was registered")
	}
	if transport.calls != 0 {
		t.Fatal("construction or catalog discovery performed I/O")
	}
}

func TestRequestTranslationAndLosslessResponse(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: 200, Body: []byte(receiptResponse)}}
	adapter := adapterFor(t, transport)
	input := validInput()
	input.Document = "data:application/pdf;base64," + input.Document
	input.SessionID, input.ConvID, input.ReactID = "session-1", "conv-1", "react-1"
	result, err := adapter.Call(t.Context(), requestFor(t, input))
	if err != nil {
		t.Fatal(err)
	}
	r := transport.request
	if transport.calls != 1 || r.Method != http.MethodPost || r.URL != "https://proxy.example.test/llm/expense/parse" || r.MaxResponseBytes != maxResponseBytes {
		t.Fatalf("incorrect dispatch: %+v", r)
	}
	if r.Headers["Content-Type"][0] != "application/json" || r.SecretHeaders["Authorization"][0] != "Bearer sk-runtime-secret" || len(r.Headers["Authorization"]) != 0 || len(r.Headers["Content-Encoding"]) != 0 {
		t.Fatal("incorrect credential or body transport")
	}
	var body ParseExpenseInput
	if err := json.Unmarshal(r.Body, &body); err != nil {
		t.Fatal(err)
	}
	if body.Document != validInput().Document || body.MIMEType != input.MIMEType || body.SessionID != input.SessionID || body.ConvID != input.ConvID || body.ReactID != input.ReactID {
		t.Fatalf("request=%+v", body)
	}
	serialized, _ := json.Marshal(r)
	if strings.Contains(string(serialized), "sk-runtime-secret") || strings.Contains(string(result.Payload), "sk-runtime-secret") {
		t.Fatal("credential leaked into serialized public data")
	}
	var output ParseExpenseOutput
	if err := json.Unmarshal(result.Payload, &output); err != nil {
		t.Fatal(err)
	}
	if result.ResponseRef != "http:200" || output.Text != "Coffee receipt" || len(output.Entities) != 2 || output.Entities[0].PageRefs[0].Page != 0 || len(output.Entities[1].Properties) != 1 {
		t.Fatalf("output=%+v ref=%q", output, result.ResponseRef)
	}
	if !strings.Contains(string(output.Entities[0].NormalizedValue), `"moneyValue"`) || !strings.Contains(string(output.Entities[1].Properties[0].NormalizedValue), `9007199254740993`) || !strings.Contains(string(output.Entities[1].Properties[0].NormalizedValue), `"dateValue"`) {
		t.Fatal("normalized values lost structure or precision")
	}
	if left := time.Until(transport.deadline); left <= 0 || left > 90*time.Second {
		t.Fatalf("unexpected deadline: %v", left)
	}
}

func TestDocumentFormatsAndBase64Variants(t *testing.T) {
	files := map[string][]byte{
		"application/pdf": []byte("%PDF-1.7"), "image/tiff": {'M', 'M', 0, 42},
		"image/jpeg": {0xff, 0xd8, 0xff}, "image/png": []byte("\x89PNG\r\n\x1a\n"),
		"image/bmp": []byte("BM"), "image/gif": []byte("GIF89a"), "image/webp": []byte("RIFFxxxxWEBP"),
	}
	for mimeType, data := range files {
		for _, value := range []string{base64.StdEncoding.EncodeToString(data), base64.RawStdEncoding.EncodeToString(data), "DATA:" + strings.ToUpper(mimeType) + ";BASE64," + base64.StdEncoding.EncodeToString(data) + "\r\n"} {
			input, err := canonicalInput(ParseExpenseInput{Document: value, MIMEType: strings.ToUpper(mimeType)})
			if err != nil || input.Document != base64.StdEncoding.EncodeToString(data) || input.MIMEType != mimeType {
				t.Fatalf("format=%s error=%v", mimeType, err)
			}
		}
	}
}

func TestInvalidDocumentNeverCallsUpstream(t *testing.T) {
	cases := []struct {
		name  string
		input ParseExpenseInput
		code  string
	}{
		{"empty", ParseExpenseInput{MIMEType: "image/png"}, "document_required"},
		{"bad base64", ParseExpenseInput{MIMEType: "image/png", Document: "not base64!"}, "document_invalid"},
		{"unsupported MIME", ParseExpenseInput{MIMEType: "text/plain", Document: "eA=="}, "mime_type_invalid"},
		{"signature mismatch", ParseExpenseInput{MIMEType: "image/png", Document: "eA=="}, "mime_type_mismatch"},
		{"data URL mismatch", ParseExpenseInput{MIMEType: "image/png", Document: "data:image/jpeg;base64,eA=="}, "document_invalid"},
		{"extra metadata", ParseExpenseInput{MIMEType: "image/png", Document: "data:image/png;charset=utf-8;base64,eA=="}, "document_invalid"},
		{"empty data URL", ParseExpenseInput{MIMEType: "image/png", Document: "data:image/png;base64,"}, "document_invalid"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			transport := &recordingTransport{}
			_, err := adapterFor(t, transport).Call(t.Context(), requestFor(t, test.input))
			assertError(t, err, connector.ErrorPermanent, test.code)
			if transport.calls != 0 {
				t.Fatal("invalid document was sent upstream")
			}
		})
	}
	input := validInput()
	input.SessionID = "receipt\nsecret"
	_, err := canonicalInput(input)
	assertError(t, err, connector.ErrorPermanent, "correlation_invalid")
}

func TestDocumentSizeBoundary(t *testing.T) {
	data := make([]byte, MaxDocumentBytes+1)
	copy(data, []byte("%PDF-"))
	if _, err := canonicalInput(ParseExpenseInput{Document: base64.StdEncoding.EncodeToString(data[:MaxDocumentBytes]), MIMEType: "application/pdf"}); err != nil {
		t.Fatalf("20 MiB rejected: %v", err)
	}
	_, err := canonicalInput(ParseExpenseInput{Document: base64.StdEncoding.EncodeToString(data), MIMEType: "application/pdf"})
	assertError(t, err, connector.ErrorPermanent, "document_too_large")
}

func TestConfigCredentialsAndInvocationFailBeforeDispatch(t *testing.T) {
	for _, endpoint := range []string{"", "http://proxy.example.test", "https://user:password@proxy.example.test", "https://proxy.example.test/llm", "https://proxy.example.test/?api_key=secret", "https://proxy.example.test/#", "file:///tmp/proxy"} {
		transport := &recordingTransport{}
		request := requestFor(t, validInput())
		request.Connection.Config["base_url"] = endpoint
		_, err := adapterFor(t, transport).Call(t.Context(), request)
		assertError(t, err, connector.ErrorPermanent, "endpoint_invalid")
		if transport.calls != 0 {
			t.Fatal("invalid configuration reached transport")
		}
	}
	for _, token := range []string{"", "Bearer secret", "secret\r\nInjected: true"} {
		transport := &recordingTransport{}
		request := requestFor(t, validInput())
		request.Secrets["api_token"] = token
		_, err := adapterFor(t, transport).Call(t.Context(), request)
		assertError(t, err, connector.ErrorPermanent, "api_token_invalid")
		if transport.calls != 0 {
			t.Fatal("invalid credential reached transport")
		}
	}
	for _, timeout := range []any{0, -1, 301, 1.5, "oops", nil} {
		transport := &recordingTransport{}
		request := requestFor(t, validInput())
		request.Connection.Config["timeout_seconds"] = timeout
		_, err := adapterFor(t, transport).Call(t.Context(), request)
		assertError(t, err, connector.ErrorPermanent, "timeout_invalid")
		if transport.calls != 0 {
			t.Fatal("invalid timeout reached transport")
		}
	}
	for _, mutation := range []func(*connector.CallRequest){
		func(r *connector.CallRequest) { r.ContractSHA256 = strings.Repeat("a", 64) },
		func(r *connector.CallRequest) { r.Mode = connector.ModeEnqueue },
		func(r *connector.CallRequest) {
			r.Payload = []byte(`{"document":"eA==","mime_type":"image/png","processor_id":"client-override"}`)
		},
	} {
		transport := &recordingTransport{}
		request := requestFor(t, validInput())
		mutation(&request)
		if _, err := adapterFor(t, transport).Call(t.Context(), request); err == nil || transport.calls != 0 {
			t.Fatal("invalid contract invocation was accepted")
		}
	}
}

func TestFailureClassificationAndNoSecretOrDocumentInErrorChain(t *testing.T) {
	cases := []struct {
		status int
		want   connector.ErrorClassification
		code   string
	}{
		{400, connector.ErrorPermanent, "document_rejected"}, {413, connector.ErrorPermanent, "document_rejected"}, {415, connector.ErrorPermanent, "document_rejected"},
		{401, connector.ErrorPermanent, "unauthorized"}, {403, connector.ErrorPermanent, "forbidden"}, {404, connector.ErrorPermanent, "endpoint_unavailable"},
		{429, connector.ErrorRetryable, "rate_limited"}, {503, connector.ErrorPermanent, "service_unavailable"},
		{500, connector.ErrorUncertain, "upstream_error"}, {502, connector.ErrorUncertain, "upstream_error"}, {504, connector.ErrorUncertain, "upstream_error"},
	}
	for _, test := range cases {
		t.Run(fmt.Sprint(test.status), func(t *testing.T) {
			transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: test.status, Body: []byte("sensitive-receipt sk-runtime-secret")}}
			result, err := adapterFor(t, transport).Call(t.Context(), requestFor(t, validInput()))
			assertError(t, err, test.want, test.code)
			if result.ResponseRef != fmt.Sprintf("http:%d", test.status) || len(result.Payload) != 0 || transport.calls != 1 {
				t.Fatal("failure metadata or dispatch count is wrong")
			}
			for cause := err; cause != nil; cause = errors.Unwrap(cause) {
				if strings.Contains(cause.Error(), "sensitive-receipt") || strings.Contains(cause.Error(), "sk-runtime-secret") {
					t.Fatal("sensitive error detail leaked")
				}
			}
		})
	}
	transport := &recordingTransport{err: errors.New("sensitive-receipt sk-runtime-secret")}
	_, err := adapterFor(t, transport).Call(t.Context(), requestFor(t, validInput()))
	assertError(t, err, connector.ErrorUncertain, "network_error")
	if transport.calls != 1 {
		t.Fatal("provider retried the recognition request")
	}
	for cause := err; cause != nil; cause = errors.Unwrap(cause) {
		if strings.Contains(cause.Error(), "sensitive-receipt") || strings.Contains(cause.Error(), "sk-runtime-secret") {
			t.Fatal("transport cause leaked")
		}
	}
}

func TestStrictSuccessEnvelopeAndBusinessErrors(t *testing.T) {
	for _, body := range []string{`null`, `{}`, `{"data":{"text":"","entities":[]}}`, `{"code":0}`, `{"code":0,"data":null}`, `{"code":0,"data":{}}`, `{"code":0,"data":{"text":null,"entities":[]}}`, `{"code":0,"data":{"text":"","entities":null}}`, `{"code":0,"data":{"text":"","entities":[{"type":"amount","normalized_value":"lost money"}]}}`, `{"code":0,"data":{"text":"","entities":[{"type":"amount","confidence":2}]}}`, `{"code":0,"data":{"text":"","entities":[{"type":"amount","page_refs":[{"page":-1}]}]}}`, `{"code":0} trailing`} {
		transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: 200, Body: []byte(body)}}
		_, err := adapterFor(t, transport).Call(t.Context(), requestFor(t, validInput()))
		assertError(t, err, connector.ErrorUncertain, "response_invalid")
	}
	for _, code := range []int{401, 429, 503} {
		transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: 200, Body: []byte(fmt.Sprintf(`{"code":%d,"msg":"private error"}`, code))}}
		_, err := adapterFor(t, transport).Call(t.Context(), requestFor(t, validInput()))
		want, _ := connector.ErrorClassificationOf(statusError(code))
		wantCode, _ := connector.ProviderErrorCodeOf(statusError(code))
		assertError(t, err, want, strings.TrimPrefix(wantCode, "llm_proxy_expense."))
	}
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"code":0,"data":{"text":"","entities":[]},"future_field":true}`)}}
	result, err := adapterFor(t, transport).Call(t.Context(), requestFor(t, validInput()))
	if err != nil || string(result.Payload) != `{"text":"","entities":[]}` {
		t.Fatalf("empty valid recognition rejected: %s %v", result.Payload, err)
	}
	transport.response.Body = make([]byte, maxResponseBytes+1)
	_, err = adapterFor(t, transport).Call(t.Context(), requestFor(t, validInput()))
	assertError(t, err, connector.ErrorUncertain, "response_invalid")
}

func TestContextDeadlineAndCancellation(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: 200, Body: []byte(receiptResponse)}}
	adapter := adapterFor(t, transport)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	if _, err := adapter.Call(ctx, requestFor(t, validInput())); err != nil {
		t.Fatal(err)
	}
	if !transport.deadline.Equal(deadline) {
		t.Fatal("Runtime deadline was extended")
	}
	cancel()
	_, err := adapter.Call(ctx, requestFor(t, validInput()))
	assertError(t, err, connector.ErrorPermanent, "request_cancelled")
	if transport.calls != 1 {
		t.Fatal("cancelled request was dispatched")
	}
}

func TestIsolatedHTTPProtocol(t *testing.T) {
	requests := make(chan ParseExpenseInput, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != parsePath || r.Header.Get("Authorization") != "Bearer sk-runtime-secret" || r.Header.Get("Content-Type") != "application/json" || r.Header.Get("Content-Encoding") != "" {
			t.Error("incorrect wire request")
			w.WriteHeader(400)
			return
		}
		var input ParseExpenseInput
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		requests <- input
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(receiptResponse))
	}))
	defer server.Close()
	transport := releaseverify.HTTPTransport{Client: server.Client()}
	request := requestFor(t, validInput())
	request.Connection.Config["base_url"] = server.URL
	result, err := adapterFor(t, transport).Call(t.Context(), request)
	if err != nil || !strings.Contains(string(result.Payload), "Coffee receipt") {
		t.Fatalf("isolated roundtrip failed: %v", err)
	}
	select {
	case input := <-requests:
		if input.Document != validInput().Document {
			t.Fatal("wire document differs")
		}
	default:
		t.Fatal("no HTTP request received")
	}
}
