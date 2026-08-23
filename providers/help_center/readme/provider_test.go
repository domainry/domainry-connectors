package readme

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
)

type recordingTransport struct {
	requests []connector.HTTPRequest
	response connector.HTTPResponse
	err      error
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, r connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, r)
	return t.response, t.err
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestContract(t *testing.T) {
	adapter, err := New(&recordingTransport{})
	if err != nil {
		t.Fatal(err)
	}
	if err = contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	got := adapter.Descriptor()
	if got.ConnectorKey != ConnectorKey || got.ProviderKey != ProviderKey || len(got.Operations) != 6 || len(got.SecretFields) != 1 {
		t.Fatalf("descriptor=%+v", got)
	}
}

func TestReadOperationsUseV2PathsAndBoundedPagination(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"slug":"install","data":[]}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		operation connector.OperationDescriptor
		input     any
		path      string
		query     map[string]string
	}{
		{ListArticles.Descriptor(), ListArticlesInput{Cursor: "2", PageSize: 25}, "/v2/branches/stable/categories/guides/Getting%20Started/pages", map[string]string{"page": "2", "per_page": "25"}},
		{GetArticle.Descriptor(), GetArticleInput{ArticleID: "install"}, "/v2/branches/stable/guides/install", nil},
		{SearchArticles.Descriptor(), SearchArticlesInput{Query: "billing", Cursor: "3", PageSize: 20}, "/v2/search", map[string]string{"query": "billing", "section": "guides", "version": "stable", "page": "3", "per_page": "20"}},
		{ListCollections.Descriptor(), ListCollectionsInput{}, "/v2/branches/stable/categories/guides", nil},
	}
	for _, test := range tests {
		_, err = adapter.Call(t.Context(), callRequest(test.operation, mustJSON(test.input)))
		if err != nil {
			t.Fatalf("operation=%s error=%v", test.operation.Key, err)
		}
		request := transport.requests[len(transport.requests)-1]
		parsed, parseErr := url.Parse(request.URL)
		if parseErr != nil || parsed.EscapedPath() != test.path {
			t.Fatalf("operation=%s URL=%s path=%s", test.operation.Key, request.URL, parsed.EscapedPath())
		}
		for key, value := range test.query {
			if parsed.Query().Get(key) != value {
				t.Fatalf("operation=%s query=%v", test.operation.Key, parsed.Query())
			}
		}
		if request.SecretHeaders["Authorization"][0] != "Bearer key" || request.Headers["Authorization"] != nil || strings.Contains(request.URL, "key") {
			t.Fatalf("secret escaped envelope: %+v", request)
		}
	}
	probe, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: connection(), Secrets: map[string]string{"api_key": "key"}})
	if err != nil || !probe.Connected {
		t.Fatalf("probe=%+v error=%v", probe, err)
	}
}

func TestCreateBuildsCategoryURIAndV2Content(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusCreated, Body: []byte(`{"uri":"/branches/stable/guides/install"}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	position := float64(2)
	input := CreateArticleInput{CollectionID: "Explicit Category", Input: CreateGuideBody{Title: "Install", Slug: "install", Content: &GuideContent{Body: "# Help", Type: "markdown"}, State: "current", AllowCrawlers: "enabled", Position: &position, Metadata: json.RawMessage(`{"description":"Install"}`)}}
	result, err := adapter.Call(t.Context(), callRequest(CreateArticle.Descriptor(), mustJSON(input)))
	if err != nil || result.ResponseRef != "readme:guide:/branches/stable/guides/install" {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	request := transport.requests[0]
	var body map[string]any
	if err = json.Unmarshal(request.Body, &body); err != nil {
		t.Fatal(err)
	}
	category := body["category"].(map[string]any)
	content := body["content"].(map[string]any)
	if category["uri"] != "/branches/stable/categories/guides/Explicit%20Category" || content["body"] != "# Help" || content["type"] != "markdown" {
		t.Fatalf("body=%v", body)
	}
	for _, invalid := range []CreateArticleInput{{CollectionID: "", Input: input.Input}, {CollectionID: "Guides", Input: CreateGuideBody{}}, {CollectionID: "Guides", Input: CreateGuideBody{Title: "t", Content: &GuideContent{Type: "binary"}}}, {CollectionID: "Guides", Input: CreateGuideBody{Title: "t", State: "draft"}}} {
		if _, err = adapter.Call(t.Context(), callRequest(CreateArticle.Descriptor(), mustJSON(invalid))); err == nil {
			t.Fatalf("invalid create accepted: %+v", invalid)
		}
	}
}

func TestUpdateStrictFieldsAndWriteClassification(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"slug":"install"}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	input := UpdateArticleInput{ArticleID: "install", Input: UpdateGuideBody{Title: "Updated", Category: json.RawMessage(`{"uri":"/branches/stable/categories/guides/Other"}`)}}
	if _, err = adapter.Call(t.Context(), callRequest(UpdateArticle.Descriptor(), mustJSON(input))); err != nil {
		t.Fatal(err)
	}
	request := transport.requests[0]
	if request.Method != http.MethodPatch || !strings.HasSuffix(request.URL, "/branches/stable/guides/install") {
		t.Fatalf("request=%+v", request)
	}
	for _, invalid := range []UpdateArticleInput{{}, {ArticleID: "install"}, {ArticleID: "install", Input: UpdateGuideBody{Category: json.RawMessage(`[]`)}}} {
		if _, err = adapter.Call(t.Context(), callRequest(UpdateArticle.Descriptor(), mustJSON(invalid))); err == nil {
			t.Fatalf("invalid update accepted: %+v", invalid)
		}
	}
	transport.err = errors.New("connection reset")
	_, err = adapter.Call(t.Context(), callRequest(UpdateArticle.Descriptor(), mustJSON(input)))
	if class, ok := connector.ErrorClassificationOf(err); !ok || class != connector.ErrorUncertain {
		t.Fatalf("network=%v class=%q", err, class)
	}
	transport.err = nil
	transport.response = connector.HTTPResponse{StatusCode: http.StatusBadGateway, Body: []byte(`{"error":"UPSTREAM/FAILED"}`)}
	_, err = adapter.Call(t.Context(), callRequest(UpdateArticle.Descriptor(), mustJSON(input)))
	if class, ok := connector.ErrorClassificationOf(err); !ok || class != connector.ErrorUncertain || !strings.Contains(err.Error(), "provider_upstream_failed") {
		t.Fatalf("5xx=%v class=%q", err, class)
	}
	transport.response = connector.HTTPResponse{StatusCode: http.StatusTooManyRequests, Body: []byte(`{"error":"RATE_LIMIT"}`)}
	_, err = adapter.Call(t.Context(), callRequest(UpdateArticle.Descriptor(), mustJSON(input)))
	if class, ok := connector.ErrorClassificationOf(err); !ok || class != connector.ErrorRetryable {
		t.Fatalf("429=%v class=%q", err, class)
	}
}

func TestConfigAndInputEdges(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	validator := adapter.(connector.ConfigValidator)
	for _, invalid := range []connector.Connection{{Config: map[string]any{"base_url": "http://remote.example", "branch": "stable", "collection_id": "Guides"}}, {Config: map[string]any{"base_url": "https://user@example.com", "branch": "stable", "collection_id": "Guides"}}, {Config: map[string]any{"base_url": "https://api.readme.com/v2", "collection_id": "Guides"}}, {Config: map[string]any{"base_url": "https://api.readme.com/v2", "branch": "stable"}}} {
		if err = validator.ValidateConfig(invalid); err == nil {
			t.Fatalf("invalid config accepted: %+v", invalid)
		}
	}
	for _, test := range []struct {
		operation connector.OperationDescriptor
		input     any
	}{{ListArticles.Descriptor(), ListArticlesInput{UpdatedSince: "2026-01-01"}}, {ListArticles.Descriptor(), ListArticlesInput{PageSize: 101}}, {SearchArticles.Descriptor(), SearchArticlesInput{}}, {SearchArticles.Descriptor(), SearchArticlesInput{Query: "q", Cursor: "21", PageSize: 50}}, {ListCollections.Descriptor(), ListCollectionsInput{Cursor: "2"}}, {GetArticle.Descriptor(), GetArticleInput{}}} {
		if _, callErr := adapter.Call(t.Context(), callRequest(test.operation, mustJSON(test.input))); callErr == nil {
			t.Fatalf("invalid input accepted for %s", test.operation.Key)
		}
	}
	if _, err = adapter.Call(t.Context(), callRequest(ListArticles.Descriptor(), []byte(`{"unknown":true}`))); err == nil {
		t.Fatal("unknown input accepted")
	}
}

func connection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080/v2", "branch": "stable", "collection_id": "Getting Started"}}
}
func callRequest(operation connector.OperationDescriptor, payload []byte) connector.CallRequest {
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: operation.Key, ContractSHA256: operation.ContractSHA256, Mode: connector.ModeCall, Connection: connection(), Secrets: map[string]string{"api_key": "key"}, Payload: payload}
}
func mustJSON(value any) []byte {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return raw
}
