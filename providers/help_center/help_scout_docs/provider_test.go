package helpscoutdocs

import (
	"context"
	"encoding/base64"
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

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, request)
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
	if err := contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	if got := adapter.Descriptor(); got.ConnectorKey != ConnectorKey || got.ProviderKey != ProviderKey || len(got.Operations) != 6 || len(got.SecretFields) != 1 {
		t.Fatalf("descriptor=%+v", got)
	}
}

func TestReadOperationsAndRuntimeOnlyBasicAuth(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"article":{"id":"article-1"},"items":[]}`)}}
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
		{ListArticles.Descriptor(), ListArticlesInput{Cursor: "2", PageSize: 25}, "/v1/collections/collection-1/articles", map[string]string{"page": "2", "pageSize": "25"}},
		{GetArticle.Descriptor(), GetArticleInput{ArticleID: "article/1"}, "/v1/articles/article%2F1", nil},
		{SearchArticles.Descriptor(), SearchArticlesInput{Query: "billing", Cursor: "3"}, "/v1/search/articles", map[string]string{"query": "billing", "page": "3", "status": "published", "collectionId": "collection-1"}},
		{ListCollections.Descriptor(), ListCollectionsInput{}, "/v1/collections", map[string]string{"pageSize": "50"}},
	}
	for _, test := range tests {
		_, err := adapter.Call(t.Context(), callRequest(test.operation, mustJSON(test.input)))
		if err != nil {
			t.Fatalf("operation=%s error=%v", test.operation.Key, err)
		}
		request := transport.requests[len(transport.requests)-1]
		parsed, parseErr := url.Parse(request.URL)
		if parseErr != nil || parsed.EscapedPath() != test.path {
			t.Fatalf("operation=%s URL=%q path=%q error=%v", test.operation.Key, request.URL, parsed.EscapedPath(), parseErr)
		}
		for key, value := range test.query {
			if parsed.Query().Get(key) != value {
				t.Fatalf("operation=%s query=%v", test.operation.Key, parsed.Query())
			}
		}
		expected := "Basic " + base64.StdEncoding.EncodeToString([]byte("docs-key:X"))
		if request.SecretHeaders["Authorization"][0] != expected || request.Headers["Authorization"] != nil || strings.Contains(request.URL, "docs-key") {
			t.Fatalf("secret escaped Runtime-only envelope: %+v", request)
		}
	}
	probe, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: connection(), Secrets: map[string]string{"api_key": "docs-key"}})
	if err != nil || !probe.Connected {
		t.Fatalf("probe=%+v error=%v", probe, err)
	}
}

func TestWritesAreStrictAndPreserveNullableLists(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusCreated, Body: []byte(`{"article":{"id":"new-1"}}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	create := CreateArticleInput{CollectionID: "collection-2", Input: CreateArticleBody{Name: "Billing", Text: "Help", Status: "published", Categories: []string{"category-1"}}}
	result, err := adapter.Call(t.Context(), callRequest(CreateArticle.Descriptor(), mustJSON(create)))
	if err != nil || result.ResponseRef != "help_scout_docs:article:new-1" {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	request := transport.requests[len(transport.requests)-1]
	if request.Method != http.MethodPost || !strings.HasSuffix(request.URL, "/articles?reload=true") {
		t.Fatalf("request=%+v", request)
	}
	var created map[string]any
	if err := json.Unmarshal(request.Body, &created); err != nil {
		t.Fatal(err)
	}
	if created["collectionId"] != "collection-2" || created["name"] != "Billing" {
		t.Fatalf("body=%v", created)
	}

	transport.response = connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"article":{"id":"article-1"}}`)}
	update := UpdateArticleInput{ArticleID: "article-1", Input: UpdateArticleBody{Name: "Updated", Categories: json.RawMessage(`null`), Keywords: json.RawMessage(`["billing"]`)}}
	if _, err := adapter.Call(t.Context(), callRequest(UpdateArticle.Descriptor(), mustJSON(update))); err != nil {
		t.Fatal(err)
	}
	request = transport.requests[len(transport.requests)-1]
	var updated map[string]any
	if err := json.Unmarshal(request.Body, &updated); err != nil {
		t.Fatal(err)
	}
	if value, exists := updated["categories"]; !exists || value != nil {
		t.Fatalf("nullable categories lost: %v", updated)
	}
	if request.Method != http.MethodPut || !strings.Contains(request.URL, "/articles/article-1") {
		t.Fatalf("request=%+v", request)
	}

	invalid := []struct {
		operation connector.OperationDescriptor
		payload   []byte
	}{
		{CreateArticle.Descriptor(), mustJSON(CreateArticleInput{CollectionID: "c", Input: CreateArticleBody{Name: "missing text"}})},
		{CreateArticle.Descriptor(), []byte(`{"collection_id":"c","input":{"name":"n","text":"t","provider_private":true}}`)},
		{UpdateArticle.Descriptor(), mustJSON(UpdateArticleInput{ArticleID: "a"})},
		{UpdateArticle.Descriptor(), mustJSON(UpdateArticleInput{ArticleID: "a", Input: UpdateArticleBody{Categories: json.RawMessage(`{"bad":true}`)}})},
		{UpdateArticle.Descriptor(), mustJSON(UpdateArticleInput{ArticleID: "a", Input: UpdateArticleBody{Status: "draft"}})},
	}
	for _, test := range invalid {
		if _, err := adapter.Call(t.Context(), callRequest(test.operation, test.payload)); err == nil {
			t.Fatalf("invalid input accepted: %s", test.payload)
		}
	}
}

func TestValidationPaginationAndOutcomeClassification(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	validator := adapter.(connector.ConfigValidator)
	for _, invalid := range []connector.Connection{{Config: map[string]any{"base_url": "http://remote.example", "collection_id": "c"}}, {Config: map[string]any{"base_url": "https://user@example.com", "collection_id": "c"}}, {Config: map[string]any{"base_url": "https://docsapi.helpscout.net/v1"}}} {
		if err := validator.ValidateConfig(invalid); err == nil {
			t.Fatalf("invalid config accepted: %+v", invalid)
		}
	}
	invalidInputs := []struct {
		operation connector.OperationDescriptor
		input     any
	}{
		{ListArticles.Descriptor(), ListArticlesInput{UpdatedSince: "2026-01-01"}},
		{ListArticles.Descriptor(), ListArticlesInput{Locale: "en"}},
		{ListArticles.Descriptor(), ListArticlesInput{Cursor: "next"}},
		{ListArticles.Descriptor(), ListArticlesInput{PageSize: 101}},
		{SearchArticles.Descriptor(), SearchArticlesInput{}},
		{SearchArticles.Descriptor(), SearchArticlesInput{Query: "q", PageSize: 10}},
	}
	for _, test := range invalidInputs {
		if _, err := adapter.Call(t.Context(), callRequest(test.operation, mustJSON(test.input))); err == nil {
			t.Fatalf("invalid input accepted for %s", test.operation.Key)
		}
	}

	transport.err = errors.New("connection reset")
	_, readErr := adapter.Call(t.Context(), callRequest(ListArticles.Descriptor(), mustJSON(ListArticlesInput{})))
	if class, ok := connector.ErrorClassificationOf(readErr); !ok || class != connector.ErrorRetryable {
		t.Fatalf("read error=%v class=%q", readErr, class)
	}
	_, writeErr := adapter.Call(t.Context(), callRequest(CreateArticle.Descriptor(), mustJSON(CreateArticleInput{CollectionID: "c", Input: CreateArticleBody{Name: "n", Text: "t"}})))
	if class, ok := connector.ErrorClassificationOf(writeErr); !ok || class != connector.ErrorUncertain {
		t.Fatalf("write error=%v class=%q", writeErr, class)
	}
	transport.err = nil
	transport.response = connector.HTTPResponse{StatusCode: http.StatusBadGateway, Body: []byte(`{"error":"UPSTREAM/FAILED"}`)}
	_, writeErr = adapter.Call(t.Context(), callRequest(UpdateArticle.Descriptor(), mustJSON(UpdateArticleInput{ArticleID: "a", Input: UpdateArticleBody{Name: "n"}})))
	if class, ok := connector.ErrorClassificationOf(writeErr); !ok || class != connector.ErrorUncertain || !strings.Contains(writeErr.Error(), "provider_upstream_failed") {
		t.Fatalf("write 5xx=%v class=%q", writeErr, class)
	}
	transport.response = connector.HTTPResponse{StatusCode: http.StatusTooManyRequests, Body: []byte(`{"error":"RATE_LIMIT"}`)}
	_, writeErr = adapter.Call(t.Context(), callRequest(CreateArticle.Descriptor(), mustJSON(CreateArticleInput{CollectionID: "c", Input: CreateArticleBody{Name: "n", Text: "t"}})))
	if class, ok := connector.ErrorClassificationOf(writeErr); !ok || class != connector.ErrorRetryable {
		t.Fatalf("write 429=%v class=%q", writeErr, class)
	}
}

func connection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080/v1", "collection_id": "collection-1"}}
}
func callRequest(operation connector.OperationDescriptor, payload []byte) connector.CallRequest {
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: operation.Key, ContractSHA256: operation.ContractSHA256, Mode: connector.ModeCall, Connection: connection(), Secrets: map[string]string{"api_key": "docs-key"}, Payload: payload}
}
func mustJSON(value any) []byte {
	payload, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return payload
}
