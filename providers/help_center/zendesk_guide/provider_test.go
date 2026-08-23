package zendeskguide

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
	if err = contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	descriptor := adapter.Descriptor()
	if descriptor.ConnectorKey != ConnectorKey || descriptor.ProviderKey != ProviderKey || len(descriptor.Operations) != 6 || len(descriptor.SecretFields) != 1 {
		t.Fatalf("descriptor=%+v", descriptor)
	}
}

func TestReadRoutesUseEndpointSpecificPagination(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"article":{"id":42},"articles":[],"categories":[],"results":[]}`)}}
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
		{ListArticles.Descriptor(), ListArticlesInput{Locale: "en-us", Cursor: "next", PageSize: 75}, "/api/v2/help_center/en-us/articles", map[string]string{"page[after]": "next", "page[size]": "75"}},
		{ListArticles.Descriptor(), ListArticlesInput{UpdatedSince: "2026-07-01T00:00:00Z", PageSize: 25}, "/api/v2/help_center/incremental/articles", map[string]string{"start_time": "1782864000", "page[size]": "25"}},
		{GetArticle.Descriptor(), GetArticleInput{ArticleID: "42", Locale: "en-us"}, "/api/v2/help_center/en-us/articles/42", nil},
		{SearchArticles.Descriptor(), SearchArticlesInput{Query: "billing", Locale: "en-us", Cursor: "3", PageSize: 20}, "/api/v2/help_center/articles/search", map[string]string{"query": "billing", "locale": "en-us", "page": "3", "per_page": "20"}},
		{ListCollections.Descriptor(), ListCollectionsInput{Cursor: "next", PageSize: 40}, "/api/v2/help_center/categories", map[string]string{"page[after]": "next", "page[size]": "40"}},
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
		expectedAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("agent@example.com/token:token"))
		if request.SecretHeaders["Authorization"][0] != expectedAuth || request.Headers["Authorization"] != nil || strings.Contains(request.URL, "token") {
			t.Fatalf("secret escaped envelope: %+v", request)
		}
	}
	probe, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: connection(), Secrets: map[string]string{"api_token": "token"}})
	if err != nil || !probe.Connected {
		t.Fatalf("probe=%+v error=%v", probe, err)
	}
}

func TestCreateUsesLocaleSectionRouteAndStrictBody(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusCreated, Body: []byte(`{"article":{"id":42}}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	notify := false
	input := CreateArticleInput{CollectionID: "7", Input: CreateArticleRequest{Article: CreateArticleBody{Title: "Billing", Locale: "en-us", Body: "Help", LabelNames: []string{"billing"}}, NotifySubscribers: &notify}}
	result, err := adapter.Call(t.Context(), callRequest(CreateArticle.Descriptor(), mustJSON(input)))
	if err != nil || result.ResponseRef != "zendesk_guide:article:42" {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	request := transport.requests[0]
	parsed, _ := url.Parse(request.URL)
	if request.Method != http.MethodPost || parsed.EscapedPath() != "/api/v2/help_center/en-us/sections/7/articles" {
		t.Fatalf("request=%+v", request)
	}
	var body map[string]any
	if err = json.Unmarshal(request.Body, &body); err != nil {
		t.Fatal(err)
	}
	article := body["article"].(map[string]any)
	if article["title"] != "Billing" || article["locale"] != "en-us" || body["notify_subscribers"] != false {
		t.Fatalf("body=%v", body)
	}
	segment := int64(4)
	for _, invalid := range []CreateArticleInput{{}, {CollectionID: "7"}, {CollectionID: "7", Input: CreateArticleRequest{Article: CreateArticleBody{Title: "Title"}}}, {CollectionID: "7", Input: CreateArticleRequest{Article: CreateArticleBody{Title: "Title", Locale: "en-us", UserSegmentID: &segment, UserSegmentIDs: []int64{4}}}}} {
		if _, err = adapter.Call(t.Context(), callRequest(CreateArticle.Descriptor(), mustJSON(invalid))); err == nil {
			t.Fatalf("invalid create accepted: %+v", invalid)
		}
	}
}

func TestUpdateAllowsOnlyMetadataAndClassifiesAmbiguousWrites(t *testing.T) {
	promoted := true
	position := 3
	input := UpdateArticleInput{ArticleID: "42", Input: UpdateArticleRequest{Article: UpdateArticleBody{Promoted: &promoted, Position: &position, LabelNames: []string{"billing"}}}}
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"article":{"id":42}}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = adapter.Call(t.Context(), callRequest(UpdateArticle.Descriptor(), mustJSON(input))); err != nil {
		t.Fatal(err)
	}
	request := transport.requests[0]
	if request.Method != http.MethodPut || !strings.HasSuffix(request.URL, "/api/v2/help_center/articles/42") {
		t.Fatalf("request=%+v", request)
	}
	for _, invalid := range []UpdateArticleInput{{}, {ArticleID: "42"}} {
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
	for _, invalid := range []connector.Connection{{Config: map[string]any{"base_url": "http://remote.example", "email": "agent@example.com"}}, {Config: map[string]any{"base_url": "https://user@example.com", "email": "agent@example.com"}}, {Config: map[string]any{"base_url": "https://example.zendesk.com"}}} {
		if err = validator.ValidateConfig(invalid); err == nil {
			t.Fatalf("invalid config accepted: %+v", invalid)
		}
	}
	for _, test := range []struct {
		operation connector.OperationDescriptor
		input     any
	}{{ListArticles.Descriptor(), ListArticlesInput{UpdatedSince: "yesterday"}}, {ListArticles.Descriptor(), ListArticlesInput{PageSize: 101}}, {SearchArticles.Descriptor(), SearchArticlesInput{}}, {SearchArticles.Descriptor(), SearchArticlesInput{Query: "q", Cursor: "next"}}, {SearchArticles.Descriptor(), SearchArticlesInput{Query: "q", Cursor: "11", PageSize: 100}}, {ListCollections.Descriptor(), ListCollectionsInput{PageSize: -1}}, {GetArticle.Descriptor(), GetArticleInput{}}} {
		if _, callErr := adapter.Call(t.Context(), callRequest(test.operation, mustJSON(test.input))); callErr == nil {
			t.Fatalf("invalid input accepted for %s", test.operation.Key)
		}
	}
	if _, err = adapter.Call(t.Context(), callRequest(ListArticles.Descriptor(), []byte(`{"unknown":true}`))); err == nil {
		t.Fatal("unknown input accepted")
	}
	missingSecret := callRequest(ListArticles.Descriptor(), mustJSON(ListArticlesInput{}))
	missingSecret.Secrets = nil
	if _, err = adapter.Call(t.Context(), missingSecret); err == nil {
		t.Fatal("missing token accepted")
	}
}

func connection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080", "email": "agent@example.com"}}
}
func callRequest(operation connector.OperationDescriptor, payload []byte) connector.CallRequest {
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: operation.Key, ContractSHA256: operation.ContractSHA256, Mode: connector.ModeCall, Connection: connection(), Secrets: map[string]string{"api_token": "token"}, Payload: payload}
}
func mustJSON(value any) []byte {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return raw
}
