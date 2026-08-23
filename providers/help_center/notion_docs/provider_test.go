package notiondocs

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
	requests  []connector.HTTPRequest
	responses []connector.HTTPResponse
	err       error
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, request)
	if t.err != nil {
		return connector.HTTPResponse{}, t.err
	}
	if len(t.responses) == 0 {
		return connector.HTTPResponse{}, nil
	}
	response := t.responses[0]
	if len(t.responses) > 1 {
		t.responses = t.responses[1:]
	}
	return response, nil
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
	if got.ConnectorKey != ConnectorKey || got.ProviderKey != ProviderKey || len(got.Operations) != 6 || got.ConfigFields[2].Key != "notion_version" {
		t.Fatalf("descriptor=%+v", got)
	}
}

func TestReadOperationsUseCurrentDataSourceContract(t *testing.T) {
	response := connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"id":"page-1","results":[]}`)}
	transport := &recordingTransport{responses: []connector.HTTPResponse{response, response, response, response, response, response}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	list := ListArticlesInput{Cursor: "next", PageSize: 25, UpdatedSince: "2026-07-01T00:00:00Z"}
	if _, err = adapter.Call(t.Context(), callRequest(ListArticles.Descriptor(), mustJSON(list))); err != nil {
		t.Fatal(err)
	}
	request := transport.requests[len(transport.requests)-1]
	if request.Method != http.MethodPost || !strings.HasSuffix(request.URL, "/data_sources/source-1/query") {
		t.Fatalf("query request=%+v", request)
	}
	var listBody map[string]any
	if err = json.Unmarshal(request.Body, &listBody); err != nil {
		t.Fatal(err)
	}
	if listBody["start_cursor"] != "next" || listBody["page_size"] != float64(25) || listBody["filter"] == nil {
		t.Fatalf("body=%v", listBody)
	}
	if _, err = adapter.Call(t.Context(), callRequest(GetArticle.Descriptor(), mustJSON(GetArticleInput{ArticleID: "page/1", Cursor: "blocks-next", PageSize: 10}))); err != nil {
		t.Fatal(err)
	}
	if len(transport.requests) != 3 {
		t.Fatalf("get article calls=%d", len(transport.requests))
	}
	pageURL, _ := url.Parse(transport.requests[1].URL)
	blocksURL, _ := url.Parse(transport.requests[2].URL)
	if pageURL.EscapedPath() != "/v1/pages/page%2F1" || blocksURL.EscapedPath() != "/v1/blocks/page%2F1/children" || blocksURL.Query().Get("start_cursor") != "blocks-next" {
		t.Fatalf("page=%s blocks=%s", pageURL, blocksURL)
	}
	if _, err = adapter.Call(t.Context(), callRequest(SearchArticles.Descriptor(), mustJSON(SearchArticlesInput{Query: "billing", PageSize: 20}))); err != nil {
		t.Fatal(err)
	}
	var searchBody map[string]any
	_ = json.Unmarshal(transport.requests[3].Body, &searchBody)
	if searchBody["query"] != "billing" || searchBody["filter"].(map[string]any)["value"] != "page" {
		t.Fatalf("search=%v", searchBody)
	}
	if _, err = adapter.Call(t.Context(), callRequest(ListCollections.Descriptor(), mustJSON(ListCollectionsInput{}))); err != nil {
		t.Fatal(err)
	}
	var collectionsBody map[string]any
	_ = json.Unmarshal(transport.requests[4].Body, &collectionsBody)
	if collectionsBody["filter"].(map[string]any)["value"] != "data_source" {
		t.Fatalf("collections=%v", collectionsBody)
	}
	for _, request := range transport.requests {
		if request.Headers["Notion-Version"][0] != "2026-03-11" || request.SecretHeaders["Authorization"][0] != "Bearer token" || request.Headers["Authorization"] != nil {
			t.Fatalf("request=%+v", request)
		}
	}
}

func TestCreateUsesOperationCollectionAndTypedPageFields(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: http.StatusOK, Body: []byte(`{"id":"created-1"}`)}}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	input := CreateArticleInput{CollectionID: "target-source", Input: CreateArticleBody{Properties: json.RawMessage(`{"Title":{"title":[]}}`), Children: json.RawMessage(`[{"object":"block","type":"paragraph"}]`), Icon: json.RawMessage(`{"type":"emoji","emoji":"📘"}`)}}
	result, err := adapter.Call(t.Context(), callRequest(CreateArticle.Descriptor(), mustJSON(input)))
	if err != nil || result.ResponseRef != "notion_docs:page:created-1" {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	request := transport.requests[0]
	var body map[string]any
	if err = json.Unmarshal(request.Body, &body); err != nil {
		t.Fatal(err)
	}
	parent := body["parent"].(map[string]any)
	if request.Method != http.MethodPost || request.URL != "http://localhost:8080/v1/pages" || parent["data_source_id"] != "target-source" || parent["type"] != "data_source_id" {
		t.Fatalf("request=%+v body=%v", request, body)
	}
	for _, invalid := range []CreateArticleInput{{CollectionID: "", Input: input.Input}, {CollectionID: "source", Input: CreateArticleBody{}}, {CollectionID: "source", Input: CreateArticleBody{Properties: json.RawMessage(`[]`)}}, {CollectionID: "source", Input: CreateArticleBody{Properties: input.Input.Properties, Children: json.RawMessage(`[]`), Template: json.RawMessage(`{"type":"default"}`)}}} {
		if _, err = adapter.Call(t.Context(), callRequest(CreateArticle.Descriptor(), mustJSON(invalid))); err == nil {
			t.Fatalf("invalid create accepted: %+v", invalid)
		}
	}
}

func TestUpdateValidationAndOutcomeClassification(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: http.StatusOK, Body: []byte(`{"id":"page-1"}`)}}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	inTrash := true
	update := UpdateArticleInput{ArticleID: "page-1", Input: UpdateArticleBody{Properties: json.RawMessage(`{"Status":{"status":{"name":"Done"}}}`), Cover: json.RawMessage(`null`), InTrash: &inTrash}}
	if _, err = adapter.Call(t.Context(), callRequest(UpdateArticle.Descriptor(), mustJSON(update))); err != nil {
		t.Fatal(err)
	}
	request := transport.requests[0]
	if request.Method != http.MethodPatch || !strings.HasSuffix(request.URL, "/pages/page-1") {
		t.Fatalf("request=%+v", request)
	}
	var body map[string]any
	_ = json.Unmarshal(request.Body, &body)
	if body["in_trash"] != true {
		t.Fatalf("body=%v", body)
	}
	for _, invalid := range []UpdateArticleInput{{}, {ArticleID: "page"}, {ArticleID: "page", Input: UpdateArticleBody{Properties: json.RawMessage(`[]`)}}} {
		if _, err = adapter.Call(t.Context(), callRequest(UpdateArticle.Descriptor(), mustJSON(invalid))); err == nil {
			t.Fatalf("invalid update accepted: %+v", invalid)
		}
	}
	transport.err = errors.New("connection reset")
	_, err = adapter.Call(t.Context(), callRequest(UpdateArticle.Descriptor(), mustJSON(update)))
	if class, ok := connector.ErrorClassificationOf(err); !ok || class != connector.ErrorUncertain {
		t.Fatalf("write error=%v class=%q", err, class)
	}
	transport.err = nil
	transport.responses = []connector.HTTPResponse{{StatusCode: http.StatusBadGateway, Body: []byte(`{"code":"internal/server_error"}`)}}
	_, err = adapter.Call(t.Context(), callRequest(UpdateArticle.Descriptor(), mustJSON(update)))
	if class, ok := connector.ErrorClassificationOf(err); !ok || class != connector.ErrorUncertain || !strings.Contains(err.Error(), "provider_internal_server_error") {
		t.Fatalf("5xx=%v class=%q", err, class)
	}
	transport.responses = []connector.HTTPResponse{{StatusCode: http.StatusTooManyRequests, Body: []byte(`{"code":"rate_limited"}`)}}
	_, err = adapter.Call(t.Context(), callRequest(UpdateArticle.Descriptor(), mustJSON(update)))
	if class, ok := connector.ErrorClassificationOf(err); !ok || class != connector.ErrorRetryable {
		t.Fatalf("429=%v class=%q", err, class)
	}
}

func TestConfigAndInputEdges(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: http.StatusOK, Body: []byte(`{"id":"me"}`)}}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	validator := adapter.(connector.ConfigValidator)
	for _, invalid := range []connector.Connection{{Config: map[string]any{"base_url": "http://remote.example", "data_source_id": "s", "notion_version": "2026-03-11"}}, {Config: map[string]any{"base_url": "https://user@example.com", "data_source_id": "s", "notion_version": "2026-03-11"}}, {Config: map[string]any{"base_url": "https://api.notion.com/v1", "notion_version": "2026-03-11"}}, {Config: map[string]any{"base_url": "https://api.notion.com/v1", "data_source_id": "s", "notion_version": "latest"}}} {
		if err = validator.ValidateConfig(invalid); err == nil {
			t.Fatalf("invalid config accepted: %+v", invalid)
		}
	}
	probe, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: connection(), Secrets: map[string]string{"access_token": "token"}})
	if err != nil || !probe.Connected {
		t.Fatalf("probe=%+v error=%v", probe, err)
	}
	for _, test := range []struct {
		operation connector.OperationDescriptor
		input     any
	}{{ListArticles.Descriptor(), ListArticlesInput{PageSize: 101}}, {ListArticles.Descriptor(), ListArticlesInput{UpdatedSince: "not-time"}}, {ListArticles.Descriptor(), ListArticlesInput{Locale: "en"}}, {GetArticle.Descriptor(), GetArticleInput{}}, {SearchArticles.Descriptor(), SearchArticlesInput{}}} {
		if _, callErr := adapter.Call(t.Context(), callRequest(test.operation, mustJSON(test.input))); callErr == nil {
			t.Fatalf("invalid input accepted for %s", test.operation.Key)
		}
	}
	if _, err = adapter.Call(t.Context(), callRequest(ListArticles.Descriptor(), []byte(`{"unknown":true}`))); err == nil {
		t.Fatal("unknown input accepted")
	}
}

func connection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080/v1", "data_source_id": "source-1", "notion_version": "2026-03-11"}}
}
func callRequest(operation connector.OperationDescriptor, payload []byte) connector.CallRequest {
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: operation.Key, ContractSHA256: operation.ContractSHA256, Mode: connector.ModeCall, Connection: connection(), Secrets: map[string]string{"access_token": "token"}, Payload: payload}
}
func mustJSON(value any) []byte {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return raw
}
