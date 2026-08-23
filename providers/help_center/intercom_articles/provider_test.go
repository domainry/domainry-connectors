package intercomarticles

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
	adapter, e := New(&recordingTransport{})
	if e != nil {
		t.Fatal(e)
	}
	if e = contracttest.ValidateAdapter(adapter); e != nil {
		t.Fatal(e)
	}
	got := adapter.Descriptor()
	if got.ConnectorKey != ConnectorKey || got.ProviderKey != ProviderKey || len(got.Operations) != 6 || got.ConfigFields[1].Key != "api_version" {
		t.Fatalf("descriptor=%+v", got)
	}
}

func TestReadPathsPaginationVersionAndSecretEnvelope(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"id":"42","data":[]}`)}}
	adapter, e := New(transport)
	if e != nil {
		t.Fatal(e)
	}
	tests := []struct {
		operation connector.OperationDescriptor
		input     any
		path      string
		query     map[string]string
	}{
		{ListArticles.Descriptor(), ListArticlesInput{Cursor: "2", PageSize: 25}, "/articles", map[string]string{"page": "2", "per_page": "25"}},
		{GetArticle.Descriptor(), GetArticleInput{ArticleID: "42"}, "/articles/42", nil},
		{SearchArticles.Descriptor(), SearchArticlesInput{Query: "billing"}, "/articles/search", map[string]string{"phrase": "billing", "state": "published"}},
		{ListCollections.Descriptor(), ListCollectionsInput{}, "/help_center/collections", map[string]string{"page": "1", "per_page": "25"}},
	}
	for _, test := range tests {
		_, e = adapter.Call(t.Context(), callRequest(test.operation, mustJSON(test.input)))
		if e != nil {
			t.Fatalf("operation=%s error=%v", test.operation.Key, e)
		}
		request := transport.requests[len(transport.requests)-1]
		parsed, parseErr := url.Parse(request.URL)
		if parseErr != nil || parsed.EscapedPath() != test.path {
			t.Fatalf("URL=%q path=%q error=%v", request.URL, parsed.EscapedPath(), parseErr)
		}
		for key, value := range test.query {
			if parsed.Query().Get(key) != value {
				t.Fatalf("operation=%s query=%v", test.operation.Key, parsed.Query())
			}
		}
		if request.Headers["Intercom-Version"][0] != "2.15" || request.SecretHeaders["Authorization"][0] != "Bearer token" || request.Headers["Authorization"] != nil || strings.Contains(request.URL, "token") {
			t.Fatalf("headers=%+v", request)
		}
	}
	probe, e := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: connection(), Secrets: map[string]string{"access_token": "token"}})
	if e != nil || !probe.Connected {
		t.Fatalf("probe=%+v error=%v", probe, e)
	}
}

func TestTypedMutationsAndWriteUncertainty(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusCreated, Body: []byte(`{"id":"43","type":"article"}`)}}
	adapter, e := New(transport)
	if e != nil {
		t.Fatal(e)
	}
	create := CreateArticleInput{CollectionID: "7", Input: CreateArticleBody{Title: "Billing", AuthorID: 123, Body: "<p>Help</p>", State: "published", TranslatedContent: json.RawMessage(`{"fr":{"title":"Facturation"}}`)}}
	result, e := adapter.Call(t.Context(), callRequest(CreateArticle.Descriptor(), mustJSON(create)))
	if e != nil || result.ResponseRef != "intercom_articles:article:43" {
		t.Fatalf("result=%+v error=%v", result, e)
	}
	request := transport.requests[len(transport.requests)-1]
	var body map[string]any
	if e = json.Unmarshal(request.Body, &body); e != nil {
		t.Fatal(e)
	}
	if request.Method != http.MethodPost || body["parent_id"] != float64(7) || body["parent_type"] != "collection" || body["author_id"] != float64(123) {
		t.Fatalf("request=%+v body=%v", request, body)
	}
	description := ""
	update := UpdateArticleInput{ArticleID: "42", Input: UpdateArticleBody{Title: "Updated", Description: &description, ParentID: 8, ParentType: "section"}}
	transport.response = connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"id":"42"}`)}
	if _, e = adapter.Call(t.Context(), callRequest(UpdateArticle.Descriptor(), mustJSON(update))); e != nil {
		t.Fatal(e)
	}
	request = transport.requests[len(transport.requests)-1]
	if request.Method != http.MethodPut || !strings.HasSuffix(request.URL, "/articles/42") {
		t.Fatalf("request=%+v", request)
	}
	for _, test := range []struct {
		operation connector.OperationDescriptor
		input     any
	}{
		{CreateArticle.Descriptor(), CreateArticleInput{CollectionID: "not-an-id", Input: CreateArticleBody{Title: "t", AuthorID: 1}}},
		{CreateArticle.Descriptor(), CreateArticleInput{CollectionID: "7", Input: CreateArticleBody{AuthorID: 1}}},
		{CreateArticle.Descriptor(), CreateArticleInput{CollectionID: "7", Input: CreateArticleBody{Title: "t", AuthorID: 1, State: "public"}}},
		{UpdateArticle.Descriptor(), UpdateArticleInput{ArticleID: "42"}},
		{UpdateArticle.Descriptor(), UpdateArticleInput{ArticleID: "42", Input: UpdateArticleBody{ParentID: 8, ParentType: "folder"}}},
	} {
		if _, err := adapter.Call(t.Context(), callRequest(test.operation, mustJSON(test.input))); err == nil {
			t.Fatalf("invalid input accepted: %+v", test.input)
		}
	}
	transport.err = errors.New("connection reset")
	_, e = adapter.Call(t.Context(), callRequest(CreateArticle.Descriptor(), mustJSON(create)))
	if class, ok := connector.ErrorClassificationOf(e); !ok || class != connector.ErrorUncertain {
		t.Fatalf("network error=%v class=%q", e, class)
	}
	transport.err = nil
	transport.response = connector.HTTPResponse{StatusCode: http.StatusBadGateway, Body: []byte(`{"errors":[{"code":"SERVER/ERROR"}]}`)}
	_, e = adapter.Call(t.Context(), callRequest(UpdateArticle.Descriptor(), mustJSON(update)))
	if class, ok := connector.ErrorClassificationOf(e); !ok || class != connector.ErrorUncertain || !strings.Contains(e.Error(), "provider_server_error") {
		t.Fatalf("5xx=%v class=%q", e, class)
	}
}

func TestConfigInputAndReadFailureValidation(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{}`)}}
	adapter, e := New(transport)
	if e != nil {
		t.Fatal(e)
	}
	validator := adapter.(connector.ConfigValidator)
	for _, invalid := range []connector.Connection{{Config: map[string]any{"base_url": "http://remote.example", "api_version": "2.16"}}, {Config: map[string]any{"base_url": "https://user@example.com", "api_version": "2.16"}}, {Config: map[string]any{"base_url": "https://api.intercom.io", "api_version": "Preview\nInjected"}}} {
		if e = validator.ValidateConfig(invalid); e == nil {
			t.Fatalf("invalid config accepted: %+v", invalid)
		}
	}
	for _, test := range []struct {
		operation connector.OperationDescriptor
		input     any
	}{{ListArticles.Descriptor(), ListArticlesInput{UpdatedSince: "2026-01-01"}}, {ListArticles.Descriptor(), ListArticlesInput{Cursor: "next"}}, {ListArticles.Descriptor(), ListArticlesInput{PageSize: 151}}, {GetArticle.Descriptor(), GetArticleInput{}}, {SearchArticles.Descriptor(), SearchArticlesInput{}}, {SearchArticles.Descriptor(), SearchArticlesInput{Query: "q", Cursor: "2"}}} {
		if _, err := adapter.Call(t.Context(), callRequest(test.operation, mustJSON(test.input))); err == nil {
			t.Fatalf("invalid input accepted for %s", test.operation.Key)
		}
	}
	if _, e = adapter.Call(t.Context(), callRequest(ListArticles.Descriptor(), []byte(`{"unknown":true}`))); e == nil {
		t.Fatal("unknown input accepted")
	}
	transport.err = errors.New("timeout")
	_, e = adapter.Call(t.Context(), callRequest(ListArticles.Descriptor(), mustJSON(ListArticlesInput{})))
	if class, ok := connector.ErrorClassificationOf(e); !ok || class != connector.ErrorRetryable {
		t.Fatalf("read error=%v class=%q", e, class)
	}
}

func connection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080", "api_version": "2.15"}}
}
func callRequest(operation connector.OperationDescriptor, payload []byte) connector.CallRequest {
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: operation.Key, ContractSHA256: operation.ContractSHA256, Mode: connector.ModeCall, Connection: connection(), Secrets: map[string]string{"access_token": "token"}, Payload: payload}
}
func mustJSON(value any) []byte {
	raw, e := json.Marshal(value)
	if e != nil {
		panic(e)
	}
	return raw
}
