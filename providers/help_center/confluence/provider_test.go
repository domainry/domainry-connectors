package confluence

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
	if descriptor.ConnectorKey != ConnectorKey || descriptor.ProviderKey != ProviderKey || len(descriptor.Operations) != 6 || len(descriptor.ConfigFields) != 5 || len(descriptor.SecretFields) != 1 {
		t.Fatalf("descriptor=%+v", descriptor)
	}
}

func TestReadRoutesPaginationCQLAndSecretBoundary(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"id":"42","results":[]}`)}}
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
		{ListArticles.Descriptor(), ListArticlesInput{Cursor: "next", PageSize: 75}, "/wiki/api/v2/spaces/100/pages", map[string]string{"cursor": "next", "limit": "75", "status": "current"}},
		{ListArticles.Descriptor(), ListArticlesInput{UpdatedSince: "2026-07-01T12:34:00Z", PageSize: 25}, "/wiki/rest/api/search", map[string]string{"limit": "25", "cql": `type=page AND lastmodified >= "2026-07-01 12:34" AND space="DOCS"`}},
		{GetArticle.Descriptor(), GetArticleInput{ArticleID: "42"}, "/wiki/api/v2/pages/42", map[string]string{"body-format": "storage"}},
		{SearchArticles.Descriptor(), SearchArticlesInput{Query: `a\"b`, Cursor: "next", PageSize: 20}, "/wiki/rest/api/search", map[string]string{"limit": "20", "cursor": "next", "cql": `type=page AND text ~ "a\\\"b" AND space="DOCS"`}},
		{ListCollections.Descriptor(), ListCollectionsInput{Cursor: "next", PageSize: 40}, "/wiki/api/v2/spaces", map[string]string{"limit": "40", "cursor": "next"}},
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
				t.Fatalf("operation=%s key=%s got=%q want=%q query=%v", test.operation.Key, key, parsed.Query().Get(key), value, parsed.Query())
			}
		}
		expectedAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("agent@example.com:token"))
		if request.SecretHeaders["Authorization"][0] != expectedAuth || request.Headers["Authorization"] != nil || strings.Contains(request.URL, "token") {
			t.Fatalf("secret escaped envelope: %+v", request)
		}
	}
	probe, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: connection(), Secrets: map[string]string{"api_token": "token"}})
	if err != nil || !probe.Connected {
		t.Fatalf("probe=%+v error=%v", probe, err)
	}
}

func TestCreateUsesOperationCollectionAsTargetSpace(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"id":"42"}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	input := CreateArticleInput{CollectionID: "200", Input: CreatePageBody{Status: "current", Title: "Billing", ParentID: "7", Body: &PageBody{Representation: "storage", Value: "<p>Help</p>"}}}
	result, err := adapter.Call(t.Context(), callRequest(CreateArticle.Descriptor(), mustJSON(input)))
	if err != nil || result.ResponseRef != "confluence:page:42" {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	request := transport.requests[0]
	if request.Method != http.MethodPost || !strings.HasSuffix(request.URL, "/wiki/api/v2/pages") {
		t.Fatalf("request=%+v", request)
	}
	var body map[string]any
	if err = json.Unmarshal(request.Body, &body); err != nil {
		t.Fatal(err)
	}
	if body["spaceId"] != "200" || body["title"] != "Billing" || body["parentId"] != "7" {
		t.Fatalf("body=%v", body)
	}
	for _, invalid := range []CreateArticleInput{{}, {CollectionID: "200"}, {CollectionID: "200", Input: CreatePageBody{Status: "archived", Title: "x"}}, {CollectionID: "200", Input: CreatePageBody{Title: "x", Body: &PageBody{Representation: "view", Value: "x"}}}} {
		if _, err = adapter.Call(t.Context(), callRequest(CreateArticle.Descriptor(), mustJSON(invalid))); err == nil {
			t.Fatalf("invalid create accepted: %+v", invalid)
		}
	}
}

func TestUpdateRequiresVersionedCompleteBodyAndClassifiesWrites(t *testing.T) {
	minor := true
	input := UpdateArticleInput{ArticleID: "42", Input: UpdatePageBody{ID: "42", Status: "current", Title: "Updated", Body: PageBody{Representation: "storage", Value: "<p>Updated</p>"}, Version: PageVersion{Number: 3, Message: "Update", MinorEdit: &minor}}}
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"id":"42"}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = adapter.Call(t.Context(), callRequest(UpdateArticle.Descriptor(), mustJSON(input))); err != nil {
		t.Fatal(err)
	}
	request := transport.requests[0]
	if request.Method != http.MethodPut || !strings.HasSuffix(request.URL, "/wiki/api/v2/pages/42") {
		t.Fatalf("request=%+v", request)
	}
	for _, invalid := range []UpdateArticleInput{{}, {ArticleID: "42"}, {ArticleID: "42", Input: UpdatePageBody{ID: "43", Status: "current", Title: "x", Body: input.Input.Body, Version: PageVersion{Number: 2}}}, {ArticleID: "42", Input: UpdatePageBody{ID: "42", Status: "current", Title: "x", Body: input.Input.Body}}} {
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
	transport.response = connector.HTTPResponse{StatusCode: http.StatusBadGateway, Body: []byte(`{"key":"UPSTREAM/FAILED"}`)}
	_, err = adapter.Call(t.Context(), callRequest(UpdateArticle.Descriptor(), mustJSON(input)))
	if class, ok := connector.ErrorClassificationOf(err); !ok || class != connector.ErrorUncertain || !strings.Contains(err.Error(), "provider_upstream_failed") {
		t.Fatalf("5xx=%v class=%q", err, class)
	}
	transport.response = connector.HTTPResponse{StatusCode: http.StatusTooManyRequests, Body: []byte(`{"key":"RATE_LIMIT"}`)}
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
	for _, invalid := range []connector.Connection{{Config: map[string]any{"base_url": "http://remote.example", "email": "agent@example.com", "space_id": "100"}}, {Config: map[string]any{"base_url": "https://user@example.com", "email": "agent@example.com", "space_id": "100"}}, {Config: map[string]any{"base_url": "https://example.atlassian.net", "space_id": "100"}}, {Config: map[string]any{"base_url": "https://example.atlassian.net", "email": "agent@example.com"}}} {
		if err = validator.ValidateConfig(invalid); err == nil {
			t.Fatalf("invalid config accepted: %+v", invalid)
		}
	}
	for _, test := range []struct {
		operation connector.OperationDescriptor
		input     any
	}{{ListArticles.Descriptor(), ListArticlesInput{UpdatedSince: "yesterday"}}, {ListArticles.Descriptor(), ListArticlesInput{UpdatedSince: "2026-01-01", PageSize: 101}}, {ListCollections.Descriptor(), ListCollectionsInput{PageSize: 251}}, {SearchArticles.Descriptor(), SearchArticlesInput{}}, {SearchArticles.Descriptor(), SearchArticlesInput{Query: "q", PageSize: 101}}, {GetArticle.Descriptor(), GetArticleInput{}}} {
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
	return connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080", "email": "agent@example.com", "space_id": "100", "space_key": "DOCS"}}
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
