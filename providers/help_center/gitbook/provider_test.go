package gitbook

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
	if got := adapter.Descriptor(); got.ConnectorKey != ConnectorKey || got.ProviderKey != ProviderKey || len(got.Operations) != 4 {
		t.Fatalf("descriptor=%+v", got)
	}
}

func TestOperationsUseTypedInputsAndRuntimeOnlyBearerToken(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"id":"page-1","items":[]}`)}}
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
		{ListArticles.Descriptor(), ListArticlesInput{}, "/v1/spaces/space-1/content/pages", nil},
		{GetArticle.Descriptor(), GetArticleInput{ArticleID: "page/1"}, "/v1/spaces/space-1/content/page/page%2F1", map[string]string{"format": "markdown"}},
		{SearchArticles.Descriptor(), SearchArticlesInput{Query: "billing", Cursor: "next", PageSize: 17}, "/v1/spaces/space-1/search", map[string]string{"query": "billing", "page": "next", "limit": "17"}},
		{ListCollections.Descriptor(), ListCollectionsInput{Cursor: "next", PageSize: 19}, "/v1/orgs/org-1/collections", map[string]string{"page": "next", "limit": "19"}},
	}
	for _, test := range tests {
		payload, marshalErr := json.Marshal(test.input)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if _, err := adapter.Call(t.Context(), callRequest(test.operation, payload)); err != nil {
			t.Fatalf("operation=%s error=%v", test.operation.Key, err)
		}
		request := transport.requests[len(transport.requests)-1]
		parsed, parseErr := url.Parse(request.URL)
		if parseErr != nil || parsed.EscapedPath() != test.path {
			t.Fatalf("operation=%s URL=%q parsed=%+v error=%v", test.operation.Key, request.URL, parsed, parseErr)
		}
		for key, value := range test.query {
			if parsed.Query().Get(key) != value {
				t.Fatalf("operation=%s query=%v", test.operation.Key, parsed.Query())
			}
		}
		if request.SecretHeaders["Authorization"][0] != "Bearer token" || request.Headers["Authorization"] != nil || strings.Contains(request.URL, "token") {
			t.Fatalf("secret escaped Runtime-only envelope: %+v", request)
		}
	}
}

func TestValidationAndFailureClassification(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"id":"user-1"}`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	validator := adapter.(connector.ConfigValidator)
	for _, invalid := range []connector.Connection{
		{Config: map[string]any{"base_url": "http://remote.example", "space_id": "space", "organization_id": "org"}},
		{Config: map[string]any{"base_url": "https://user@example.com", "space_id": "space", "organization_id": "org"}},
		{Config: map[string]any{"base_url": "https://api.gitbook.com/v1", "organization_id": "org"}},
		{Config: map[string]any{"base_url": "https://api.gitbook.com/v1", "space_id": "space"}},
	} {
		if err := validator.ValidateConfig(invalid); err == nil {
			t.Fatalf("invalid config accepted: %+v", invalid)
		}
	}
	probe, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: connection(), Secrets: map[string]string{"access_token": "token"}})
	if err != nil || !probe.Connected || transport.requests[len(transport.requests)-1].URL != "http://localhost:8080/v1/user" {
		t.Fatalf("probe=%+v error=%v requests=%+v", probe, err, transport.requests)
	}
	for _, test := range []struct {
		descriptor connector.OperationDescriptor
		payload    []byte
	}{
		{ListArticles.Descriptor(), mustJSON(ListArticlesInput{UpdatedSince: "2026-01-01T00:00:00Z"})},
		{GetArticle.Descriptor(), mustJSON(GetArticleInput{})},
		{SearchArticles.Descriptor(), mustJSON(SearchArticlesInput{})},
		{SearchArticles.Descriptor(), mustJSON(SearchArticlesInput{Query: "q", PageSize: 1001})},
		{ListCollections.Descriptor(), []byte(`{"unknown":true}`)},
	} {
		if _, err := adapter.Call(t.Context(), callRequest(test.descriptor, test.payload)); err == nil {
			t.Fatalf("invalid input accepted for %s: %s", test.descriptor.Key, test.payload)
		}
	}
	transport.err = errors.New("connection reset")
	_, err = adapter.Call(t.Context(), callRequest(ListArticles.Descriptor(), mustJSON(ListArticlesInput{})))
	if class, ok := connector.ErrorClassificationOf(err); !ok || class != connector.ErrorRetryable {
		t.Fatalf("network error=%v class=%q", err, class)
	}
	transport.err = nil
	transport.response = connector.HTTPResponse{StatusCode: http.StatusTooManyRequests, Body: []byte(`{"code":"RATE/LIMIT"}`)}
	_, err = adapter.Call(t.Context(), callRequest(ListArticles.Descriptor(), mustJSON(ListArticlesInput{})))
	if class, ok := connector.ErrorClassificationOf(err); !ok || class != connector.ErrorRetryable || !strings.Contains(err.Error(), "gitbook.provider_rate_limit") {
		t.Fatalf("rate error=%v class=%q", err, class)
	}
	transport.response = connector.HTTPResponse{StatusCode: http.StatusBadRequest, Body: []byte(`{"code":"BAD REQUEST"}`)}
	_, err = adapter.Call(t.Context(), callRequest(ListArticles.Descriptor(), mustJSON(ListArticlesInput{})))
	if class, ok := connector.ErrorClassificationOf(err); !ok || class != connector.ErrorPermanent {
		t.Fatalf("client error=%v class=%q", err, class)
	}
}

func connection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080/v1", "space_id": "space-1", "organization_id": "org-1"}}
}
func callRequest(operation connector.OperationDescriptor, payload []byte) connector.CallRequest {
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: operation.Key, ContractSHA256: operation.ContractSHA256, Mode: connector.ModeCall, Connection: connection(), Secrets: map[string]string{"access_token": "token"}, Payload: payload}
}
func mustJSON(value any) []byte {
	payload, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return payload
}
