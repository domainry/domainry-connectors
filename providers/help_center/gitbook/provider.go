// Package gitbook implements the official GitBook help-center Provider.
package gitbook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	ConnectorKey         = "help_center"
	ProviderKey          = "gitbook"
	defaultBaseURL       = "https://api.gitbook.com/v1"
	responseLimit  int64 = 4 << 20
)

var ListArticles = connector.CallOperation[ListArticlesInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_articles", ContractSHA256: "a45fa9b5ee9c273cd8046e83b34b67be26b4098f018644e956d25c06cb225dd6", Reliability: readReliability()}
var GetArticle = connector.CallOperation[GetArticleInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "get_article", ContractSHA256: "1916ca191c9366feba71009db3bbf3cdb2290f1ddc3bdd469f4231abe177198d", Reliability: readReliability()}
var SearchArticles = connector.CallOperation[SearchArticlesInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "search_articles", ContractSHA256: "79a5f5f9af7b0ad49508900a3a7f38fb0fbabe138bf399eb89358271a2452f36", Reliability: readReliability()}
var ListCollections = connector.CallOperation[ListCollectionsInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_collections", ContractSHA256: "ffddef53d4878282741447a24b469090a75b893e4033fe7dd933f719c7c14374", Reliability: readReliability()}

type ListArticlesInput struct {
	UpdatedSince string `json:"updated_since,omitempty"`
}

type GetArticleInput struct {
	ArticleID string `json:"article_id"`
}

type SearchArticlesInput struct {
	Query    string `json:"query"`
	Cursor   string `json:"cursor,omitempty"`
	PageSize int    `json:"page_size,omitempty"`
}

type ListCollectionsInput struct {
	Cursor   string `json:"cursor,omitempty"`
	PageSize int    `json:"page_size,omitempty"`
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("GitBook transport is required")
	}
	p := &provider{transport: transport}
	listArticles, err := connector.BindCall(ListArticles, p.listArticles)
	if err != nil {
		return nil, err
	}
	getArticle, err := connector.BindCall(GetArticle, p.getArticle)
	if err != nil {
		return nil, err
	}
	searchArticles, err := connector.BindCall(SearchArticles, p.searchArticles)
	if err != nil {
		return nil, err
	}
	listCollections, err := connector.BindCall(ListCollections, p.listCollections)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), listArticles, getArticle, searchArticles, listCollections)
	if err != nil {
		return nil, err
	}
	p.Adapter = bound
	return p, nil
}

func readReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

func schema() connector.ProviderSchema {
	minimum, maximum := float64(1), float64(120)
	return connector.ProviderSchema{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0",
		ConfigFields: []connector.ConfigField{
			{Key: "base_url", Name: "GitBook API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://api.gitbook.com/v1"`)},
			{Key: "space_id", Name: "Space ID", Type: connector.ConfigFieldText, Required: true},
			{Key: "organization_id", Name: "Organization ID", Type: connector.ConfigFieldText, Required: true},
			{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}},
		},
		SecretFields: []connector.SecretField{{Key: "access_token", Name: "Personal access token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}},
	}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(baseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return connector.PermanentError("gitbook.endpoint_invalid", errors.New("HTTPS or loopback HTTP endpoint is required"))
	}
	if configString(connection.Config, "space_id") == "" {
		return connector.PermanentError("gitbook.space_id_required", errors.New("space ID is required"))
	}
	if configString(connection.Config, "organization_id") == "" {
		return connector.PermanentError("gitbook.organization_id_required", errors.New("organization ID is required"))
	}
	return nil
}

func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, "/user", nil)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, err := json.Marshal(map[string]any{"user": payload, "response_ref": ref})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) listArticles(ctx context.Context, request connector.TypedRequest[ListArticlesInput]) (connector.TypedResult[map[string]any], error) {
	if strings.TrimSpace(request.Input.UpdatedSince) != "" {
		return connector.TypedResult[map[string]any]{}, connector.PermanentError("gitbook.incremental_filter_unsupported", errors.New("GitBook list pages does not support updated_since"))
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, "/spaces/"+url.PathEscape(spaceID(request.Connection))+"/content/pages", nil)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}

func (p *provider) getArticle(ctx context.Context, request connector.TypedRequest[GetArticleInput]) (connector.TypedResult[map[string]any], error) {
	id := strings.TrimSpace(request.Input.ArticleID)
	if id == "" {
		return connector.TypedResult[map[string]any]{}, connector.PermanentError("gitbook.article_id_required", errors.New("article ID is required"))
	}
	query := url.Values{"format": {"markdown"}}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, "/spaces/"+url.PathEscape(spaceID(request.Connection))+"/content/page/"+url.PathEscape(id), query)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}

func (p *provider) searchArticles(ctx context.Context, request connector.TypedRequest[SearchArticlesInput]) (connector.TypedResult[map[string]any], error) {
	queryText := strings.TrimSpace(request.Input.Query)
	if queryText == "" {
		return connector.TypedResult[map[string]any]{}, connector.PermanentError("gitbook.query_required", errors.New("search query is required"))
	}
	query, err := pageQuery(request.Input.Cursor, request.Input.PageSize)
	if err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	query.Set("query", queryText)
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, "/spaces/"+url.PathEscape(spaceID(request.Connection))+"/search", query)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}

func (p *provider) listCollections(ctx context.Context, request connector.TypedRequest[ListCollectionsInput]) (connector.TypedResult[map[string]any], error) {
	query, err := pageQuery(request.Input.Cursor, request.Input.PageSize)
	if err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, "/orgs/"+url.PathEscape(organizationID(request.Connection))+"/collections", query)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}

func pageQuery(cursor string, pageSize int) (url.Values, error) {
	if pageSize == 0 {
		pageSize = 50
	}
	if pageSize < 1 || pageSize > 1000 {
		return nil, connector.PermanentError("gitbook.page_size_invalid", errors.New("page size must be between 1 and 1000"))
	}
	query := url.Values{"limit": {strconv.Itoa(pageSize)}}
	if cursor = strings.TrimSpace(cursor); cursor != "" {
		query.Set("page", cursor)
	}
	return query, nil
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, path string, query url.Values) (map[string]any, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", err
	}
	token := strings.TrimSpace(secrets["access_token"])
	if token == "" {
		return nil, "", connector.PermanentError("gitbook.access_token_required", errors.New("resolved access token is required"))
	}
	endpoint, err := url.Parse(baseURL(connection) + path)
	if err != nil {
		return nil, "", connector.PermanentError("gitbook.request_invalid", err)
	}
	values := endpoint.Query()
	for name, items := range query {
		for _, value := range items {
			values.Add(name, value)
		}
	}
	endpoint.RawQuery = values.Encode()
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodGet, URL: endpoint.String(), Headers: map[string][]string{"Accept": {"application/json"}}, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, MaxResponseBytes: responseLimit})
	if err != nil {
		return nil, "", connector.RetryableError("gitbook.network_error", err)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := map[string]any{}
	if json.Unmarshal(response.Body, &payload) != nil {
		return nil, ref, connector.PermanentError("gitbook.response_invalid", errors.New("provider response is invalid JSON"))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code := providerErrorCode(payload, response.StatusCode)
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			return payload, ref, connector.RetryableError(code, cause)
		}
		return payload, ref, connector.PermanentError(code, cause)
	}
	if id := configString(payload, "id"); id != "" {
		ref = "gitbook:" + id
	}
	return payload, ref, nil
}

func providerErrorCode(payload map[string]any, status int) string {
	if value := configString(payload, "code"); value != "" {
		return "gitbook.provider_" + codeToken(value)
	}
	return "gitbook.http_" + strconv.Itoa(status)
}

func codeToken(value string) string {
	var token strings.Builder
	for _, current := range strings.ToLower(strings.TrimSpace(value)) {
		if current >= 'a' && current <= 'z' || current >= '0' && current <= '9' {
			token.WriteRune(current)
		} else if token.Len() > 0 {
			token.WriteByte('_')
		}
	}
	if result := strings.Trim(token.String(), "_"); result != "" {
		return result
	}
	return "unknown"
}

func baseURL(connection connector.Connection) string {
	if value := strings.TrimRight(configString(connection.Config, "base_url"), "/"); value != "" {
		return value
	}
	return defaultBaseURL
}
func spaceID(connection connector.Connection) string {
	return configString(connection.Config, "space_id")
}
func organizationID(connection connector.Connection) string {
	return configString(connection.Config, "organization_id")
}
func configString(values map[string]any, key string) string {
	if values[key] == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(values[key]))
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
