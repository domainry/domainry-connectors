// Package helpscoutdocs implements the official Help Scout Docs Provider.
package helpscoutdocs

import (
	"context"
	"encoding/base64"
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
	ProviderKey          = "help_scout_docs"
	defaultBaseURL       = "https://docsapi.helpscout.net/v1"
	responseLimit  int64 = 4 << 20
)

var ListArticles = connector.CallOperation[ListArticlesInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_articles", ContractSHA256: "a45fa9b5ee9c273cd8046e83b34b67be26b4098f018644e956d25c06cb225dd6", Reliability: readReliability()}
var GetArticle = connector.CallOperation[GetArticleInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "get_article", ContractSHA256: "1916ca191c9366feba71009db3bbf3cdb2290f1ddc3bdd469f4231abe177198d", Reliability: readReliability()}
var SearchArticles = connector.CallOperation[SearchArticlesInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "search_articles", ContractSHA256: "79a5f5f9af7b0ad49508900a3a7f38fb0fbabe138bf399eb89358271a2452f36", Reliability: readReliability()}
var ListCollections = connector.CallOperation[ListCollectionsInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_collections", ContractSHA256: "ffddef53d4878282741447a24b469090a75b893e4033fe7dd933f719c7c14374", Reliability: readReliability()}
var CreateArticle = connector.CallOperation[CreateArticleInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "create_article", ContractSHA256: "cce53c916d333a914c28902f4c8360abbf20a76e06147d95fb891fecc2425f2d", Reliability: writeReliability()}
var UpdateArticle = connector.CallOperation[UpdateArticleInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "update_article", ContractSHA256: "b9057eb433a9bd4e5511b80365d769bd88ed8c3355736c1a2a909ba068eacc95", Reliability: writeReliability()}

type ListArticlesInput struct {
	Locale       string `json:"locale,omitempty"`
	Cursor       string `json:"cursor,omitempty"`
	PageSize     int    `json:"page_size,omitempty"`
	UpdatedSince string `json:"updated_since,omitempty"`
}
type GetArticleInput struct {
	ArticleID string `json:"article_id"`
	Locale    string `json:"locale,omitempty"`
}
type SearchArticlesInput struct {
	Query    string `json:"query"`
	Locale   string `json:"locale,omitempty"`
	Cursor   string `json:"cursor,omitempty"`
	PageSize int    `json:"page_size,omitempty"`
}
type ListCollectionsInput struct {
	Locale   string `json:"locale,omitempty"`
	Cursor   string `json:"cursor,omitempty"`
	PageSize int    `json:"page_size,omitempty"`
}
type CreateArticleInput struct {
	CollectionID string            `json:"collection_id"`
	Input        CreateArticleBody `json:"input"`
}
type UpdateArticleInput struct {
	ArticleID string            `json:"article_id"`
	Input     UpdateArticleBody `json:"input"`
}
type CreateArticleBody struct {
	Status     string   `json:"status,omitempty"`
	Slug       string   `json:"slug,omitempty"`
	Name       string   `json:"name"`
	Text       string   `json:"text"`
	Categories []string `json:"categories,omitempty"`
	Related    []string `json:"related,omitempty"`
	Keywords   []string `json:"keywords,omitempty"`
}
type UpdateArticleBody struct {
	Status     string          `json:"status,omitempty"`
	Slug       string          `json:"slug,omitempty"`
	Name       string          `json:"name,omitempty"`
	Text       string          `json:"text,omitempty"`
	Categories json.RawMessage `json:"categories,omitempty"`
	Related    json.RawMessage `json:"related,omitempty"`
	Keywords   json.RawMessage `json:"keywords,omitempty"`
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Help Scout Docs transport is required")
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
	createArticle, err := connector.BindCall(CreateArticle, p.createArticle)
	if err != nil {
		return nil, err
	}
	updateArticle, err := connector.BindCall(UpdateArticle, p.updateArticle)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), listArticles, getArticle, searchArticles, listCollections, createArticle, updateArticle)
	if err != nil {
		return nil, err
	}
	p.Adapter = bound
	return p, nil
}

func readReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}
func writeReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNone}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}
func schema() connector.ProviderSchema {
	minimum, maximum := float64(1), float64(120)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "base_url", Name: "Docs API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://docsapi.helpscout.net/v1"`)}, {Key: "collection_id", Name: "Default collection ID", Type: connector.ConfigFieldText, Required: true}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}}}, SecretFields: []connector.SecretField{{Key: "api_key", Name: "Docs API key", Required: true, CredentialKind: connector.SecretCredentialAPIKey, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(baseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return connector.PermanentError("help_scout_docs.endpoint_invalid", errors.New("HTTPS or loopback HTTP endpoint is required"))
	}
	if collectionID(connection) == "" {
		return connector.PermanentError("help_scout_docs.collection_id_required", errors.New("default collection ID is required"))
	}
	return nil
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/collections", url.Values{"page": {"1"}}, nil, false)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, err := json.Marshal(map[string]any{"collections": payload, "response_ref": ref})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) listArticles(ctx context.Context, request connector.TypedRequest[ListArticlesInput]) (connector.TypedResult[map[string]any], error) {
	if strings.TrimSpace(request.Input.UpdatedSince) != "" {
		return connector.TypedResult[map[string]any]{}, unsupported("incremental_filter")
	}
	if strings.TrimSpace(request.Input.Locale) != "" {
		return connector.TypedResult[map[string]any]{}, unsupported("locale")
	}
	query, err := pageQuery(request.Input.Cursor, request.Input.PageSize, true)
	if err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/collections/"+url.PathEscape(collectionID(request.Connection))+"/articles", query, nil, false)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) getArticle(ctx context.Context, request connector.TypedRequest[GetArticleInput]) (connector.TypedResult[map[string]any], error) {
	if strings.TrimSpace(request.Input.Locale) != "" {
		return connector.TypedResult[map[string]any]{}, unsupported("locale")
	}
	id := strings.TrimSpace(request.Input.ArticleID)
	if id == "" {
		return connector.TypedResult[map[string]any]{}, permanent("article_id_required", "article ID is required")
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/articles/"+url.PathEscape(id), nil, nil, false)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) searchArticles(ctx context.Context, request connector.TypedRequest[SearchArticlesInput]) (connector.TypedResult[map[string]any], error) {
	if strings.TrimSpace(request.Input.Locale) != "" {
		return connector.TypedResult[map[string]any]{}, unsupported("locale")
	}
	if request.Input.PageSize != 0 {
		return connector.TypedResult[map[string]any]{}, unsupported("search_page_size")
	}
	text := strings.TrimSpace(request.Input.Query)
	if text == "" {
		return connector.TypedResult[map[string]any]{}, permanent("query_required", "search query is required")
	}
	query, err := pageQuery(request.Input.Cursor, 0, false)
	if err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	query.Set("query", text)
	query.Set("status", "published")
	query.Set("collectionId", collectionID(request.Connection))
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/search/articles", query, nil, false)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) listCollections(ctx context.Context, request connector.TypedRequest[ListCollectionsInput]) (connector.TypedResult[map[string]any], error) {
	if strings.TrimSpace(request.Input.Locale) != "" {
		return connector.TypedResult[map[string]any]{}, unsupported("locale")
	}
	query, err := pageQuery(request.Input.Cursor, request.Input.PageSize, true)
	if err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/collections", query, nil, false)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) createArticle(ctx context.Context, request connector.TypedRequest[CreateArticleInput]) (connector.TypedResult[map[string]any], error) {
	collection := strings.TrimSpace(request.Input.CollectionID)
	if collection == "" {
		return connector.TypedResult[map[string]any]{}, permanent("collection_id_required", "collection ID is required")
	}
	if err := validateCreate(request.Input.Input); err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	body := map[string]any{"collectionId": collection, "name": strings.TrimSpace(request.Input.Input.Name), "text": request.Input.Input.Text}
	copyCreateFields(body, request.Input.Input)
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodPost, "/articles", url.Values{"reload": {"true"}}, body, true)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) updateArticle(ctx context.Context, request connector.TypedRequest[UpdateArticleInput]) (connector.TypedResult[map[string]any], error) {
	id := strings.TrimSpace(request.Input.ArticleID)
	if id == "" {
		return connector.TypedResult[map[string]any]{}, permanent("article_id_required", "article ID is required")
	}
	body, err := updateBody(request.Input.Input)
	if err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodPut, "/articles/"+url.PathEscape(id), url.Values{"reload": {"true"}}, body, true)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}

func validateCreate(input CreateArticleBody) error {
	if strings.TrimSpace(input.Name) == "" || strings.TrimSpace(input.Text) == "" {
		return permanent("create_fields_required", "article name and text are required")
	}
	return validateStatus(input.Status)
}
func copyCreateFields(body map[string]any, input CreateArticleBody) {
	if value := strings.TrimSpace(input.Status); value != "" {
		body["status"] = value
	}
	if value := strings.TrimSpace(input.Slug); value != "" {
		body["slug"] = value
	}
	if input.Categories != nil {
		body["categories"] = input.Categories
	}
	if input.Related != nil {
		body["related"] = input.Related
	}
	if input.Keywords != nil {
		body["keywords"] = input.Keywords
	}
}
func updateBody(input UpdateArticleBody) (map[string]any, error) {
	if err := validateStatus(input.Status); err != nil {
		return nil, err
	}
	body := map[string]any{}
	for key, value := range map[string]string{"status": input.Status, "slug": input.Slug, "name": input.Name, "text": input.Text} {
		if value = strings.TrimSpace(value); value != "" {
			body[key] = value
		}
	}
	for key, raw := range map[string]json.RawMessage{"categories": input.Categories, "related": input.Related, "keywords": input.Keywords} {
		if len(raw) == 0 {
			continue
		}
		var values []string
		if string(raw) != "null" && json.Unmarshal(raw, &values) != nil {
			return nil, permanent("article_field_invalid", key+" must be null or an array of strings")
		}
		if string(raw) == "null" {
			body[key] = nil
		} else {
			body[key] = values
		}
	}
	if len(body) == 0 {
		return nil, permanent("update_fields_required", "at least one supported article field is required")
	}
	return body, nil
}
func validateStatus(status string) error {
	status = strings.TrimSpace(status)
	if status != "" && status != "published" && status != "notpublished" {
		return permanent("status_invalid", "status must be published or notpublished")
	}
	return nil
}
func pageQuery(cursor string, pageSize int, supportsPageSize bool) (url.Values, error) {
	query := url.Values{}
	if cursor = strings.TrimSpace(cursor); cursor != "" {
		page, err := strconv.Atoi(cursor)
		if err != nil || page < 1 {
			return nil, permanent("cursor_invalid", "cursor must be a positive page number")
		}
		query.Set("page", strconv.Itoa(page))
	}
	if supportsPageSize {
		if pageSize == 0 {
			pageSize = 50
		}
		if pageSize < 1 || pageSize > 100 {
			return nil, permanent("page_size_invalid", "page size must be between 1 and 100")
		}
		query.Set("pageSize", strconv.Itoa(pageSize))
	}
	return query, nil
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, method, path string, query url.Values, body map[string]any, write bool) (map[string]any, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", err
	}
	key := strings.TrimSpace(secrets["api_key"])
	if key == "" {
		return nil, "", permanent("api_key_required", "resolved API key is required")
	}
	endpoint, err := url.Parse(baseURL(connection) + path)
	if err != nil {
		return nil, "", connector.PermanentError("help_scout_docs.request_invalid", err)
	}
	values := endpoint.Query()
	for name, items := range query {
		for _, value := range items {
			values.Add(name, value)
		}
	}
	endpoint.RawQuery = values.Encode()
	var raw []byte
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return nil, "", connector.PermanentError("help_scout_docs.request_invalid", err)
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	authorization := "Basic " + base64.StdEncoding.EncodeToString([]byte(key+":X"))
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint.String(), Headers: headers, SecretHeaders: map[string][]string{"Authorization": {authorization}}, Body: raw, MaxResponseBytes: responseLimit})
	if err != nil {
		if write {
			return nil, "", connector.UncertainError("help_scout_docs.network_error", err)
		}
		return nil, "", connector.RetryableError("help_scout_docs.network_error", err)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := map[string]any{}
	if len(strings.TrimSpace(string(response.Body))) > 0 && json.Unmarshal(response.Body, &payload) != nil {
		return nil, ref, permanent("response_invalid", "provider response is invalid JSON")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code, cause := providerErrorCode(payload, response.StatusCode), fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests {
			return payload, ref, connector.RetryableError(code, cause)
		}
		if response.StatusCode >= 500 {
			if write {
				return payload, ref, connector.UncertainError(code, cause)
			}
			return payload, ref, connector.RetryableError(code, cause)
		}
		return payload, ref, connector.PermanentError(code, cause)
	}
	if article, ok := payload["article"].(map[string]any); ok {
		if id := configString(article, "id"); id != "" {
			ref = "help_scout_docs:article:" + id
		}
	}
	return payload, ref, nil
}

func providerErrorCode(payload map[string]any, status int) string {
	if value := configString(payload, "error"); value != "" {
		return "help_scout_docs.provider_" + codeToken(value)
	}
	return "help_scout_docs.http_" + strconv.Itoa(status)
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
func unsupported(feature string) error {
	return permanent(feature+"_unsupported", feature+" is not supported by Help Scout Docs")
}
func permanent(code, message string) error {
	return connector.PermanentError("help_scout_docs."+code, errors.New(message))
}
func baseURL(connection connector.Connection) string {
	if value := strings.TrimRight(configString(connection.Config, "base_url"), "/"); value != "" {
		return value
	}
	return defaultBaseURL
}
func collectionID(connection connector.Connection) string {
	return configString(connection.Config, "collection_id")
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
