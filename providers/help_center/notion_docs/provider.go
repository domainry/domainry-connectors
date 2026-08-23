// Package notiondocs implements the official Notion Docs Provider.
package notiondocs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	ConnectorKey               = "help_center"
	ProviderKey                = "notion_docs"
	defaultBaseURL             = "https://api.notion.com/v1"
	defaultNotionVersion       = "2026-03-11"
	responseLimit        int64 = 4 << 20
)

var versionPattern = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}$`)

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
	Cursor    string `json:"cursor,omitempty"`
	PageSize  int    `json:"page_size,omitempty"`
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
	Properties json.RawMessage `json:"properties"`
	Children   json.RawMessage `json:"children,omitempty"`
	Icon       json.RawMessage `json:"icon,omitempty"`
	Cover      json.RawMessage `json:"cover,omitempty"`
	Template   json.RawMessage `json:"template,omitempty"`
}
type UpdateArticleBody struct {
	Properties json.RawMessage `json:"properties,omitempty"`
	Icon       json.RawMessage `json:"icon,omitempty"`
	Cover      json.RawMessage `json:"cover,omitempty"`
	InTrash    *bool           `json:"in_trash,omitempty"`
	IsLocked   *bool           `json:"is_locked,omitempty"`
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Notion Docs transport is required")
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
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "base_url", Name: "Notion API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://api.notion.com/v1"`)}, {Key: "data_source_id", Name: "Default article data source ID", Type: connector.ConfigFieldText, Required: true}, {Key: "notion_version", Name: "Notion API version", Type: connector.ConfigFieldText, Default: json.RawMessage(`"2026-03-11"`)}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}}}, SecretFields: []connector.SecretField{{Key: "access_token", Name: "Notion access token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(baseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return permanent("endpoint_invalid", "HTTPS or loopback HTTP endpoint is required")
	}
	if dataSourceID(connection) == "" {
		return permanent("data_source_id_required", "default data source ID is required")
	}
	if !versionPattern.MatchString(notionVersion(connection)) {
		return permanent("notion_version_invalid", "Notion version must use YYYY-MM-DD format")
	}
	return nil
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/users/me", nil, nil, false)
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
	if strings.TrimSpace(request.Input.Locale) != "" {
		return connector.TypedResult[map[string]any]{}, unsupported("locale")
	}
	body, err := paginationBody(request.Input.Cursor, request.Input.PageSize)
	if err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	if since := strings.TrimSpace(request.Input.UpdatedSince); since != "" {
		if _, err := time.Parse(time.RFC3339Nano, since); err != nil {
			return connector.TypedResult[map[string]any]{}, permanent("updated_since_invalid", "updated_since must be RFC3339")
		}
		body["filter"] = map[string]any{"timestamp": "last_edited_time", "last_edited_time": map[string]any{"on_or_after": since}}
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodPost, "/data_sources/"+url.PathEscape(dataSourceID(request.Connection))+"/query", nil, body, false)
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
	page, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/pages/"+url.PathEscape(id), nil, nil, false)
	if err != nil {
		return connector.TypedResult[map[string]any]{Output: page, ResponseRef: ref}, err
	}
	query, err := paginationQuery(request.Input.Cursor, request.Input.PageSize)
	if err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	blocks, _, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/blocks/"+url.PathEscape(id)+"/children", query, nil, false)
	if err != nil {
		return connector.TypedResult[map[string]any]{Output: page, ResponseRef: ref}, err
	}
	page["content_blocks"] = blocks
	return connector.TypedResult[map[string]any]{Output: page, ResponseRef: ref}, nil
}
func (p *provider) searchArticles(ctx context.Context, request connector.TypedRequest[SearchArticlesInput]) (connector.TypedResult[map[string]any], error) {
	if strings.TrimSpace(request.Input.Locale) != "" {
		return connector.TypedResult[map[string]any]{}, unsupported("locale")
	}
	query := strings.TrimSpace(request.Input.Query)
	if query == "" {
		return connector.TypedResult[map[string]any]{}, permanent("query_required", "search query is required")
	}
	body, err := paginationBody(request.Input.Cursor, request.Input.PageSize)
	if err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	body["query"] = query
	body["filter"] = map[string]any{"property": "object", "value": "page"}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodPost, "/search", nil, body, false)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) listCollections(ctx context.Context, request connector.TypedRequest[ListCollectionsInput]) (connector.TypedResult[map[string]any], error) {
	if strings.TrimSpace(request.Input.Locale) != "" {
		return connector.TypedResult[map[string]any]{}, unsupported("locale")
	}
	body, err := paginationBody(request.Input.Cursor, request.Input.PageSize)
	if err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	body["filter"] = map[string]any{"property": "object", "value": "data_source"}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodPost, "/search", nil, body, false)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) createArticle(ctx context.Context, request connector.TypedRequest[CreateArticleInput]) (connector.TypedResult[map[string]any], error) {
	target := strings.TrimSpace(request.Input.CollectionID)
	if target == "" {
		return connector.TypedResult[map[string]any]{}, permanent("collection_id_required", "target data source ID is required")
	}
	body, err := createBody(target, request.Input.Input)
	if err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodPost, "/pages", nil, body, true)
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
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodPatch, "/pages/"+url.PathEscape(id), nil, body, true)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}

func createBody(dataSource string, input CreateArticleBody) (map[string]any, error) {
	properties, err := requiredObject(input.Properties, "properties")
	if err != nil || len(properties) == 0 {
		return nil, permanent("properties_required", "non-empty page properties are required")
	}
	body := map[string]any{"parent": map[string]any{"type": "data_source_id", "data_source_id": dataSource}, "properties": properties}
	for key, value := range map[string]json.RawMessage{"children": input.Children, "icon": input.Icon, "cover": input.Cover, "template": input.Template} {
		if len(value) == 0 {
			continue
		}
		var decoded any
		if json.Unmarshal(value, &decoded) != nil {
			return nil, permanent(key+"_invalid", key+" must be valid JSON")
		}
		if key == "children" {
			if _, ok := decoded.([]any); !ok {
				return nil, permanent("children_invalid", "children must be an array")
			}
		} else if _, ok := decoded.(map[string]any); !ok {
			return nil, permanent(key+"_invalid", key+" must be an object")
		}
		body[key] = decoded
	}
	if body["children"] != nil && body["template"] != nil {
		return nil, permanent("template_children_conflict", "template and children cannot be used together")
	}
	return body, nil
}
func updateBody(input UpdateArticleBody) (map[string]any, error) {
	body := map[string]any{}
	for key, value := range map[string]json.RawMessage{"properties": input.Properties, "icon": input.Icon, "cover": input.Cover} {
		if len(value) == 0 {
			continue
		}
		var decoded any
		if json.Unmarshal(value, &decoded) != nil {
			return nil, permanent(key+"_invalid", key+" must be valid JSON")
		}
		if key == "properties" {
			if _, ok := decoded.(map[string]any); !ok {
				return nil, permanent("properties_invalid", "properties must be an object")
			}
		} else if decoded != nil {
			if _, ok := decoded.(map[string]any); !ok {
				return nil, permanent(key+"_invalid", key+" must be an object or null")
			}
		}
		body[key] = decoded
	}
	if input.InTrash != nil {
		body["in_trash"] = *input.InTrash
	}
	if input.IsLocked != nil {
		body["is_locked"] = *input.IsLocked
	}
	if len(body) == 0 {
		return nil, permanent("update_fields_required", "at least one supported page field is required")
	}
	return body, nil
}
func requiredObject(raw json.RawMessage, key string) (map[string]any, error) {
	var value map[string]any
	if len(raw) == 0 || json.Unmarshal(raw, &value) != nil || value == nil {
		return nil, permanent(key+"_invalid", key+" must be a JSON object")
	}
	return value, nil
}
func paginationBody(cursor string, size int) (map[string]any, error) {
	if size == 0 {
		size = 50
	}
	if size < 1 || size > 100 {
		return nil, permanent("page_size_invalid", "page size must be between 1 and 100")
	}
	body := map[string]any{"page_size": size}
	if cursor = strings.TrimSpace(cursor); cursor != "" {
		body["start_cursor"] = cursor
	}
	return body, nil
}
func paginationQuery(cursor string, size int) (url.Values, error) {
	body, err := paginationBody(cursor, size)
	if err != nil {
		return nil, err
	}
	query := url.Values{"page_size": {strconv.Itoa(body["page_size"].(int))}}
	if cursor := configString(body, "start_cursor"); cursor != "" {
		query.Set("start_cursor", cursor)
	}
	return query, nil
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, method, path string, query url.Values, body map[string]any, write bool) (map[string]any, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", err
	}
	token := strings.TrimSpace(secrets["access_token"])
	if token == "" {
		return nil, "", permanent("access_token_required", "resolved access token is required")
	}
	endpoint, err := url.Parse(baseURL(connection) + path)
	if err != nil {
		return nil, "", connector.PermanentError("notion_docs.request_invalid", err)
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
			return nil, "", connector.PermanentError("notion_docs.request_invalid", err)
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}, "Notion-Version": {notionVersion(connection)}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint.String(), Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, Body: raw, MaxResponseBytes: responseLimit})
	if err != nil {
		if write {
			return nil, "", connector.UncertainError("notion_docs.network_error", err)
		}
		return nil, "", connector.RetryableError("notion_docs.network_error", err)
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
	if id := configString(payload, "id"); id != "" {
		ref = "notion_docs:page:" + id
	}
	return payload, ref, nil
}
func providerErrorCode(payload map[string]any, status int) string {
	if value := configString(payload, "code"); value != "" {
		return "notion_docs.provider_" + codeToken(value)
	}
	return "notion_docs.http_" + strconv.Itoa(status)
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
	return permanent(feature+"_unsupported", feature+" is not supported by Notion Docs")
}
func permanent(code, message string) error {
	return connector.PermanentError("notion_docs."+code, errors.New(message))
}
func baseURL(connection connector.Connection) string {
	if value := strings.TrimRight(configString(connection.Config, "base_url"), "/"); value != "" {
		return value
	}
	return defaultBaseURL
}
func dataSourceID(connection connector.Connection) string {
	return configString(connection.Config, "data_source_id")
}
func notionVersion(connection connector.Connection) string {
	if value := configString(connection.Config, "notion_version"); value != "" {
		return value
	}
	return defaultNotionVersion
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
