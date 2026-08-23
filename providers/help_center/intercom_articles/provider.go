// Package intercomarticles implements the official Intercom Articles Provider.
package intercomarticles

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

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	ConnectorKey            = "help_center"
	ProviderKey             = "intercom_articles"
	defaultBaseURL          = "https://api.intercom.io"
	defaultAPIVersion       = "2.16"
	responseLimit     int64 = 4 << 20
)

var versionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+$`)

var ListArticles = connector.CallOperation[ListArticlesInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_articles", ContractSHA256: "a45fa9b5ee9c273cd8046e83b34b67be26b4098f018644e956d25c06cb225dd6", Reliability: readReliability()}
var GetArticle = connector.CallOperation[GetArticleInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "get_article", ContractSHA256: "1916ca191c9366feba71009db3bbf3cdb2290f1ddc3bdd469f4231abe177198d", Reliability: readReliability()}
var SearchArticles = connector.CallOperation[SearchArticlesInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "search_articles", ContractSHA256: "79a5f5f9af7b0ad49508900a3a7f38fb0fbabe138bf399eb89358271a2452f36", Reliability: readReliability()}
var ListCollections = connector.CallOperation[ListCollectionsInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_collections", ContractSHA256: "ffddef53d4878282741447a24b469090a75b893e4033fe7dd933f719c7c14374", Reliability: readReliability()}
var CreateArticle = connector.CallOperation[CreateArticleInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "create_article", ContractSHA256: "cce53c916d333a914c28902f4c8360abbf20a76e06147d95fb891fecc2425f2d", Reliability: writeReliability()}
var UpdateArticle = connector.CallOperation[UpdateArticleInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "update_article", ContractSHA256: "b9057eb433a9bd4e5511b80365d769bd88ed8c3355736c1a2a909ba068eacc95", Reliability: writeReliability()}

type ListArticlesInput struct {
	Locale       string `json:"locale,omitempty"`
	Cursor       string `json:"cursor,omitempty"`
	UpdatedSince string `json:"updated_since,omitempty"`
	PageSize     int    `json:"page_size,omitempty"`
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
	Title             string          `json:"title"`
	Description       string          `json:"description,omitempty"`
	Body              string          `json:"body,omitempty"`
	AuthorID          int64           `json:"author_id"`
	State             string          `json:"state,omitempty"`
	TranslatedContent json.RawMessage `json:"translated_content,omitempty"`
}
type UpdateArticleBody struct {
	Title             string          `json:"title,omitempty"`
	Description       *string         `json:"description,omitempty"`
	Body              *string         `json:"body,omitempty"`
	AuthorID          int64           `json:"author_id,omitempty"`
	State             string          `json:"state,omitempty"`
	ParentID          int64           `json:"parent_id,omitempty"`
	ParentType        string          `json:"parent_type,omitempty"`
	TranslatedContent json.RawMessage `json:"translated_content,omitempty"`
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Intercom Articles transport is required")
	}
	p := &provider{transport: transport}
	la, e := connector.BindCall(ListArticles, p.listArticles)
	if e != nil {
		return nil, e
	}
	ga, e := connector.BindCall(GetArticle, p.getArticle)
	if e != nil {
		return nil, e
	}
	sa, e := connector.BindCall(SearchArticles, p.searchArticles)
	if e != nil {
		return nil, e
	}
	lc, e := connector.BindCall(ListCollections, p.listCollections)
	if e != nil {
		return nil, e
	}
	ca, e := connector.BindCall(CreateArticle, p.createArticle)
	if e != nil {
		return nil, e
	}
	ua, e := connector.BindCall(UpdateArticle, p.updateArticle)
	if e != nil {
		return nil, e
	}
	bound, e := connector.NewProvider(schema(), la, ga, sa, lc, ca, ua)
	if e != nil {
		return nil, e
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
	min, max := float64(1), float64(120)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "base_url", Name: "Intercom API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://api.intercom.io"`)}, {Key: "api_version", Name: "Intercom API version", Type: connector.ConfigFieldText, Default: json.RawMessage(`"2.16"`)}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &min, Max: &max}}}, SecretFields: []connector.SecretField{{Key: "access_token", Name: "Access token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, e := url.Parse(baseURL(connection))
	if e != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return permanent("endpoint_invalid", "HTTPS or loopback HTTP endpoint is required")
	}
	if !versionPattern.MatchString(apiVersion(connection)) {
		return permanent("api_version_invalid", "API version must be a numeric major.minor version")
	}
	return nil
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	payload, ref, e := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/me", nil, nil, false)
	if e != nil {
		return connector.TestConnectionResult{}, e
	}
	details, e := json.Marshal(map[string]any{"admin": payload, "response_ref": ref})
	if e != nil {
		return connector.TestConnectionResult{}, e
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) listArticles(ctx context.Context, r connector.TypedRequest[ListArticlesInput]) (connector.TypedResult[map[string]any], error) {
	if strings.TrimSpace(r.Input.UpdatedSince) != "" {
		return connector.TypedResult[map[string]any]{}, unsupported("incremental_filter")
	}
	if strings.TrimSpace(r.Input.Locale) != "" {
		return connector.TypedResult[map[string]any]{}, unsupported("locale")
	}
	q, e := pageQuery(r.Input.Cursor, r.Input.PageSize)
	if e != nil {
		return connector.TypedResult[map[string]any]{}, e
	}
	payload, ref, e := p.execute(ctx, r.Connection, r.Secrets, http.MethodGet, "/articles", q, nil, false)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, e
}
func (p *provider) getArticle(ctx context.Context, r connector.TypedRequest[GetArticleInput]) (connector.TypedResult[map[string]any], error) {
	if strings.TrimSpace(r.Input.Locale) != "" {
		return connector.TypedResult[map[string]any]{}, unsupported("locale")
	}
	id := strings.TrimSpace(r.Input.ArticleID)
	if id == "" {
		return connector.TypedResult[map[string]any]{}, permanent("article_id_required", "article ID is required")
	}
	payload, ref, e := p.execute(ctx, r.Connection, r.Secrets, http.MethodGet, "/articles/"+url.PathEscape(id), nil, nil, false)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, e
}
func (p *provider) searchArticles(ctx context.Context, r connector.TypedRequest[SearchArticlesInput]) (connector.TypedResult[map[string]any], error) {
	if strings.TrimSpace(r.Input.Locale) != "" {
		return connector.TypedResult[map[string]any]{}, unsupported("locale")
	}
	if strings.TrimSpace(r.Input.Cursor) != "" || r.Input.PageSize != 0 {
		return connector.TypedResult[map[string]any]{}, unsupported("search_pagination")
	}
	phrase := strings.TrimSpace(r.Input.Query)
	if phrase == "" {
		return connector.TypedResult[map[string]any]{}, permanent("query_required", "search query is required")
	}
	q := url.Values{"phrase": {phrase}, "state": {"published"}}
	payload, ref, e := p.execute(ctx, r.Connection, r.Secrets, http.MethodGet, "/articles/search", q, nil, false)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, e
}
func (p *provider) listCollections(ctx context.Context, r connector.TypedRequest[ListCollectionsInput]) (connector.TypedResult[map[string]any], error) {
	if strings.TrimSpace(r.Input.Locale) != "" {
		return connector.TypedResult[map[string]any]{}, unsupported("locale")
	}
	q, e := pageQuery(r.Input.Cursor, r.Input.PageSize)
	if e != nil {
		return connector.TypedResult[map[string]any]{}, e
	}
	payload, ref, e := p.execute(ctx, r.Connection, r.Secrets, http.MethodGet, "/help_center/collections", q, nil, false)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, e
}
func (p *provider) createArticle(ctx context.Context, r connector.TypedRequest[CreateArticleInput]) (connector.TypedResult[map[string]any], error) {
	parent, e := positiveID(r.Input.CollectionID, "collection_id")
	if e != nil {
		return connector.TypedResult[map[string]any]{}, e
	}
	if e = validateCreate(r.Input.Input); e != nil {
		return connector.TypedResult[map[string]any]{}, e
	}
	body := map[string]any{"title": strings.TrimSpace(r.Input.Input.Title), "author_id": r.Input.Input.AuthorID, "parent_id": parent, "parent_type": "collection"}
	copyCreate(body, r.Input.Input)
	payload, ref, e := p.execute(ctx, r.Connection, r.Secrets, http.MethodPost, "/articles", nil, body, true)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, e
}
func (p *provider) updateArticle(ctx context.Context, r connector.TypedRequest[UpdateArticleInput]) (connector.TypedResult[map[string]any], error) {
	id := strings.TrimSpace(r.Input.ArticleID)
	if id == "" {
		return connector.TypedResult[map[string]any]{}, permanent("article_id_required", "article ID is required")
	}
	body, e := updateBody(r.Input.Input)
	if e != nil {
		return connector.TypedResult[map[string]any]{}, e
	}
	payload, ref, e := p.execute(ctx, r.Connection, r.Secrets, http.MethodPut, "/articles/"+url.PathEscape(id), nil, body, true)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, e
}

func validateCreate(input CreateArticleBody) error {
	if strings.TrimSpace(input.Title) == "" || input.AuthorID < 1 {
		return permanent("create_fields_required", "title and positive author_id are required")
	}
	if e := validateState(input.State); e != nil {
		return e
	}
	return validateObject(input.TranslatedContent, "translated_content")
}
func copyCreate(body map[string]any, input CreateArticleBody) {
	if input.Description != "" {
		body["description"] = input.Description
	}
	if input.Body != "" {
		body["body"] = input.Body
	}
	if value := strings.TrimSpace(input.State); value != "" {
		body["state"] = value
	}
	if len(input.TranslatedContent) > 0 {
		var value any
		_ = json.Unmarshal(input.TranslatedContent, &value)
		body["translated_content"] = value
	}
}
func updateBody(input UpdateArticleBody) (map[string]any, error) {
	if e := validateState(input.State); e != nil {
		return nil, e
	}
	if input.ParentID < 0 {
		return nil, permanent("parent_id_invalid", "parent_id must be positive")
	}
	if input.ParentID > 0 && (input.ParentType != "collection" && input.ParentType != "section") {
		return nil, permanent("parent_type_invalid", "parent_type must be collection or section when parent_id is set")
	}
	if input.ParentID == 0 && strings.TrimSpace(input.ParentType) != "" {
		return nil, permanent("parent_id_required", "parent_id is required when parent_type is set")
	}
	if e := validateObject(input.TranslatedContent, "translated_content"); e != nil {
		return nil, e
	}
	body := map[string]any{}
	if value := strings.TrimSpace(input.Title); value != "" {
		body["title"] = value
	}
	if input.Description != nil {
		body["description"] = *input.Description
	}
	if input.Body != nil {
		body["body"] = *input.Body
	}
	if input.AuthorID > 0 {
		body["author_id"] = input.AuthorID
	}
	if value := strings.TrimSpace(input.State); value != "" {
		body["state"] = value
	}
	if input.ParentID > 0 {
		body["parent_id"] = input.ParentID
		body["parent_type"] = input.ParentType
	}
	if len(input.TranslatedContent) > 0 {
		var value any
		_ = json.Unmarshal(input.TranslatedContent, &value)
		body["translated_content"] = value
	}
	if len(body) == 0 {
		return nil, permanent("update_fields_required", "at least one supported article field is required")
	}
	return body, nil
}
func validateState(value string) error {
	value = strings.TrimSpace(value)
	if value != "" && value != "published" && value != "draft" {
		return permanent("state_invalid", "state must be published or draft")
	}
	return nil
}
func validateObject(raw json.RawMessage, key string) error {
	if len(raw) == 0 {
		return nil
	}
	var value map[string]any
	if json.Unmarshal(raw, &value) != nil || value == nil {
		return permanent(key+"_invalid", key+" must be a JSON object")
	}
	return nil
}
func positiveID(value, key string) (int64, error) {
	id, e := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if e != nil || id < 1 {
		return 0, permanent(key+"_invalid", key+" must be a positive integer")
	}
	return id, nil
}
func pageQuery(cursor string, size int) (url.Values, error) {
	page := int64(1)
	if strings.TrimSpace(cursor) != "" {
		var e error
		page, e = strconv.ParseInt(strings.TrimSpace(cursor), 10, 64)
		if e != nil || page < 1 {
			return nil, permanent("cursor_invalid", "cursor must be a positive page number")
		}
	}
	if size == 0 {
		size = 25
	}
	if size < 1 || size > 150 {
		return nil, permanent("page_size_invalid", "page size must be between 1 and 150")
	}
	return url.Values{"page": {strconv.FormatInt(page, 10)}, "per_page": {strconv.Itoa(size)}}, nil
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, method, path string, query url.Values, body map[string]any, write bool) (map[string]any, string, error) {
	if e := p.ValidateConfig(connection); e != nil {
		return nil, "", e
	}
	token := strings.TrimSpace(secrets["access_token"])
	if token == "" {
		return nil, "", permanent("access_token_required", "resolved access token is required")
	}
	endpoint, e := url.Parse(baseURL(connection) + path)
	if e != nil {
		return nil, "", connector.PermanentError("intercom_articles.request_invalid", e)
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
		raw, e = json.Marshal(body)
		if e != nil {
			return nil, "", connector.PermanentError("intercom_articles.request_invalid", e)
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}, "Intercom-Version": {apiVersion(connection)}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	response, e := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint.String(), Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, Body: raw, MaxResponseBytes: responseLimit})
	if e != nil {
		if write {
			return nil, "", connector.UncertainError("intercom_articles.network_error", e)
		}
		return nil, "", connector.RetryableError("intercom_articles.network_error", e)
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
		ref = "intercom_articles:article:" + id
	}
	return payload, ref, nil
}
func providerErrorCode(payload map[string]any, status int) string {
	if value := configString(payload, "type"); value != "" {
		return "intercom_articles.provider_" + codeToken(value)
	}
	if errorsValue, ok := payload["errors"].([]any); ok && len(errorsValue) > 0 {
		if first, ok := errorsValue[0].(map[string]any); ok {
			if value := configString(first, "code"); value != "" {
				return "intercom_articles.provider_" + codeToken(value)
			}
		}
	}
	return "intercom_articles.http_" + strconv.Itoa(status)
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
	return permanent(feature+"_unsupported", feature+" is not supported by Intercom Articles")
}
func permanent(code, message string) error {
	return connector.PermanentError("intercom_articles."+code, errors.New(message))
}
func baseURL(connection connector.Connection) string {
	if value := strings.TrimRight(configString(connection.Config, "base_url"), "/"); value != "" {
		return value
	}
	return defaultBaseURL
}
func apiVersion(connection connector.Connection) string {
	if value := configString(connection.Config, "api_version"); value != "" {
		return value
	}
	return defaultAPIVersion
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
