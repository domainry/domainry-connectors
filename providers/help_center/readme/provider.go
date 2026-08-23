// Package readme implements the official ReadMe Guides Provider.
package readme

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
	ProviderKey          = "readme"
	defaultBaseURL       = "https://api.readme.com/v2"
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
	CollectionID string          `json:"collection_id"`
	Input        CreateGuideBody `json:"input"`
}
type UpdateArticleInput struct {
	ArticleID string          `json:"article_id"`
	Input     UpdateGuideBody `json:"input"`
}
type GuideContent struct {
	Body string `json:"body"`
	Type string `json:"type,omitempty"`
}
type CreateGuideBody struct {
	Title         string          `json:"title"`
	Slug          string          `json:"slug,omitempty"`
	Content       *GuideContent   `json:"content,omitempty"`
	State         string          `json:"state,omitempty"`
	Type          string          `json:"type,omitempty"`
	AllowCrawlers string          `json:"allow_crawlers,omitempty"`
	Position      *float64        `json:"position,omitempty"`
	Appearance    json.RawMessage `json:"appearance,omitempty"`
	Metadata      json.RawMessage `json:"metadata,omitempty"`
	Parent        json.RawMessage `json:"parent,omitempty"`
	Privacy       json.RawMessage `json:"privacy,omitempty"`
}
type UpdateGuideBody struct {
	Title         string          `json:"title,omitempty"`
	Slug          string          `json:"slug,omitempty"`
	Content       *GuideContent   `json:"content,omitempty"`
	State         string          `json:"state,omitempty"`
	Type          string          `json:"type,omitempty"`
	AllowCrawlers string          `json:"allow_crawlers,omitempty"`
	Position      *float64        `json:"position,omitempty"`
	Appearance    json.RawMessage `json:"appearance,omitempty"`
	Category      json.RawMessage `json:"category,omitempty"`
	Metadata      json.RawMessage `json:"metadata,omitempty"`
	Parent        json.RawMessage `json:"parent,omitempty"`
	Privacy       json.RawMessage `json:"privacy,omitempty"`
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("ReadMe transport is required")
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
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "base_url", Name: "ReadMe API v2 base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://api.readme.com/v2"`)}, {Key: "branch", Name: "Branch", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"stable"`)}, {Key: "collection_id", Name: "Default guide category title", Type: connector.ConfigFieldText, Required: true}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &min, Max: &max}}}, SecretFields: []connector.SecretField{{Key: "api_key", Name: "ReadMe API key", Required: true, CredentialKind: connector.SecretCredentialAPIKey, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, e := url.Parse(baseURL(connection))
	if e != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return permanent("endpoint_invalid", "HTTPS or loopback HTTP endpoint is required")
	}
	if branch(connection) == "" {
		return permanent("branch_required", "branch is required")
	}
	if collectionID(connection) == "" {
		return permanent("collection_id_required", "default guide category title is required")
	}
	return nil
}
func (p *provider) TestConnection(ctx context.Context, r connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	payload, ref, e := p.execute(ctx, r.Connection, r.Secrets, http.MethodGet, "/projects/me", nil, nil, false)
	if e != nil {
		return connector.TestConnectionResult{}, e
	}
	details, e := json.Marshal(map[string]any{"project": payload, "response_ref": ref})
	if e != nil {
		return connector.TestConnectionResult{}, e
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}
func (p *provider) listArticles(ctx context.Context, r connector.TypedRequest[ListArticlesInput]) (connector.TypedResult[map[string]any], error) {
	if strings.TrimSpace(r.Input.Locale) != "" {
		return connector.TypedResult[map[string]any]{}, unsupported("locale")
	}
	if strings.TrimSpace(r.Input.UpdatedSince) != "" {
		return connector.TypedResult[map[string]any]{}, unsupported("incremental_filter")
	}
	q, e := pageQuery(r.Input.Cursor, r.Input.PageSize, 100)
	if e != nil {
		return connector.TypedResult[map[string]any]{}, e
	}
	path := "/branches/" + url.PathEscape(branch(r.Connection)) + "/categories/guides/" + url.PathEscape(collectionID(r.Connection)) + "/pages"
	payload, ref, e := p.execute(ctx, r.Connection, r.Secrets, http.MethodGet, path, q, nil, false)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, e
}
func (p *provider) getArticle(ctx context.Context, r connector.TypedRequest[GetArticleInput]) (connector.TypedResult[map[string]any], error) {
	if strings.TrimSpace(r.Input.Locale) != "" {
		return connector.TypedResult[map[string]any]{}, unsupported("locale")
	}
	id := strings.TrimSpace(r.Input.ArticleID)
	if id == "" {
		return connector.TypedResult[map[string]any]{}, permanent("article_id_required", "article slug is required")
	}
	path := "/branches/" + url.PathEscape(branch(r.Connection)) + "/guides/" + url.PathEscape(id)
	payload, ref, e := p.execute(ctx, r.Connection, r.Secrets, http.MethodGet, path, nil, nil, false)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, e
}
func (p *provider) searchArticles(ctx context.Context, r connector.TypedRequest[SearchArticlesInput]) (connector.TypedResult[map[string]any], error) {
	if strings.TrimSpace(r.Input.Locale) != "" {
		return connector.TypedResult[map[string]any]{}, unsupported("locale")
	}
	text := strings.TrimSpace(r.Input.Query)
	if text == "" {
		return connector.TypedResult[map[string]any]{}, permanent("query_required", "search query is required")
	}
	q, e := pageQuery(r.Input.Cursor, r.Input.PageSize, 50)
	if e != nil {
		return connector.TypedResult[map[string]any]{}, e
	}
	page, _ := strconv.Atoi(q.Get("page"))
	perPage, _ := strconv.Atoi(q.Get("per_page"))
	if page*perPage > 1000 {
		return connector.TypedResult[map[string]any]{}, permanent("pagination_window_invalid", "page multiplied by page_size cannot exceed 1000")
	}
	q.Set("query", text)
	q.Set("section", "guides")
	q.Set("version", branch(r.Connection))
	payload, ref, e := p.execute(ctx, r.Connection, r.Secrets, http.MethodGet, "/search", q, nil, false)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, e
}
func (p *provider) listCollections(ctx context.Context, r connector.TypedRequest[ListCollectionsInput]) (connector.TypedResult[map[string]any], error) {
	if strings.TrimSpace(r.Input.Locale) != "" {
		return connector.TypedResult[map[string]any]{}, unsupported("locale")
	}
	if strings.TrimSpace(r.Input.Cursor) != "" || r.Input.PageSize != 0 {
		return connector.TypedResult[map[string]any]{}, unsupported("collection_pagination")
	}
	path := "/branches/" + url.PathEscape(branch(r.Connection)) + "/categories/guides"
	payload, ref, e := p.execute(ctx, r.Connection, r.Secrets, http.MethodGet, path, nil, nil, false)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, e
}
func (p *provider) createArticle(ctx context.Context, r connector.TypedRequest[CreateArticleInput]) (connector.TypedResult[map[string]any], error) {
	category := strings.TrimSpace(r.Input.CollectionID)
	if category == "" {
		return connector.TypedResult[map[string]any]{}, permanent("collection_id_required", "guide category title is required")
	}
	body, e := createBody(r.Connection, category, r.Input.Input)
	if e != nil {
		return connector.TypedResult[map[string]any]{}, e
	}
	path := "/branches/" + url.PathEscape(branch(r.Connection)) + "/guides"
	payload, ref, e := p.execute(ctx, r.Connection, r.Secrets, http.MethodPost, path, nil, body, true)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, e
}
func (p *provider) updateArticle(ctx context.Context, r connector.TypedRequest[UpdateArticleInput]) (connector.TypedResult[map[string]any], error) {
	id := strings.TrimSpace(r.Input.ArticleID)
	if id == "" {
		return connector.TypedResult[map[string]any]{}, permanent("article_id_required", "article slug is required")
	}
	body, e := updateBody(r.Input.Input)
	if e != nil {
		return connector.TypedResult[map[string]any]{}, e
	}
	path := "/branches/" + url.PathEscape(branch(r.Connection)) + "/guides/" + url.PathEscape(id)
	payload, ref, e := p.execute(ctx, r.Connection, r.Secrets, http.MethodPatch, path, nil, body, true)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, e
}

func createBody(connection connector.Connection, category string, input CreateGuideBody) (map[string]any, error) {
	if strings.TrimSpace(input.Title) == "" {
		return nil, permanent("title_required", "guide title is required")
	}
	if e := validateEnums(input.State, input.Type, input.AllowCrawlers); e != nil {
		return nil, e
	}
	body := map[string]any{"title": strings.TrimSpace(input.Title), "category": map[string]any{"uri": categoryURI(connection, category)}}
	if e := copyGuideFields(body, input.Slug, input.Content, input.State, input.Type, input.AllowCrawlers, input.Position, map[string]json.RawMessage{"appearance": input.Appearance, "metadata": input.Metadata, "parent": input.Parent, "privacy": input.Privacy}); e != nil {
		return nil, e
	}
	return body, nil
}
func updateBody(input UpdateGuideBody) (map[string]any, error) {
	if e := validateEnums(input.State, input.Type, input.AllowCrawlers); e != nil {
		return nil, e
	}
	body := map[string]any{}
	if value := strings.TrimSpace(input.Title); value != "" {
		body["title"] = value
	}
	objects := map[string]json.RawMessage{"appearance": input.Appearance, "category": input.Category, "metadata": input.Metadata, "parent": input.Parent, "privacy": input.Privacy}
	if e := copyGuideFields(body, input.Slug, input.Content, input.State, input.Type, input.AllowCrawlers, input.Position, objects); e != nil {
		return nil, e
	}
	if len(body) == 0 {
		return nil, permanent("update_fields_required", "at least one supported guide field is required")
	}
	return body, nil
}
func copyGuideFields(body map[string]any, slug string, content *GuideContent, state, guideType, crawlers string, position *float64, objects map[string]json.RawMessage) error {
	if value := strings.TrimSpace(slug); value != "" {
		body["slug"] = value
	}
	if content != nil {
		contentType := strings.TrimSpace(content.Type)
		if contentType == "" {
			contentType = "markdown"
		}
		if contentType != "markdown" && contentType != "html" {
			return permanent("content_type_invalid", "content type must be markdown or html")
		}
		body["content"] = map[string]any{"body": content.Body, "type": contentType}
	}
	for key, value := range map[string]string{"state": state, "type": guideType, "allow_crawlers": crawlers} {
		if value = strings.TrimSpace(value); value != "" {
			body[key] = value
		}
	}
	if position != nil {
		if *position < 0 {
			return permanent("position_invalid", "position cannot be negative")
		}
		body["position"] = *position
	}
	for key, raw := range objects {
		if len(raw) == 0 {
			continue
		}
		var value map[string]any
		if json.Unmarshal(raw, &value) != nil || value == nil {
			return permanent(key+"_invalid", key+" must be a JSON object")
		}
		body[key] = value
	}
	return nil
}
func validateEnums(state, guideType, crawlers string) error {
	state = strings.TrimSpace(state)
	if state != "" && state != "current" && state != "deprecated" {
		return permanent("state_invalid", "state must be current or deprecated")
	}
	guideType = strings.TrimSpace(guideType)
	if guideType != "" && guideType != "basic" && guideType != "link" {
		return permanent("type_invalid", "guide type must be basic or link")
	}
	crawlers = strings.TrimSpace(crawlers)
	if crawlers != "" && crawlers != "enabled" && crawlers != "disabled" {
		return permanent("allow_crawlers_invalid", "allow_crawlers must be enabled or disabled")
	}
	return nil
}
func categoryURI(connection connector.Connection, category string) string {
	return "/branches/" + url.PathEscape(branch(connection)) + "/categories/guides/" + url.PathEscape(category)
}
func pageQuery(cursor string, size, max int) (url.Values, error) {
	page := 1
	if strings.TrimSpace(cursor) != "" {
		var e error
		page, e = strconv.Atoi(strings.TrimSpace(cursor))
		if e != nil || page < 1 {
			return nil, permanent("cursor_invalid", "cursor must be a positive page number")
		}
	}
	if size == 0 {
		size = 50
	}
	if size < 1 || size > max {
		return nil, permanent("page_size_invalid", fmt.Sprintf("page size must be between 1 and %d", max))
	}
	return url.Values{"page": {strconv.Itoa(page)}, "per_page": {strconv.Itoa(size)}}, nil
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, method, path string, query url.Values, body map[string]any, write bool) (map[string]any, string, error) {
	if e := p.ValidateConfig(connection); e != nil {
		return nil, "", e
	}
	key := strings.TrimSpace(secrets["api_key"])
	if key == "" {
		return nil, "", permanent("api_key_required", "resolved API key is required")
	}
	endpoint, e := url.Parse(baseURL(connection) + path)
	if e != nil {
		return nil, "", connector.PermanentError("readme.request_invalid", e)
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
			return nil, "", connector.PermanentError("readme.request_invalid", e)
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	response, e := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint.String(), Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + key}}, Body: raw, MaxResponseBytes: responseLimit})
	if e != nil {
		if write {
			return nil, "", connector.UncertainError("readme.network_error", e)
		}
		return nil, "", connector.RetryableError("readme.network_error", e)
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
	for _, field := range []string{"slug", "uri", "id"} {
		if value := configString(payload, field); value != "" {
			ref = "readme:guide:" + value
			break
		}
	}
	return payload, ref, nil
}
func providerErrorCode(payload map[string]any, status int) string {
	if value := configString(payload, "error"); value != "" {
		return "readme.provider_" + codeToken(value)
	}
	return "readme.http_" + strconv.Itoa(status)
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
	return permanent(feature+"_unsupported", feature+" is not supported by ReadMe")
}
func permanent(code, message string) error {
	return connector.PermanentError("readme."+code, errors.New(message))
}
func baseURL(connection connector.Connection) string {
	if value := strings.TrimRight(configString(connection.Config, "base_url"), "/"); value != "" {
		return value
	}
	return defaultBaseURL
}
func branch(connection connector.Connection) string { return configString(connection.Config, "branch") }
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
