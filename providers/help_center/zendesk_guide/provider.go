// Package zendeskguide implements the official Zendesk Guide Provider.
package zendeskguide

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
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	ConnectorKey        = "help_center"
	ProviderKey         = "zendesk_guide"
	responseLimit int64 = 4 << 20
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
	CollectionID string               `json:"collection_id"`
	Input        CreateArticleRequest `json:"input"`
}
type CreateArticleRequest struct {
	Article           CreateArticleBody `json:"article"`
	NotifySubscribers *bool             `json:"notify_subscribers,omitempty"`
}
type CreateArticleBody struct {
	Title             string   `json:"title"`
	Locale            string   `json:"locale"`
	Body              string   `json:"body,omitempty"`
	AuthorID          *int64   `json:"author_id,omitempty"`
	CommentsDisabled  *bool    `json:"comments_disabled,omitempty"`
	ContentTagIDs     []string `json:"content_tag_ids,omitempty"`
	Draft             *bool    `json:"draft,omitempty"`
	LabelNames        []string `json:"label_names,omitempty"`
	PermissionGroupID *int64   `json:"permission_group_id,omitempty"`
	Promoted          *bool    `json:"promoted,omitempty"`
	UserSegmentID     *int64   `json:"user_segment_id,omitempty"`
	UserSegmentIDs    []int64  `json:"user_segment_ids,omitempty"`
}
type UpdateArticleInput struct {
	ArticleID string               `json:"article_id"`
	Input     UpdateArticleRequest `json:"input"`
}
type UpdateArticleRequest struct {
	Article UpdateArticleBody `json:"article"`
}
type UpdateArticleBody struct {
	AuthorID          *int64   `json:"author_id,omitempty"`
	CommentsDisabled  *bool    `json:"comments_disabled,omitempty"`
	LabelNames        []string `json:"label_names,omitempty"`
	PermissionGroupID *int64   `json:"permission_group_id,omitempty"`
	Position          *int     `json:"position,omitempty"`
	Promoted          *bool    `json:"promoted,omitempty"`
	SectionID         *int64   `json:"section_id,omitempty"`
	UserSegmentID     *int64   `json:"user_segment_id,omitempty"`
	UserSegmentIDs    []int64  `json:"user_segment_ids,omitempty"`
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Zendesk Guide transport is required")
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
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "base_url", Name: "Zendesk account base URL", Type: connector.ConfigFieldText, Required: true}, {Key: "email", Name: "Agent email", Type: connector.ConfigFieldEmail, Required: true}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}}}, SecretFields: []connector.SecretField{{Key: "api_token", Name: "Zendesk API token", Required: true, CredentialKind: connector.SecretCredentialAPIKey, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(baseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return permanent("endpoint_invalid", "HTTPS or loopback HTTP endpoint is required")
	}
	if email(connection) == "" {
		return permanent("email_required", "agent email is required")
	}
	return nil
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/api/v2/help_center/categories", url.Values{"page[size]": {"1"}}, nil, false)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, err := json.Marshal(map[string]any{"categories": payload, "response_ref": ref})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}
func (p *provider) listArticles(ctx context.Context, request connector.TypedRequest[ListArticlesInput]) (connector.TypedResult[map[string]any], error) {
	query, err := cursorQuery(request.Input.Cursor, request.Input.PageSize)
	if err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	path := "/api/v2/help_center/articles"
	if since := strings.TrimSpace(request.Input.UpdatedSince); since != "" {
		start, parseErr := unixTimestamp(since)
		if parseErr != nil {
			return connector.TypedResult[map[string]any]{}, parseErr
		}
		path = "/api/v2/help_center/incremental/articles"
		query.Set("start_time", start)
	} else if locale := strings.TrimSpace(request.Input.Locale); locale != "" {
		path = "/api/v2/help_center/" + url.PathEscape(locale) + "/articles"
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, path, query, nil, false)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) getArticle(ctx context.Context, request connector.TypedRequest[GetArticleInput]) (connector.TypedResult[map[string]any], error) {
	id := strings.TrimSpace(request.Input.ArticleID)
	if id == "" {
		return connector.TypedResult[map[string]any]{}, permanent("article_id_required", "article ID is required")
	}
	path := "/api/v2/help_center/articles/" + url.PathEscape(id)
	if locale := strings.TrimSpace(request.Input.Locale); locale != "" {
		path = "/api/v2/help_center/" + url.PathEscape(locale) + "/articles/" + url.PathEscape(id)
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, path, nil, nil, false)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) searchArticles(ctx context.Context, request connector.TypedRequest[SearchArticlesInput]) (connector.TypedResult[map[string]any], error) {
	text := strings.TrimSpace(request.Input.Query)
	if text == "" {
		return connector.TypedResult[map[string]any]{}, permanent("query_required", "search query is required")
	}
	query, err := offsetQuery(request.Input.Cursor, request.Input.PageSize)
	if err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	query.Set("query", text)
	if locale := strings.TrimSpace(request.Input.Locale); locale != "" {
		query.Set("locale", locale)
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/api/v2/help_center/articles/search", query, nil, false)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) listCollections(ctx context.Context, request connector.TypedRequest[ListCollectionsInput]) (connector.TypedResult[map[string]any], error) {
	query, err := cursorQuery(request.Input.Cursor, request.Input.PageSize)
	if err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	path := "/api/v2/help_center/categories"
	if locale := strings.TrimSpace(request.Input.Locale); locale != "" {
		path = "/api/v2/help_center/" + url.PathEscape(locale) + "/categories"
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, path, query, nil, false)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) createArticle(ctx context.Context, request connector.TypedRequest[CreateArticleInput]) (connector.TypedResult[map[string]any], error) {
	sectionID := strings.TrimSpace(request.Input.CollectionID)
	article := request.Input.Input.Article
	if sectionID == "" {
		return connector.TypedResult[map[string]any]{}, permanent("collection_id_required", "section ID is required")
	}
	if strings.TrimSpace(article.Title) == "" {
		return connector.TypedResult[map[string]any]{}, permanent("title_required", "article title is required")
	}
	locale := strings.TrimSpace(article.Locale)
	if locale == "" {
		return connector.TypedResult[map[string]any]{}, permanent("locale_required", "article locale is required")
	}
	if err := validateAudience(article.UserSegmentID, article.UserSegmentIDs); err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	path := "/api/v2/help_center/" + url.PathEscape(locale) + "/sections/" + url.PathEscape(sectionID) + "/articles"
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodPost, path, nil, request.Input.Input, true)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) updateArticle(ctx context.Context, request connector.TypedRequest[UpdateArticleInput]) (connector.TypedResult[map[string]any], error) {
	id := strings.TrimSpace(request.Input.ArticleID)
	if id == "" {
		return connector.TypedResult[map[string]any]{}, permanent("article_id_required", "article ID is required")
	}
	article := request.Input.Input.Article
	if err := validateAudience(article.UserSegmentID, article.UserSegmentIDs); err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	if emptyUpdate(article) {
		return connector.TypedResult[map[string]any]{}, permanent("update_fields_required", "at least one supported article metadata field is required")
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodPut, "/api/v2/help_center/articles/"+url.PathEscape(id), nil, request.Input.Input, true)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}

func validateAudience(single *int64, multiple []int64) error {
	if single != nil && multiple != nil {
		return permanent("user_segments_conflict", "user_segment_id and user_segment_ids are mutually exclusive")
	}
	return nil
}
func emptyUpdate(input UpdateArticleBody) bool {
	return input.AuthorID == nil && input.CommentsDisabled == nil && input.LabelNames == nil && input.PermissionGroupID == nil && input.Position == nil && input.Promoted == nil && input.SectionID == nil && input.UserSegmentID == nil && input.UserSegmentIDs == nil
}
func cursorQuery(cursor string, size int) (url.Values, error) {
	if size == 0 {
		size = 50
	}
	if size < 1 || size > 100 {
		return nil, permanent("page_size_invalid", "page size must be between 1 and 100")
	}
	query := url.Values{"page[size]": {strconv.Itoa(size)}}
	if cursor = strings.TrimSpace(cursor); cursor != "" {
		query.Set("page[after]", cursor)
	}
	return query, nil
}
func offsetQuery(cursor string, size int) (url.Values, error) {
	page := 1
	if cursor = strings.TrimSpace(cursor); cursor != "" {
		var err error
		page, err = strconv.Atoi(cursor)
		if err != nil || page < 1 {
			return nil, permanent("cursor_invalid", "search cursor must be a positive page number")
		}
	}
	if size == 0 {
		size = 25
	}
	if size < 1 || size > 100 {
		return nil, permanent("page_size_invalid", "page size must be between 1 and 100")
	}
	if page*size > 1000 {
		return nil, permanent("pagination_window_invalid", "page multiplied by page_size cannot exceed 1000")
	}
	return url.Values{"page": {strconv.Itoa(page)}, "per_page": {strconv.Itoa(size)}}, nil
}
func unixTimestamp(value string) (string, error) {
	if epoch, err := strconv.ParseInt(value, 10, 64); err == nil && epoch >= 0 {
		return strconv.FormatInt(epoch, 10), nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return "", permanent("updated_since_invalid", "updated_since must be a non-negative Unix timestamp or RFC3339 value")
	}
	return strconv.FormatInt(parsed.Unix(), 10), nil
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, method, path string, query url.Values, body any, write bool) (map[string]any, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", err
	}
	token := strings.TrimSpace(secrets["api_token"])
	if token == "" {
		return nil, "", permanent("api_token_required", "resolved API token is required")
	}
	endpoint, err := url.Parse(baseURL(connection) + path)
	if err != nil {
		return nil, "", connector.PermanentError("zendesk_guide.request_invalid", err)
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
			return nil, "", connector.PermanentError("zendesk_guide.request_invalid", err)
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	credentials := email(connection) + "/token:" + token
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint.String(), Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Basic " + base64.StdEncoding.EncodeToString([]byte(credentials))}}, Body: raw, MaxResponseBytes: responseLimit})
	if err != nil {
		if write {
			return nil, "", connector.UncertainError("zendesk_guide.network_error", err)
		}
		return nil, "", connector.RetryableError("zendesk_guide.network_error", err)
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
			ref = "zendesk_guide:article:" + id
		}
	}
	return payload, ref, nil
}
func providerErrorCode(payload map[string]any, status int) string {
	if value := configString(payload, "error"); value != "" {
		return "zendesk_guide.provider_" + codeToken(value)
	}
	return "zendesk_guide.http_" + strconv.Itoa(status)
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
func permanent(code, message string) error {
	return connector.PermanentError("zendesk_guide."+code, errors.New(message))
}
func baseURL(connection connector.Connection) string {
	return strings.TrimRight(configString(connection.Config, "base_url"), "/")
}
func email(connection connector.Connection) string { return configString(connection.Config, "email") }
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
