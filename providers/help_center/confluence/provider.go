// Package confluence implements the official Confluence Cloud Provider.
package confluence

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
	ProviderKey         = "confluence"
	responseLimit int64 = 4 << 20
)

var ListArticles = connector.CallOperation[ListArticlesInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_articles", ContractSHA256: "a45fa9b5ee9c273cd8046e83b34b67be26b4098f018644e956d25c06cb225dd6", Reliability: readReliability()}
var GetArticle = connector.CallOperation[GetArticleInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "get_article", ContractSHA256: "1916ca191c9366feba71009db3bbf3cdb2290f1ddc3bdd469f4231abe177198d", Reliability: readReliability()}
var SearchArticles = connector.CallOperation[SearchArticlesInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "search_articles", ContractSHA256: "79a5f5f9af7b0ad49508900a3a7f38fb0fbabe138bf399eb89358271a2452f36", Reliability: readReliability()}
var ListCollections = connector.CallOperation[ListCollectionsInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_collections", ContractSHA256: "ffddef53d4878282741447a24b469090a75b893e4033fe7dd933f719c7c14374", Reliability: readReliability()}
var CreateArticle = connector.CallOperation[CreateArticleInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "create_article", ContractSHA256: "cce53c916d333a914c28902f4c8360abbf20a76e06147d95fb891fecc2425f2d", Reliability: writeReliability()}
var UpdateArticle = connector.CallOperation[UpdateArticleInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "update_article", ContractSHA256: "b9057eb433a9bd4e5511b80365d769bd88ed8c3355736c1a2a909ba068eacc95", Reliability: writeReliability()}

type ListArticlesInput struct {
	Cursor       string `json:"cursor,omitempty"`
	PageSize     int    `json:"page_size,omitempty"`
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
type CreateArticleInput struct {
	CollectionID string         `json:"collection_id"`
	Input        CreatePageBody `json:"input"`
}
type UpdateArticleInput struct {
	ArticleID string         `json:"article_id"`
	Input     UpdatePageBody `json:"input"`
}
type PageBody struct {
	Representation string `json:"representation"`
	Value          string `json:"value"`
}
type CreatePageBody struct {
	Status   string    `json:"status,omitempty"`
	Title    string    `json:"title"`
	ParentID string    `json:"parentId,omitempty"`
	Body     *PageBody `json:"body,omitempty"`
	Subtype  string    `json:"subtype,omitempty"`
}
type UpdatePageBody struct {
	ID       string      `json:"id"`
	Status   string      `json:"status"`
	Title    string      `json:"title"`
	SpaceID  string      `json:"spaceId,omitempty"`
	ParentID string      `json:"parentId,omitempty"`
	OwnerID  string      `json:"ownerId,omitempty"`
	Body     PageBody    `json:"body"`
	Version  PageVersion `json:"version"`
}
type PageVersion struct {
	Number    int    `json:"number"`
	Message   string `json:"message,omitempty"`
	MinorEdit *bool  `json:"minorEdit,omitempty"`
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Confluence transport is required")
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
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "base_url", Name: "Atlassian site URL", Type: connector.ConfigFieldText, Required: true}, {Key: "email", Name: "Atlassian account email", Type: connector.ConfigFieldEmail, Required: true}, {Key: "space_id", Name: "Default Confluence space ID", Type: connector.ConfigFieldText, Required: true}, {Key: "space_key", Name: "Default Confluence space key", Type: connector.ConfigFieldText}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}}}, SecretFields: []connector.SecretField{{Key: "api_token", Name: "Atlassian API token", Required: true, CredentialKind: connector.SecretCredentialAPIKey, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(baseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return permanent("endpoint_invalid", "HTTPS or loopback HTTP endpoint is required")
	}
	if email(connection) == "" {
		return permanent("email_required", "Atlassian account email is required")
	}
	if spaceID(connection) == "" {
		return permanent("space_id_required", "default Confluence space ID is required")
	}
	return nil
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/wiki/api/v2/spaces", url.Values{"limit": {"1"}}, nil, false)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, err := json.Marshal(map[string]any{"spaces": payload, "response_ref": ref})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}
func (p *provider) listArticles(ctx context.Context, request connector.TypedRequest[ListArticlesInput]) (connector.TypedResult[map[string]any], error) {
	query, err := pageQuery(request.Input.Cursor, request.Input.PageSize, 250)
	if err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	path := "/wiki/api/v2/spaces/" + url.PathEscape(spaceID(request.Connection)) + "/pages"
	query.Set("status", "current")
	if since := strings.TrimSpace(request.Input.UpdatedSince); since != "" {
		date, parseErr := cqlDate(since)
		if parseErr != nil {
			return connector.TypedResult[map[string]any]{}, parseErr
		}
		path = "/wiki/rest/api/search"
		query, err = pageQuery(request.Input.Cursor, request.Input.PageSize, 100)
		if err != nil {
			return connector.TypedResult[map[string]any]{}, err
		}
		query.Set("cql", `type=page AND lastmodified >= "`+escapeCQL(date)+`"`)
		if key := spaceKey(request.Connection); key != "" {
			query.Set("cql", query.Get("cql")+` AND space="`+escapeCQL(key)+`"`)
		}
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, path, query, nil, false)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) getArticle(ctx context.Context, request connector.TypedRequest[GetArticleInput]) (connector.TypedResult[map[string]any], error) {
	id := strings.TrimSpace(request.Input.ArticleID)
	if id == "" {
		return connector.TypedResult[map[string]any]{}, permanent("article_id_required", "page ID is required")
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/wiki/api/v2/pages/"+url.PathEscape(id), url.Values{"body-format": {"storage"}}, nil, false)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) searchArticles(ctx context.Context, request connector.TypedRequest[SearchArticlesInput]) (connector.TypedResult[map[string]any], error) {
	text := strings.TrimSpace(request.Input.Query)
	if text == "" {
		return connector.TypedResult[map[string]any]{}, permanent("query_required", "search query is required")
	}
	query, err := pageQuery(request.Input.Cursor, request.Input.PageSize, 100)
	if err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	cql := `type=page AND text ~ "` + escapeCQL(text) + `"`
	if key := spaceKey(request.Connection); key != "" {
		cql += ` AND space="` + escapeCQL(key) + `"`
	}
	query.Set("cql", cql)
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/wiki/rest/api/search", query, nil, false)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) listCollections(ctx context.Context, request connector.TypedRequest[ListCollectionsInput]) (connector.TypedResult[map[string]any], error) {
	query, err := pageQuery(request.Input.Cursor, request.Input.PageSize, 250)
	if err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/wiki/api/v2/spaces", query, nil, false)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) createArticle(ctx context.Context, request connector.TypedRequest[CreateArticleInput]) (connector.TypedResult[map[string]any], error) {
	targetSpace := strings.TrimSpace(request.Input.CollectionID)
	if targetSpace == "" {
		return connector.TypedResult[map[string]any]{}, permanent("collection_id_required", "target space ID is required")
	}
	if err := validateCreate(request.Input.Input); err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	body := struct {
		SpaceID string `json:"spaceId"`
		CreatePageBody
	}{SpaceID: targetSpace, CreatePageBody: request.Input.Input}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodPost, "/wiki/api/v2/pages", nil, body, true)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) updateArticle(ctx context.Context, request connector.TypedRequest[UpdateArticleInput]) (connector.TypedResult[map[string]any], error) {
	id := strings.TrimSpace(request.Input.ArticleID)
	if id == "" {
		return connector.TypedResult[map[string]any]{}, permanent("article_id_required", "page ID is required")
	}
	if err := validateUpdate(id, request.Input.Input); err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodPut, "/wiki/api/v2/pages/"+url.PathEscape(id), nil, request.Input.Input, true)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}

func validateCreate(input CreatePageBody) error {
	status := strings.TrimSpace(input.Status)
	if status == "" {
		status = "current"
	}
	if status != "current" && status != "draft" {
		return permanent("status_invalid", "page status must be current or draft")
	}
	if status == "current" && strings.TrimSpace(input.Title) == "" {
		return permanent("title_required", "published page title is required")
	}
	if input.Body != nil {
		return validateBody(*input.Body)
	}
	return nil
}
func validateUpdate(id string, input UpdatePageBody) error {
	if strings.TrimSpace(input.ID) != id {
		return permanent("id_mismatch", "payload id must match article_id")
	}
	if input.Status != "current" && input.Status != "draft" {
		return permanent("status_invalid", "page status must be current or draft")
	}
	if strings.TrimSpace(input.Title) == "" {
		return permanent("title_required", "page title is required")
	}
	if input.Version.Number < 1 {
		return permanent("version_invalid", "next page version number must be positive")
	}
	return validateBody(input.Body)
}
func validateBody(body PageBody) error {
	if body.Representation != "storage" && body.Representation != "atlas_doc_format" {
		return permanent("body_representation_invalid", "body representation must be storage or atlas_doc_format")
	}
	if strings.TrimSpace(body.Value) == "" {
		return permanent("body_value_required", "page body value is required")
	}
	return nil
}
func pageQuery(cursor string, size, maximum int) (url.Values, error) {
	if size == 0 {
		size = 50
	}
	if size < 1 || size > maximum {
		return nil, permanent("page_size_invalid", fmt.Sprintf("page size must be between 1 and %d", maximum))
	}
	query := url.Values{"limit": {strconv.Itoa(size)}}
	if cursor = strings.TrimSpace(cursor); cursor != "" {
		query.Set("cursor", cursor)
	}
	return query, nil
}
func cqlDate(value string) (string, error) {
	if parsed, err := time.Parse("2006-01-02", value); err == nil {
		return parsed.Format("2006-01-02"), nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return "", permanent("updated_since_invalid", "updated_since must be YYYY-MM-DD or RFC3339")
	}
	return parsed.UTC().Format("2006-01-02 15:04"), nil
}
func escapeCQL(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	return strings.ReplaceAll(value, `"`, `\"`)
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
		return nil, "", connector.PermanentError("confluence.request_invalid", err)
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
			return nil, "", connector.PermanentError("confluence.request_invalid", err)
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	credentials := email(connection) + ":" + token
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint.String(), Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Basic " + base64.StdEncoding.EncodeToString([]byte(credentials))}}, Body: raw, MaxResponseBytes: responseLimit})
	if err != nil {
		if write {
			return nil, "", connector.UncertainError("confluence.network_error", err)
		}
		return nil, "", connector.RetryableError("confluence.network_error", err)
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
		ref = "confluence:page:" + id
	}
	return payload, ref, nil
}
func providerErrorCode(payload map[string]any, status int) string {
	if value := configString(payload, "key"); value != "" {
		return "confluence.provider_" + codeToken(value)
	}
	return "confluence.http_" + strconv.Itoa(status)
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
	return connector.PermanentError("confluence."+code, errors.New(message))
}
func baseURL(connection connector.Connection) string {
	return strings.TrimRight(configString(connection.Config, "base_url"), "/")
}
func email(connection connector.Connection) string { return configString(connection.Config, "email") }
func spaceID(connection connector.Connection) string {
	return configString(connection.Config, "space_id")
}
func spaceKey(connection connector.Connection) string {
	return configString(connection.Config, "space_key")
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
