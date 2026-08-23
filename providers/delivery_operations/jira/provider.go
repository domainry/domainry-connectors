// Package jira implements the official Jira delivery-operations Provider.
package jira

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
	ConnectorKey  = "delivery_operations"
	ProviderKey   = "jira"
	responseLimit = 4 << 20
)

type ListProjectsInput struct {
	StartAt    int    `json:"startAt,omitempty"`
	MaxResults int    `json:"maxResults,omitempty"`
	OrderBy    string `json:"orderBy,omitempty"`
	Query      string `json:"query,omitempty"`
	Status     string `json:"status,omitempty"`
}
type ListItemsInput struct {
	JQL           string `json:"jql,omitempty"`
	MaxResults    int    `json:"maxResults,omitempty"`
	NextPageToken string `json:"nextPageToken,omitempty"`
	Fields        any    `json:"fields,omitempty"`
	Expand        any    `json:"expand,omitempty"`
	Properties    any    `json:"properties,omitempty"`
}
type CreateItemInput struct {
	Fields map[string]any `json:"fields"`
}
type UpdateItemInput struct {
	ItemKey         string         `json:"item_key"`
	Fields          map[string]any `json:"fields,omitempty"`
	Update          map[string]any `json:"update,omitempty"`
	HistoryMetadata map[string]any `json:"historyMetadata,omitempty"`
	Properties      any            `json:"properties,omitempty"`
}
type TransitionItemInput struct {
	ItemKey      string         `json:"item_key"`
	TransitionID string         `json:"transition_id"`
	Fields       map[string]any `json:"fields,omitempty"`
}
type Response map[string]any

var (
	ListDeliveryProjects   = connector.CallOperation[ListProjectsInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_delivery_projects", ContractSHA256: "044bd4d279359354f6fe310a889682bc10ce2b4b9f5e6e58cf89a4f5a66e202c", Reliability: readReliability()}
	ListDeliveryItems      = connector.CallOperation[ListItemsInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_delivery_items", ContractSHA256: "b3503478e21cbb172628db3b0e0e4eb10afeb50a0c7451f067369dc4fbadb4a6", Reliability: readReliability()}
	CreateDeliveryItem     = connector.CallOperation[CreateItemInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "create_delivery_item", ContractSHA256: "9a097cf1ba8cda7547fc06582abe5babfc4a4f7a7b58d8d824fc436fdc36cbd6", Reliability: writeReliability()}
	UpdateDeliveryItem     = connector.CallOperation[UpdateItemInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "update_delivery_item", ContractSHA256: "b076f0651614ba18b76b0a70c89e7672495ba0bb0b52f23014e353f7c1666630", Reliability: writeReliability()}
	TransitionDeliveryItem = connector.CallOperation[TransitionItemInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "transition_delivery_item", ContractSHA256: "5d455d95a0b8a58ce17bb61ff1f971ba648863c67de0c3b6bde8424f0e8d150e", Reliability: writeReliability()}
	TestConnection         = connector.CallOperation[struct{}, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection", ContractSHA256: "e105356cce844443110d0fe5f4b4ab76bb3dc52bff4d7abf2147f02fffb0df9f", Reliability: readReliability()}
)

func readReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}
func writeReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNone}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Jira transport is required")
	}
	p := &provider{transport: transport}
	projects, err := connector.BindCall(ListDeliveryProjects, p.listProjects)
	if err != nil {
		return nil, err
	}
	items, err := connector.BindCall(ListDeliveryItems, p.listItems)
	if err != nil {
		return nil, err
	}
	create, err := connector.BindCall(CreateDeliveryItem, p.createItem)
	if err != nil {
		return nil, err
	}
	update, err := connector.BindCall(UpdateDeliveryItem, p.updateItem)
	if err != nil {
		return nil, err
	}
	transition, err := connector.BindCall(TransitionDeliveryItem, p.transitionItem)
	if err != nil {
		return nil, err
	}
	test, err := connector.BindCall(TestConnection, p.callTestConnection)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), projects, items, create, update, transition, test)
	if err != nil {
		return nil, err
	}
	p.Adapter = bound
	return p, nil
}

func schema() connector.ProviderSchema {
	minimum, maximum := float64(1), float64(120)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "base_url", Name: "Jira site URL", Type: connector.ConfigFieldText, Required: true}, {Key: "account_email", Name: "Atlassian account email", Type: connector.ConfigFieldText}, {Key: "project_key", Name: "Default project key", Type: connector.ConfigFieldText}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}}}, SecretFields: []connector.SecretField{{Key: "api_token", Name: "Jira OAuth or API token", Required: true, CredentialKind: connector.SecretCredentialAPIKey, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	raw := config(connection, "base_url")
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || strings.Trim(parsed.Path, "/") != "" {
		return permanent("endpoint_invalid", "Jira site root URL is required")
	}
	if parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname())) {
		return permanent("endpoint_invalid", "HTTPS Jira site or loopback HTTP is required")
	}
	if seconds := intValue(connection.Config["timeout_seconds"], 30); seconds < 1 || seconds > 120 {
		return permanent("timeout_invalid", "timeout_seconds must be between 1 and 120")
	}
	return nil
}

func (p *provider) listProjects(ctx context.Context, request connector.TypedRequest[ListProjectsInput]) (connector.TypedResult[Response], error) {
	q := url.Values{}
	if request.Input.StartAt != 0 {
		q.Set("startAt", strconv.Itoa(request.Input.StartAt))
	}
	if request.Input.MaxResults != 0 {
		q.Set("maxResults", strconv.Itoa(request.Input.MaxResults))
	}
	setQuery(q, "orderBy", request.Input.OrderBy)
	setQuery(q, "query", request.Input.Query)
	setQuery(q, "status", request.Input.Status)
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/rest/api/3/project/search", q, nil, false)
}
func (p *provider) listItems(ctx context.Context, request connector.TypedRequest[ListItemsInput]) (connector.TypedResult[Response], error) {
	jql := strings.TrimSpace(request.Input.JQL)
	if jql == "" {
		if project := config(request.Connection, "project_key"); project != "" {
			jql = "project = " + project + " ORDER BY updated DESC"
		} else {
			jql = "order by updated DESC"
		}
	}
	maximum := request.Input.MaxResults
	if maximum == 0 {
		maximum = 50
	}
	body := map[string]any{"jql": jql, "maxResults": maximum}
	if request.Input.NextPageToken != "" {
		body["nextPageToken"] = request.Input.NextPageToken
	}
	if request.Input.Fields != nil {
		body["fields"] = request.Input.Fields
	}
	if request.Input.Expand != nil {
		body["expand"] = request.Input.Expand
	}
	if request.Input.Properties != nil {
		body["properties"] = request.Input.Properties
	}
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodPost, "/rest/api/3/search/jql", nil, body, false)
}
func (p *provider) createItem(ctx context.Context, request connector.TypedRequest[CreateItemInput]) (connector.TypedResult[Response], error) {
	if len(request.Input.Fields) == 0 {
		return connector.TypedResult[Response]{}, permanent("fields_required", "issue fields are required")
	}
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodPost, "/rest/api/3/issue", nil, map[string]any{"fields": cloneMap(request.Input.Fields)}, true)
}
func (p *provider) updateItem(ctx context.Context, request connector.TypedRequest[UpdateItemInput]) (connector.TypedResult[Response], error) {
	if strings.TrimSpace(request.Input.ItemKey) == "" {
		return connector.TypedResult[Response]{}, permanent("item_key_required", "item_key is required")
	}
	body := map[string]any{}
	if request.Input.Fields != nil {
		body["fields"] = cloneMap(request.Input.Fields)
	}
	if request.Input.Update != nil {
		body["update"] = cloneMap(request.Input.Update)
	}
	if request.Input.HistoryMetadata != nil {
		body["historyMetadata"] = cloneMap(request.Input.HistoryMetadata)
	}
	if request.Input.Properties != nil {
		body["properties"] = request.Input.Properties
	}
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodPut, "/rest/api/3/issue/"+url.PathEscape(request.Input.ItemKey), url.Values{"returnIssue": {"true"}}, body, true)
}
func (p *provider) transitionItem(ctx context.Context, request connector.TypedRequest[TransitionItemInput]) (connector.TypedResult[Response], error) {
	if strings.TrimSpace(request.Input.ItemKey) == "" || strings.TrimSpace(request.Input.TransitionID) == "" {
		return connector.TypedResult[Response]{}, permanent("transition_fields_required", "item_key and transition_id are required")
	}
	body := map[string]any{"transition": map[string]any{"id": request.Input.TransitionID}}
	if request.Input.Fields != nil {
		body["fields"] = cloneMap(request.Input.Fields)
	}
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodPost, "/rest/api/3/issue/"+url.PathEscape(request.Input.ItemKey)+"/transitions", nil, body, true)
}
func (p *provider) callTestConnection(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/rest/api/3/myself", nil, nil, false)
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.callTestConnection(ctx, connector.TypedRequest[struct{}]{Connection: request.Connection, Secrets: request.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, method, path string, query url.Values, body map[string]any, write bool) (connector.TypedResult[Response], error) {
	if err := p.ValidateConfig(connection); err != nil {
		return connector.TypedResult[Response]{}, err
	}
	token := strings.TrimSpace(secrets["api_token"])
	if token == "" {
		return connector.TypedResult[Response]{}, permanent("api_token_required", "resolved Jira API token is required")
	}
	endpoint := strings.TrimRight(config(connection, "base_url"), "/") + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	raw, err := json.Marshal(body)
	if body == nil {
		raw = nil
		err = nil
	}
	if err != nil {
		return connector.TypedResult[Response]{}, permanent("request_invalid", "Jira request body is invalid")
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	authorization := "Bearer " + token
	if email := config(connection, "account_email"); email != "" {
		authorization = "Basic " + base64.StdEncoding.EncodeToString([]byte(email+":"+token))
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint, Headers: headers, SecretHeaders: map[string][]string{"Authorization": {authorization}}, Body: raw, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		if write {
			return connector.TypedResult[Response]{}, connector.UncertainError("jira.network_error", transportErr)
		}
		return connector.TypedResult[Response]{}, connector.RetryableError("jira.network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := Response{}
	valid := len(response.Body) == 0 || json.Unmarshal(response.Body, &payload) == nil
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("Jira returned HTTP %d", response.StatusCode)
		code := jiraErrorCode(payload, response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests {
			return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, connector.RetryableError(code, cause)
		}
		if response.StatusCode == http.StatusRequestTimeout || response.StatusCode >= 500 {
			if write {
				return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, connector.UncertainError(code, cause)
			}
			return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, connector.RetryableError(code, cause)
		}
		return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, connector.PermanentError(code, cause)
	}
	if !valid {
		if write {
			return connector.TypedResult[Response]{ResponseRef: ref}, connector.UncertainError("jira.response_invalid", errors.New("Jira response is invalid JSON"))
		}
		return connector.TypedResult[Response]{ResponseRef: ref}, permanent("response_invalid", "Jira response is invalid JSON")
	}
	if key := mapString(payload, "key"); key != "" {
		ref = "jira:" + key
	} else if id := mapString(payload, "id"); id != "" {
		ref = "jira:" + id
	}
	return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, nil
}

func jiraErrorCode(payload map[string]any, status int) string {
	if messages, ok := payload["errorMessages"].([]any); ok && len(messages) > 0 {
		return fmt.Sprintf("jira.http_%d.validation", status)
	}
	return fmt.Sprintf("jira.http_%d", status)
}
func cloneMap(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	target := make(map[string]any, len(source))
	for key, value := range source {
		target[key] = value
	}
	return target
}
func setQuery(query url.Values, key, value string) {
	if value = strings.TrimSpace(value); value != "" {
		query.Set(key, value)
	}
}
func config(connection connector.Connection, key string) string {
	return mapString(connection.Config, key)
}
func mapString(values map[string]any, key string) string {
	if values == nil {
		return ""
	}
	value, _ := values[key].(string)
	return strings.TrimSpace(value)
}
func intValue(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		return typed
	case float64:
		return int(typed)
	case json.Number:
		result, err := strconv.Atoi(typed.String())
		if err == nil {
			return result
		}
	}
	return fallback
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
func permanent(code, message string) error {
	return connector.PermanentError("jira."+code, errors.New(message))
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
