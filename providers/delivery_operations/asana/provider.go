// Package asana implements the official Asana delivery-operations Provider.
package asana

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
	ConnectorKey   = "delivery_operations"
	ProviderKey    = "asana"
	defaultAPIBase = "https://app.asana.com/api/1.0"
	responseLimit  = 4 << 20
)

type ListProjectsInput struct {
	Workspace    string `json:"workspace,omitempty"`
	WorkspaceGID string `json:"workspace_gid,omitempty"`
	Team         string `json:"team,omitempty"`
	Archived     *bool  `json:"archived,omitempty"`
	Limit        int    `json:"limit,omitempty"`
	PageSize     int    `json:"page_size,omitempty"`
	Offset       string `json:"offset,omitempty"`
	Cursor       string `json:"cursor,omitempty"`
	OptFields    string `json:"opt_fields,omitempty"`
}
type ListItemsInput struct {
	Project        string `json:"project,omitempty"`
	ProjectGID     string `json:"project_gid,omitempty"`
	CompletedSince string `json:"completed_since,omitempty"`
	Limit          int    `json:"limit,omitempty"`
	PageSize       int    `json:"page_size,omitempty"`
	Offset         string `json:"offset,omitempty"`
	Cursor         string `json:"cursor,omitempty"`
	OptFields      string `json:"opt_fields,omitempty"`
}
type CreateItemInput struct {
	Input  map[string]any `json:"input,omitempty"`
	Fields map[string]any `json:"fields,omitempty"`
}
type UpdateItemInput struct {
	ItemKey string         `json:"item_key"`
	Input   map[string]any `json:"input,omitempty"`
	Fields  map[string]any `json:"fields,omitempty"`
}
type TransitionItemInput struct {
	ItemKey      string `json:"item_key"`
	TransitionID string `json:"transition_id"`
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
		return nil, errors.New("Asana transport is required")
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
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "api_base_url", Name: "Asana API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://app.asana.com/api/1.0"`)}, {Key: "workspace_gid", Name: "Default workspace GID", Type: connector.ConfigFieldText}, {Key: "project_gid", Name: "Default project GID", Type: connector.ConfigFieldText}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}}}, SecretFields: []connector.SecretField{{Key: "access_token", Name: "Asana OAuth or personal access token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(apiBase(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return permanent("endpoint_invalid", "valid Asana API endpoint is required")
	}
	if !(parsed.Scheme == "http" && isLoopback(parsed.Hostname())) && (parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), "app.asana.com") || strings.TrimRight(parsed.EscapedPath(), "/") != "/api/1.0") {
		return permanent("endpoint_invalid", "official Asana API v1 endpoint or loopback HTTP is required")
	}
	if seconds := intValue(connection.Config["timeout_seconds"], 30); seconds < 1 || seconds > 120 {
		return permanent("timeout_invalid", "timeout_seconds must be between 1 and 120")
	}
	return nil
}
func (p *provider) listProjects(ctx context.Context, request connector.TypedRequest[ListProjectsInput]) (connector.TypedResult[Response], error) {
	input := request.Input
	workspace := first(input.Workspace, input.WorkspaceGID, config(request.Connection, "workspace_gid", ""))
	if workspace == "" && strings.TrimSpace(input.Team) == "" {
		return connector.TypedResult[Response]{}, permanent("project_scope_required", "workspace or team is required")
	}
	query := url.Values{}
	setQuery(query, "workspace", workspace)
	setQuery(query, "team", input.Team)
	if input.Archived != nil {
		query.Set("archived", strconv.FormatBool(*input.Archived))
	}
	setQuery(query, "opt_fields", input.OptFields)
	setPaging(query, input.Limit, input.PageSize, input.Offset, input.Cursor)
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/projects", query, nil, false)
}
func (p *provider) listItems(ctx context.Context, request connector.TypedRequest[ListItemsInput]) (connector.TypedResult[Response], error) {
	input := request.Input
	project := first(input.Project, input.ProjectGID, config(request.Connection, "project_gid", ""))
	if project == "" {
		return connector.TypedResult[Response]{}, permanent("project_required", "project is required")
	}
	query := url.Values{}
	setQuery(query, "completed_since", input.CompletedSince)
	setQuery(query, "opt_fields", input.OptFields)
	setPaging(query, input.Limit, input.PageSize, input.Offset, input.Cursor)
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/projects/"+url.PathEscape(project)+"/tasks", query, nil, false)
}
func (p *provider) createItem(ctx context.Context, request connector.TypedRequest[CreateItemInput]) (connector.TypedResult[Response], error) {
	input := providerInput(request.Input.Input, request.Input.Fields)
	if mapString(input, "name") == "" {
		return connector.TypedResult[Response]{}, permanent("input_required", "task name is required")
	}
	if input["workspace"] == nil && input["projects"] == nil && input["parent"] == nil {
		if project := config(request.Connection, "project_gid", ""); project != "" {
			input["projects"] = []string{project}
		} else if workspace := config(request.Connection, "workspace_gid", ""); workspace != "" {
			input["workspace"] = workspace
		} else {
			return connector.TypedResult[Response]{}, permanent("task_scope_required", "task workspace, project, or parent is required")
		}
	}
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodPost, "/tasks", nil, map[string]any{"data": input}, true)
}
func (p *provider) updateItem(ctx context.Context, request connector.TypedRequest[UpdateItemInput]) (connector.TypedResult[Response], error) {
	input := providerInput(request.Input.Input, request.Input.Fields)
	if strings.TrimSpace(request.Input.ItemKey) == "" || len(input) == 0 {
		return connector.TypedResult[Response]{}, permanent("update_fields_required", "item_key and update fields are required")
	}
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodPut, "/tasks/"+url.PathEscape(request.Input.ItemKey), nil, map[string]any{"data": input}, true)
}
func (p *provider) transitionItem(ctx context.Context, request connector.TypedRequest[TransitionItemInput]) (connector.TypedResult[Response], error) {
	transition := strings.ToLower(strings.TrimSpace(request.Input.TransitionID))
	if strings.TrimSpace(request.Input.ItemKey) == "" || (transition != "completed" && transition != "incomplete") {
		return connector.TypedResult[Response]{}, permanent("transition_invalid", "item_key and completed or incomplete transition are required")
	}
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodPut, "/tasks/"+url.PathEscape(request.Input.ItemKey), nil, map[string]any{"data": map[string]any{"completed": transition == "completed"}}, true)
}
func (p *provider) callTestConnection(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/users/me", nil, nil, false)
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
	token := strings.TrimSpace(secrets["access_token"])
	if token == "" {
		return connector.TypedResult[Response]{}, permanent("access_token_required", "resolved Asana access token is required")
	}
	endpoint, err := url.Parse(apiBase(connection) + path)
	if err != nil {
		return connector.TypedResult[Response]{}, permanent("request_invalid", "Asana request URL is invalid")
	}
	endpoint.RawQuery = query.Encode()
	var raw []byte
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return connector.TypedResult[Response]{}, permanent("request_invalid", "Asana request body is invalid")
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint.String(), Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, Body: raw, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		if write {
			return connector.TypedResult[Response]{}, connector.UncertainError("asana.network_error", transportErr)
		}
		return connector.TypedResult[Response]{}, connector.RetryableError("asana.network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := Response{}
	valid := len(response.Body) == 0 || json.Unmarshal(response.Body, &payload) == nil
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("Asana returned HTTP %d", response.StatusCode)
		code := "asana." + providerErrorCode(payload, response.StatusCode)
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
			return connector.TypedResult[Response]{ResponseRef: ref}, connector.UncertainError("asana.response_invalid", errors.New("Asana response is invalid JSON"))
		}
		return connector.TypedResult[Response]{ResponseRef: ref}, permanent("response_invalid", "Asana response is invalid JSON")
	}
	if data, ok := payload["data"].(map[string]any); ok {
		if gid := mapString(data, "gid"); gid != "" {
			ref = "asana:" + gid
		}
	}
	return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, nil
}
func providerInput(primary, secondary map[string]any) map[string]any {
	source := primary
	if source == nil {
		source = secondary
	}
	if source == nil {
		return nil
	}
	target := make(map[string]any, len(source))
	for key, value := range source {
		target[key] = value
	}
	return target
}
func providerErrorCode(payload map[string]any, status int) string {
	if items, ok := payload["errors"].([]any); ok && len(items) > 0 {
		if detail, ok := items[0].(map[string]any); ok {
			if phrase := mapString(detail, "phrase"); phrase != "" {
				return sanitizeCode(phrase)
			}
		}
	}
	return "http_" + strconv.Itoa(status)
}
func sanitizeCode(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' || r == '-' {
			return r
		}
		return '_'
	}, value)
}
func setPaging(query url.Values, limit, pageSize int, offset, cursor string) {
	if limit <= 0 {
		limit = pageSize
	}
	if limit > 0 {
		query.Set("limit", strconv.Itoa(limit))
	}
	setQuery(query, "offset", first(offset, cursor))
}
func setQuery(query url.Values, key, value string) {
	if value = strings.TrimSpace(value); value != "" {
		query.Set(key, value)
	}
}
func apiBase(connection connector.Connection) string {
	return strings.TrimRight(config(connection, "api_base_url", defaultAPIBase), "/")
}
func config(connection connector.Connection, key, fallback string) string {
	if value := mapString(connection.Config, key); value != "" {
		return value
	}
	return fallback
}
func mapString(values map[string]any, key string) string {
	if values == nil || values[key] == nil {
		return ""
	}
	value := strings.TrimSpace(fmt.Sprint(values[key]))
	if value == "<nil>" {
		return ""
	}
	return value
}
func intValue(value any, fallback int) int {
	if value == nil {
		return fallback
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(fmt.Sprint(value)))
	if err != nil {
		return 0
	}
	return parsed
}
func first(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
func permanent(code, message string) error {
	return connector.PermanentError("asana."+code, errors.New(message))
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
