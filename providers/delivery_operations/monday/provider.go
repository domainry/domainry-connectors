// Package monday implements the official monday.com delivery-operations Provider.
package monday

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
	ConnectorKey      = "delivery_operations"
	ProviderKey       = "monday"
	defaultEndpoint   = "https://api.monday.com/v2"
	defaultAPIVersion = "2026-04"
	responseLimit     = 4 << 20
)

var apiVersionPattern = regexp.MustCompile(`^[0-9]{4}-(0[1-9]|1[0-2])$`)

type ListProjectsInput struct {
	PageSize     int `json:"page_size,omitempty"`
	MaxResults   int `json:"maxResults,omitempty"`
	Limit        int `json:"limit,omitempty"`
	Page         int `json:"page,omitempty"`
	BoardIDs     any `json:"board_ids,omitempty"`
	WorkspaceIDs any `json:"workspace_ids,omitempty"`
}
type ListItemsInput struct {
	BoardID       string `json:"board_id,omitempty"`
	Project       string `json:"project,omitempty"`
	ProjectGID    string `json:"project_gid,omitempty"`
	PageSize      int    `json:"page_size,omitempty"`
	MaxResults    int    `json:"maxResults,omitempty"`
	Limit         int    `json:"limit,omitempty"`
	Cursor        string `json:"cursor,omitempty"`
	NextPageToken string `json:"nextPageToken,omitempty"`
}
type CreateItemInput struct {
	BoardID    string         `json:"board_id,omitempty"`
	Project    string         `json:"project,omitempty"`
	ProjectGID string         `json:"project_gid,omitempty"`
	Input      map[string]any `json:"input,omitempty"`
	Fields     map[string]any `json:"fields,omitempty"`
}
type UpdateItemInput struct {
	BoardID    string         `json:"board_id,omitempty"`
	Project    string         `json:"project,omitempty"`
	ProjectGID string         `json:"project_gid,omitempty"`
	ItemKey    string         `json:"item_key"`
	Input      map[string]any `json:"input,omitempty"`
	Fields     map[string]any `json:"fields,omitempty"`
}
type TransitionItemInput struct {
	BoardID        string `json:"board_id,omitempty"`
	Project        string `json:"project,omitempty"`
	ProjectGID     string `json:"project_gid,omitempty"`
	ItemKey        string `json:"item_key"`
	TransitionID   string `json:"transition_id"`
	StatusColumnID string `json:"status_column_id,omitempty"`
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
		return nil, errors.New("monday.com transport is required")
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
	min, max := float64(1), float64(120)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "endpoint", Name: "monday.com GraphQL endpoint", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://api.monday.com/v2"`)}, {Key: "api_version", Name: "monday.com API version", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"2026-04"`)}, {Key: "board_id", Name: "Default board ID", Type: connector.ConfigFieldText}, {Key: "status_column_id", Name: "Default status column ID", Type: connector.ConfigFieldText}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &min, Max: &max}}}, SecretFields: []connector.SecretField{{Key: "api_token", Name: "monday.com API or OAuth token", Required: true, CredentialKind: connector.SecretCredentialAPIKey, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(endpoint(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return permanent("endpoint_invalid", "valid monday.com GraphQL endpoint is required")
	}
	if !(parsed.Scheme == "http" && isLoopback(parsed.Hostname())) && (parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), "api.monday.com") || strings.TrimRight(parsed.EscapedPath(), "/") != "/v2") {
		return permanent("endpoint_invalid", "official monday.com GraphQL endpoint or loopback HTTP is required")
	}
	if !apiVersionPattern.MatchString(config(connection, "api_version", defaultAPIVersion)) {
		return permanent("api_version_invalid", "api_version must use YYYY-MM")
	}
	if seconds := intValue(connection.Config["timeout_seconds"], 30); seconds < 1 || seconds > 120 {
		return permanent("timeout_invalid", "timeout_seconds must be between 1 and 120")
	}
	return nil
}

func (p *provider) listProjects(ctx context.Context, r connector.TypedRequest[ListProjectsInput]) (connector.TypedResult[Response], error) {
	page := r.Input.Page
	if page <= 0 {
		page = 1
	}
	variables := map[string]any{"limit": firstPositive(r.Input.PageSize, r.Input.MaxResults, r.Input.Limit, 50), "page": page, "ids": optionalIDs(r.Input.BoardIDs), "workspace": optionalIDs(r.Input.WorkspaceIDs)}
	return p.execute(ctx, r.Connection, r.Secrets, `query Boards($limit:Int,$page:Int,$ids:[ID!],$workspace:[ID!]){boards(limit:$limit,page:$page,ids:$ids,workspace_ids:$workspace){id name state board_kind url updated_at columns{id title type settings_str}}}`, variables, false)
}
func (p *provider) listItems(ctx context.Context, r connector.TypedRequest[ListItemsInput]) (connector.TypedResult[Response], error) {
	board, err := boardID(r.Connection, r.Input.BoardID, r.Input.Project, r.Input.ProjectGID)
	if err != nil {
		return connector.TypedResult[Response]{}, err
	}
	variables := map[string]any{"board": []string{board}, "limit": firstPositive(r.Input.PageSize, r.Input.MaxResults, r.Input.Limit, 50), "cursor": nullable(firstString(r.Input.Cursor, r.Input.NextPageToken))}
	return p.execute(ctx, r.Connection, r.Secrets, `query Items($board:[ID!]!,$limit:Int!,$cursor:String){boards(ids:$board){id name items_page(limit:$limit,cursor:$cursor){cursor items{id name url created_at updated_at group{id title} column_values{id type text value}}}}}`, variables, false)
}
func (p *provider) createItem(ctx context.Context, r connector.TypedRequest[CreateItemInput]) (connector.TypedResult[Response], error) {
	board, err := boardID(r.Connection, r.Input.BoardID, r.Input.Project, r.Input.ProjectGID)
	if err != nil {
		return connector.TypedResult[Response]{}, err
	}
	input := providerInput(r.Input.Input, r.Input.Fields)
	name := firstMapString(input, "item_name", "name")
	if name == "" {
		return connector.TypedResult[Response]{}, permanent("item_name_required", "item_name is required")
	}
	values, err := jsonValue(input["column_values"], map[string]any{})
	if err != nil {
		return connector.TypedResult[Response]{}, err
	}
	variables := map[string]any{"board": board, "group": nullable(input["group_id"]), "name": name, "values": values}
	return p.execute(ctx, r.Connection, r.Secrets, `mutation Create($board:ID!,$group:String,$name:String!,$values:JSON!){create_item(board_id:$board,group_id:$group,item_name:$name,column_values:$values){id name url}}`, variables, true)
}
func (p *provider) updateItem(ctx context.Context, r connector.TypedRequest[UpdateItemInput]) (connector.TypedResult[Response], error) {
	board, err := boardID(r.Connection, r.Input.BoardID, r.Input.Project, r.Input.ProjectGID)
	if err != nil {
		return connector.TypedResult[Response]{}, err
	}
	input := providerInput(r.Input.Input, r.Input.Fields)
	value := input["column_values"]
	if value == nil {
		value = input["values"]
	}
	values, err := jsonValue(value, nil)
	if strings.TrimSpace(r.Input.ItemKey) == "" || err != nil {
		return connector.TypedResult[Response]{}, permanent("update_fields_required", "item_key and valid column values are required")
	}
	return p.execute(ctx, r.Connection, r.Secrets, `mutation Update($board:ID!,$item:ID!,$values:JSON!){change_multiple_column_values(board_id:$board,item_id:$item,column_values:$values){id name url}}`, map[string]any{"board": board, "item": r.Input.ItemKey, "values": values}, true)
}
func (p *provider) transitionItem(ctx context.Context, r connector.TypedRequest[TransitionItemInput]) (connector.TypedResult[Response], error) {
	board, err := boardID(r.Connection, r.Input.BoardID, r.Input.Project, r.Input.ProjectGID)
	if err != nil {
		return connector.TypedResult[Response]{}, err
	}
	column := firstString(r.Input.StatusColumnID, config(r.Connection, "status_column_id", ""))
	if strings.TrimSpace(r.Input.ItemKey) == "" || strings.TrimSpace(r.Input.TransitionID) == "" || column == "" {
		return connector.TypedResult[Response]{}, permanent("transition_fields_required", "item_key, transition_id, and status column are required")
	}
	status := any(map[string]any{"label": r.Input.TransitionID})
	if strings.HasPrefix(strings.TrimSpace(r.Input.TransitionID), "{") {
		decoded := map[string]any{}
		if json.Unmarshal([]byte(r.Input.TransitionID), &decoded) != nil {
			return connector.TypedResult[Response]{}, permanent("transition_invalid", "transition JSON is invalid")
		}
		status = decoded
	}
	values, _ := jsonValue(map[string]any{column: status}, nil)
	return p.execute(ctx, r.Connection, r.Secrets, `mutation Transition($board:ID!,$item:ID!,$values:JSON!){change_multiple_column_values(board_id:$board,item_id:$item,column_values:$values){id name url}}`, map[string]any{"board": board, "item": r.Input.ItemKey, "values": values}, true)
}
func (p *provider) callTestConnection(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	return p.execute(ctx, r.Connection, r.Secrets, `query Viewer { me { id name email account { id name } } }`, nil, false)
}
func (p *provider) TestConnection(ctx context.Context, r connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.callTestConnection(ctx, connector.TypedRequest[struct{}]{Connection: r.Connection, Secrets: r.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, query string, variables map[string]any, write bool) (connector.TypedResult[Response], error) {
	if err := p.ValidateConfig(connection); err != nil {
		return connector.TypedResult[Response]{}, err
	}
	token := strings.TrimSpace(secrets["api_token"])
	if token == "" {
		return connector.TypedResult[Response]{}, permanent("api_token_required", "resolved monday.com API token is required")
	}
	raw, err := json.Marshal(map[string]any{"query": query, "variables": variables})
	if err != nil {
		return connector.TypedResult[Response]{}, permanent("request_invalid", "monday.com GraphQL request is invalid")
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodPost, URL: endpoint(connection), Headers: map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/json"}, "API-Version": {config(connection, "api_version", defaultAPIVersion)}}, SecretHeaders: map[string][]string{"Authorization": {token}}, Body: raw, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		if write {
			return connector.TypedResult[Response]{}, connector.UncertainError("monday.network_error", transportErr)
		}
		return connector.TypedResult[Response]{}, connector.RetryableError("monday.network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := Response{}
	valid := len(response.Body) == 0 || json.Unmarshal(response.Body, &payload) == nil
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("monday.com returned HTTP %d", response.StatusCode)
		code := fmt.Sprintf("monday.http_%d", response.StatusCode)
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
			return connector.TypedResult[Response]{ResponseRef: ref}, connector.UncertainError("monday.response_invalid", errors.New("monday.com response is invalid JSON"))
		}
		return connector.TypedResult[Response]{ResponseRef: ref}, permanent("response_invalid", "monday.com response is invalid JSON")
	}
	if list, ok := payload["errors"].([]any); ok && len(list) > 0 {
		code, retryable := mondayGraphQLError(list)
		if retryable {
			return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, connector.RetryableError(code, errors.New("monday.com rate limited the operation"))
		}
		return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, connector.PermanentError(code, errors.New("monday.com rejected the GraphQL operation"))
	}
	if id := mondayRef(payload); id != "" {
		ref = "monday:" + id
	}
	return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, nil
}

func mondayGraphQLError(items []any) (string, bool) {
	code := "monday.graphql_error"
	retryable := false
	for _, raw := range items {
		item, _ := raw.(map[string]any)
		ext, _ := item["extensions"].(map[string]any)
		candidate := strings.ToLower(mapString(ext, "code"))
		if candidate != "" {
			code = "monday." + sanitizeCode(candidate)
		}
		if strings.Contains(candidate, "limit") || intValue(ext["retry_in_seconds"], 0) > 0 {
			retryable = true
		}
	}
	return code, retryable
}
func mondayRef(payload map[string]any) string {
	data, _ := payload["data"].(map[string]any)
	for _, key := range []string{"create_item", "change_multiple_column_values"} {
		item, _ := data[key].(map[string]any)
		if id := mapString(item, "id"); id != "" {
			return id
		}
	}
	return ""
}
func boardID(connection connector.Connection, values ...string) (string, error) {
	board := firstString(values...)
	if board == "" {
		board = config(connection, "board_id", "")
	}
	if board == "" {
		return "", permanent("board_required", "board is required")
	}
	return board, nil
}
func providerInput(primary, secondary map[string]any) map[string]any {
	source := primary
	if source == nil {
		source = secondary
	}
	if source == nil {
		return map[string]any{}
	}
	target := make(map[string]any, len(source))
	for key, value := range source {
		target[key] = value
	}
	return target
}
func jsonValue(value any, fallback map[string]any) (string, error) {
	if value == nil {
		value = fallback
	}
	if value == nil {
		return "", permanent("column_values_invalid", "column values are required")
	}
	if text, ok := value.(string); ok {
		if !json.Valid([]byte(text)) {
			return "", permanent("column_values_invalid", "column values JSON is invalid")
		}
		return text, nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", permanent("column_values_invalid", "column values are invalid")
	}
	return string(raw), nil
}
func optionalIDs(value any) any {
	if value == nil {
		return nil
	}
	switch typed := value.(type) {
	case []any, []string:
		return typed
	case string:
		if strings.TrimSpace(typed) == "" {
			return nil
		}
		parts := strings.Split(typed, ",")
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		return parts
	default:
		return []string{fmt.Sprint(value)}
	}
}
func nullable(value any) any {
	if value == nil || strings.TrimSpace(fmt.Sprint(value)) == "" {
		return nil
	}
	return value
}
func firstPositive(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}
func firstString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
func firstMapString(values map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := mapString(values, key); value != "" {
			return value
		}
	}
	return ""
}
func endpoint(connection connector.Connection) string {
	return config(connection, "endpoint", defaultEndpoint)
}
func config(connection connector.Connection, key, fallback string) string {
	if value := mapString(connection.Config, key); value != "" {
		return value
	}
	return fallback
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
func sanitizeCode(value string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' || r == '-' {
			return r
		}
		return '_'
	}, strings.ToLower(strings.TrimSpace(value)))
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
func permanent(code, message string) error {
	return connector.PermanentError("monday."+code, errors.New(message))
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
