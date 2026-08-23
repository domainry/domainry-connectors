// Package odoo implements the official Odoo 19+ delivery-operations Provider.
package odoo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	connector "github.com/domainry/domainry-connector-sdk"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const (
	ConnectorKey  = "delivery_operations"
	ProviderKey   = "odoo"
	json2Path     = "/json/2"
	responseLimit = 4 << 20
)

type ListInput struct {
	ProjectID      string         `json:"project_id,omitempty"`
	Domain         []any          `json:"domain,omitempty"`
	ProviderFields any            `json:"provider_fields,omitempty"`
	PageSize       int            `json:"page_size,omitempty"`
	MaxResults     int            `json:"maxResults,omitempty"`
	Limit          int            `json:"limit,omitempty"`
	Offset         int            `json:"offset,omitempty"`
	StartAt        int            `json:"startAt,omitempty"`
	Context        map[string]any `json:"context,omitempty"`
}
type CreateItemInput struct {
	Input   map[string]any `json:"input,omitempty"`
	Fields  map[string]any `json:"fields,omitempty"`
	Context map[string]any `json:"context,omitempty"`
}
type UpdateItemInput struct {
	ItemKey string         `json:"item_key"`
	Input   map[string]any `json:"input,omitempty"`
	Fields  map[string]any `json:"fields,omitempty"`
	Context map[string]any `json:"context,omitempty"`
}
type TransitionItemInput struct {
	ItemKey      string         `json:"item_key"`
	TransitionID string         `json:"transition_id"`
	Context      map[string]any `json:"context,omitempty"`
}
type Response map[string]any

var (
	ListDeliveryProjects   = connector.CallOperation[ListInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_delivery_projects", ContractSHA256: "044bd4d279359354f6fe310a889682bc10ce2b4b9f5e6e58cf89a4f5a66e202c", Reliability: readReliability()}
	ListDeliveryItems      = connector.CallOperation[ListInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_delivery_items", ContractSHA256: "b3503478e21cbb172628db3b0e0e4eb10afeb50a0c7451f067369dc4fbadb4a6", Reliability: readReliability()}
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
		return nil, errors.New("Odoo transport is required")
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
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "api_base_url", Name: "Odoo JSON-2 API base URL", Type: connector.ConfigFieldText, Required: true}, {Key: "database", Name: "Odoo database", Type: connector.ConfigFieldText}, {Key: "project_id", Name: "Default project ID", Type: connector.ConfigFieldText}, {Key: "context_language", Name: "Context language", Type: connector.ConfigFieldText, Default: json.RawMessage(`"en_US"`)}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &min, Max: &max}}}, SecretFields: []connector.SecretField{{Key: "api_key", Name: "Odoo API key", Required: true, CredentialKind: connector.SecretCredentialAPIKey, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryRequired, TestRequirement: connector.SecretTestWhenBound}}}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(baseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || strings.TrimRight(parsed.EscapedPath(), "/") != json2Path {
		return permanent("endpoint_invalid", "Odoo JSON-2 base URL is required")
	}
	if parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname())) {
		return permanent("endpoint_invalid", "HTTPS Odoo instance or loopback HTTP is required")
	}
	if seconds := intValue(connection.Config["timeout_seconds"], 30); seconds < 1 || seconds > 120 {
		return permanent("timeout_invalid", "timeout_seconds must be between 1 and 120")
	}
	return nil
}
func (p *provider) listProjects(ctx context.Context, r connector.TypedRequest[ListInput]) (connector.TypedResult[Response], error) {
	body := map[string]any{"context": contextValue(r.Connection, r.Input.Context), "domain": cloneSlice(r.Input.Domain), "fields": fieldValue(r.Input.ProviderFields, []string{"id", "name", "active", "partner_id", "date_start", "date", "stage_id", "task_count", "write_date"}), "limit": pageSize(r.Input.PageSize, r.Input.MaxResults, r.Input.Limit), "offset": offset(r.Input.Offset, r.Input.StartAt)}
	return p.execute(ctx, r.Connection, r.Secrets, "project.project", "search_read", body, false)
}
func (p *provider) listItems(ctx context.Context, r connector.TypedRequest[ListInput]) (connector.TypedResult[Response], error) {
	domain := cloneSlice(r.Input.Domain)
	if project := projectID(r.Connection, r.Input.ProjectID); project > 0 {
		domain = append(domain, []any{"project_id", "=", project})
	}
	body := map[string]any{"context": contextValue(r.Connection, r.Input.Context), "domain": domain, "fields": fieldValue(r.Input.ProviderFields, []string{"id", "name", "active", "project_id", "stage_id", "user_ids", "partner_id", "date_deadline", "priority", "write_date"}), "limit": pageSize(r.Input.PageSize, r.Input.MaxResults, r.Input.Limit), "offset": offset(r.Input.Offset, r.Input.StartAt)}
	return p.execute(ctx, r.Connection, r.Secrets, "project.task", "search_read", body, false)
}
func (p *provider) createItem(ctx context.Context, r connector.TypedRequest[CreateItemInput]) (connector.TypedResult[Response], error) {
	vals := providerFields(r.Input.Input, r.Input.Fields)
	if mapString(vals, "name") == "" {
		return connector.TypedResult[Response]{}, permanent("task_name_required", "task name is required")
	}
	if vals["project_id"] == nil {
		if project := projectID(r.Connection, ""); project > 0 {
			vals["project_id"] = project
		}
	}
	body := map[string]any{"context": contextValue(r.Connection, r.Input.Context), "vals_list": []any{vals}}
	return p.execute(ctx, r.Connection, r.Secrets, "project.task", "create", body, true)
}
func (p *provider) updateItem(ctx context.Context, r connector.TypedRequest[UpdateItemInput]) (connector.TypedResult[Response], error) {
	id := parseID(r.Input.ItemKey)
	vals := providerFields(r.Input.Input, r.Input.Fields)
	if id <= 0 || len(vals) == 0 {
		return connector.TypedResult[Response]{}, permanent("update_fields_required", "positive item_key and fields are required")
	}
	return p.execute(ctx, r.Connection, r.Secrets, "project.task", "write", map[string]any{"context": contextValue(r.Connection, r.Input.Context), "ids": []int64{id}, "vals": vals}, true)
}
func (p *provider) transitionItem(ctx context.Context, r connector.TypedRequest[TransitionItemInput]) (connector.TypedResult[Response], error) {
	id, stage := parseID(r.Input.ItemKey), parseID(r.Input.TransitionID)
	if id <= 0 || stage <= 0 {
		return connector.TypedResult[Response]{}, permanent("transition_fields_required", "positive item_key and transition_id are required")
	}
	return p.execute(ctx, r.Connection, r.Secrets, "project.task", "write", map[string]any{"context": contextValue(r.Connection, r.Input.Context), "ids": []int64{id}, "vals": map[string]any{"stage_id": stage}}, true)
}
func (p *provider) callTestConnection(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	return p.execute(ctx, r.Connection, r.Secrets, "project.project", "search_count", map[string]any{"domain": []any{}}, false)
}
func (p *provider) TestConnection(ctx context.Context, r connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.callTestConnection(ctx, connector.TypedRequest[struct{}]{Connection: r.Connection, Secrets: r.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}
func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, model, method string, body map[string]any, write bool) (connector.TypedResult[Response], error) {
	if err := p.ValidateConfig(connection); err != nil {
		return connector.TypedResult[Response]{}, err
	}
	key := strings.TrimSpace(secrets["api_key"])
	if key == "" {
		return connector.TypedResult[Response]{}, permanent("api_key_required", "resolved Odoo API key is required")
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return connector.TypedResult[Response]{}, permanent("request_invalid", "Odoo request body is invalid")
	}
	endpoint := strings.TrimRight(baseURL(connection), "/") + "/" + url.PathEscape(model) + "/" + url.PathEscape(method)
	headers := map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/json; charset=utf-8"}, "User-Agent": {"Domainry-Connector/1.0"}}
	if database := config(connection, "database", ""); database != "" {
		headers["X-Odoo-Database"] = []string{database}
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodPost, URL: endpoint, Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"bearer " + key}}, Body: raw, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		if write {
			return connector.TypedResult[Response]{}, connector.UncertainError("odoo.network_error", transportErr)
		}
		return connector.TypedResult[Response]{}, connector.RetryableError("odoo.network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	var value any
	valid := len(strings.TrimSpace(string(response.Body))) == 0 || json.Unmarshal(response.Body, &value) == nil
	payload := Response{"data": value}
	if object, ok := value.(map[string]any); ok {
		payload = object
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("Odoo returned HTTP %d", response.StatusCode)
		code := "odoo.http_" + strconv.Itoa(response.StatusCode)
		if name := mapString(payload, "name"); name != "" {
			code = "odoo." + sanitizeCode(name)
		}
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
			return connector.TypedResult[Response]{ResponseRef: ref}, connector.UncertainError("odoo.response_invalid", errors.New("Odoo response is invalid JSON"))
		}
		return connector.TypedResult[Response]{ResponseRef: ref}, permanent("response_invalid", "Odoo response is invalid JSON")
	}
	if method == "create" {
		if id := createdID(value); id != "" {
			ref = "odoo:" + id
		}
	}
	return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, nil
}
func createdID(value any) string {
	switch typed := value.(type) {
	case float64:
		return strconv.FormatInt(int64(typed), 10)
	case []any:
		if len(typed) > 0 {
			return strings.TrimSuffix(fmt.Sprint(typed[0]), ".0")
		}
	}
	return ""
}
func providerFields(primary, secondary map[string]any) map[string]any {
	source := primary
	if source == nil {
		source = secondary
	}
	target := map[string]any{}
	for key, value := range source {
		target[key] = value
	}
	return target
}
func contextValue(connection connector.Connection, provided map[string]any) map[string]any {
	result := map[string]any{}
	for key, value := range provided {
		result[key] = value
	}
	if result["lang"] == nil {
		if language := config(connection, "context_language", ""); language != "" {
			result["lang"] = language
		}
	}
	return result
}
func cloneSlice(source []any) []any { return append([]any(nil), source...) }
func fieldValue(value any, fallback []string) any {
	if value != nil {
		return value
	}
	return fallback
}
func projectID(connection connector.Connection, provided string) int64 {
	if id := parseID(provided); id > 0 {
		return id
	}
	return parseID(config(connection, "project_id", ""))
}
func parseID(value string) int64 {
	id, _ := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if id < 1 {
		return 0
	}
	return id
}
func pageSize(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 50
}
func offset(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}
func baseURL(connection connector.Connection) string { return config(connection, "api_base_url", "") }
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
	return connector.PermanentError("odoo."+code, errors.New(message))
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
