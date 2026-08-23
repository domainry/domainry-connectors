// Package netsuite implements the official Oracle NetSuite delivery-operations Provider.
package netsuite

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
	ProviderKey   = "netsuite"
	recordPath    = "/services/rest/record/v1"
	responseLimit = 4 << 20
)

type ListInput struct {
	Query              string `json:"q,omitempty"`
	Limit              int    `json:"limit,omitempty"`
	PageSize           int    `json:"page_size,omitempty"`
	MaxResults         int    `json:"maxResults,omitempty"`
	Offset             int    `json:"offset,omitempty"`
	StartAt            int    `json:"startAt,omitempty"`
	ExpandSubResources any    `json:"expand_sub_resources,omitempty"`
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
	ItemKey      string         `json:"item_key"`
	TransitionID string         `json:"transition_id"`
	Fields       map[string]any `json:"fields,omitempty"`
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
		return nil, errors.New("NetSuite transport is required")
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
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "record_base_url", Name: "SuiteTalk Record API base URL", Type: connector.ConfigFieldText, Required: true}, {Key: "project_id", Name: "Default Job/Project ID", Type: connector.ConfigFieldText}, {Key: "status_field", Name: "Project Task status field", Type: connector.ConfigFieldText, Default: json.RawMessage(`"status"`)}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &min, Max: &max}}}, SecretFields: []connector.SecretField{{Key: "access_token", Name: "NetSuite OAuth 2.0 access token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(baseURL(connection))
	path := strings.TrimRight(parsedPath(parsed), "/")
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || path != recordPath {
		return permanent("endpoint_invalid", "SuiteTalk Record API v1 base URL is required")
	}
	host := strings.ToLower(parsed.Hostname())
	if !(parsed.Scheme == "http" && isLoopback(host)) && (parsed.Scheme != "https" || !strings.HasSuffix(host, ".suitetalk.api.netsuite.com") || host == "suitetalk.api.netsuite.com") {
		return permanent("endpoint_invalid", "official account-specific SuiteTalk host or loopback HTTP is required")
	}
	if seconds := intValue(connection.Config["timeout_seconds"], 30); seconds < 1 || seconds > 120 {
		return permanent("timeout_invalid", "timeout_seconds must be between 1 and 120")
	}
	return nil
}
func (p *provider) listProjects(ctx context.Context, r connector.TypedRequest[ListInput]) (connector.TypedResult[Response], error) {
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodGet, "/job", listQuery(r.Input), nil, false)
}
func (p *provider) listItems(ctx context.Context, r connector.TypedRequest[ListInput]) (connector.TypedResult[Response], error) {
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodGet, "/projecttask", listQuery(r.Input), nil, false)
}
func (p *provider) createItem(ctx context.Context, r connector.TypedRequest[CreateItemInput]) (connector.TypedResult[Response], error) {
	fields := providerFields(r.Input.Input, r.Input.Fields)
	if mapString(fields, "title") == "" {
		return connector.TypedResult[Response]{}, permanent("task_fields_required", "task title is required")
	}
	if fields["company"] == nil {
		project := config(r.Connection, "project_id", "")
		if project == "" {
			return connector.TypedResult[Response]{}, permanent("project_required", "project is required")
		}
		fields["company"] = map[string]any{"id": project}
	}
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodPost, "/projecttask", nil, fields, true)
}
func (p *provider) updateItem(ctx context.Context, r connector.TypedRequest[UpdateItemInput]) (connector.TypedResult[Response], error) {
	fields := providerFields(r.Input.Input, r.Input.Fields)
	if strings.TrimSpace(r.Input.ItemKey) == "" || len(fields) == 0 {
		return connector.TypedResult[Response]{}, permanent("update_fields_required", "item_key and fields are required")
	}
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodPatch, "/projecttask/"+url.PathEscape(r.Input.ItemKey), nil, fields, true)
}
func (p *provider) transitionItem(ctx context.Context, r connector.TypedRequest[TransitionItemInput]) (connector.TypedResult[Response], error) {
	if strings.TrimSpace(r.Input.ItemKey) == "" || strings.TrimSpace(r.Input.TransitionID) == "" {
		return connector.TypedResult[Response]{}, permanent("transition_fields_required", "item_key and transition_id are required")
	}
	field := config(r.Connection, "status_field", "status")
	body := map[string]any{field: map[string]any{"id": r.Input.TransitionID}}
	for key, value := range r.Input.Fields {
		body[key] = value
	}
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodPatch, "/projecttask/"+url.PathEscape(r.Input.ItemKey), nil, body, true)
}
func (p *provider) callTestConnection(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodGet, "/job", url.Values{"limit": {"1"}}, nil, false)
}
func (p *provider) TestConnection(ctx context.Context, r connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.callTestConnection(ctx, connector.TypedRequest[struct{}]{Connection: r.Connection, Secrets: r.Secrets})
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
		return connector.TypedResult[Response]{}, permanent("access_token_required", "resolved NetSuite access token is required")
	}
	endpoint := strings.TrimRight(baseURL(connection), "/") + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	var raw []byte
	var err error
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return connector.TypedResult[Response]{}, permanent("request_invalid", "NetSuite request body is invalid")
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint, Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, Body: raw, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		if write {
			return connector.TypedResult[Response]{}, connector.UncertainError("netsuite.network_error", transportErr)
		}
		return connector.TypedResult[Response]{}, connector.RetryableError("netsuite.network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := Response{}
	valid := len(response.Body) == 0 || json.Unmarshal(response.Body, &payload) == nil
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("NetSuite returned HTTP %d", response.StatusCode)
		code := netSuiteErrorCode(payload, response.StatusCode)
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
			return connector.TypedResult[Response]{ResponseRef: ref}, connector.UncertainError("netsuite.response_invalid", errors.New("NetSuite response is invalid JSON"))
		}
		return connector.TypedResult[Response]{ResponseRef: ref}, permanent("response_invalid", "NetSuite response is invalid JSON")
	}
	if id := mapString(payload, "id"); id != "" {
		ref = "netsuite:" + id
	}
	return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, nil
}
func listQuery(input ListInput) url.Values {
	query := url.Values{}
	setQuery(query, "q", input.Query)
	limit := firstPositive(input.Limit, input.PageSize, input.MaxResults, 100)
	query.Set("limit", strconv.Itoa(limit))
	offset := input.Offset
	if offset == 0 {
		offset = input.StartAt
	}
	query.Set("offset", strconv.Itoa(offset))
	if boolValue(input.ExpandSubResources) {
		query.Set("expandSubResources", "true")
	}
	return query
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
func netSuiteErrorCode(payload map[string]any, status int) string {
	if details, ok := payload["o:errorDetails"].([]any); ok && len(details) > 0 {
		detail, _ := details[0].(map[string]any)
		if code := mapString(detail, "o:errorCode"); code != "" {
			return "netsuite." + sanitizeCode(code)
		}
	}
	return "netsuite.http_" + strconv.Itoa(status)
}
func baseURL(connection connector.Connection) string {
	return config(connection, "record_base_url", "")
}
func parsedPath(parsed *url.URL) string {
	if parsed == nil {
		return ""
	}
	return parsed.EscapedPath()
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
func setQuery(query url.Values, key, value string) {
	if value = strings.TrimSpace(value); value != "" {
		query.Set(key, value)
	}
}
func firstPositive(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}
func boolValue(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "true")
	}
	return false
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
func isLoopback(host string) bool { return host == "localhost" || host == "127.0.0.1" || host == "::1" }
func permanent(code, message string) error {
	return connector.PermanentError("netsuite."+code, errors.New(message))
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
