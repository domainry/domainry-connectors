// Package sapbusinessone implements the official SAP Business One delivery-operations Provider.
package sapbusinessone

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
	ProviderKey   = "sap_business_one"
	servicePath   = "/b1s/v2"
	responseLimit = 4 << 20
)

type ListInput struct {
	Select     string `json:"select,omitempty"`
	Filter     string `json:"filter,omitempty"`
	OrderBy    string `json:"orderby,omitempty"`
	Expand     string `json:"expand,omitempty"`
	Search     string `json:"search,omitempty"`
	PageSize   int    `json:"page_size,omitempty"`
	MaxResults int    `json:"maxResults,omitempty"`
	Top        int    `json:"top,omitempty"`
	StartAt    int    `json:"startAt,omitempty"`
	Skip       int    `json:"skip,omitempty"`
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
		return nil, errors.New("SAP Business One transport is required")
	}
	p := &provider{transport: transport}
	projects, err := connector.BindCall(ListDeliveryProjects, p.list)
	if err != nil {
		return nil, err
	}
	items, err := connector.BindCall(ListDeliveryItems, p.list)
	if err != nil {
		return nil, err
	}
	create, err := connector.BindCall(CreateDeliveryItem, p.create)
	if err != nil {
		return nil, err
	}
	update, err := connector.BindCall(UpdateDeliveryItem, p.update)
	if err != nil {
		return nil, err
	}
	transition, err := connector.BindCall(TransitionDeliveryItem, p.transition)
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
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "service_root", Name: "Service Layer OData v4 root", Type: connector.ConfigFieldText, Required: true}, {Key: "company_id", Name: "SAP Business One company ID", Type: connector.ConfigFieldText, Required: true}, {Key: "status_field", Name: "Project status field", Type: connector.ConfigFieldText, Default: json.RawMessage(`"ProjectStatus"`)}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &min, Max: &max}}}, SecretFields: []connector.SecretField{{Key: "access_token", Name: "Service Layer access token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(root(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || strings.TrimRight(parsed.EscapedPath(), "/") != servicePath {
		return permanent("endpoint_invalid", "Service Layer OData v4 root is required")
	}
	if parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname())) {
		return permanent("endpoint_invalid", "HTTPS Service Layer or loopback HTTP is required")
	}
	if config(connection, "company_id", "") == "" {
		return permanent("company_id_required", "company_id is required")
	}
	if seconds := intValue(connection.Config["timeout_seconds"], 30); seconds < 1 || seconds > 120 {
		return permanent("timeout_invalid", "timeout_seconds must be between 1 and 120")
	}
	return nil
}
func (p *provider) list(ctx context.Context, r connector.TypedRequest[ListInput]) (connector.TypedResult[Response], error) {
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodGet, "/ProjectManagements", odataQuery(r.Input), nil, false)
}
func (p *provider) create(ctx context.Context, r connector.TypedRequest[CreateItemInput]) (connector.TypedResult[Response], error) {
	fields := providerFields(r.Input.Input, r.Input.Fields)
	if mapString(fields, "ProjectName") == "" {
		return connector.TypedResult[Response]{}, permanent("project_name_required", "ProjectName is required")
	}
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodPost, "/ProjectManagements", nil, fields, true)
}
func (p *provider) update(ctx context.Context, r connector.TypedRequest[UpdateItemInput]) (connector.TypedResult[Response], error) {
	id := recordID(r.Input.ItemKey)
	fields := providerFields(r.Input.Input, r.Input.Fields)
	if id <= 0 || len(fields) == 0 {
		return connector.TypedResult[Response]{}, permanent("update_fields_required", "positive item_key and fields are required")
	}
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodPatch, "/ProjectManagements("+strconv.FormatInt(id, 10)+")", nil, fields, true)
}
func (p *provider) transition(ctx context.Context, r connector.TypedRequest[TransitionItemInput]) (connector.TypedResult[Response], error) {
	id := recordID(r.Input.ItemKey)
	if id <= 0 || strings.TrimSpace(r.Input.TransitionID) == "" {
		return connector.TypedResult[Response]{}, permanent("transition_fields_required", "positive item_key and transition_id are required")
	}
	field := config(r.Connection, "status_field", "ProjectStatus")
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodPatch, "/ProjectManagements("+strconv.FormatInt(id, 10)+")", nil, map[string]any{field: r.Input.TransitionID}, true)
}
func (p *provider) callTestConnection(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	return p.execute(ctx, r.Connection, r.Secrets, http.MethodPost, "/UsersService_GetCurrentUser", nil, map[string]any{}, false)
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
		return connector.TypedResult[Response]{}, permanent("access_token_required", "resolved Service Layer access token is required")
	}
	endpoint := strings.TrimRight(root(connection), "/") + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	var raw []byte
	var err error
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return connector.TypedResult[Response]{}, permanent("request_invalid", "SAP Business One request body is invalid")
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}, "X-b1-companyid": {config(connection, "company_id", "")}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint, Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, Body: raw, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		if write {
			return connector.TypedResult[Response]{}, connector.UncertainError("sap_business_one.network_error", transportErr)
		}
		return connector.TypedResult[Response]{}, connector.RetryableError("sap_business_one.network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := Response{}
	valid := len(response.Body) == 0 || json.Unmarshal(response.Body, &payload) == nil
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("SAP Business One returned HTTP %d", response.StatusCode)
		code := errorCode(payload, response.StatusCode)
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
			return connector.TypedResult[Response]{ResponseRef: ref}, connector.UncertainError("sap_business_one.response_invalid", errors.New("SAP Business One response is invalid JSON"))
		}
		return connector.TypedResult[Response]{ResponseRef: ref}, permanent("response_invalid", "SAP Business One response is invalid JSON")
	}
	if id := numberString(payload["AbsEntry"]); id != "" {
		ref = "sap_business_one:" + id
	}
	return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, nil
}
func odataQuery(input ListInput) url.Values {
	query := url.Values{}
	for key, value := range map[string]string{"$select": input.Select, "$filter": input.Filter, "$orderby": input.OrderBy, "$expand": input.Expand, "$search": input.Search} {
		if strings.TrimSpace(value) != "" {
			query.Set(key, value)
		}
	}
	query.Set("$top", strconv.Itoa(firstPositive(input.PageSize, input.MaxResults, input.Top, 50)))
	query.Set("$skip", strconv.Itoa(firstNonnegative(input.StartAt, input.Skip)))
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
func recordID(value string) int64 {
	id, _ := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if id < 1 {
		return 0
	}
	return id
}
func errorCode(payload map[string]any, status int) string {
	detail, _ := payload["error"].(map[string]any)
	if code := numberString(detail["code"]); code != "" {
		return "sap_business_one." + sanitizeCode(code)
	}
	return "sap_business_one.http_" + strconv.Itoa(status)
}
func numberString(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case float64:
		return strconv.FormatInt(int64(typed), 10)
	case json.Number:
		return typed.String()
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	}
	return ""
}
func root(connection connector.Connection) string { return config(connection, "service_root", "") }
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
func firstPositive(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}
func firstNonnegative(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
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
	return connector.PermanentError("sap_business_one."+code, errors.New(message))
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
