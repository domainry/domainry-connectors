// Package trustpilot implements the official Trustpilot feedback Provider.
package trustpilot

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
	ConnectorKey         = "customer_feedback"
	ProviderKey          = "trustpilot"
	defaultBaseURL       = "https://api.trustpilot.com/v1"
	responseLimit  int64 = 4 << 20
)

var GetForm = connector.CallOperation[struct{}, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "get_form", ContractSHA256: "7adc05c1b448a5c935e78c0c50a8d20fea52ea1b541e250c75c33de841af0cf9", Reliability: readReliability()}
var ListResponses = connector.CallOperation[ListResponsesInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_responses", ContractSHA256: "4a91cb108284b0275c4e5b2980a3492b766f207dc5b8891b0ead4aae66fec922", Reliability: readReliability()}

type ListResponsesInput struct {
	Since    string `json:"since,omitempty"`
	Until    string `json:"until,omitempty"`
	After    string `json:"after,omitempty"`
	PageSize int    `json:"page_size,omitempty"`
}
type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Trustpilot transport is required")
	}
	p := &provider{transport: transport}
	getForm, err := connector.BindCall(GetForm, p.getForm)
	if err != nil {
		return nil, err
	}
	listResponses, err := connector.BindCall(ListResponses, p.listResponses)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), getForm, listResponses)
	if err != nil {
		return nil, err
	}
	p.Adapter = bound
	return p, nil
}
func readReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}
func schema() connector.ProviderSchema {
	minimum, maximum := float64(1), float64(120)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "base_url", Name: "Trustpilot API v1 base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://api.trustpilot.com/v1"`)}, {Key: "business_unit_id", Name: "Business Unit ID", Type: connector.ConfigFieldText, Required: true}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}}}, SecretFields: []connector.SecretField{{Key: "api_key", Name: "Trustpilot API key", Required: true, CredentialKind: connector.SecretCredentialAPIKey, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(baseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return connector.PermanentError("trustpilot.endpoint_invalid", errors.New("HTTPS or loopback HTTP endpoint is required"))
	}
	if businessUnitID(connection) == "" {
		return connector.PermanentError("trustpilot.business_unit_id_required", errors.New("business unit ID is required"))
	}
	return nil
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	payload, ref, err := p.profile(ctx, request.Connection, request.Secrets)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, err := json.Marshal(map[string]any{"business_unit": payload, "response_ref": ref})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}
func (p *provider) getForm(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[map[string]any], error) {
	payload, ref, err := p.profile(ctx, request.Connection, request.Secrets)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) profile(ctx context.Context, connection connector.Connection, secrets map[string]string) (map[string]any, string, error) {
	payload, ref, err := p.execute(ctx, connection, secrets, "/domain-units/"+url.PathEscape(businessUnitID(connection))+"/profileinfo", nil)
	if err == nil {
		payload["id"] = businessUnitID(connection)
	}
	return payload, ref, err
}
func (p *provider) listResponses(ctx context.Context, request connector.TypedRequest[ListResponsesInput]) (connector.TypedResult[map[string]any], error) {
	pageSize := request.Input.PageSize
	if pageSize == 0 {
		pageSize = 50
	}
	if pageSize < 1 || pageSize > 100 {
		return connector.TypedResult[map[string]any]{}, connector.PermanentError("trustpilot.page_size_invalid", errors.New("page size must be between 1 and 100"))
	}
	query := url.Values{"perPage": {strconv.Itoa(pageSize)}, "orderBy": {"createdat.asc"}}
	if value := strings.TrimSpace(request.Input.Since); value != "" {
		query.Set("startDateTime", value)
	}
	if value := strings.TrimSpace(request.Input.Until); value != "" {
		query.Set("endDateTime", value)
	}
	if value := strings.TrimSpace(request.Input.After); value != "" {
		query.Set("page", value)
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, "/domain-units/"+url.PathEscape(businessUnitID(request.Connection))+"/reviews", query)
	if err == nil {
		payload["items"] = payload["reviews"]
	}
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, path string, query url.Values) (map[string]any, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", err
	}
	key := strings.TrimSpace(secrets["api_key"])
	if key == "" {
		return nil, "", connector.PermanentError("trustpilot.api_key_required", errors.New("resolved API key is required"))
	}
	endpoint, err := url.Parse(baseURL(connection) + path)
	if err != nil {
		return nil, "", connector.PermanentError("trustpilot.request_invalid", err)
	}
	values := endpoint.Query()
	for name, items := range query {
		for _, value := range items {
			values.Add(name, value)
		}
	}
	endpoint.RawQuery = values.Encode()
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodGet, URL: endpoint.String(), Headers: map[string][]string{"Accept": {"application/json"}}, SecretHeaders: map[string][]string{"Apikey": {key}}, MaxResponseBytes: responseLimit})
	if err != nil {
		return nil, "", connector.RetryableError("trustpilot.network_error", err)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := map[string]any{}
	if json.Unmarshal(response.Body, &payload) != nil {
		return nil, ref, connector.PermanentError("trustpilot.response_invalid", errors.New("provider response is invalid JSON"))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code := errorCode(payload, response.StatusCode)
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			return payload, ref, connector.RetryableError(code, cause)
		}
		return payload, ref, connector.PermanentError(code, cause)
	}
	return payload, ref, nil
}
func businessUnitID(connection connector.Connection) string {
	return configString(connection.Config, "business_unit_id")
}
func baseURL(connection connector.Connection) string {
	if value := strings.TrimRight(configString(connection.Config, "base_url"), "/"); value != "" {
		return value
	}
	return defaultBaseURL
}
func configString(values map[string]any, key string) string {
	value := values[key]
	if value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}
func errorCode(payload map[string]any, status int) string {
	if value := configString(payload, "fault"); value != "" {
		return "trustpilot.provider_" + codeToken(value)
	}
	return "trustpilot.http_" + strconv.Itoa(status)
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
	result := strings.Trim(token.String(), "_")
	if result == "" {
		return "unknown"
	}
	return result
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
