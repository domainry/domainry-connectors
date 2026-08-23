// Package delighted implements the official Delighted feedback Provider.
package delighted

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	connector "github.com/domainry/domainry-connector-sdk"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	ConnectorKey         = "customer_feedback"
	ProviderKey          = "delighted"
	defaultBaseURL       = "https://api.delighted.com/v1"
	responseLimit  int64 = 4 << 20
)

var ListResponses = connector.CallOperation[ListResponsesInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_responses", ContractSHA256: "4a91cb108284b0275c4e5b2980a3492b766f207dc5b8891b0ead4aae66fec922", Reliability: connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}}

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
		return nil, errors.New("Delighted transport is required")
	}
	p := &provider{transport: transport}
	operation, err := connector.BindCall(ListResponses, p.listResponses)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), operation)
	if err != nil {
		return nil, err
	}
	p.Adapter = bound
	return p, nil
}
func schema() connector.ProviderSchema {
	minimum, maximum := float64(1), float64(120)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "base_url", Name: "Delighted API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://api.delighted.com/v1"`)}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}}}, SecretFields: []connector.SecretField{{Key: "api_key", Name: "Project API key", Required: true, CredentialKind: connector.SecretCredentialAPIKey, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(baseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return connector.PermanentError("delighted.endpoint_invalid", errors.New("HTTPS or loopback HTTP endpoint is required"))
	}
	return nil
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	payload, ref, err := p.executeObject(ctx, request.Connection, request.Secrets, "/metrics.json", nil)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, err := json.Marshal(map[string]any{"metrics": payload, "response_ref": ref})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}
func (p *provider) listResponses(ctx context.Context, request connector.TypedRequest[ListResponsesInput]) (connector.TypedResult[map[string]any], error) {
	pageSize := request.Input.PageSize
	if pageSize == 0 {
		pageSize = 50
	}
	if pageSize < 1 || pageSize > 100 {
		return connector.TypedResult[map[string]any]{}, connector.PermanentError("delighted.page_size_invalid", errors.New("page size must be between 1 and 100"))
	}
	query := url.Values{"per_page": {strconv.Itoa(pageSize)}, "expand[]": {"person", "notes"}}
	if value := strings.TrimSpace(request.Input.Since); value != "" {
		timestamp, err := unixTimestamp(value)
		if err != nil {
			return connector.TypedResult[map[string]any]{}, err
		}
		query.Set("since", timestamp)
	}
	if value := strings.TrimSpace(request.Input.Until); value != "" {
		timestamp, err := unixTimestamp(value)
		if err != nil {
			return connector.TypedResult[map[string]any]{}, err
		}
		query.Set("until", timestamp)
	}
	if value := strings.TrimSpace(request.Input.After); value != "" {
		query.Set("page", value)
	}
	items, ref, err := p.executeArray(ctx, request.Connection, request.Secrets, "/survey_responses.json", query)
	return connector.TypedResult[map[string]any]{Output: map[string]any{"items": items}, ResponseRef: ref}, err
}
func (p *provider) request(ctx context.Context, connection connector.Connection, secrets map[string]string, path string, query url.Values) (connector.HTTPResponse, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return connector.HTTPResponse{}, "", err
	}
	key := strings.TrimSpace(secrets["api_key"])
	if key == "" {
		return connector.HTTPResponse{}, "", connector.PermanentError("delighted.api_key_required", errors.New("resolved API key is required"))
	}
	endpoint, err := url.Parse(baseURL(connection) + path)
	if err != nil {
		return connector.HTTPResponse{}, "", connector.PermanentError("delighted.request_invalid", err)
	}
	values := endpoint.Query()
	for name, items := range query {
		for _, value := range items {
			values.Add(name, value)
		}
	}
	endpoint.RawQuery = values.Encode()
	credential := base64.StdEncoding.EncodeToString([]byte(key + ":"))
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodGet, URL: endpoint.String(), Headers: map[string][]string{"Accept": {"application/json"}}, SecretHeaders: map[string][]string{"Authorization": {"Basic " + credential}}, MaxResponseBytes: responseLimit})
	if err != nil {
		return connector.HTTPResponse{}, "", connector.RetryableError("delighted.network_error", err)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		code := "delighted.http_" + strconv.Itoa(response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			return response, ref, connector.RetryableError(code, cause)
		}
		return response, ref, connector.PermanentError(code, cause)
	}
	return response, ref, nil
}
func (p *provider) executeObject(ctx context.Context, connection connector.Connection, secrets map[string]string, path string, query url.Values) (map[string]any, string, error) {
	response, ref, err := p.request(ctx, connection, secrets, path, query)
	if err != nil {
		return nil, ref, err
	}
	payload := map[string]any{}
	if json.Unmarshal(response.Body, &payload) != nil {
		return nil, ref, connector.PermanentError("delighted.response_invalid", errors.New("provider response is not a JSON object"))
	}
	return payload, ref, nil
}
func (p *provider) executeArray(ctx context.Context, connection connector.Connection, secrets map[string]string, path string, query url.Values) ([]any, string, error) {
	response, ref, err := p.request(ctx, connection, secrets, path, query)
	if err != nil {
		return nil, ref, err
	}
	if len(strings.TrimSpace(string(response.Body))) == 0 {
		return []any{}, ref, nil
	}
	items := []any{}
	if json.Unmarshal(response.Body, &items) != nil {
		return nil, ref, connector.PermanentError("delighted.response_invalid", errors.New("provider response is not a JSON array"))
	}
	return items, ref, nil
}
func unixTimestamp(value string) (string, error) {
	if epoch, err := strconv.ParseInt(value, 10, 64); err == nil {
		return strconv.FormatInt(epoch, 10), nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return "", connector.PermanentError("delighted.time_invalid", errors.New("time must be an epoch or RFC3339 value"))
	}
	return strconv.FormatInt(parsed.Unix(), 10), nil
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
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
