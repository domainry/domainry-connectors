// Package prometheus implements the official Prometheus observability Provider.
package prometheus

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
)

const (
	ConnectorKey        = "observability_compliance"
	ProviderKey         = "prometheus"
	responseLimit int64 = 4 << 20
)

var Query = connector.CallOperation[QueryInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "query", ContractSHA256: "0be7ab94ddbd24c068df3d1d68181b5eb1375566ee02f153084c1b03f02be53e", Reliability: readReliability()}
var QueryRange = connector.CallOperation[QueryRangeInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "query_range", ContractSHA256: "4bd93814810003573be08b1985c93394694c6619a5d233d760c7e2539e06d131", Reliability: readReliability()}

type QueryInput struct {
	Query string `json:"query"`
	Limit int    `json:"limit,omitempty"`
}
type QueryRangeInput struct {
	Query   string `json:"query"`
	Start   string `json:"start"`
	End     string `json:"end"`
	Step    string `json:"step"`
	Timeout string `json:"timeout,omitempty"`
	Limit   int    `json:"limit,omitempty"`
}
type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Prometheus transport is required")
	}
	p := &provider{transport: transport}
	query, err := connector.BindCall(Query, p.query)
	if err != nil {
		return nil, err
	}
	rangeQuery, err := connector.BindCall(QueryRange, p.queryRange)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), query, rangeQuery)
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
	min, max := float64(1), float64(300)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "base_url", Name: "Prometheus Base URL", Type: connector.ConfigFieldText, Required: true}, {Key: "username", Name: "Basic Auth Username", Type: connector.ConfigFieldText}, {Key: "timeout_seconds", Name: "Timeout Seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &min, Max: &max}}}, SecretFields: []connector.SecretField{{Key: "bearer_token", Name: "Bearer Token", CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}, {Key: "basic_auth_password", Name: "Basic Auth Password", CredentialKind: connector.SecretCredentialBasicAuthPassword, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(configString(connection.Config, "base_url"))
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return connector.PermanentError("prometheus.endpoint_invalid", errors.New("HTTPS or loopback HTTP endpoint is required"))
	}
	return nil
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	payload, ref, err := p.get(ctx, request.Connection, request.Secrets, "/api/v1/status/runtimeinfo", nil)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, err := json.Marshal(map[string]any{"provider_response": payload, "response_ref": ref})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}
func (p *provider) query(ctx context.Context, request connector.TypedRequest[QueryInput]) (connector.TypedResult[map[string]any], error) {
	query := strings.TrimSpace(request.Input.Query)
	if query == "" {
		return connector.TypedResult[map[string]any]{}, connector.PermanentError("prometheus.query_required", errors.New("PromQL query is required"))
	}
	if request.Input.Limit < 0 {
		return connector.TypedResult[map[string]any]{}, connector.PermanentError("prometheus.limit_invalid", errors.New("limit cannot be negative"))
	}
	values := url.Values{"query": {query}}
	if request.Input.Limit > 0 {
		values.Set("limit", strconv.Itoa(request.Input.Limit))
	}
	payload, ref, err := p.get(ctx, request.Connection, request.Secrets, "/api/v1/query", values)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) queryRange(ctx context.Context, request connector.TypedRequest[QueryRangeInput]) (connector.TypedResult[map[string]any], error) {
	input := request.Input
	if strings.TrimSpace(input.Query) == "" || strings.TrimSpace(input.Start) == "" || strings.TrimSpace(input.End) == "" || strings.TrimSpace(input.Step) == "" {
		return connector.TypedResult[map[string]any]{}, connector.PermanentError("prometheus.range_required", errors.New("query, start, end and step are required"))
	}
	if input.Limit < 0 {
		return connector.TypedResult[map[string]any]{}, connector.PermanentError("prometheus.limit_invalid", errors.New("limit cannot be negative"))
	}
	values := url.Values{"query": {strings.TrimSpace(input.Query)}, "start": {strings.TrimSpace(input.Start)}, "end": {strings.TrimSpace(input.End)}, "step": {strings.TrimSpace(input.Step)}}
	if value := strings.TrimSpace(input.Timeout); value != "" {
		values.Set("timeout", value)
	}
	if input.Limit > 0 {
		values.Set("limit", strconv.Itoa(input.Limit))
	}
	payload, ref, err := p.get(ctx, request.Connection, request.Secrets, "/api/v1/query_range", values)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) get(ctx context.Context, connection connector.Connection, secrets map[string]string, path string, query url.Values) (map[string]any, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", err
	}
	bearer, password := strings.TrimSpace(secrets["bearer_token"]), strings.TrimSpace(secrets["basic_auth_password"])
	if bearer != "" && password != "" {
		return nil, "", connector.PermanentError("prometheus.credentials_conflict", errors.New("Bearer and Basic credentials cannot be used together"))
	}
	secretHeaders := map[string][]string{}
	if bearer != "" {
		secretHeaders["Authorization"] = []string{"Bearer " + bearer}
	}
	if password != "" {
		username := configString(connection.Config, "username")
		if username == "" {
			return nil, "", connector.PermanentError("prometheus.username_required", errors.New("username is required for Basic auth"))
		}
		secretHeaders["Authorization"] = []string{"Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))}
	}
	endpoint, err := url.Parse(strings.TrimRight(configString(connection.Config, "base_url"), "/") + path)
	if err != nil {
		return nil, "", connector.PermanentError("prometheus.endpoint_invalid", err)
	}
	endpoint.RawQuery = query.Encode()
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodGet, URL: endpoint.String(), Headers: map[string][]string{"Accept": {"application/json"}}, SecretHeaders: secretHeaders, MaxResponseBytes: responseLimit})
	if err != nil {
		return nil, "", connector.RetryableError("prometheus.network_error", err)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := map[string]any{}
	if json.Unmarshal(response.Body, &payload) != nil {
		return nil, ref, connector.PermanentError("prometheus.response_invalid", errors.New("provider response is invalid JSON"))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code, cause := "prometheus.http_"+strconv.Itoa(response.StatusCode), fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			return payload, ref, connector.RetryableError(code, cause)
		}
		return payload, ref, connector.PermanentError(code, cause)
	}
	if status := configString(payload, "status"); status != "success" {
		return payload, ref, connector.PermanentError("prometheus.query_failed", errors.New("Prometheus returned a non-success status"))
	}
	return payload, ref, nil
}
func configString(values map[string]any, key string) string {
	if values == nil || values[key] == nil {
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
