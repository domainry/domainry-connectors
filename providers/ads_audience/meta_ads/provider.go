// Package metaads implements the official Meta Ads custom-audience Provider.
package metaads

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
	ConnectorKey         = "ads_audience"
	ProviderKey          = "meta_ads"
	defaultBaseURL       = "https://graph.facebook.com/v25.0"
	responseLimit  int64 = 4 << 20
)

var ListCustomAudiences = connector.CallOperation[ListCustomAudiencesInput, map[string]any]{
	ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_custom_audiences", ContractSHA256: "28a016c19174c6ed2b92a5245ecd1fefd937a2abc13887aa6927ec52c30c8700",
	Reliability: connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}},
}

type ListCustomAudiencesInput struct {
	Fields string `json:"fields,omitempty"`
	Limit  int    `json:"limit,omitempty"`
	After  string `json:"after,omitempty"`
	Before string `json:"before,omitempty"`
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Meta Ads transport is required")
	}
	p := &provider{transport: transport}
	operation, err := connector.BindCall(ListCustomAudiences, p.listCustomAudiences)
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
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{
		{Key: "ad_account_id", Name: "Ad account ID", Type: connector.ConfigFieldText, Required: true},
		{Key: "base_url", Name: "Meta Graph API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://graph.facebook.com/v25.0"`)},
		{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}},
	}, SecretFields: []connector.SecretField{{Key: "access_token", Name: "Marketing API access token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	if configString(connection.Config, "ad_account_id") == "" {
		return connector.PermanentError("meta_ads.ad_account_id_required", errors.New("ad account ID is required"))
	}
	parsed, err := url.Parse(baseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return connector.PermanentError("meta_ads.endpoint_invalid", errors.New("HTTPS or loopback HTTP endpoint is required"))
	}
	return nil
}

func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, "/"+accountID(request.Connection), url.Values{"fields": {"id,name,account_status,currency,timezone_name"}})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, err := json.Marshal(map[string]any{"account": payload, "response_ref": ref})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) listCustomAudiences(ctx context.Context, request connector.TypedRequest[ListCustomAudiencesInput]) (connector.TypedResult[map[string]any], error) {
	query := url.Values{"fields": {"id,name,subtype,approximate_count_lower_bound,approximate_count_upper_bound,delivery_status,operation_status"}}
	if value := strings.TrimSpace(request.Input.Fields); value != "" {
		query.Set("fields", value)
	}
	if request.Input.Limit < 0 {
		return connector.TypedResult[map[string]any]{}, connector.PermanentError("meta_ads.limit_invalid", errors.New("limit cannot be negative"))
	}
	if request.Input.Limit > 0 {
		query.Set("limit", strconv.Itoa(request.Input.Limit))
	}
	if value := strings.TrimSpace(request.Input.After); value != "" {
		query.Set("after", value)
	}
	if value := strings.TrimSpace(request.Input.Before); value != "" {
		query.Set("before", value)
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, "/"+accountID(request.Connection)+"/customaudiences", query)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, path string, query url.Values) (map[string]any, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", err
	}
	token := strings.TrimSpace(secrets["access_token"])
	if token == "" {
		return nil, "", connector.PermanentError("meta_ads.access_token_required", errors.New("resolved access token is required"))
	}
	endpoint, err := url.Parse(baseURL(connection) + path)
	if err != nil {
		return nil, "", connector.PermanentError("meta_ads.request_invalid", err)
	}
	values := endpoint.Query()
	for key, items := range query {
		for _, value := range items {
			values.Add(key, value)
		}
	}
	endpoint.RawQuery = values.Encode()
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodGet, URL: endpoint.String(), Headers: map[string][]string{"Accept": {"application/json"}}, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, MaxResponseBytes: responseLimit})
	if err != nil {
		return nil, "", connector.RetryableError("meta_ads.network_error", err)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := map[string]any{}
	if json.Unmarshal(response.Body, &payload) != nil {
		return nil, ref, connector.PermanentError("meta_ads.response_invalid", errors.New("provider response is invalid JSON"))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code := errorCode(payload, response.StatusCode)
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			return payload, ref, connector.RetryableError(code, cause)
		}
		return payload, ref, connector.PermanentError(code, cause)
	}
	if id := configString(payload, "id"); id != "" {
		ref = "meta_ads:" + id
	}
	return payload, ref, nil
}

func accountID(connection connector.Connection) string {
	id := configString(connection.Config, "ad_account_id")
	if !strings.HasPrefix(id, "act_") {
		id = "act_" + id
	}
	return url.PathEscape(id)
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
	if object, ok := payload["error"].(map[string]any); ok {
		if code := configString(object, "code"); code != "" {
			return "meta_ads.provider_" + providerCodeToken(code)
		}
	}
	return "meta_ads.http_" + strconv.Itoa(status)
}
func providerCodeToken(value string) string {
	var token strings.Builder
	for _, current := range strings.ToLower(strings.TrimSpace(value)) {
		if current >= 'a' && current <= 'z' || current >= '0' && current <= '9' {
			token.WriteRune(current)
		} else if token.Len() > 0 {
			token.WriteByte('_')
		}
	}
	return strings.Trim(token.String(), "_")
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
