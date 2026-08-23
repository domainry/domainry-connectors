// Package etsy implements the official Etsy marketplace Provider.
package etsy

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
	ConnectorKey               = "marketplace"
	ProviderKey                = "etsy"
	defaultBaseURL             = "https://api.etsy.com/v3"
	minimumEtsyTimestamp int64 = 946684800
	responseLimit        int64 = 4 << 20
)

type PageInput struct {
	Limit  int `json:"limit,omitempty"`
	Offset int `json:"offset,omitempty"`
}
type OrderPageInput struct {
	Limit      int   `json:"limit,omitempty"`
	Offset     int   `json:"offset,omitempty"`
	MinCreated int64 `json:"min_created,omitempty"`
	MaxCreated int64 `json:"max_created,omitempty"`
}
type EtsyResponse map[string]any

var (
	ListOrders     = connector.CallOperation[OrderPageInput, EtsyResponse]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_orders", ContractSHA256: "7786ead8684eac7098bbd8352cacb464897691aee8fc02fc7db7b02668797f9c", Reliability: readReliability()}
	ListListings   = connector.CallOperation[PageInput, EtsyResponse]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_listings", ContractSHA256: "df992b4774ae6237cb3bf7a64771bedfed46ed88d00ac92b13781e064c903327", Reliability: readReliability()}
	TestConnection = connector.CallOperation[struct{}, EtsyResponse]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection", ContractSHA256: "b098b0312858c224a55e668d19d9c71b77489b4f868c9dada7b9d6939cb6668d", Reliability: readReliability()}
)

func readReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Etsy transport is required")
	}
	p := &provider{transport: transport}
	orders, err := connector.BindCall(ListOrders, p.listOrders)
	if err != nil {
		return nil, err
	}
	listings, err := connector.BindCall(ListListings, p.listListings)
	if err != nil {
		return nil, err
	}
	test, err := connector.BindCall(TestConnection, p.callTestConnection)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), orders, listings, test)
	if err != nil {
		return nil, err
	}
	p.Adapter = bound
	return p, nil
}

func schema() connector.ProviderSchema {
	minimum, maximum := float64(1), float64(120)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{
		{Key: "shop_id", Name: "Etsy shop ID", Type: connector.ConfigFieldText, Required: true},
		{Key: "base_url", Name: "Etsy API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://api.etsy.com/v3"`)},
		{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}},
	}, SecretFields: []connector.SecretField{
		{Key: "api_key", Name: "Etsy App API key", Description: "The keystring and shared secret separated by a colon.", Required: true, CredentialKind: connector.SecretCredentialAPIKey, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound},
		{Key: "access_token", Name: "OAuth access token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound},
	}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	shopID, err := strconv.ParseInt(config(connection, "shop_id", ""), 10, 64)
	if err != nil || shopID < 1 {
		return permanent("shop_id_invalid", "positive numeric Etsy shop ID is required")
	}
	parsed, err := url.Parse(baseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return permanent("endpoint_invalid", "valid Etsy API endpoint is required")
	}
	if parsed.Scheme == "http" && isLoopback(parsed.Hostname()) {
		return nil
	}
	if parsed.Scheme != "https" {
		return permanent("endpoint_invalid", "Etsy API endpoint must use HTTPS")
	}
	for _, host := range []string{"api.etsy.com", "openapi.etsy.com"} {
		if strings.EqualFold(parsed.Hostname(), host) {
			return nil
		}
	}
	return permanent("endpoint_invalid", "official Etsy API endpoint or loopback HTTP is required")
}

func (p *provider) listOrders(ctx context.Context, request connector.TypedRequest[OrderPageInput]) (connector.TypedResult[EtsyResponse], error) {
	query, err := orderQuery(request.Input)
	if err != nil {
		return connector.TypedResult[EtsyResponse]{}, err
	}
	return p.execute(ctx, request.Connection, request.Secrets, "/application/shops/"+url.PathEscape(shopID(request.Connection))+"/receipts", query)
}
func (p *provider) listListings(ctx context.Context, request connector.TypedRequest[PageInput]) (connector.TypedResult[EtsyResponse], error) {
	query, err := pageQuery(request.Input.Limit, request.Input.Offset)
	if err != nil {
		return connector.TypedResult[EtsyResponse]{}, err
	}
	return p.execute(ctx, request.Connection, request.Secrets, "/application/shops/"+url.PathEscape(shopID(request.Connection))+"/listings/active", query)
}
func (p *provider) callTestConnection(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[EtsyResponse], error) {
	return p.execute(ctx, request.Connection, request.Secrets, "/application/users/__SELF__/shops", nil)
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.execute(ctx, request.Connection, request.Secrets, "/application/users/__SELF__/shops", nil)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, marshalErr := json.Marshal(map[string]any{"shops": result.Output["results"], "response_ref": result.ResponseRef})
	if marshalErr != nil {
		return connector.TestConnectionResult{}, marshalErr
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func pageQuery(limit, offset int) (url.Values, error) {
	if limit < 0 || limit > 100 {
		return nil, permanent("limit_invalid", "limit must be between 1 and 100 when set")
	}
	if offset < 0 {
		return nil, permanent("offset_invalid", "offset must not be negative")
	}
	query := url.Values{}
	if limit > 0 {
		query.Set("limit", strconv.Itoa(limit))
	}
	if offset > 0 {
		query.Set("offset", strconv.Itoa(offset))
	}
	return query, nil
}
func orderQuery(input OrderPageInput) (url.Values, error) {
	query, err := pageQuery(input.Limit, input.Offset)
	if err != nil {
		return nil, err
	}
	if input.MinCreated != 0 && input.MinCreated < minimumEtsyTimestamp {
		return nil, permanent("min_created_invalid", "min_created is outside Etsy's supported range")
	}
	if input.MaxCreated != 0 && input.MaxCreated < minimumEtsyTimestamp {
		return nil, permanent("max_created_invalid", "max_created is outside Etsy's supported range")
	}
	if input.MinCreated != 0 && input.MaxCreated != 0 && input.MinCreated > input.MaxCreated {
		return nil, permanent("created_range_invalid", "min_created must not exceed max_created")
	}
	if input.MinCreated != 0 {
		query.Set("min_created", strconv.FormatInt(input.MinCreated, 10))
	}
	if input.MaxCreated != 0 {
		query.Set("max_created", strconv.FormatInt(input.MaxCreated, 10))
	}
	return query, nil
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, path string, query url.Values) (connector.TypedResult[EtsyResponse], error) {
	if err := p.ValidateConfig(connection); err != nil {
		return connector.TypedResult[EtsyResponse]{}, err
	}
	apiKey, token := strings.TrimSpace(secrets["api_key"]), strings.TrimSpace(secrets["access_token"])
	if apiKey == "" {
		return connector.TypedResult[EtsyResponse]{}, permanent("api_key_required", "resolved Etsy App API key is required")
	}
	if token == "" {
		return connector.TypedResult[EtsyResponse]{}, permanent("access_token_required", "resolved OAuth access token is required")
	}
	endpoint, err := url.Parse(baseURL(connection) + path)
	if err != nil {
		return connector.TypedResult[EtsyResponse]{}, connector.PermanentError("etsy.request_invalid", err)
	}
	endpoint.RawQuery = query.Encode()
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodGet, URL: endpoint.String(), Headers: map[string][]string{"Accept": {"application/json"}}, SecretHeaders: map[string][]string{"x-api-key": {apiKey}, "Authorization": {"Bearer " + token}}, MaxResponseBytes: responseLimit})
	if err != nil {
		return connector.TypedResult[EtsyResponse]{}, connector.RetryableError("etsy.network_error", err)
	}
	ref, payload := "http:"+strconv.Itoa(response.StatusCode), EtsyResponse{}
	validJSON := json.Unmarshal(response.Body, &payload) == nil
	typed := connector.TypedResult[EtsyResponse]{Output: payload, ResponseRef: ref}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code, cause := "etsy.http_"+strconv.Itoa(response.StatusCode), fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			return typed, connector.RetryableError(code, cause)
		}
		return typed, connector.PermanentError(code, cause)
	}
	if !validJSON {
		return connector.TypedResult[EtsyResponse]{ResponseRef: ref}, permanent("response_invalid", "provider response is invalid JSON")
	}
	if count := mapString(payload, "count"); count != "" {
		typed.ResponseRef = "etsy:page:count:" + count
	}
	return typed, nil
}

func shopID(connection connector.Connection) string { return config(connection, "shop_id", "") }
func baseURL(connection connector.Connection) string {
	if value := strings.TrimRight(config(connection, "base_url", ""), "/"); value != "" {
		return value
	}
	return defaultBaseURL
}
func config(connection connector.Connection, key, fallback string) string {
	if value := mapString(connection.Config, key); value != "" {
		return value
	}
	return fallback
}
func mapString(values map[string]any, key string) string {
	if values[key] == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(values[key]))
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
func permanent(code, message string) error {
	return connector.PermanentError("etsy."+code, errors.New(message))
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
