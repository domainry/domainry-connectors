// Package freightos implements a private Freightos Custom Site Provider.
package freightos

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	ConnectorKey                = "logistics"
	ProviderKey                 = "freightos"
	defaultCalculatorPath       = "/api/shippingCalculator"
	responseLimit         int64 = 4 << 20
)

var QuoteRates = connector.CallOperation[QuoteRatesInput, QuoteRatesOutput]{
	ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "quote_rates",
	ContractSHA256: "a6627675c6255587f553fc126f777f470f464b92d8b48514f211c2f01ba8eee5",
	Reliability:    connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}},
}

type QuoteRatesInput struct {
	Input RouteInput `json:"input"`
}
type RouteInput struct {
	Origin         string `json:"origin"`
	Destination    string `json:"destination"`
	ContainerType  string `json:"container_type"`
	ContainerCount int    `json:"container_count"`
	WeightKG       string `json:"weight_kg,omitempty"`
	Currency       string `json:"currency"`
}
type QuoteRatesOutput struct {
	Products map[string]any `json:"products"`
}

type provider struct {
	connector.Adapter
	transport connector.Transport
	now       func() time.Time
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("freightos transport is required")
	}
	p := &provider{transport: transport, now: time.Now}
	operation, err := connector.BindCall(QuoteRates, p.quoteRates)
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
	minimum, maximum := float64(1), float64(90)
	return connector.ProviderSchema{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0",
		ConfigFields: []connector.ConfigField{
			{Key: "base_url", Name: "Private Freightos Custom Site base URL", Type: connector.ConfigFieldText, Required: true},
			{Key: "calculator_path", Name: "Shipping calculator path", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"/api/shippingCalculator"`)},
			{Key: "authorization_ref", Name: "Commercial authorization reference", Type: connector.ConfigFieldText, Required: true},
			{Key: "test_origin", Name: "Test origin UN/LOCODE", Type: connector.ConfigFieldText, Required: true},
			{Key: "test_destination", Name: "Test destination UN/LOCODE", Type: connector.ConfigFieldText, Required: true},
			{Key: "test_container_type", Name: "Test container type", Type: connector.ConfigFieldSelect, Required: true, Validation: connector.ConfigValidation{Options: []string{"20gp", "40gp", "40hc", "40hq"}}},
			{Key: "test_weight_kg", Name: "Test gross weight in kilograms", Type: connector.ConfigFieldText, Required: true},
			{Key: "test_currency", Name: "Test comparison currency", Type: connector.ConfigFieldSelect, Required: true, Validation: connector.ConfigValidation{Options: []string{"BRL", "USD"}}},
			{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}},
		},
		SecretFields: []connector.SecretField{{Key: "api_key", Name: "Freightos Custom Site API key", Required: true, CredentialKind: connector.SecretCredentialAPIKey, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}},
	}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	base := strings.TrimRight(configString(connection.Config, "base_url"), "/")
	parsed, err := url.Parse(base)
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return connector.PermanentError("freightos.endpoint_invalid", errors.New("HTTPS or loopback HTTP endpoint is required"))
	}
	if strings.EqualFold(parsed.Hostname(), "ship.freightos.com") {
		return connector.PermanentError("freightos.private_site_required", errors.New("private Custom Site is required"))
	}
	if configString(connection.Config, "authorization_ref") == "" {
		return connector.PermanentError("freightos.authorization_ref_required", errors.New("authorization reference is required"))
	}
	return validatePath(calculatorPath(connection))
}

func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	input := RouteInput{Origin: configString(request.Connection.Config, "test_origin"), Destination: configString(request.Connection.Config, "test_destination"), ContainerType: configString(request.Connection.Config, "test_container_type"), ContainerCount: 1, WeightKG: configString(request.Connection.Config, "test_weight_kg"), Currency: configString(request.Connection.Config, "test_currency")}
	products, ref, err := p.execute(ctx, request.Connection, request.Secrets, input)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, err := json.Marshal(map[string]any{"products": products, "response_ref": ref})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) quoteRates(ctx context.Context, request connector.TypedRequest[QuoteRatesInput]) (connector.TypedResult[QuoteRatesOutput], error) {
	products, ref, err := p.execute(ctx, request.Connection, request.Secrets, request.Input.Input)
	return connector.TypedResult[QuoteRatesOutput]{Output: QuoteRatesOutput{Products: products}, ResponseRef: ref}, err
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, input RouteInput) (map[string]any, string, error) {
	query, canonical, err := freightosQuery(input)
	if err != nil {
		return nil, "", err
	}
	apiKey := strings.TrimSpace(secrets["api_key"])
	if apiKey == "" {
		return nil, "", connector.PermanentError("freightos.api_key_required", errors.New("resolved API key is required"))
	}
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", err
	}
	queriedAt := p.now().UTC()
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodGet, URL: strings.TrimRight(configString(connection.Config, "base_url"), "/") + calculatorPath(connection) + "?" + query.Encode(), Headers: map[string][]string{"Accept": {"application/json"}}, SecretQuery: map[string]string{"apiKey": apiKey}, MaxResponseBytes: responseLimit})
	if err != nil {
		return nil, "", connector.RetryableError("freightos.network_error", err)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		switch response.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden:
			return nil, ref, connector.PermanentError("freightos.unauthorized", cause)
		case http.StatusTooManyRequests:
			return nil, ref, connector.RetryableError("freightos.rate_limited", cause)
		}
		if response.StatusCode >= 500 {
			return nil, ref, connector.RetryableError("freightos.http_"+strconv.Itoa(response.StatusCode), cause)
		}
		return nil, ref, connector.PermanentError("freightos.http_"+strconv.Itoa(response.StatusCode), cause)
	}
	payload := map[string]any{}
	if err := json.Unmarshal(response.Body, &payload); err != nil {
		return nil, ref, connector.PermanentError("freightos.response_invalid", errors.New("provider response is not valid JSON"))
	}
	requestRaw, _ := json.Marshal(canonical)
	requestDigest, responseDigest := sha256.Sum256(requestRaw), sha256.Sum256(response.Body)
	evidence := map[string]any{"provider": ProviderKey, "trust_mode": "production_authorized", "authorization_ref": configString(connection.Config, "authorization_ref"), "request": canonical, "request_sha256": "sha256:" + hex.EncodeToString(requestDigest[:]), "queried_at": queriedAt.Format(time.RFC3339Nano), "raw_response_base64": base64.StdEncoding.EncodeToString(response.Body), "raw_response_sha256": "sha256:" + hex.EncodeToString(responseDigest[:]), "raw_response_bytes": len(response.Body), "response_ref": ref, "response_status": response.StatusCode, "provider_payload": payload}
	if value := firstHeader(response.Headers, "Date"); value != "" {
		evidence["provider_date_header"] = value
	}
	if value := firstHeader(response.Headers, "Expires"); value != "" {
		evidence["provider_expires_header"] = value
	}
	return evidence, ref, nil
}

func freightosQuery(input RouteInput) (url.Values, map[string]any, error) {
	origin, destination := strings.TrimSpace(input.Origin), strings.TrimSpace(input.Destination)
	containerType, currency, weight := strings.ToLower(strings.TrimSpace(input.ContainerType)), strings.ToUpper(strings.TrimSpace(input.Currency)), strings.TrimSpace(input.WeightKG)
	if origin == "" || destination == "" || input.ContainerCount < 1 || currency == "" {
		return nil, nil, connector.PermanentError("freightos.route_required", errors.New("complete route is required"))
	}
	loadType := map[string]string{"20gp": "container20", "40gp": "container40", "40hc": "container40HC", "40hq": "container40HC"}[containerType]
	if loadType == "" {
		return nil, nil, connector.PermanentError("freightos.container_type_invalid", errors.New("container type is invalid"))
	}
	canonical := map[string]any{"origin": origin, "destination": destination, "container_type": containerType, "container_count": input.ContainerCount, "weight_kg": weight, "currency": currency, "mode": "FCL", "result_set": "cheapestEachMode", "rfq_type": "ON_BEHALF_OF_CUSTOMER"}
	query := url.Values{"format": {"json"}, "origin": {origin}, "destination": {destination}, "loadtype": {loadType}, "quantity": {strconv.Itoa(input.ContainerCount)}, "currency": {currency}, "mode": {"FCL"}, "resultSet": {"cheapestEachMode"}, "saveThisQuote": {"true"}, "rFQType": {"ON_BEHALF_OF_CUSTOMER"}}
	if weight != "" {
		query.Set("weight", weight+"kg")
	}
	return query, canonical, nil
}

func calculatorPath(connection connector.Connection) string {
	if value := configString(connection.Config, "calculator_path"); value != "" {
		return value
	}
	return defaultCalculatorPath
}
func validatePath(path string) error {
	if !strings.HasPrefix(path, "/") || strings.Contains(path, "..") || strings.ContainsAny(path, "?#") {
		return connector.PermanentError("freightos.path_invalid", errors.New("calculator path is invalid"))
	}
	return nil
}
func firstHeader(headers map[string][]string, key string) string {
	for current, values := range headers {
		if strings.EqualFold(current, key) && len(values) > 0 {
			return strings.TrimSpace(values[0])
		}
	}
	return ""
}
func configString(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return strings.TrimSpace(value)
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
