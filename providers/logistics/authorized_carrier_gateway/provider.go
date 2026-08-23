// Package authorizedcarriergateway implements a customer-authorized carrier rate gateway Provider.
package authorizedcarriergateway

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
	ConnectorKey           = "logistics"
	ProviderKey            = "authorized_carrier_gateway"
	defaultQuotePath       = "/v1/ocean-fcl/quotes"
	responseLimit    int64 = 4 << 20
)

var QuoteRates = connector.CallOperation[QuoteRatesInput, QuoteRatesOutput]{
	ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "quote_rates",
	ContractSHA256: "a6627675c6255587f553fc126f777f470f464b92d8b48514f211c2f01ba8eee5",
	Reliability: connector.ReliabilityContract{
		Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural},
		Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone},
	},
}

type QuoteRatesInput struct {
	Input RouteInput `json:"input"`
}

type RouteInput struct {
	Origin         string `json:"origin"`
	Destination    string `json:"destination"`
	ContainerType  string `json:"container_type"`
	ContainerCount int    `json:"container_count"`
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

// New constructs the Provider without performing network access.
func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("authorized carrier gateway transport is required")
	}
	implementation := &provider{transport: transport, now: time.Now}
	operation, err := connector.BindCall(QuoteRates, implementation.quoteRates)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), operation)
	if err != nil {
		return nil, err
	}
	implementation.Adapter = bound
	return implementation, nil
}

func schema() connector.ProviderSchema {
	minimum, maximum := float64(1), float64(90)
	return connector.ProviderSchema{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0",
		ConfigFields: []connector.ConfigField{
			{Key: "base_url", Name: "Authorized gateway base URL", Type: connector.ConfigFieldText, Required: true, Validation: connector.ConfigValidation{MaxLength: 2048, Pattern: `^https?://`}},
			{Key: "quote_path", Name: "Quote path", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"/v1/ocean-fcl/quotes"`)},
			{Key: "provider_identity", Name: "Actual carrier or aggregator identity", Type: connector.ConfigFieldText, Required: true},
			{Key: "authorization_ref", Name: "Commercial authorization reference", Type: connector.ConfigFieldText, Required: true},
			{Key: "test_origin", Name: "Test origin UN/LOCODE", Type: connector.ConfigFieldText, Required: true},
			{Key: "test_destination", Name: "Test destination UN/LOCODE", Type: connector.ConfigFieldText, Required: true},
			{Key: "test_container_type", Name: "Test container type", Type: connector.ConfigFieldSelect, Required: true, Validation: connector.ConfigValidation{Options: []string{"20gp", "40gp", "40hc", "40hq"}}},
			{Key: "test_currency", Name: "Test comparison currency", Type: connector.ConfigFieldSelect, Required: true, Validation: connector.ConfigValidation{Options: []string{"BRL", "USD"}}},
			{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}},
		},
		SecretFields: []connector.SecretField{{
			Key: "api_token", Name: "Authorized gateway API token", Required: true,
			CredentialKind: connector.SecretCredentialAPIKey, MaterialFormat: connector.SecretMaterialOpaque,
			RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound,
		}},
	}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	base := strings.TrimRight(configString(connection.Config, "base_url"), "/")
	parsed, err := url.Parse(base)
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return connector.PermanentError("authorized_carrier_gateway.endpoint_invalid", errors.New("HTTPS or loopback HTTP endpoint is required"))
	}
	if configString(connection.Config, "provider_identity") == "" {
		return connector.PermanentError("authorized_carrier_gateway.provider_identity_required", errors.New("provider identity is required"))
	}
	if configString(connection.Config, "authorization_ref") == "" {
		return connector.PermanentError("authorized_carrier_gateway.authorization_ref_required", errors.New("authorization reference is required"))
	}
	return validatePath(quotePath(connection))
}

func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	input := RouteInput{
		Origin: configString(request.Connection.Config, "test_origin"), Destination: configString(request.Connection.Config, "test_destination"),
		ContainerType: configString(request.Connection.Config, "test_container_type"), ContainerCount: 1,
		Currency: configString(request.Connection.Config, "test_currency"),
	}
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
	canonical, err := canonicalRequest(input)
	if err != nil {
		return nil, "", err
	}
	token := strings.TrimSpace(secrets["api_token"])
	if token == "" {
		return nil, "", connector.PermanentError("authorized_carrier_gateway.api_token_required", errors.New("resolved API token is required"))
	}
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", err
	}
	body, err := json.Marshal(canonical)
	if err != nil {
		return nil, "", err
	}
	queriedAt := p.now().UTC()
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{
		Method: http.MethodPost, URL: strings.TrimRight(configString(connection.Config, "base_url"), "/") + quotePath(connection), Body: body, MaxResponseBytes: responseLimit,
		Headers: map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/json"}, "Authorization": {"Bearer " + token}},
	})
	if err != nil {
		return nil, "", connector.RetryableError("authorized_carrier_gateway.network_error", err)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	if int64(len(response.Body)) > responseLimit {
		return nil, ref, connector.PermanentError("authorized_carrier_gateway.response_invalid", errors.New("response exceeds size limit"))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code := "authorized_carrier_gateway.http_" + strconv.Itoa(response.StatusCode)
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		switch response.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden:
			return nil, ref, connector.PermanentError("authorized_carrier_gateway.unauthorized", cause)
		case http.StatusTooManyRequests:
			return nil, ref, connector.RetryableError("authorized_carrier_gateway.rate_limited", cause)
		default:
			if response.StatusCode >= 500 {
				return nil, ref, connector.RetryableError(code, cause)
			}
			return nil, ref, connector.PermanentError(code, cause)
		}
	}
	payload := map[string]any{}
	if err := json.Unmarshal(response.Body, &payload); err != nil || !validPayload(payload) {
		return nil, ref, connector.PermanentError("authorized_carrier_gateway.response_contract_invalid", errors.New("provider response contract is invalid"))
	}
	requestDigest, responseDigest := sha256.Sum256(body), sha256.Sum256(response.Body)
	evidence := map[string]any{
		"provider": configString(connection.Config, "provider_identity"), "trust_mode": "production_authorized",
		"authorization_ref": configString(connection.Config, "authorization_ref"), "request": canonical,
		"request_sha256": "sha256:" + hex.EncodeToString(requestDigest[:]), "queried_at": queriedAt.Format(time.RFC3339Nano),
		"raw_response_base64": base64.StdEncoding.EncodeToString(response.Body), "raw_response_sha256": "sha256:" + hex.EncodeToString(responseDigest[:]),
		"raw_response_bytes": len(response.Body), "response_ref": ref, "response_status": response.StatusCode, "provider_payload": payload,
	}
	if value := firstHeader(response.Headers, "Date"); value != "" {
		evidence["provider_date_header"] = value
	}
	if value := firstHeader(response.Headers, "Expires"); value != "" {
		evidence["provider_expires_header"] = value
	}
	return evidence, ref, nil
}

func canonicalRequest(input RouteInput) (map[string]any, error) {
	origin, destination := strings.TrimSpace(input.Origin), strings.TrimSpace(input.Destination)
	containerType, currency := strings.ToLower(strings.TrimSpace(input.ContainerType)), strings.ToUpper(strings.TrimSpace(input.Currency))
	if origin == "" || destination == "" || input.ContainerCount < 1 || currency == "" {
		return nil, connector.PermanentError("authorized_carrier_gateway.route_required", errors.New("complete route is required"))
	}
	if _, ok := map[string]bool{"20gp": true, "40gp": true, "40hc": true, "40hq": true}[containerType]; !ok {
		return nil, connector.PermanentError("authorized_carrier_gateway.container_type_invalid", errors.New("container type is invalid"))
	}
	return map[string]any{"origin": origin, "destination": destination, "container_type": containerType, "container_count": input.ContainerCount, "currency": currency, "mode": "FCL", "result_set": "cheapestEachMode", "rfq_type": "ON_BEHALF_OF_CUSTOMER"}, nil
}

func validPayload(payload map[string]any) bool {
	response, ok := payload["response"].(map[string]any)
	if !ok || configString(response, "queryId") == "" {
		return false
	}
	quotes, ok := response["quotes"].([]any)
	return ok && len(quotes) == 1
}

func quotePath(connection connector.Connection) string {
	if value := configString(connection.Config, "quote_path"); value != "" {
		return value
	}
	return defaultQuotePath
}

func validatePath(path string) error {
	if !strings.HasPrefix(path, "/") || strings.Contains(path, "..") || strings.ContainsAny(path, "?#") {
		return connector.PermanentError("authorized_carrier_gateway.path_invalid", errors.New("quote path is invalid"))
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
	host = strings.TrimSpace(strings.ToLower(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
