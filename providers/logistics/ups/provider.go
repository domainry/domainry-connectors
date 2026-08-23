// Package ups implements the official UPS logistics Provider.
package ups

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
	"github.com/domainry/domainry-connectors/internal/oauth2"
)

const (
	ConnectorKey                   = "logistics"
	ProviderKey                    = "ups"
	defaultBaseURL                 = "https://onlinetools.ups.com"
	defaultTokenURL                = "https://onlinetools.ups.com/security/v1/oauth/token"
	defaultAPIVersion              = "v2409"
	defaultTransactionSource       = "domainry-runtime"
	defaultTimeout                 = 30
	maximumTimeout                 = 120
	responseLimit            int64 = 8 << 20
)

type ProviderInput struct {
	Input map[string]any `json:"input"`
}
type TrackShipmentInput struct {
	TrackingNumber   string `json:"tracking_number"`
	Locale           string `json:"locale,omitempty"`
	ReturnSignature  string `json:"returnSignature,omitempty"`
	ReturnMilestones string `json:"returnMilestones,omitempty"`
	ReturnPOD        string `json:"returnPOD,omitempty"`
}

var (
	CreateShipment = operation[ProviderInput]("create_shipment", "8f1c2297627d8f3654725508a02f3b796fd5a90efe178bffe8587316abbb062a", connector.EffectWrite, connector.IdempotencyNone)
	QuoteRates     = operation[ProviderInput]("quote_rates", "a6627675c6255587f553fc126f777f470f464b92d8b48514f211c2f01ba8eee5", connector.EffectRead, connector.IdempotencyNatural)
	TestConnection = operation[struct{}]("test_connection", "a2879c297028e5fee189b49203b9a3f9c4c96182c7b68f928006edc6bd93408d", connector.EffectRead, connector.IdempotencyNatural)
	TrackShipment  = operation[TrackShipmentInput]("track_shipment", "bfe77eb2a7601443a0c0f731a79fbfcf490315105b98c084adf77f312ef4f24e", connector.EffectRead, connector.IdempotencyNatural)
)

func operation[I any](key, hash string, effect connector.OperationEffect, idempotency connector.IdempotencyStrategy) connector.CallOperation[I, map[string]any] {
	return connector.CallOperation[I, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: key, ContractSHA256: hash, Reliability: connector.ReliabilityContract{Effect: effect, Idempotency: connector.IdempotencyContract{Strategy: idempotency}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("UPS transport is required")
	}
	p := &provider{transport: transport}
	bindings := []func() (connector.BoundOperation, error){
		func() (connector.BoundOperation, error) { return connector.BindCall(CreateShipment, p.createShipment) },
		func() (connector.BoundOperation, error) { return connector.BindCall(QuoteRates, p.quoteRates) },
		func() (connector.BoundOperation, error) { return connector.BindCall(TestConnection, p.test) },
		func() (connector.BoundOperation, error) { return connector.BindCall(TrackShipment, p.trackShipment) },
	}
	operations := make([]connector.BoundOperation, 0, len(bindings))
	for _, bind := range bindings {
		bound, err := bind()
		if err != nil {
			return nil, err
		}
		operations = append(operations, bound)
	}
	adapter, err := connector.NewProvider(schema(), operations...)
	if err != nil {
		return nil, err
	}
	p.Adapter = adapter
	return p, nil
}

func schema() connector.ProviderSchema {
	minimum, maximum := float64(1), float64(maximumTimeout)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{
		{Key: "base_url", Name: "UPS API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://onlinetools.ups.com"`)},
		{Key: "token_url", Name: "UPS OAuth token URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://onlinetools.ups.com/security/v1/oauth/token"`)},
		{Key: "rating_version", Name: "Rating API version", Type: connector.ConfigFieldText, Default: json.RawMessage(`"v2409"`)},
		{Key: "shipping_version", Name: "Shipping API version", Type: connector.ConfigFieldText, Default: json.RawMessage(`"v2409"`)},
		{Key: "transaction_source", Name: "Transaction source", Type: connector.ConfigFieldText, Default: json.RawMessage(`"domainry-runtime"`)},
		{Key: "test_rate_request", Name: "Connection test Rating request", Type: connector.ConfigFieldJSON, Required: true},
		{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}},
	}, SecretFields: []connector.SecretField{
		secret("access_token", "UPS OAuth access token", true, connector.SecretCredentialBearerToken, connector.SecretRotationOAuthRefresh),
		secret("client_id", "UPS OAuth client ID", false, connector.SecretCredentialIdentifier, connector.SecretRotationManual),
		secret("client_secret", "UPS OAuth client secret", false, connector.SecretCredentialOAuthClientSecret, connector.SecretRotationManual),
	}}
}

func secret(key, name string, required bool, kind connector.SecretCredentialKind, rotation connector.SecretRotationPolicy) connector.SecretField {
	return connector.SecretField{Key: key, Name: name, Required: required, CredentialKind: kind, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: rotation, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	for _, raw := range []string{baseURL(connection), tokenURL(connection)} {
		endpoint, err := url.Parse(raw)
		if err != nil || endpoint.Host == "" || endpoint.User != nil || endpoint.Fragment != "" || (endpoint.Scheme != "https" && !(endpoint.Scheme == "http" && loopback(endpoint.Hostname()))) {
			return permanent("endpoint_invalid", "HTTPS or loopback HTTP endpoints are required")
		}
	}
	for _, key := range []string{"rating_version", "shipping_version"} {
		if !validVersion(configDefault(connection, key, defaultAPIVersion)) {
			return permanent("api_version_invalid", key+" is invalid")
		}
	}
	if source := configDefault(connection, "transaction_source", defaultTransactionSource); source == "" || strings.ContainsAny(source, "\r\n") {
		return permanent("transaction_source_invalid", "transaction_source is invalid")
	}
	timeout := integer(connection.Config["timeout_seconds"], defaultTimeout)
	if timeout < 1 || timeout > maximumTimeout {
		return permanent("timeout_invalid", "timeout_seconds is invalid")
	}
	return nil
}

func (p *provider) test(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[map[string]any], error) {
	body, ok := request.Connection.Config["test_rate_request"].(map[string]any)
	if !ok || len(body) == 0 {
		return empty(), permanent("test_rate_request_required", "test_rate_request is required")
	}
	return p.executeWithRefresh(ctx, request.Connection, request.Secrets, request.RequestRef, http.MethodPost, ratingPath(request.Connection), nil, body, false)
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.test(ctx, connector.TypedRequest[struct{}]{Connection: request.Connection, Secrets: request.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}
func (p *provider) quoteRates(ctx context.Context, request connector.TypedRequest[ProviderInput]) (connector.TypedResult[map[string]any], error) {
	return p.inputOperation(ctx, request, ratingPath(request.Connection), false)
}
func (p *provider) createShipment(ctx context.Context, request connector.TypedRequest[ProviderInput]) (connector.TypedResult[map[string]any], error) {
	return p.inputOperation(ctx, request, "/api/shipments/"+apiVersion(request.Connection, "shipping_version")+"/ship", true)
}
func (p *provider) inputOperation(ctx context.Context, request connector.TypedRequest[ProviderInput], path string, write bool) (connector.TypedResult[map[string]any], error) {
	if len(request.Input.Input) == 0 {
		return empty(), permanent("input_required", "provider input is required")
	}
	return p.executeWithRefresh(ctx, request.Connection, request.Secrets, request.RequestRef, http.MethodPost, path, nil, request.Input.Input, write)
}
func (p *provider) trackShipment(ctx context.Context, request connector.TypedRequest[TrackShipmentInput]) (connector.TypedResult[map[string]any], error) {
	tracking := strings.TrimSpace(request.Input.TrackingNumber)
	if tracking == "" {
		return empty(), permanent("tracking_number_required", "tracking number is required")
	}
	query := url.Values{}
	for key, value := range map[string]string{"locale": request.Input.Locale, "returnSignature": request.Input.ReturnSignature, "returnMilestones": request.Input.ReturnMilestones, "returnPOD": request.Input.ReturnPOD} {
		if strings.TrimSpace(value) != "" {
			query.Set(key, strings.TrimSpace(value))
		}
	}
	return p.executeWithRefresh(ctx, request.Connection, request.Secrets, request.RequestRef, http.MethodGet, "/api/track/v1/details/"+url.PathEscape(tracking), query, nil, false)
}

func (p *provider) executeWithRefresh(ctx context.Context, connection connector.Connection, secrets map[string]string, requestRef, method, path string, query url.Values, body map[string]any, write bool) (connector.TypedResult[map[string]any], error) {
	token := strings.TrimSpace(secrets["access_token"])
	if token == "" {
		return empty(), permanent("access_token_required", "resolved access token is required")
	}
	result, status, err := p.execute(ctx, connection, requestRef, method, path, query, body, token, write)
	if status != http.StatusUnauthorized {
		return result, err
	}
	clientID, clientSecret := strings.TrimSpace(secrets["client_id"]), strings.TrimSpace(secrets["client_secret"])
	if clientID == "" || clientSecret == "" {
		return result, err
	}
	refreshed, refreshErr := oauth2.ClientCredentials(ctx, p.transport, oauth2.ClientCredentialsRequest{Endpoint: tokenURL(connection), ClientID: clientID, ClientSecret: clientSecret, ErrorPrefix: "ups", ClientAuthentication: oauth2.ClientAuthenticationBasic})
	if refreshErr != nil {
		return connector.TypedResult[map[string]any]{ResponseRef: "oauth:refresh_failed"}, refreshErr
	}
	result, _, err = p.execute(ctx, connection, requestRef, method, path, query, body, refreshed.AccessToken, write)
	result.SecretUpdates = map[string]string{"access_token": refreshed.AccessToken}
	return result, err
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, requestRef, method, path string, query url.Values, body map[string]any, token string, write bool) (connector.TypedResult[map[string]any], int, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return empty(), 0, err
	}
	endpoint, _ := url.Parse(strings.TrimRight(baseURL(connection), "/") + path)
	endpoint.RawQuery = query.Encode()
	var raw []byte
	var err error
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return empty(), 0, permanent("request_invalid", "request body is invalid")
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}, "transId": {transactionID(requestRef)}, "transactionSrc": {configDefault(connection, "transaction_source", defaultTransactionSource)}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint.String(), Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, Body: raw, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		return empty(), 0, transportFailure(write, "network_error", transportErr)
	}
	status, ref := response.StatusCode, "http:"+strconv.Itoa(response.StatusCode)
	payload := map[string]any{}
	valid := len(response.Body) == 0 || json.Unmarshal(response.Body, &payload) == nil
	if status < 200 || status >= 300 {
		code := upsErrorCode(payload, status)
		cause := fmt.Errorf("UPS returned HTTP %d", status)
		if status == http.StatusUnauthorized {
			return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, status, connector.PermanentError(code, cause)
		}
		if status == http.StatusTooManyRequests {
			return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, status, connector.RetryableError(code, cause)
		}
		if status == http.StatusRequestTimeout || status >= 500 {
			return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, status, transportFailure(write, strings.TrimPrefix(code, "ups."), cause)
		}
		return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, status, connector.PermanentError(code, cause)
	}
	if !valid {
		return connector.TypedResult[map[string]any]{ResponseRef: ref}, status, transportFailure(write, "response_invalid", errors.New("UPS response is invalid JSON"))
	}
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: upsResponseRef(payload, ref)}, status, nil
}

func upsErrorCode(payload map[string]any, status int) string {
	if response, ok := payload["response"].(map[string]any); ok {
		if items, ok := response["errors"].([]any); ok && len(items) > 0 {
			if detail, ok := items[0].(map[string]any); ok {
				if code := mapString(detail, "code"); code != "" {
					return "ups." + strings.ToLower(code)
				}
			}
		}
	}
	return "ups.http_" + strconv.Itoa(status)
}
func upsResponseRef(payload map[string]any, fallback string) string {
	if shipment, ok := payload["ShipmentResponse"].(map[string]any); ok {
		if results, ok := shipment["ShipmentResults"].(map[string]any); ok {
			if number := mapString(results, "ShipmentIdentificationNumber"); number != "" {
				return "ups:" + number
			}
		}
	}
	return fallback
}
func ratingPath(connection connector.Connection) string {
	return "/api/rating/" + apiVersion(connection, "rating_version") + "/Rate"
}
func apiVersion(connection connector.Connection, key string) string {
	return url.PathEscape(strings.Trim(configDefault(connection, key, defaultAPIVersion), "/"))
}
func validVersion(value string) bool {
	value = strings.Trim(value, "/")
	if value == "" || len(value) > 32 {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '-' || r == '_') {
			return false
		}
	}
	return true
}
func transactionID(requestRef string) string {
	if value := strings.TrimSpace(requestRef); value != "" {
		return value
	}
	return defaultTransactionSource
}
func transportFailure(write bool, suffix string, cause error) error {
	if write {
		return connector.UncertainError("ups."+suffix, cause)
	}
	return connector.RetryableError("ups."+suffix, cause)
}
func baseURL(connection connector.Connection) string {
	return configDefault(connection, "base_url", defaultBaseURL)
}
func tokenURL(connection connector.Connection) string {
	return configDefault(connection, "token_url", defaultTokenURL)
}
func configDefault(connection connector.Connection, key, fallback string) string {
	if value := config(connection, key); value != "" {
		return value
	}
	return fallback
}
func config(connection connector.Connection, key string) string {
	value, _ := connection.Config[key].(string)
	return strings.TrimSpace(value)
}
func mapString(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return strings.TrimSpace(value)
}
func integer(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		return typed
	case float64:
		return int(typed)
	case json.Number:
		if parsed, err := strconv.Atoi(typed.String()); err == nil {
			return parsed
		}
	}
	return fallback
}
func loopback(host string) bool                    { return host == "localhost" || host == "127.0.0.1" || host == "::1" }
func empty() connector.TypedResult[map[string]any] { return connector.TypedResult[map[string]any]{} }
func permanent(code, message string) error {
	return connector.PermanentError("ups."+code, errors.New(message))
}
