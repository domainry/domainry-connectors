// Package dhl implements the official DHL Express MyDHL Provider.
package dhl

import (
	"context"
	"encoding/base64"
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
	ConnectorKey   = "logistics"
	ProviderKey    = "dhl"
	defaultBaseURL = "https://express.api.dhl.com/mydhlapi"
	responseLimit  = 8 << 20
	defaultTimeout = 30
	maximumTimeout = 120
)

type ProviderInput struct {
	Input map[string]any `json:"input"`
}

type TrackShipmentInput struct {
	TrackingNumber string `json:"tracking_number"`
	TrackingView   string `json:"trackingView,omitempty"`
	LevelOfDetail  string `json:"levelOfDetail,omitempty"`
}

type UpdatePickupInput struct {
	PickupID string         `json:"pickup_id"`
	Input    map[string]any `json:"input"`
}

type CancelPickupInput struct {
	PickupID      string `json:"pickup_id"`
	RequestorName string `json:"requestorName,omitempty"`
}

var (
	CancelPickup   = writeOp[CancelPickupInput]("cancel_pickup", "17d2b2701de1e7671dd1423881e4b8ea75012bc029b37a9bc5cfd6c658191251")
	CreatePickup   = writeOp[ProviderInput]("create_pickup", "7c0941e68c3960504e16c80709fe8141a8f4b7362b8f0282b055e098b519aa3c")
	CreateShipment = writeOp[ProviderInput]("create_shipment", "8f1c2297627d8f3654725508a02f3b796fd5a90efe178bffe8587316abbb062a")
	QuoteRates     = readOp[ProviderInput]("quote_rates", "a6627675c6255587f553fc126f777f470f464b92d8b48514f211c2f01ba8eee5")
	TestConnection = readOp[struct{}]("test_connection", "a2879c297028e5fee189b49203b9a3f9c4c96182c7b68f928006edc6bd93408d")
	TrackShipment  = readOp[TrackShipmentInput]("track_shipment", "bfe77eb2a7601443a0c0f731a79fbfcf490315105b98c084adf77f312ef4f24e")
	UpdatePickup   = writeOp[UpdatePickupInput]("update_pickup", "ed13aed64a970c35ebd948d689dd6ee16a0b9c8280b90aeba03eafaaeb874d7e")
)

func readOp[I any](key, hash string) connector.CallOperation[I, map[string]any] {
	return operation[I](key, hash, connector.EffectRead, connector.IdempotencyNatural)
}

func writeOp[I any](key, hash string) connector.CallOperation[I, map[string]any] {
	return operation[I](key, hash, connector.EffectWrite, connector.IdempotencyNone)
}

func operation[I any](key, hash string, effect connector.OperationEffect, idempotency connector.IdempotencyStrategy) connector.CallOperation[I, map[string]any] {
	return connector.CallOperation[I, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: key, ContractSHA256: hash, Reliability: connector.ReliabilityContract{Effect: effect, Idempotency: connector.IdempotencyContract{Strategy: idempotency}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
	now       func() time.Time
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("DHL transport is required")
	}
	p := &provider{transport: transport, now: time.Now}
	operations := make([]connector.BoundOperation, 0, 7)
	bindings := []func() (connector.BoundOperation, error){
		func() (connector.BoundOperation, error) { return connector.BindCall(CancelPickup, p.cancelPickup) },
		func() (connector.BoundOperation, error) { return connector.BindCall(CreatePickup, p.createPickup) },
		func() (connector.BoundOperation, error) { return connector.BindCall(CreateShipment, p.createShipment) },
		func() (connector.BoundOperation, error) { return connector.BindCall(QuoteRates, p.quoteRates) },
		func() (connector.BoundOperation, error) { return connector.BindCall(TestConnection, p.test) },
		func() (connector.BoundOperation, error) { return connector.BindCall(TrackShipment, p.trackShipment) },
		func() (connector.BoundOperation, error) { return connector.BindCall(UpdatePickup, p.updatePickup) },
	}
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
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "base_url", Name: "MyDHL API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://express.api.dhl.com/mydhlapi"`)}, {Key: "account_number", Name: "DHL Express account number", Type: connector.ConfigFieldText}, {Key: "origin_country_code", Name: "Test origin country code", Type: connector.ConfigFieldText, Required: true}, {Key: "origin_postal_code", Name: "Test origin postal code", Type: connector.ConfigFieldText, Required: true}, {Key: "origin_city_name", Name: "Test origin city", Type: connector.ConfigFieldText, Required: true}, {Key: "destination_country_code", Name: "Test destination country code", Type: connector.ConfigFieldText, Required: true}, {Key: "destination_postal_code", Name: "Test destination postal code", Type: connector.ConfigFieldText, Required: true}, {Key: "destination_city_name", Name: "Test destination city", Type: connector.ConfigFieldText, Required: true}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}}}, SecretFields: []connector.SecretField{secret("api_key", "MyDHL API key", connector.SecretCredentialIdentifier), secret("api_secret", "MyDHL API secret", connector.SecretCredentialBasicAuthPassword)}}
}

func secret(key, name string, kind connector.SecretCredentialKind) connector.SecretField {
	return connector.SecretField{Key: key, Name: name, Required: true, CredentialKind: kind, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	endpoint, err := url.Parse(baseURL(connection))
	if err != nil || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || (endpoint.Scheme != "https" && !(endpoint.Scheme == "http" && loopback(endpoint.Hostname()))) {
		return permanent("endpoint_invalid", "HTTPS or loopback HTTP endpoint is required")
	}
	timeout := integer(connection.Config["timeout_seconds"], defaultTimeout)
	if timeout < 1 || timeout > maximumTimeout {
		return permanent("timeout_invalid", "timeout_seconds is invalid")
	}
	return nil
}

func (p *provider) test(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[map[string]any], error) {
	query := url.Values{"originCountryCode": {config(request.Connection, "origin_country_code")}, "originPostalCode": {config(request.Connection, "origin_postal_code")}, "originCityName": {config(request.Connection, "origin_city_name")}, "destinationCountryCode": {config(request.Connection, "destination_country_code")}, "destinationPostalCode": {config(request.Connection, "destination_postal_code")}, "destinationCityName": {config(request.Connection, "destination_city_name")}, "weight": {"1"}, "length": {"10"}, "width": {"10"}, "height": {"10"}, "plannedShippingDate": {p.now().UTC().Add(24 * time.Hour).Format("2006-01-02")}, "isCustomsDeclarable": {"false"}, "unitOfMeasurement": {"metric"}}
	for _, key := range []string{"originCountryCode", "originPostalCode", "originCityName", "destinationCountryCode", "destinationPostalCode", "destinationCityName"} {
		if strings.TrimSpace(query.Get(key)) == "" {
			return empty(), permanent("test_route_required", "complete test route is required")
		}
	}
	if account := config(request.Connection, "account_number"); account != "" {
		query.Set("accountNumber", account)
	}
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/rates", query, nil, false)
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
	return p.inputOperation(ctx, request.Connection, request.Secrets, http.MethodPost, "/rates", request.Input.Input, false)
}

func (p *provider) createShipment(ctx context.Context, request connector.TypedRequest[ProviderInput]) (connector.TypedResult[map[string]any], error) {
	return p.inputOperation(ctx, request.Connection, request.Secrets, http.MethodPost, "/shipments", request.Input.Input, true)
}

func (p *provider) createPickup(ctx context.Context, request connector.TypedRequest[ProviderInput]) (connector.TypedResult[map[string]any], error) {
	return p.inputOperation(ctx, request.Connection, request.Secrets, http.MethodPost, "/pickups", request.Input.Input, true)
}

func (p *provider) inputOperation(ctx context.Context, connection connector.Connection, secrets map[string]string, method, path string, input map[string]any, write bool) (connector.TypedResult[map[string]any], error) {
	if len(input) == 0 {
		return empty(), permanent("input_required", "provider input is required")
	}
	return p.execute(ctx, connection, secrets, method, path, nil, input, write)
}

func (p *provider) trackShipment(ctx context.Context, request connector.TypedRequest[TrackShipmentInput]) (connector.TypedResult[map[string]any], error) {
	tracking := strings.TrimSpace(request.Input.TrackingNumber)
	if tracking == "" {
		return empty(), permanent("tracking_number_required", "tracking number is required")
	}
	query := url.Values{}
	if request.Input.TrackingView != "" {
		query.Set("trackingView", request.Input.TrackingView)
	}
	if request.Input.LevelOfDetail != "" {
		query.Set("levelOfDetail", request.Input.LevelOfDetail)
	}
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/shipments/"+url.PathEscape(tracking)+"/tracking", query, nil, false)
}

func (p *provider) updatePickup(ctx context.Context, request connector.TypedRequest[UpdatePickupInput]) (connector.TypedResult[map[string]any], error) {
	pickup := strings.TrimSpace(request.Input.PickupID)
	if pickup == "" || len(request.Input.Input) == 0 {
		return empty(), permanent("pickup_fields_required", "pickup ID and input are required")
	}
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodPatch, "/pickups/"+url.PathEscape(pickup), nil, request.Input.Input, true)
}

func (p *provider) cancelPickup(ctx context.Context, request connector.TypedRequest[CancelPickupInput]) (connector.TypedResult[map[string]any], error) {
	pickup := strings.TrimSpace(request.Input.PickupID)
	if pickup == "" {
		return empty(), permanent("pickup_id_required", "pickup ID is required")
	}
	query := url.Values{}
	if request.Input.RequestorName != "" {
		query.Set("requestorName", request.Input.RequestorName)
	}
	return p.execute(ctx, request.Connection, request.Secrets, http.MethodDelete, "/pickups/"+url.PathEscape(pickup), query, nil, true)
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, method, path string, query url.Values, body map[string]any, write bool) (connector.TypedResult[map[string]any], error) {
	if err := p.ValidateConfig(connection); err != nil {
		return empty(), err
	}
	key, secretValue := strings.TrimSpace(secrets["api_key"]), strings.TrimSpace(secrets["api_secret"])
	if key == "" || secretValue == "" {
		return empty(), permanent("credentials_required", "resolved API key and secret are required")
	}
	endpoint, _ := url.Parse(strings.TrimRight(baseURL(connection), "/") + path)
	endpoint.RawQuery = query.Encode()
	var raw []byte
	var err error
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return empty(), permanent("request_invalid", "request body is invalid")
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	authorization := "Basic " + base64.StdEncoding.EncodeToString([]byte(key+":"+secretValue))
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint.String(), Headers: headers, SecretHeaders: map[string][]string{"Authorization": {authorization}}, Body: raw, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		return empty(), transportFailure(write, "network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := map[string]any{}
	valid := len(response.Body) == 0 || json.Unmarshal(response.Body, &payload) == nil
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code := "http_" + strconv.Itoa(response.StatusCode)
		if title := firstString(payload, "title", "detail"); title != "" {
			code = sanitize(title)
		}
		cause := fmt.Errorf("DHL returned HTTP %d", response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests {
			return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, connector.RetryableError("dhl."+code, cause)
		}
		if response.StatusCode == http.StatusRequestTimeout || response.StatusCode >= 500 {
			return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, transportFailure(write, code, cause)
		}
		return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, connector.PermanentError("dhl."+code, cause)
	}
	if !valid {
		return connector.TypedResult[map[string]any]{ResponseRef: ref}, transportFailure(write, "response_invalid", errors.New("DHL response is invalid JSON"))
	}
	if id := firstString(payload, "shipmentTrackingNumber", "dispatchConfirmationNumber"); id != "" {
		ref = "dhl:" + id
	}
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, nil
}

func transportFailure(write bool, suffix string, cause error) error {
	if write {
		return connector.UncertainError("dhl."+suffix, cause)
	}
	return connector.RetryableError("dhl."+suffix, cause)
}

func baseURL(connection connector.Connection) string {
	if value := config(connection, "base_url"); value != "" {
		return value
	}
	return defaultBaseURL
}

func config(connection connector.Connection, key string) string {
	value, _ := connection.Config[key].(string)
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

func firstString(values map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := values[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func sanitize(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	return strings.Map(func(char rune) rune {
		if char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '_' || char == '-' {
			return char
		}
		return '_'
	}, value)
}

func loopback(host string) bool {
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

func empty() connector.TypedResult[map[string]any] { return connector.TypedResult[map[string]any]{} }

func permanent(code, message string) error {
	return connector.PermanentError("dhl."+code, errors.New(message))
}
