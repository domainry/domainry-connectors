// Package fedex implements the official FedEx logistics Provider.
package fedex

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
	ConnectorKey    = "logistics"
	ProviderKey     = "fedex"
	defaultBaseURL  = "https://apis.fedex.com"
	defaultTokenURL = "https://apis.fedex.com/oauth/token"
	defaultTimeout  = 30
	maximumTimeout  = 120
	responseLimit   = 8 << 20
)

type ProviderInput struct {
	Input map[string]any `json:"input"`
}

type TrackShipmentInput struct {
	TrackingNumber       string `json:"tracking_number"`
	IncludeDetailedScans string `json:"include_detailed_scans,omitempty"`
}

type CancelPickupInput struct {
	PickupID string `json:"pickup_id"`
}

var (
	CancelPickup   = writeOp[CancelPickupInput]("cancel_pickup", "17d2b2701de1e7671dd1423881e4b8ea75012bc029b37a9bc5cfd6c658191251")
	CreatePickup   = writeOp[ProviderInput]("create_pickup", "7c0941e68c3960504e16c80709fe8141a8f4b7362b8f0282b055e098b519aa3c")
	CreateShipment = writeOp[ProviderInput]("create_shipment", "8f1c2297627d8f3654725508a02f3b796fd5a90efe178bffe8587316abbb062a")
	QuoteRates     = readOp[ProviderInput]("quote_rates", "a6627675c6255587f553fc126f777f470f464b92d8b48514f211c2f01ba8eee5")
	TestConnection = readOp[struct{}]("test_connection", "a2879c297028e5fee189b49203b9a3f9c4c96182c7b68f928006edc6bd93408d")
	TrackShipment  = readOp[TrackShipmentInput]("track_shipment", "bfe77eb2a7601443a0c0f731a79fbfcf490315105b98c084adf77f312ef4f24e")
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
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("FedEx transport is required")
	}
	p := &provider{transport: transport}
	operations := make([]connector.BoundOperation, 0, 6)
	bindings := []func() (connector.BoundOperation, error){
		func() (connector.BoundOperation, error) { return connector.BindCall(CancelPickup, p.cancelPickup) },
		func() (connector.BoundOperation, error) { return connector.BindCall(CreatePickup, p.createPickup) },
		func() (connector.BoundOperation, error) { return connector.BindCall(CreateShipment, p.createShipment) },
		func() (connector.BoundOperation, error) { return connector.BindCall(QuoteRates, p.quoteRates) },
		func() (connector.BoundOperation, error) { return connector.BindCall(TestConnection, p.test) },
		func() (connector.BoundOperation, error) { return connector.BindCall(TrackShipment, p.trackShipment) },
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
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "base_url", Name: "FedEx API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://apis.fedex.com"`)}, {Key: "token_url", Name: "FedEx OAuth token URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://apis.fedex.com/oauth/token"`)}, {Key: "test_rate_request", Name: "Connection test Rate request", Type: connector.ConfigFieldJSON, Required: true}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}}}, SecretFields: []connector.SecretField{secret("access_token", "FedEx OAuth access token", connector.SecretCredentialBearerToken, connector.SecretRotationOAuthRefresh), secret("client_id", "FedEx API key / client ID", connector.SecretCredentialIdentifier, connector.SecretRotationManual), secret("client_secret", "FedEx secret key / client secret", connector.SecretCredentialOAuthClientSecret, connector.SecretRotationManual)}}
}

func secret(key, name string, kind connector.SecretCredentialKind, rotation connector.SecretRotationPolicy) connector.SecretField {
	return connector.SecretField{Key: key, Name: name, Required: true, CredentialKind: kind, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: rotation, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	for _, raw := range []string{baseURL(connection), tokenURL(connection)} {
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && loopback(parsed.Hostname()))) {
			return permanent("endpoint_invalid", "HTTPS or loopback HTTP endpoints are required")
		}
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
	return p.executeWithRefresh(ctx, request.Connection, request.Secrets, "/rate/v1/rates/quotes", body, false)
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
	return p.inputOperation(ctx, request.Connection, request.Secrets, "/rate/v1/rates/quotes", request.Input.Input, false)
}

func (p *provider) createShipment(ctx context.Context, request connector.TypedRequest[ProviderInput]) (connector.TypedResult[map[string]any], error) {
	return p.inputOperation(ctx, request.Connection, request.Secrets, "/ship/v1/shipments", request.Input.Input, true)
}

func (p *provider) createPickup(ctx context.Context, request connector.TypedRequest[ProviderInput]) (connector.TypedResult[map[string]any], error) {
	return p.inputOperation(ctx, request.Connection, request.Secrets, "/pickup/v1/pickups", request.Input.Input, true)
}

func (p *provider) inputOperation(ctx context.Context, connection connector.Connection, secrets map[string]string, path string, input map[string]any, write bool) (connector.TypedResult[map[string]any], error) {
	if len(input) == 0 {
		return empty(), permanent("input_required", "provider input is required")
	}
	return p.executeWithRefresh(ctx, connection, secrets, path, input, write)
}

func (p *provider) trackShipment(ctx context.Context, request connector.TypedRequest[TrackShipmentInput]) (connector.TypedResult[map[string]any], error) {
	tracking := strings.TrimSpace(request.Input.TrackingNumber)
	if tracking == "" {
		return empty(), permanent("tracking_number_required", "tracking number is required")
	}
	body := map[string]any{"includeDetailedScans": strings.TrimSpace(request.Input.IncludeDetailedScans) != "false", "trackingInfo": []any{map[string]any{"trackingNumberInfo": map[string]any{"trackingNumber": tracking}}}}
	return p.executeWithRefresh(ctx, request.Connection, request.Secrets, "/track/v1/trackingnumbers", body, false)
}

func (p *provider) cancelPickup(ctx context.Context, request connector.TypedRequest[CancelPickupInput]) (connector.TypedResult[map[string]any], error) {
	pickup := strings.TrimSpace(request.Input.PickupID)
	if pickup == "" {
		return empty(), permanent("pickup_id_required", "pickup ID is required")
	}
	return p.executeWithRefresh(ctx, request.Connection, request.Secrets, "/pickup/v1/pickups/cancel", map[string]any{"pickupConfirmationCode": pickup}, true)
}

func (p *provider) executeWithRefresh(ctx context.Context, connection connector.Connection, secrets map[string]string, path string, body map[string]any, write bool) (connector.TypedResult[map[string]any], error) {
	token := strings.TrimSpace(secrets["access_token"])
	if token == "" {
		return empty(), permanent("access_token_required", "resolved access token is required")
	}
	result, status, err := p.execute(ctx, connection, path, body, token, write)
	if status != http.StatusUnauthorized {
		return result, err
	}
	clientID, clientSecret := strings.TrimSpace(secrets["client_id"]), strings.TrimSpace(secrets["client_secret"])
	if clientID == "" || clientSecret == "" {
		return result, err
	}
	refreshed, refreshErr := oauth2.ClientCredentials(ctx, p.transport, oauth2.ClientCredentialsRequest{Endpoint: tokenURL(connection), ClientID: clientID, ClientSecret: clientSecret, ErrorPrefix: "fedex"})
	if refreshErr != nil {
		return connector.TypedResult[map[string]any]{ResponseRef: "oauth:refresh_failed"}, refreshErr
	}
	result, _, err = p.execute(ctx, connection, path, body, refreshed.AccessToken, write)
	result.SecretUpdates = map[string]string{"access_token": refreshed.AccessToken}
	return result, err
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, path string, body map[string]any, token string, write bool) (connector.TypedResult[map[string]any], int, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return empty(), 0, err
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return empty(), 0, permanent("request_invalid", "request body is invalid")
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodPost, URL: strings.TrimRight(baseURL(connection), "/") + path, Headers: map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/json"}}, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, Body: raw, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		return empty(), 0, transportFailure(write, "network_error", transportErr)
	}
	status, ref := response.StatusCode, "http:"+strconv.Itoa(response.StatusCode)
	payload := map[string]any{}
	valid := len(response.Body) == 0 || json.Unmarshal(response.Body, &payload) == nil
	if status < 200 || status >= 300 {
		code := fedexErrorCode(payload, status)
		cause := fmt.Errorf("FedEx returned HTTP %d", status)
		if status == http.StatusUnauthorized {
			return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, status, connector.PermanentError(code, cause)
		}
		if status == http.StatusTooManyRequests {
			return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, status, connector.RetryableError(code, cause)
		}
		if status == http.StatusRequestTimeout || status >= 500 {
			return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, status, transportFailure(write, strings.TrimPrefix(code, "fedex."), cause)
		}
		return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, status, connector.PermanentError(code, cause)
	}
	if !valid {
		return connector.TypedResult[map[string]any]{ResponseRef: ref}, status, transportFailure(write, "response_invalid", errors.New("FedEx response is invalid JSON"))
	}
	ref = fedexResponseRef(payload, ref)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, status, nil
}

func fedexErrorCode(payload map[string]any, status int) string {
	if items, ok := payload["errors"].([]any); ok && len(items) > 0 {
		if detail, ok := items[0].(map[string]any); ok {
			if code := mapString(detail, "code"); code != "" {
				return "fedex." + strings.ToLower(code)
			}
		}
	}
	return "fedex.http_" + strconv.Itoa(status)
}

func fedexResponseRef(payload map[string]any, fallback string) string {
	output, _ := payload["output"].(map[string]any)
	if transactions, ok := output["transactionShipments"].([]any); ok && len(transactions) > 0 {
		if shipment, ok := transactions[0].(map[string]any); ok {
			if id := mapString(shipment, "masterTrackingNumber"); id != "" {
				return "fedex:" + id
			}
		}
	}
	if confirmation := mapString(output, "pickupConfirmationCode"); confirmation != "" {
		return "fedex:" + confirmation
	}
	return fallback
}

func transportFailure(write bool, suffix string, cause error) error {
	if write {
		return connector.UncertainError("fedex."+suffix, cause)
	}
	return connector.RetryableError("fedex."+suffix, cause)
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

func mapString(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return strings.TrimSpace(value)
}

func loopback(host string) bool                    { return host == "localhost" || host == "127.0.0.1" || host == "::1" }
func empty() connector.TypedResult[map[string]any] { return connector.TypedResult[map[string]any]{} }
func permanent(code, message string) error {
	return connector.PermanentError("fedex."+code, errors.New(message))
}
