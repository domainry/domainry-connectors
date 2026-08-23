// Package sfexpress implements the official SF Express Open Platform Provider.
package sfexpress

import (
	"context"
	"crypto/md5"
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
	ConnectorKey                  = "logistics"
	ProviderKey                   = "sf_express"
	defaultBaseURL                = "https://sfapi.sf-express.com/std/service"
	createOrderServiceCode        = "EXP_RECE_CREATE_ORDER"
	searchRoutesServiceCode       = "EXP_RECE_SEARCH_ROUTES"
	defaultTimeout                = 30
	maximumTimeout                = 120
	responseLimit           int64 = 8 << 20
)

type ProviderInput struct {
	Input map[string]any `json:"input"`
}

type TrackShipmentInput struct {
	TrackingNumber string `json:"tracking_number"`
}

var (
	CreateShipment          = callOperation[ProviderInput]("create_shipment", "8f1c2297627d8f3654725508a02f3b796fd5a90efe178bffe8587316abbb062a", connector.EffectWrite, connector.IdempotencyNone)
	CreateShipmentOperation = connector.StartOperation[ProviderInput]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "create_shipment_operation", ContractSHA256: "bb880e48d6809395f2383c10ead2d2b2a411bcc1542b8996ee957e11ad057f4e", Reliability: reliability(connector.EffectWrite, connector.IdempotencyNone)}
	TestConnection          = callOperation[struct{}]("test_connection", "a2879c297028e5fee189b49203b9a3f9c4c96182c7b68f928006edc6bd93408d", connector.EffectRead, connector.IdempotencyNatural)
	TrackShipment           = callOperation[TrackShipmentInput]("track_shipment", "bfe77eb2a7601443a0c0f731a79fbfcf490315105b98c084adf77f312ef4f24e", connector.EffectRead, connector.IdempotencyNatural)
)

func callOperation[I any](key, hash string, effect connector.OperationEffect, idempotency connector.IdempotencyStrategy) connector.CallOperation[I, map[string]any] {
	return connector.CallOperation[I, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: key, ContractSHA256: hash, Reliability: reliability(effect, idempotency)}
}

func reliability(effect connector.OperationEffect, idempotency connector.IdempotencyStrategy) connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: effect, Idempotency: connector.IdempotencyContract{Strategy: idempotency}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
	now       func() time.Time
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("SF Express transport is required")
	}
	p := &provider{transport: transport, now: time.Now}
	create, err := connector.BindCall(CreateShipment, p.createShipment)
	if err != nil {
		return nil, err
	}
	createOperation, err := connector.BindStartOperationDelivery(CreateShipmentOperation, p.createShipmentDelivery)
	if err != nil {
		return nil, err
	}
	test, err := connector.BindCall(TestConnection, p.test)
	if err != nil {
		return nil, err
	}
	track, err := connector.BindCall(TrackShipment, p.trackShipment)
	if err != nil {
		return nil, err
	}
	adapter, err := connector.NewProvider(schema(), create, createOperation, test, track)
	if err != nil {
		return nil, err
	}
	p.Adapter = adapter
	return p, nil
}

func schema() connector.ProviderSchema {
	minimum, maximum := float64(1), float64(maximumTimeout)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{
		{Key: "base_url", Name: "SF Open Platform endpoint", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://sfapi.sf-express.com/std/service"`)},
		{Key: "partner_id", Name: "Partner ID", Type: connector.ConfigFieldText, Required: true},
		{Key: "test_tracking_number", Name: "Test tracking number", Type: connector.ConfigFieldText, Required: true},
		{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}},
	}, SecretFields: []connector.SecretField{{Key: "check_word", Name: "Check word", Required: true, CredentialKind: connector.SecretCredentialSigningSecret, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	endpoint, err := url.Parse(baseURL(connection))
	if err != nil || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || (endpoint.Scheme != "https" && !(endpoint.Scheme == "http" && loopback(endpoint.Hostname()))) {
		return permanent("endpoint_invalid", "HTTPS or loopback HTTP endpoint is required")
	}
	if config(connection, "partner_id") == "" {
		return permanent("partner_id_required", "partner_id is required")
	}
	timeout := integer(connection.Config["timeout_seconds"], defaultTimeout)
	if timeout < 1 || timeout > maximumTimeout {
		return permanent("timeout_invalid", "timeout_seconds is invalid")
	}
	return nil
}

func (p *provider) createShipment(ctx context.Context, request connector.TypedRequest[ProviderInput]) (connector.TypedResult[map[string]any], error) {
	if len(request.Input.Input) == 0 {
		return empty(), permanent("input_required", "provider input is required")
	}
	return p.execute(ctx, request.Connection, request.Secrets, request.RequestRef, createOrderServiceCode, request.Input.Input, normalizeCreateShipment, true)
}

func (p *provider) createShipmentDelivery(ctx context.Context, request connector.TypedRequest[ProviderInput]) (connector.DeliveryResult, error) {
	result, err := p.createShipment(ctx, request)
	return connector.DeliveryResult{ResponseRef: result.ResponseRef, SecretUpdates: result.SecretUpdates, ResourceHealth: result.ResourceHealth}, err
}

func (p *provider) trackShipment(ctx context.Context, request connector.TypedRequest[TrackShipmentInput]) (connector.TypedResult[map[string]any], error) {
	tracking := strings.TrimSpace(request.Input.TrackingNumber)
	if tracking == "" {
		return empty(), permanent("tracking_number_required", "tracking number is required")
	}
	return p.execute(ctx, request.Connection, request.Secrets, request.RequestRef, searchRoutesServiceCode, map[string]any{"trackingType": "1", "trackingNumber": []string{tracking}}, normalizeTracking, false)
}

func (p *provider) test(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[map[string]any], error) {
	tracking := config(request.Connection, "test_tracking_number")
	if tracking == "" {
		return empty(), permanent("test_tracking_number_required", "test_tracking_number is required")
	}
	return p.execute(ctx, request.Connection, request.Secrets, request.RequestRef, searchRoutesServiceCode, map[string]any{"trackingType": "1", "trackingNumber": []string{tracking}}, normalizeTracking, false)
}

func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.test(ctx, connector.TypedRequest[struct{}]{Connection: request.Connection, Secrets: request.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

type responseNormalizer func(map[string]any) (map[string]any, string, error)

func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, requestRef, serviceCode string, message map[string]any, normalize responseNormalizer, write bool) (connector.TypedResult[map[string]any], error) {
	if err := p.ValidateConfig(connection); err != nil {
		return empty(), err
	}
	checkWord := strings.TrimSpace(secrets["check_word"])
	if checkWord == "" {
		return empty(), permanent("check_word_required", "resolved check_word is required")
	}
	messageJSON, err := json.Marshal(message)
	if err != nil {
		return empty(), permanent("request_invalid", "request body is invalid")
	}
	timestamp := strconv.FormatInt(p.now().UnixMilli(), 10)
	requestID := strings.TrimSpace(requestRef)
	if requestID == "" {
		sum := md5.Sum(append(append([]byte(nil), messageJSON...), timestamp...))
		requestID = hex.EncodeToString(sum[:])
	}
	form := url.Values{"partnerID": {config(connection, "partner_id")}, "requestID": {requestID}, "serviceCode": {serviceCode}, "timestamp": {timestamp}, "msgData": {string(messageJSON)}}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodPost, URL: baseURL(connection), Headers: map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/x-www-form-urlencoded; charset=utf-8"}}, Body: []byte(form.Encode()), SecretForm: map[string]string{"msgDigest": sfDigest(string(messageJSON), timestamp, checkWord)}, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		return empty(), transportFailure(write, "network_error", transportErr)
	}
	ref := "sf:" + requestID
	payload := map[string]any{}
	if err := json.Unmarshal(response.Body, &payload); err != nil {
		return connector.TypedResult[map[string]any]{ResponseRef: ref}, transportFailure(write, "response_invalid", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code := sfErrorCode(payload, response.StatusCode)
		cause := fmt.Errorf("SF Express returned HTTP %d", response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests {
			return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, connector.RetryableError(code, cause)
		}
		if response.StatusCode == http.StatusRequestTimeout || response.StatusCode >= 500 {
			return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, transportFailure(write, strings.TrimPrefix(code, "sf_express."), cause)
		}
		return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, connector.PermanentError(code, cause)
	}
	output, normalizedRef, err := normalizeSFResponse(payload, normalize)
	if err != nil {
		return connector.TypedResult[map[string]any]{ResponseRef: ref}, err
	}
	if normalizedRef != "" {
		ref = normalizedRef
	}
	return connector.TypedResult[map[string]any]{Output: output, ResponseRef: ref}, nil
}

func sfDigest(message, timestamp, checkWord string) string {
	escaped := url.QueryEscape(message + timestamp + checkWord)
	sum := md5.Sum([]byte(escaped))
	return base64.StdEncoding.EncodeToString(sum[:])
}

func sfErrorCode(payload map[string]any, status int) string {
	if code := mapString(payload, "apiResultCode"); code != "" && code != "A1000" {
		return "sf_express." + strings.ToLower(code)
	}
	return "sf_express.http_" + strconv.Itoa(status)
}

func normalizeSFResponse(payload map[string]any, normalize responseNormalizer) (map[string]any, string, error) {
	code := mapString(payload, "apiResultCode")
	if code != "A1000" {
		if code == "" {
			code = "response_invalid"
		}
		return nil, "", connector.PermanentError("sf_express."+strings.ToLower(code), errors.New("SF Express rejected the request"))
	}
	var result map[string]any
	switch value := payload["apiResultData"].(type) {
	case string:
		if json.Unmarshal([]byte(value), &result) != nil {
			return nil, "", permanent("response_invalid", "response body is invalid")
		}
	case map[string]any:
		result = value
	default:
		return nil, "", permanent("response_invalid", "response body is invalid")
	}
	if success, ok := result["success"].(bool); ok && !success {
		code := mapString(result, "errorCode")
		if code == "" {
			code = "business_error"
		}
		return nil, "", connector.PermanentError("sf_express."+strings.ToLower(code), errors.New("SF Express rejected the request"))
	}
	message, _ := result["msgData"].(map[string]any)
	return normalize(message)
}

func normalizeCreateShipment(message map[string]any) (map[string]any, string, error) {
	tracking := mapString(message, "waybillNo")
	if tracking == "" {
		if items, ok := message["waybillNoInfoList"].([]any); ok && len(items) > 0 {
			if first, ok := items[0].(map[string]any); ok {
				tracking = mapString(first, "waybillNo")
			}
		}
	}
	if tracking == "" {
		return nil, "", permanent("tracking_number_missing", "response has no tracking number")
	}
	result := map[string]any{"shipmentTrackingNumber": tracking}
	if documents := message["documents"]; documents != nil {
		result["documents"] = documents
	}
	return result, "sf:" + tracking, nil
}

func normalizeTracking(message map[string]any) (map[string]any, string, error) {
	shipments := message["routeResps"]
	if shipments == nil {
		shipments = []any{}
	}
	return map[string]any{"shipments": shipments}, "", nil
}

func transportFailure(write bool, suffix string, cause error) error {
	if write {
		return connector.UncertainError("sf_express."+suffix, cause)
	}
	return connector.RetryableError("sf_express."+suffix, cause)
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
	return connector.PermanentError("sf_express."+code, errors.New(message))
}
