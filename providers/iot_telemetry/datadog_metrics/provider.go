// Package datadogmetrics implements the official Datadog metrics telemetry Provider.
package datadogmetrics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	connector "github.com/domainry/domainry-connector-sdk"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	ConnectorKey         = "iot_telemetry"
	ProviderKey          = "datadog_metrics"
	defaultAPIBase       = "https://api.datadoghq.com"
	responseLimit  int64 = 4 << 20
)

var PublishTelemetry = connector.CallOperation[PublishTelemetryInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "publish_telemetry", ContractSHA256: "b6fb13a6411ad45e592310c76fba08e7ca986a148b10a9ed5f270afdc7e30714", Reliability: connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNone}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}}

type PublishTelemetryInput struct {
	DeviceID  string             `json:"device_id"`
	EventID   string             `json:"event_id"`
	Metrics   map[string]float64 `json:"metrics"`
	Timestamp string             `json:"timestamp,omitempty"`
}
type provider struct {
	connector.Adapter
	transport connector.Transport
	now       func() time.Time
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Datadog metrics transport is required")
	}
	p := &provider{transport: transport, now: time.Now}
	operation, err := connector.BindCall(PublishTelemetry, p.publishTelemetry)
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
	min, max := float64(1), float64(300)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "api_base_url", Name: "Datadog API Base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://api.datadoghq.com"`), Validation: connector.ConfigValidation{Pattern: `^https?://[^\s]+$`}, I18n: map[string]connector.FieldLocalization{"en-US": {Name: "Datadog API Base URL", Description: "Datadog API Base URL"}, "zh-CN": {Name: "Datadog API 根地址", Description: "Datadog API 根地址"}}}, {Key: "timeout_seconds", Name: "Timeout Seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`15`), Validation: connector.ConfigValidation{Min: &min, Max: &max}, I18n: map[string]connector.FieldLocalization{"en-US": {Name: "Timeout Seconds", Description: "Timeout Seconds"}, "zh-CN": {Name: "超时秒数", Description: "超时秒数"}}}}, SecretFields: []connector.SecretField{{Key: "api_key", Name: "API Key", Required: true, CredentialKind: connector.SecretCredentialAPIKey, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}, {Key: "application_key", Name: "Application Key", CredentialKind: connector.SecretCredentialAPIKey, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(baseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return connector.PermanentError("datadog_metrics.endpoint_invalid", errors.New("HTTPS or loopback HTTP endpoint is required"))
	}
	return nil
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/api/v1/validate", nil, false)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	valid, ok := payload["valid"].(bool)
	if !ok || !valid {
		return connector.TestConnectionResult{}, connector.PermanentError("datadog_metrics.credentials_invalid", errors.New("Datadog rejected the credentials"))
	}
	details, err := json.Marshal(map[string]any{"response_ref": ref})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}
func (p *provider) publishTelemetry(ctx context.Context, request connector.TypedRequest[PublishTelemetryInput]) (connector.TypedResult[map[string]any], error) {
	input := request.Input
	deviceID, eventID := strings.TrimSpace(input.DeviceID), strings.TrimSpace(input.EventID)
	if deviceID == "" || eventID == "" || len(input.Metrics) == 0 {
		return connector.TypedResult[map[string]any]{}, connector.PermanentError("datadog_metrics.payload_invalid", errors.New("device ID, event ID and metrics are required"))
	}
	timestamp := p.now().Unix()
	if value := strings.TrimSpace(input.Timestamp); value != "" {
		parsed, err := time.Parse(time.RFC3339, value)
		if err != nil {
			return connector.TypedResult[map[string]any]{}, connector.PermanentError("datadog_metrics.timestamp_invalid", errors.New("timestamp must be RFC3339"))
		}
		timestamp = parsed.Unix()
	}
	names := make([]string, 0, len(input.Metrics))
	for name, value := range input.Metrics {
		if strings.TrimSpace(name) == "" || isNonFinite(value) {
			return connector.TypedResult[map[string]any]{}, connector.PermanentError("datadog_metrics.metric_invalid", errors.New("metric names and values must be valid"))
		}
		names = append(names, name)
	}
	sort.Strings(names)
	series := make([]any, 0, len(names))
	for _, name := range names {
		series = append(series, map[string]any{"metric": name, "type": 0, "points": []any{map[string]any{"timestamp": timestamp, "value": input.Metrics[name]}}, "tags": []string{"device_id:" + deviceID, "event_id:" + eventID}})
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodPost, "/api/v2/series", map[string]any{"series": series}, true)
	return connector.TypedResult[map[string]any]{Output: map[string]any{"accepted": err == nil, "provider_response": payload}, ResponseRef: ref}, err
}
func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, method, path string, payload map[string]any, write bool) (map[string]any, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", err
	}
	apiKey := strings.TrimSpace(secrets["api_key"])
	if apiKey == "" {
		return nil, "", connector.PermanentError("datadog_metrics.api_key_required", errors.New("resolved API key is required"))
	}
	secretHeaders := map[string][]string{"Dd-Api-Key": {apiKey}}
	if appKey := strings.TrimSpace(secrets["application_key"]); appKey != "" {
		secretHeaders["Dd-Application-Key"] = []string{appKey}
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	var body []byte
	var err error
	if payload != nil {
		headers["Content-Type"] = []string{"application/json"}
		body, err = json.Marshal(payload)
		if err != nil {
			return nil, "", connector.PermanentError("datadog_metrics.encode_failed", err)
		}
	}
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: baseURL(connection) + path, Headers: headers, SecretHeaders: secretHeaders, Body: body, MaxResponseBytes: responseLimit})
	if err != nil {
		if write {
			return nil, "", connector.UncertainError("datadog_metrics.publish_outcome_uncertain", err)
		}
		return nil, "", connector.RetryableError("datadog_metrics.network_error", err)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	decoded := map[string]any{}
	if len(response.Body) > 0 && json.Unmarshal(response.Body, &decoded) != nil {
		return nil, ref, connector.PermanentError("datadog_metrics.response_invalid", errors.New("provider response is invalid JSON"))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code, cause := "datadog_metrics.http_"+strconv.Itoa(response.StatusCode), fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests {
			return decoded, ref, connector.RetryableError(code, cause)
		}
		if response.StatusCode >= 500 {
			if write {
				return decoded, ref, connector.UncertainError("datadog_metrics.publish_outcome_uncertain", cause)
			}
			return decoded, ref, connector.RetryableError(code, cause)
		}
		return decoded, ref, connector.PermanentError(code, cause)
	}
	return decoded, ref, nil
}
func baseURL(connection connector.Connection) string {
	if value := strings.TrimRight(configString(connection.Config, "api_base_url"), "/"); value != "" {
		return value
	}
	return defaultAPIBase
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
func isNonFinite(value float64) bool { return math.IsNaN(value) || math.IsInf(value, 0) }

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
