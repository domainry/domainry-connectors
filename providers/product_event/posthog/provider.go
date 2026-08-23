// Package posthog implements the official PostHog product-event Provider.
package posthog

import (
	"context"
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
	ConnectorKey         = "product_event"
	ProviderKey          = "posthog"
	defaultBaseURL       = "https://us.posthog.com"
	responseLimit  int64 = 4 << 20
)

var ListEvents = connector.CallOperation[ListEventsInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_events", ContractSHA256: "5d6ca4a461d8943d579c3f55599892a5eb2d1023f3b2862aec3029df8a1da588", Reliability: readReliability()}
var CaptureEvent = connector.CallOperation[CaptureEventInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "capture_event", ContractSHA256: "c98ecb5bf19cd010568f071d804037182eb62ca757de71dd91452aed9f9e01af", Reliability: connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNone}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}}

type ListEventsInput struct {
	Limit      int    `json:"limit,omitempty"`
	Offset     int    `json:"offset,omitempty"`
	After      string `json:"after,omitempty"`
	Before     string `json:"before,omitempty"`
	Event      string `json:"event,omitempty"`
	DistinctID string `json:"distinct_id,omitempty"`
}
type CaptureEventInput struct {
	Event      string         `json:"event"`
	DistinctID string         `json:"distinct_id"`
	Properties map[string]any `json:"properties,omitempty"`
	Timestamp  string         `json:"timestamp,omitempty"`
}
type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("PostHog transport is required")
	}
	p := &provider{transport: transport}
	list, err := connector.BindCall(ListEvents, p.listEvents)
	if err != nil {
		return nil, err
	}
	capture, err := connector.BindCall(CaptureEvent, p.captureEvent)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), list, capture)
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
	min, max := float64(1), float64(120)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "project_id", Name: "PostHog project ID", Type: connector.ConfigFieldText, Required: true}, {Key: "base_url", Name: "PostHog base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://us.posthog.com"`)}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &min, Max: &max}}}, SecretFields: []connector.SecretField{{Key: "api_token", Name: "Personal API key", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}, {Key: "project_secret", Name: "Project API key", Required: true, CredentialKind: connector.SecretCredentialIdentifier, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	if configString(connection.Config, "project_id") == "" {
		return connector.PermanentError("posthog.project_id_required", errors.New("project ID is required"))
	}
	parsed, err := url.Parse(baseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return connector.PermanentError("posthog.endpoint_invalid", errors.New("HTTPS or loopback HTTP endpoint is required"))
	}
	return nil
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	payload, ref, err := p.read(ctx, request.Connection, request.Secrets, "/api/projects/"+url.PathEscape(configString(request.Connection.Config, "project_id"))+"/", nil)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, err := json.Marshal(map[string]any{"project": payload, "response_ref": ref})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}
func (p *provider) listEvents(ctx context.Context, request connector.TypedRequest[ListEventsInput]) (connector.TypedResult[map[string]any], error) {
	input := request.Input
	if input.Limit < 0 || input.Limit > 1000 || input.Offset < 0 {
		return connector.TypedResult[map[string]any]{}, connector.PermanentError("posthog.pagination_invalid", errors.New("limit must be 0..1000 and offset cannot be negative"))
	}
	query := url.Values{}
	if input.Limit > 0 {
		query.Set("limit", strconv.Itoa(input.Limit))
	}
	if input.Offset > 0 {
		query.Set("offset", strconv.Itoa(input.Offset))
	}
	for key, value := range map[string]string{"after": input.After, "before": input.Before, "event": input.Event, "distinct_id": input.DistinctID} {
		if value = strings.TrimSpace(value); value != "" {
			query.Set(key, value)
		}
	}
	payload, ref, err := p.read(ctx, request.Connection, request.Secrets, "/api/projects/"+url.PathEscape(configString(request.Connection.Config, "project_id"))+"/events/", query)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) captureEvent(ctx context.Context, request connector.TypedRequest[CaptureEventInput]) (connector.TypedResult[map[string]any], error) {
	event, distinctID := strings.TrimSpace(request.Input.Event), strings.TrimSpace(request.Input.DistinctID)
	if event == "" || distinctID == "" {
		return connector.TypedResult[map[string]any]{}, connector.PermanentError("posthog.event_identity_required", errors.New("event and distinct ID are required"))
	}
	secret := strings.TrimSpace(request.Secrets["project_secret"])
	if secret == "" {
		return connector.TypedResult[map[string]any]{}, connector.PermanentError("posthog.project_secret_required", errors.New("resolved project API key is required"))
	}
	properties := map[string]any{}
	for key, value := range request.Input.Properties {
		properties[key] = value
	}
	properties["distinct_id"] = distinctID
	if request.RequestRef != "" {
		properties["$insert_id"] = request.RequestRef
	}
	body := map[string]any{"event": event, "properties": properties}
	if timestamp := strings.TrimSpace(request.Input.Timestamp); timestamp != "" {
		body["timestamp"] = timestamp
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return connector.TypedResult[map[string]any]{}, connector.PermanentError("posthog.request_invalid", err)
	}
	payload, ref, err := p.send(ctx, request.Connection, http.MethodPost, "/capture/", nil, raw, map[string]string{"api_key": secret}, true, nil)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) read(ctx context.Context, connection connector.Connection, secrets map[string]string, path string, query url.Values) (map[string]any, string, error) {
	token := strings.TrimSpace(secrets["api_token"])
	if token == "" {
		return nil, "", connector.PermanentError("posthog.api_token_required", errors.New("resolved personal API key is required"))
	}
	return p.send(ctx, connection, http.MethodGet, path, query, nil, nil, false, map[string][]string{"Authorization": {"Bearer " + token}})
}
func (p *provider) send(ctx context.Context, connection connector.Connection, method, path string, query url.Values, body []byte, secretJSON map[string]string, write bool, secretHeaders map[string][]string) (map[string]any, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", err
	}
	endpoint, err := url.Parse(baseURL(connection) + path)
	if err != nil {
		return nil, "", connector.PermanentError("posthog.request_invalid", err)
	}
	endpoint.RawQuery = query.Encode()
	headers := map[string][]string{"Accept": {"application/json"}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint.String(), Headers: headers, SecretHeaders: secretHeaders, SecretJSON: secretJSON, Body: body, MaxResponseBytes: responseLimit})
	if err != nil {
		if write {
			return nil, "", connector.UncertainError("posthog.capture_outcome_uncertain", err)
		}
		return nil, "", connector.RetryableError("posthog.network_error", err)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := map[string]any{}
	if json.Unmarshal(response.Body, &payload) != nil {
		return nil, ref, connector.PermanentError("posthog.response_invalid", errors.New("provider response is invalid JSON"))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code, cause := "posthog.http_"+strconv.Itoa(response.StatusCode), fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests {
			return payload, ref, connector.RetryableError(code, cause)
		}
		if response.StatusCode >= 500 {
			if write {
				return payload, ref, connector.UncertainError("posthog.capture_outcome_uncertain", cause)
			}
			return payload, ref, connector.RetryableError(code, cause)
		}
		return payload, ref, connector.PermanentError(code, cause)
	}
	if id := configString(payload, "id"); id != "" {
		ref = "posthog:" + id
	}
	return payload, ref, nil
}
func baseURL(connection connector.Connection) string {
	if value := strings.TrimRight(configString(connection.Config, "base_url"), "/"); value != "" {
		return value
	}
	return defaultBaseURL
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
