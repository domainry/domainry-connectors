// Package openai implements the official OpenAI Responses API Provider.
package openai

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
	ConnectorKey         = "openai"
	ProviderKey          = "openai"
	defaultBaseURL       = "https://api.openai.com/v1"
	responseLimit  int64 = 4 << 20
)

var GenerateResponse = connector.CallOperation[ResponseInput, ResponseOutput]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "generate_response", ContractSHA256: "bd206eb4f1c36de448f30a0d50b5793558740a475b67227d0241b9acc63f7182", Reliability: writeReliability()}
var CreateResponse = connector.EnqueueOperation[ResponseInput]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "create_response", ContractSHA256: "3029fbc7e63e1f906495eae0f4ce7447c997da3191ea54cb4c0493c5d043fd98", Reliability: writeReliability()}
var TestConnection = connector.CallOperation[struct{}, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection", ContractSHA256: "c32f989f227d070a59b68d587a1f8d4ac99c48a56fc79cdc08b4acba23955b26", Reliability: readReliability()}

type ResponseInput struct {
	Input           string `json:"input"`
	Instructions    string `json:"instructions,omitempty"`
	MaxOutputTokens int    `json:"max_output_tokens,omitempty"`
}
type ResponseOutput struct {
	ID               string         `json:"id"`
	Status           string         `json:"status,omitempty"`
	OutputText       string         `json:"output_text,omitempty"`
	ProviderResponse map[string]any `json:"provider_response"`
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("OpenAI transport is required")
	}
	p := &provider{transport: transport}
	generate, err := connector.BindCall(GenerateResponse, p.generateResponse)
	if err != nil {
		return nil, err
	}
	create, err := connector.BindEnqueueDelivery(CreateResponse, p.createResponse)
	if err != nil {
		return nil, err
	}
	test, err := connector.BindCall(TestConnection, p.callTestConnection)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), generate, create, test)
	if err != nil {
		return nil, err
	}
	p.Adapter = bound
	return p, nil
}

func readReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}
func writeReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNone}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}
func schema() connector.ProviderSchema {
	minimumTimeout, maximumTimeout := float64(1), float64(300)
	minimumTokens, maximumTokens := float64(1), float64(1000000)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{
		{Key: "model", Name: "Model", Type: connector.ConfigFieldText, Required: true},
		{Key: "base_url", Name: "OpenAI API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://api.openai.com/v1"`)},
		{Key: "organization", Name: "Organization", Type: connector.ConfigFieldText},
		{Key: "project", Name: "Project", Type: connector.ConfigFieldText},
		{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimumTimeout, Max: &maximumTimeout}},
		{Key: "max_output_tokens", Name: "Default maximum output tokens", Type: connector.ConfigFieldInteger, Validation: connector.ConfigValidation{Min: &minimumTokens, Max: &maximumTokens}},
		{Key: "store", Name: "Store response", Type: connector.ConfigFieldBoolean, Default: json.RawMessage(`false`)},
	}, SecretFields: []connector.SecretField{{Key: "api_key", Name: "OpenAI API key", Required: true, CredentialKind: connector.SecretCredentialAPIKey, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	if model(connection) == "" {
		return permanent("model_required", "model is required")
	}
	parsed, err := url.Parse(baseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return permanent("endpoint_invalid", "valid OpenAI API endpoint is required")
	}
	if parsed.Scheme == "http" && isLoopback(parsed.Hostname()) {
		return nil
	}
	if parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), "api.openai.com") {
		return permanent("endpoint_invalid", "official OpenAI Provider requires api.openai.com HTTPS or loopback HTTP")
	}
	return nil
}

func (p *provider) callTestConnection(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[map[string]any], error) {
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/models/"+url.PathEscape(model(request.Connection)), nil, false)
	return connector.TypedResult[map[string]any]{Output: map[string]any{"connected": err == nil, "model": payload}, ResponseRef: ref}, err
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/models/"+url.PathEscape(model(request.Connection)), nil, false)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, err := json.Marshal(map[string]any{"model": payload, "response_ref": ref})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}
func (p *provider) generateResponse(ctx context.Context, request connector.TypedRequest[ResponseInput]) (connector.TypedResult[ResponseOutput], error) {
	output, ref, err := p.sendResponse(ctx, request.Connection, request.Secrets, request.Input)
	return connector.TypedResult[ResponseOutput]{Output: output, ResponseRef: ref}, err
}
func (p *provider) createResponse(ctx context.Context, request connector.TypedRequest[ResponseInput]) (connector.DeliveryResult, error) {
	_, ref, err := p.sendResponse(ctx, request.Connection, request.Secrets, request.Input)
	return connector.DeliveryResult{ResponseRef: ref}, err
}
func (p *provider) sendResponse(ctx context.Context, connection connector.Connection, secrets map[string]string, input ResponseInput) (ResponseOutput, string, error) {
	text := strings.TrimSpace(input.Input)
	if text == "" {
		return ResponseOutput{}, "", permanent("input_required", "input is required")
	}
	maximum := input.MaxOutputTokens
	if maximum == 0 {
		maximum = configInt(connection.Config, "max_output_tokens")
	}
	if maximum < 0 || maximum > 1000000 {
		return ResponseOutput{}, "", permanent("max_output_tokens_invalid", "maximum output tokens must be between 1 and 1000000 when set")
	}
	body := map[string]any{"model": model(connection), "input": text, "store": configBool(connection.Config, "store")}
	if instructions := strings.TrimSpace(input.Instructions); instructions != "" {
		body["instructions"] = instructions
	}
	if maximum > 0 {
		body["max_output_tokens"] = maximum
	}
	payload, ref, err := p.execute(ctx, connection, secrets, http.MethodPost, "/responses", body, true)
	if err != nil {
		return ResponseOutput{}, ref, err
	}
	id := configString(payload, "id")
	if id == "" {
		return ResponseOutput{}, ref, connector.UncertainError("openai.response_invalid", errors.New("response lacks an identity"))
	}
	return ResponseOutput{ID: id, Status: configString(payload, "status"), OutputText: outputText(payload), ProviderResponse: payload}, "openai:response:" + id, nil
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, method, path string, body map[string]any, write bool) (map[string]any, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", err
	}
	key := strings.TrimSpace(secrets["api_key"])
	if key == "" {
		return nil, "", permanent("api_key_required", "resolved OpenAI API key is required")
	}
	endpoint, err := url.Parse(baseURL(connection) + path)
	if err != nil {
		return nil, "", connector.PermanentError("openai.request_invalid", err)
	}
	var raw []byte
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return nil, "", connector.PermanentError("openai.request_invalid", err)
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}, "User-Agent": {"domainry-openai-connector/1.0"}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	if value := configString(connection.Config, "organization"); value != "" {
		headers["OpenAI-Organization"] = []string{value}
	}
	if value := configString(connection.Config, "project"); value != "" {
		headers["OpenAI-Project"] = []string{value}
	}
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint.String(), Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + key}}, Body: raw, MaxResponseBytes: responseLimit})
	if err != nil {
		if write {
			return nil, "", connector.UncertainError("openai.network_error", err)
		}
		return nil, "", connector.RetryableError("openai.network_error", err)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := map[string]any{}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		// Error bodies are best-effort metadata. HTTP status remains authoritative
		// even when an upstream proxy returns HTML or an empty response.
		_ = json.Unmarshal(response.Body, &payload)
		code, cause := providerErrorCode(payload, response.StatusCode), fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests {
			return payload, ref, connector.RetryableError(code, cause)
		}
		if response.StatusCode >= 500 {
			if write {
				return payload, ref, connector.UncertainError(code, cause)
			}
			return payload, ref, connector.RetryableError(code, cause)
		}
		return payload, ref, connector.PermanentError(code, cause)
	}
	if json.Unmarshal(response.Body, &payload) != nil {
		if write {
			return nil, ref, connector.UncertainError("openai.response_invalid", errors.New("provider response is invalid JSON"))
		}
		return nil, ref, permanent("response_invalid", "provider response is invalid JSON")
	}
	return payload, ref, nil
}

func outputText(response map[string]any) string {
	if direct := configString(response, "output_text"); direct != "" {
		return direct
	}
	var output strings.Builder
	for _, item := range mapSlice(response["output"]) {
		for _, content := range mapSlice(item["content"]) {
			if configString(content, "type") == "output_text" {
				output.WriteString(configString(content, "text"))
			}
		}
	}
	return output.String()
}
func mapSlice(value any) []map[string]any {
	items, _ := value.([]any)
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if mapped, ok := item.(map[string]any); ok {
			result = append(result, mapped)
		}
	}
	return result
}
func providerErrorCode(payload map[string]any, status int) string {
	errorObject, _ := payload["error"].(map[string]any)
	if code := configString(errorObject, "code"); code != "" {
		return "openai.provider_" + codeToken(code)
	}
	return "openai.http_" + strconv.Itoa(status)
}
func codeToken(value string) string {
	var token strings.Builder
	for _, current := range strings.ToLower(strings.TrimSpace(value)) {
		if current >= 'a' && current <= 'z' || current >= '0' && current <= '9' {
			token.WriteRune(current)
		} else if token.Len() > 0 {
			token.WriteByte('_')
		}
	}
	if result := strings.Trim(token.String(), "_"); result != "" {
		return result
	}
	return "unknown"
}
func permanent(code, message string) error {
	return connector.PermanentError("openai."+code, errors.New(message))
}
func baseURL(connection connector.Connection) string {
	if value := strings.TrimRight(configString(connection.Config, "base_url"), "/"); value != "" {
		return value
	}
	return defaultBaseURL
}
func model(connection connector.Connection) string { return configString(connection.Config, "model") }
func configString(values map[string]any, key string) string {
	if values[key] == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(values[key]))
}
func configInt(values map[string]any, key string) int {
	value, _ := strconv.Atoi(configString(values, key))
	return value
}
func configBool(values map[string]any, key string) bool {
	value, _ := strconv.ParseBool(configString(values, key))
	return value
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
