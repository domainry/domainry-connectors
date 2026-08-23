// Package typeform implements the official Typeform feedback Provider.
package typeform

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
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
	ConnectorKey         = "customer_feedback"
	ProviderKey          = "typeform"
	defaultBaseURL       = "https://api.typeform.com"
	responseLimit  int64 = 4 << 20
)

var GetForm = connector.CallOperation[GetFormInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "get_form", ContractSHA256: "7adc05c1b448a5c935e78c0c50a8d20fea52ea1b541e250c75c33de841af0cf9", Reliability: readReliability()}
var ListResponses = connector.CallOperation[ListResponsesInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_responses", ContractSHA256: "4a91cb108284b0275c4e5b2980a3492b766f207dc5b8891b0ead4aae66fec922", Reliability: readReliability()}

type GetFormInput struct {
	FormID string `json:"form_id,omitempty"`
}

type ListResponsesInput struct {
	FormID    string `json:"form_id,omitempty"`
	PageSize  int    `json:"page_size,omitempty"`
	Since     string `json:"since,omitempty"`
	Until     string `json:"until,omitempty"`
	After     string `json:"after,omitempty"`
	Before    string `json:"before,omitempty"`
	Completed *bool  `json:"completed,omitempty"`
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Typeform transport is required")
	}
	p := &provider{transport: transport}
	getForm, err := connector.BindCall(GetForm, p.getForm)
	if err != nil {
		return nil, err
	}
	listResponses, err := connector.BindCall(ListResponses, p.listResponses)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), getForm, listResponses)
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
	minimum, maximum := float64(1), float64(120)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "survey_id", Name: "Default form ID", Type: connector.ConfigFieldText}, {Key: "base_url", Name: "Typeform API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://api.typeform.com"`)}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}}}, SecretFields: []connector.SecretField{{Key: "api_token", Name: "Personal or OAuth access token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}, {Key: "webhook_secret", Name: "Webhook secret", CredentialKind: connector.SecretCredentialSigningSecret, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(baseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return connector.PermanentError("typeform.endpoint_invalid", errors.New("HTTPS or loopback HTTP endpoint is required"))
	}
	return nil
}

func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, "/me", nil)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, err := json.Marshal(map[string]any{"account": payload, "response_ref": ref})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) getForm(ctx context.Context, request connector.TypedRequest[GetFormInput]) (connector.TypedResult[map[string]any], error) {
	id, err := formID(request.Connection, request.Input.FormID)
	if err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, "/forms/"+url.PathEscape(id), nil)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}

func (p *provider) listResponses(ctx context.Context, request connector.TypedRequest[ListResponsesInput]) (connector.TypedResult[map[string]any], error) {
	id, err := formID(request.Connection, request.Input.FormID)
	if err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	pageSize := request.Input.PageSize
	if pageSize == 0 {
		pageSize = 25
	}
	if pageSize < 1 || pageSize > 1000 {
		return connector.TypedResult[map[string]any]{}, connector.PermanentError("typeform.page_size_invalid", errors.New("page size must be between 1 and 1000"))
	}
	if strings.TrimSpace(request.Input.After) != "" && strings.TrimSpace(request.Input.Before) != "" {
		return connector.TypedResult[map[string]any]{}, connector.PermanentError("typeform.cursor_invalid", errors.New("after and before cannot be used together"))
	}
	query := url.Values{"page_size": {strconv.Itoa(pageSize)}}
	for key, value := range map[string]string{"since": request.Input.Since, "until": request.Input.Until, "after": request.Input.After, "before": request.Input.Before} {
		if value = strings.TrimSpace(value); value != "" {
			query.Set(key, value)
		}
	}
	if request.Input.Completed != nil {
		query.Set("completed", strconv.FormatBool(*request.Input.Completed))
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, "/forms/"+url.PathEscape(id)+"/responses", query)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, path string, query url.Values) (map[string]any, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", err
	}
	token := strings.TrimSpace(secrets["api_token"])
	if token == "" {
		return nil, "", connector.PermanentError("typeform.api_token_required", errors.New("resolved API token is required"))
	}
	endpoint, err := url.Parse(baseURL(connection) + path)
	if err != nil {
		return nil, "", connector.PermanentError("typeform.request_invalid", err)
	}
	values := endpoint.Query()
	for name, items := range query {
		for _, value := range items {
			values.Add(name, value)
		}
	}
	endpoint.RawQuery = values.Encode()
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodGet, URL: endpoint.String(), Headers: map[string][]string{"Accept": {"application/json"}}, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, MaxResponseBytes: responseLimit})
	if err != nil {
		return nil, "", connector.RetryableError("typeform.network_error", err)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := map[string]any{}
	if json.Unmarshal(response.Body, &payload) != nil {
		return nil, ref, connector.PermanentError("typeform.response_invalid", errors.New("provider response is invalid JSON"))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code := "typeform.http_" + strconv.Itoa(response.StatusCode)
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			return payload, ref, connector.RetryableError(code, cause)
		}
		return payload, ref, connector.PermanentError(code, cause)
	}
	if id := configString(payload, "id"); id != "" {
		ref = "typeform:" + id
	}
	return payload, ref, nil
}

func (p *provider) VerifyWebhook(ctx context.Context, request connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	if err := ctx.Err(); err != nil {
		return connector.VerifiedWebhook{}, err
	}
	secret := strings.TrimSpace(request.Secrets["webhook_secret"])
	if secret == "" {
		return connector.VerifiedWebhook{}, connector.PermanentError("typeform.webhook_secret_required", errors.New("resolved webhook secret is required"))
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(request.Body)
	expected := "sha256=" + base64.StdEncoding.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(headerValue(request.Headers, "Typeform-Signature"))) {
		return connector.VerifiedWebhook{}, connector.PermanentError("typeform.webhook_signature_invalid", errors.New("webhook signature is invalid"))
	}
	payload := map[string]any{}
	if json.Unmarshal(request.Body, &payload) != nil {
		return connector.VerifiedWebhook{}, connector.PermanentError("typeform.webhook_payload_invalid", errors.New("webhook payload is invalid JSON"))
	}
	eventType, eventID := configString(payload, "event_type"), configString(payload, "event_id")
	response, _ := payload["form_response"].(map[string]any)
	if eventID == "" {
		eventID = configString(response, "token")
	}
	if eventType == "" || eventID == "" {
		return connector.VerifiedWebhook{}, connector.PermanentError("typeform.webhook_identity_missing", errors.New("webhook event identity is missing"))
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return connector.VerifiedWebhook{}, connector.PermanentError("typeform.webhook_payload_invalid", err)
	}
	return connector.VerifiedWebhook{EventType: eventType, ExternalID: eventID, Payload: raw, Security: &connector.WebhookSecurityEvidence{SignatureVerified: true}, ExternalIdentity: typeformIdentity(response)}, nil
}

func typeformIdentity(response map[string]any) *connector.WebhookExternalIdentity {
	hidden, _ := response["hidden"].(map[string]any)
	if email := configString(hidden, "email"); strings.Contains(email, "@") {
		return &connector.WebhookExternalIdentity{Subject: email, SubjectType: "email"}
	}
	return nil
}

func formID(connection connector.Connection, input string) (string, error) {
	if value := strings.TrimSpace(input); value != "" {
		return value, nil
	}
	if value := configString(connection.Config, "survey_id"); value != "" {
		return value, nil
	}
	return "", connector.PermanentError("typeform.form_id_required", errors.New("form ID is required"))
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
func headerValue(headers map[string][]string, key string) string {
	for name, values := range headers {
		if strings.EqualFold(name, key) && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
var _ connector.WebhookVerifier = (*provider)(nil)
