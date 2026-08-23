// Package persona implements the official Persona identity-verification Provider.
package persona

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
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
	ConnectorKey         = "kyc_risk"
	ProviderKey          = "persona"
	defaultBaseURL       = "https://api.withpersona.com"
	personaVersion       = "2025-12-08"
	responseLimit  int64 = 4 << 20
)

type GetInquiryInput struct {
	InquiryID string `json:"inquiry_id"`
}
type CreateInquiryInput struct {
	InquiryTemplateID string `json:"inquiry_template_id,omitempty"`
	ReferenceID       string `json:"reference-id,omitempty"`
	Note              string `json:"note,omitempty"`
	RedirectURI       string `json:"redirect-uri,omitempty"`
}
type PersonaResponse map[string]any

var (
	GetInquiry     = connector.CallOperation[GetInquiryInput, PersonaResponse]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "get_inquiry", ContractSHA256: "a6f8e804a8e861263d105aee9c1adc6385e6a18708b1d6b625b1bd15a78e5189", Reliability: readReliability()}
	CreateInquiry  = connector.CallOperation[CreateInquiryInput, PersonaResponse]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "create_inquiry", ContractSHA256: "e395f4963677a5611366b70a74d1b6e55697b0739b681a1457f926338ac6cad9", Reliability: writeReliability()}
	TestConnection = connector.CallOperation[struct{}, PersonaResponse]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection", ContractSHA256: "fe9ce1de2e18f232c663478afd697ca16683de6b3c4977e24cdf1b1a6db9a105", Reliability: readReliability()}
)

func readReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}
func writeReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNone}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Persona transport is required")
	}
	p := &provider{transport: transport}
	get, err := connector.BindCall(GetInquiry, p.getInquiry)
	if err != nil {
		return nil, err
	}
	create, err := connector.BindCall(CreateInquiry, p.createInquiry)
	if err != nil {
		return nil, err
	}
	test, err := connector.BindCall(TestConnection, p.callTestConnection)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), get, create, test)
	if err != nil {
		return nil, err
	}
	p.Adapter = bound
	return p, nil
}

func schema() connector.ProviderSchema {
	minimum, maximum := float64(1), float64(120)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{
		{Key: "inquiry_template_id", Name: "Default inquiry template ID", Type: connector.ConfigFieldText},
		{Key: "base_url", Name: "Persona API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://api.withpersona.com"`)},
		{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}},
	}, SecretFields: []connector.SecretField{
		{Key: "api_key", Name: "API key", Required: true, CredentialKind: connector.SecretCredentialAPIKey, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound},
		{Key: "webhook_secret", Name: "Webhook secret", Required: false, CredentialKind: connector.SecretCredentialSigningSecret, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound},
	}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(baseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return permanent("endpoint_invalid", "valid Persona API endpoint is required")
	}
	if parsed.Scheme == "http" && isLoopback(parsed.Hostname()) {
		return nil
	}
	if parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), "api.withpersona.com") {
		return permanent("endpoint_invalid", "official Persona API endpoint or loopback HTTP is required")
	}
	return nil
}

func (p *provider) getInquiry(ctx context.Context, request connector.TypedRequest[GetInquiryInput]) (connector.TypedResult[PersonaResponse], error) {
	id := strings.TrimSpace(request.Input.InquiryID)
	if id == "" {
		return connector.TypedResult[PersonaResponse]{}, permanent("inquiry_id_required", "inquiry_id is required")
	}
	return p.execute(ctx, request.Connection, request.Secrets, request.RequestRef, http.MethodGet, "/api/v1/inquiries/"+url.PathEscape(id), nil, nil, false)
}

func (p *provider) createInquiry(ctx context.Context, request connector.TypedRequest[CreateInquiryInput]) (connector.TypedResult[PersonaResponse], error) {
	templateID := strings.TrimSpace(request.Input.InquiryTemplateID)
	if templateID == "" {
		templateID = config(request.Connection, "inquiry_template_id", "")
	}
	if templateID == "" {
		return connector.TypedResult[PersonaResponse]{}, permanent("template_id_required", "inquiry template ID is required")
	}
	attributes := map[string]any{"inquiry-template-id": templateID}
	optional(attributes, "reference-id", request.Input.ReferenceID)
	optional(attributes, "note", request.Input.Note)
	optional(attributes, "redirect-uri", request.Input.RedirectURI)
	payload := map[string]any{"data": map[string]any{"type": "inquiry", "attributes": attributes}}
	result, err := p.execute(ctx, request.Connection, request.Secrets, request.RequestRef, http.MethodPost, "/api/v1/inquiries", nil, payload, true)
	if err != nil {
		return result, err
	}
	data, _ := result.Output["data"].(map[string]any)
	if mapString(data, "id") == "" {
		return result, connector.UncertainError("persona.response_invalid", errors.New("inquiry creation response lacks an identity"))
	}
	return result, nil
}

func (p *provider) callTestConnection(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[PersonaResponse], error) {
	return p.execute(ctx, request.Connection, request.Secrets, request.RequestRef, http.MethodGet, "/api/v1/inquiries", url.Values{"page[size]": {"1"}}, nil, false)
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.execute(ctx, request.Connection, request.Secrets, "", http.MethodGet, "/api/v1/inquiries", url.Values{"page[size]": {"1"}}, nil, false)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, marshalErr := json.Marshal(map[string]any{"inquiries": result.Output["data"], "response_ref": result.ResponseRef})
	if marshalErr != nil {
		return connector.TestConnectionResult{}, marshalErr
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, requestRef, method, path string, query url.Values, payload map[string]any, write bool) (connector.TypedResult[PersonaResponse], error) {
	if err := p.ValidateConfig(connection); err != nil {
		return connector.TypedResult[PersonaResponse]{}, err
	}
	key := strings.TrimSpace(secrets["api_key"])
	if key == "" {
		return connector.TypedResult[PersonaResponse]{}, permanent("api_key_required", "resolved API key is required")
	}
	endpoint, err := url.Parse(baseURL(connection) + path)
	if err != nil {
		return connector.TypedResult[PersonaResponse]{}, connector.PermanentError("persona.request_invalid", err)
	}
	endpoint.RawQuery = query.Encode()
	var body []byte
	if payload != nil {
		body, err = json.Marshal(payload)
		if err != nil {
			return connector.TypedResult[PersonaResponse]{}, connector.PermanentError("persona.request_invalid", err)
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}, "Persona-Version": {personaVersion}}
	if payload != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	if method == http.MethodPost && strings.TrimSpace(requestRef) != "" {
		headers["Idempotency-Key"] = []string{strings.TrimSpace(requestRef)}
	}
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint.String(), Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + key}}, Body: body, MaxResponseBytes: responseLimit})
	if err != nil {
		if write {
			return connector.TypedResult[PersonaResponse]{}, connector.UncertainError("persona.network_outcome_unknown", err)
		}
		return connector.TypedResult[PersonaResponse]{}, connector.RetryableError("persona.network_error", err)
	}
	ref, result := "http:"+strconv.Itoa(response.StatusCode), PersonaResponse{}
	validJSON := json.Unmarshal(response.Body, &result) == nil
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code, cause := "persona.http_"+strconv.Itoa(response.StatusCode), fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		typed := connector.TypedResult[PersonaResponse]{Output: result, ResponseRef: ref}
		if response.StatusCode == http.StatusTooManyRequests {
			return typed, connector.RetryableError(code, cause)
		}
		if response.StatusCode >= 500 {
			if write {
				return typed, connector.UncertainError(code, cause)
			}
			return typed, connector.RetryableError(code, cause)
		}
		return typed, connector.PermanentError(code, cause)
	}
	if !validJSON {
		if write {
			return connector.TypedResult[PersonaResponse]{ResponseRef: ref}, connector.UncertainError("persona.response_invalid", errors.New("provider response is invalid JSON"))
		}
		return connector.TypedResult[PersonaResponse]{ResponseRef: ref}, permanent("response_invalid", "provider response is invalid JSON")
	}
	if data, ok := result["data"].(map[string]any); ok {
		if id := mapString(data, "id"); id != "" {
			ref = "persona:inquiry:" + id
		}
	}
	return connector.TypedResult[PersonaResponse]{Output: result, ResponseRef: ref}, nil
}

type signatureCandidate struct{ timestamp, signature string }

func (p *provider) VerifyWebhook(ctx context.Context, request connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	if err := ctx.Err(); err != nil {
		return connector.VerifiedWebhook{}, err
	}
	secret := strings.TrimSpace(request.Secrets["webhook_secret"])
	if secret == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_secret_required", "resolved webhook secret is required")
	}
	valid := false
	for _, candidate := range signatureCandidates(header(request.Headers, "Persona-Signature")) {
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write([]byte(candidate.timestamp + "." + string(request.Body)))
		expected := hex.EncodeToString(mac.Sum(nil))
		if hmac.Equal([]byte(expected), []byte(candidate.signature)) {
			valid = true
		}
	}
	if !valid {
		return connector.VerifiedWebhook{}, permanent("webhook_signature_invalid", "webhook signature is invalid")
	}
	payload := PersonaResponse{}
	if json.Unmarshal(request.Body, &payload) != nil {
		return connector.VerifiedWebhook{}, permanent("webhook_payload_invalid", "webhook payload is invalid JSON")
	}
	data, _ := payload["data"].(map[string]any)
	eventType, eventID := mapString(data, "type"), mapString(data, "id")
	attributes, _ := data["attributes"].(map[string]any)
	wrapped, _ := attributes["payload"].(map[string]any)
	inquiry, _ := wrapped["data"].(map[string]any)
	inquiryID := mapString(inquiry, "id")
	if eventID == "" {
		eventID = inquiryID
	}
	if eventType == "" || eventID == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_identity_missing", "webhook event identity is missing")
	}
	verified := connector.VerifiedWebhook{EventType: eventType, ExternalID: eventID, Payload: append(json.RawMessage(nil), request.Body...), Security: &connector.WebhookSecurityEvidence{SignatureVerified: true}}
	if inquiryID != "" {
		verified.ExternalIdentity = &connector.WebhookExternalIdentity{Subject: inquiryID, SubjectType: "inquiry"}
	}
	return verified, nil
}

func signatureCandidates(value string) []signatureCandidate {
	result := []signatureCandidate{}
	for _, group := range strings.Fields(value) {
		timestamp := ""
		signatures := []string{}
		for _, part := range strings.Split(group, ",") {
			pair := strings.SplitN(strings.TrimSpace(part), "=", 2)
			if len(pair) != 2 {
				continue
			}
			if pair[0] == "t" {
				timestamp = pair[1]
			}
			if pair[0] == "v1" {
				signatures = append(signatures, pair[1])
			}
		}
		if timestamp != "" {
			for _, signature := range signatures {
				if signature != "" {
					result = append(result, signatureCandidate{timestamp: timestamp, signature: signature})
				}
			}
		}
	}
	return result
}
func baseURL(connection connector.Connection) string {
	if value := strings.TrimRight(config(connection, "base_url", ""), "/"); value != "" {
		return value
	}
	return defaultBaseURL
}
func config(connection connector.Connection, key, fallback string) string {
	if value := mapString(connection.Config, key); value != "" {
		return value
	}
	return fallback
}
func mapString(values map[string]any, key string) string {
	if values[key] == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(values[key]))
}
func optional(values map[string]any, key, value string) {
	if value = strings.TrimSpace(value); value != "" {
		values[key] = value
	}
}
func header(headers map[string][]string, key string) string {
	for current, values := range headers {
		if strings.EqualFold(current, key) && len(values) > 0 {
			return strings.TrimSpace(values[0])
		}
	}
	return ""
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
func permanent(code, message string) error {
	return connector.PermanentError("persona."+code, errors.New(message))
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
var _ connector.WebhookVerifier = (*provider)(nil)
