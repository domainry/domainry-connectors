// Package llmproxy implements the llm-proxy Google Expense Parser API.
package llmproxy

import (
	"context"
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
	ConnectorKey          = "expense_ocr"
	ProviderKey           = "llm_proxy"
	parsePath             = "/llm/expense/parse"
	maxRequestBytes       = 29 << 20
	maxResponseBytes      = 8 << 20
	defaultTimeoutSeconds = 90
)

// ParseExpense consumes a billable upstream operation with no idempotency or
// result lookup contract. Correlation IDs do not make it safe to replay.
var ParseExpense = connector.CallOperation[ParseExpenseInput, ParseExpenseOutput]{
	ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "parse_expense",
	ContractSHA256: "89bef2cb4a352bc78376493d7cf182aaf313ad70211a745c15c059c17fde7b4b",
	Reliability: connector.ReliabilityContract{
		Effect:         connector.EffectWrite,
		Idempotency:    connector.IdempotencyContract{Strategy: connector.IdempotencyNone},
		Reconciliation: connector.ReconciliationNone,
		Compensation:   connector.CompensationContract{Mode: connector.CompensationNone},
	},
}

// ParseExpenseInput accepts the same base64 document and correlation fields as
// POST /llm/expense/parse. Processor selection remains owned by llm-proxy.
type ParseExpenseInput struct {
	Document  string `json:"document"`
	MIMEType  string `json:"mime_type"`
	SessionID string `json:"session_id,omitempty"`
	ConvID    string `json:"conv_id,omitempty"`
	ReactID   string `json:"react_id,omitempty"`
}

type ParseExpenseOutput struct {
	Text     string   `json:"text"`
	Entities []Entity `json:"entities"`
}

// Entity preserves recursive line items and Google's typed normalized values.
// RawMessage avoids rounding integer/money values through float64 conversion.
type Entity struct {
	Type            string          `json:"type"`
	MentionText     string          `json:"mention_text"`
	Confidence      float32         `json:"confidence"`
	NormalizedValue json.RawMessage `json:"normalized_value,omitempty"`
	PageRefs        []PageRef       `json:"page_refs,omitempty"`
	Properties      []Entity        `json:"properties,omitempty"`
}

type PageRef struct {
	Page       int64  `json:"page"`
	LayoutType string `json:"layout_type,omitempty"`
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

// New only binds the Provider; it never performs I/O or reads credentials.
func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("llm-proxy transport is required")
	}
	p := &provider{transport: transport}
	parse, err := connector.BindCall(ParseExpense, p.parseExpense)
	if err != nil {
		return nil, err
	}
	p.Adapter, err = connector.NewProvider(schema(), parse)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func schema() connector.ProviderSchema {
	minimum, maximum := float64(1), float64(300)
	return connector.ProviderSchema{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0",
		ConfigFields: []connector.ConfigField{
			{Key: "base_url", Name: "llm-proxy origin", Description: "Service origin without /llm/expense/parse; HTTPS or loopback HTTP.", Type: connector.ConfigFieldText, Required: true, Validation: connector.ConfigValidation{MaxLength: 2048}},
			{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`90`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}},
		},
		SecretFields: []connector.SecretField{{
			Key: "api_token", Name: "llm-proxy API key or access token", Required: true,
			Description:    "Raw Passport API key or token; sent as a Runtime-injected Bearer credential.",
			CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque,
			RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional,
			TestRequirement: connector.SecretTestOptional,
		}},
	}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	_, _, err := settings(connection)
	return err
}

func settings(connection connector.Connection) (string, time.Duration, error) {
	base, ok := connection.Config["base_url"].(string)
	base = strings.TrimSpace(base)
	if !ok || base == "" || len(base) > 2048 {
		return "", 0, permanent("endpoint_invalid", "llm-proxy service origin is required")
	}
	u, err := url.Parse(base)
	if err != nil || u.Hostname() == "" || u.Opaque != "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(base, "#") || (u.Path != "" && u.Path != "/") {
		return "", 0, permanent("endpoint_invalid", "base_url must contain only the service origin")
	}
	loopback := strings.EqualFold(u.Hostname(), "localhost") || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"
	if u.Scheme != "https" && !(u.Scheme == "http" && loopback) {
		return "", 0, permanent("endpoint_invalid", "HTTPS or loopback HTTP is required")
	}
	seconds := defaultTimeoutSeconds
	if value, exists := connection.Config["timeout_seconds"]; exists {
		seconds, err = strconv.Atoi(fmt.Sprint(value))
		if err != nil || seconds < 1 || seconds > 300 {
			return "", 0, permanent("timeout_invalid", "timeout_seconds must be an integer between 1 and 300")
		}
	}
	return strings.TrimRight(u.String(), "/") + parsePath, time.Duration(seconds) * time.Second, nil
}

func (p *provider) parseExpense(ctx context.Context, request connector.TypedRequest[ParseExpenseInput]) (connector.TypedResult[ParseExpenseOutput], error) {
	var result connector.TypedResult[ParseExpenseOutput]
	endpoint, timeout, err := settings(request.Connection)
	if err != nil {
		return result, err
	}
	token := strings.TrimSpace(request.Secrets["api_token"])
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return result, permanent("api_token_invalid", "a resolved raw API key or access token is required")
	}
	input, err := canonicalInput(request.Input)
	if err != nil {
		return result, err
	}
	body, err := json.Marshal(input)
	if err != nil || len(body) > maxRequestBytes {
		return result, permanent("request_too_large", "expense request exceeds 29 MiB")
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if requestCtx.Err() != nil {
		return result, permanent("request_cancelled", "request cancelled before dispatch")
	}
	response, err := p.transport.RoundTripHTTP(requestCtx, connector.HTTPRequest{
		Method: http.MethodPost, URL: endpoint, Body: body, MaxResponseBytes: maxResponseBytes,
		Headers:       map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/json"}},
		SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}},
	})
	if err != nil {
		// Never attach transport errors: they can embed documents or credentials.
		return result, connector.UncertainError("llm_proxy_expense.network_error", errors.New("expense request outcome is unknown"))
	}
	result.ResponseRef = "http:" + strconv.Itoa(response.StatusCode)
	if response.StatusCode != http.StatusOK {
		return result, statusError(response.StatusCode)
	}
	if len(response.Body) > maxResponseBytes {
		return result, invalidResponse()
	}
	var envelope struct {
		Code *int `json:"code"`
		Data *struct {
			Text     *string  `json:"text"`
			Entities []Entity `json:"entities"`
		} `json:"data"`
	}
	if json.Unmarshal(response.Body, &envelope) != nil || envelope.Code == nil {
		return result, invalidResponse()
	}
	if *envelope.Code != 0 {
		return result, statusError(*envelope.Code)
	}
	if envelope.Data == nil || envelope.Data.Text == nil || envelope.Data.Entities == nil || !validEntities(envelope.Data.Entities, 0) {
		return result, invalidResponse()
	}
	result.Output = ParseExpenseOutput{Text: *envelope.Data.Text, Entities: envelope.Data.Entities}
	return result, nil
}

func statusError(status int) error {
	switch status {
	case http.StatusBadRequest, http.StatusRequestEntityTooLarge, http.StatusUnsupportedMediaType, http.StatusUnprocessableEntity:
		return permanent("document_rejected", "llm-proxy rejected the document")
	case http.StatusUnauthorized:
		return permanent("unauthorized", "llm-proxy authentication failed")
	case http.StatusForbidden:
		return permanent("forbidden", "llm-proxy account or environment access was denied")
	case http.StatusNotFound, http.StatusMethodNotAllowed:
		return permanent("endpoint_unavailable", "expense parser route is unavailable")
	case http.StatusTooManyRequests:
		return connector.RetryableError("llm_proxy_expense.rate_limited", errors.New("expense parser rate limited"))
	case http.StatusServiceUnavailable:
		// PR #1073 returns 503 for disabled/missing processor or credentials.
		return permanent("service_unavailable", "expense parser configuration or Google credentials are unavailable")
	default:
		return connector.UncertainError("llm_proxy_expense.upstream_error", errors.New("expense request outcome is unknown"))
	}
}

func validEntities(entities []Entity, depth int) bool {
	if depth > 64 {
		return false
	}
	for _, entity := range entities {
		if strings.TrimSpace(entity.Type) == "" || entity.Confidence < 0 || entity.Confidence > 1 {
			return false
		}
		if value := strings.TrimSpace(string(entity.NormalizedValue)); value != "" && value != "null" && !strings.HasPrefix(value, "{") {
			return false
		}
		for _, ref := range entity.PageRefs {
			if ref.Page < 0 {
				return false
			}
		}
		if len(entity.Properties) > 0 && !validEntities(entity.Properties, depth+1) {
			return false
		}
	}
	return true
}

func permanent(code, message string) error {
	return connector.PermanentError("llm_proxy_expense."+code, errors.New(message))
}

func invalidResponse() error {
	return connector.UncertainError("llm_proxy_expense.response_invalid", errors.New("expense parser response contract is invalid"))
}

var _ connector.ConfigValidator = (*provider)(nil)
