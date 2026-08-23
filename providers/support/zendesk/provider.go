// Package zendesk implements the official Zendesk Support Provider.
package zendesk

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

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	ConnectorKey        = "support"
	ProviderKey         = "zendesk"
	responseLimit int64 = 4 << 20
)

type Response map[string]any
type TicketInput map[string]any
type TicketIDInput struct {
	TicketID string `json:"ticket_id"`
}

var (
	CreateTicket   = op[TicketInput]("create_ticket", "3f0d1ce44121769894129f731b88a5b83ce477391be225786229db585442ba62", connector.EffectWrite, connector.IdempotencyNone)
	GetTicket      = op[TicketIDInput]("get_ticket", "0354f269e0db3078f6a4877a7f45ba678b1b1ba76481ff4496c0526a420ebdcb", connector.EffectRead, connector.IdempotencyNatural)
	ListTickets    = op[struct{}]("list_tickets", "a931dc2ef958b59469f8355f6b263f4df98beaa173e7d55a54b9d1525143469c", connector.EffectRead, connector.IdempotencyNatural)
	TestConnection = op[struct{}]("test_connection", "5de3016b7110c0a48e1a61d761301a4727f307dc6725540d5fa335dc44d5e5c7", connector.EffectRead, connector.IdempotencyNatural)
	UpdateTicket   = op[TicketInput]("update_ticket", "2092e0a53be456bcc29319084863f6914a7a163eb1f2dc6518e1a7fc90e4f749", connector.EffectWrite, connector.IdempotencyNone)
)

func op[I any](key, hash string, effect connector.OperationEffect, idempotency connector.IdempotencyStrategy) connector.CallOperation[I, Response] {
	return connector.CallOperation[I, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: key, ContractSHA256: hash, Reliability: connector.ReliabilityContract{Effect: effect, Idempotency: connector.IdempotencyContract{Strategy: idempotency}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Zendesk transport is required")
	}
	p := &provider{transport: transport}
	bindings := []func() (connector.BoundOperation, error){func() (connector.BoundOperation, error) { return connector.BindCall(CreateTicket, p.createTicket) }, func() (connector.BoundOperation, error) { return connector.BindCall(GetTicket, p.getTicket) }, func() (connector.BoundOperation, error) { return connector.BindCall(ListTickets, p.listTickets) }, func() (connector.BoundOperation, error) { return connector.BindCall(TestConnection, p.test) }, func() (connector.BoundOperation, error) { return connector.BindCall(UpdateTicket, p.updateTicket) }}
	ops := make([]connector.BoundOperation, 0, len(bindings))
	for _, bind := range bindings {
		bound, err := bind()
		if err != nil {
			return nil, err
		}
		ops = append(ops, bound)
	}
	adapter, err := connector.NewProvider(schema(), ops...)
	if err != nil {
		return nil, err
	}
	p.Adapter = adapter
	return p, nil
}
func schema() connector.ProviderSchema {
	min, max := float64(1), float64(120)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "subdomain", Name: "Zendesk subdomain", Type: connector.ConfigFieldText}, {Key: "email", Name: "Agent email", Type: connector.ConfigFieldEmail, Required: true}, {Key: "base_url", Name: "API base URL", Type: connector.ConfigFieldText}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &min, Max: &max}}}, SecretFields: []connector.SecretField{secret("api_token", "API token", connector.SecretCredentialBearerToken, true), secret("webhook_secret", "Webhook signing secret", connector.SecretCredentialSigningSecret, false)}}
}
func secret(key, name string, kind connector.SecretCredentialKind, required bool) connector.SecretField {
	return connector.SecretField{Key: key, Name: name, Required: required, CredentialKind: kind, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}
}
func (p *provider) ValidateConfig(c connector.Connection) error {
	if config(c, "email") == "" {
		return permanent("email_required", "email is required")
	}
	endpoint, err := url.Parse(baseURL(c))
	if err != nil || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || (endpoint.Scheme != "https" && !(endpoint.Scheme == "http" && loopback(endpoint.Hostname()))) {
		return permanent("endpoint_invalid", "HTTPS or loopback HTTP endpoint is required")
	}
	return nil
}
func (p *provider) test(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	return p.execute(ctx, r.Connection, r.Secrets, r.RequestRef, http.MethodGet, "/api/v2/users/me.json", nil, false)
}
func (p *provider) TestConnection(ctx context.Context, r connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.test(ctx, connector.TypedRequest[struct{}]{Connection: r.Connection, Secrets: r.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	raw, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: raw}, nil
}
func (p *provider) listTickets(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	return p.execute(ctx, r.Connection, r.Secrets, r.RequestRef, http.MethodGet, "/api/v2/tickets.json", nil, false)
}
func (p *provider) getTicket(ctx context.Context, r connector.TypedRequest[TicketIDInput]) (connector.TypedResult[Response], error) {
	id := strings.TrimSpace(r.Input.TicketID)
	if id == "" {
		return empty(), permanent("ticket_id_required", "ticket_id is required")
	}
	return p.execute(ctx, r.Connection, r.Secrets, r.RequestRef, http.MethodGet, "/api/v2/tickets/"+url.PathEscape(id)+".json", nil, false)
}
func (p *provider) createTicket(ctx context.Context, r connector.TypedRequest[TicketInput]) (connector.TypedResult[Response], error) {
	if len(r.Input) == 0 {
		return empty(), permanent("ticket_required", "ticket input is required")
	}
	return p.execute(ctx, r.Connection, r.Secrets, r.RequestRef, http.MethodPost, "/api/v2/tickets.json", map[string]any{"ticket": r.Input}, true)
}
func (p *provider) updateTicket(ctx context.Context, r connector.TypedRequest[TicketInput]) (connector.TypedResult[Response], error) {
	id := clean(r.Input["ticket_id"])
	if id == "" {
		return empty(), permanent("ticket_id_required", "ticket_id is required")
	}
	ticket := make(map[string]any, len(r.Input)-1)
	for key, value := range r.Input {
		if key != "ticket_id" {
			ticket[key] = value
		}
	}
	return p.execute(ctx, r.Connection, r.Secrets, r.RequestRef, http.MethodPut, "/api/v2/tickets/"+url.PathEscape(id)+".json", map[string]any{"ticket": ticket}, true)
}
func (p *provider) execute(ctx context.Context, c connector.Connection, secrets map[string]string, requestRef, method, path string, payload any, write bool) (connector.TypedResult[Response], error) {
	if err := p.ValidateConfig(c); err != nil {
		return empty(), err
	}
	token := strings.TrimSpace(secrets["api_token"])
	if token == "" {
		return empty(), permanent("api_token_required", "resolved API token is required")
	}
	var body []byte
	var err error
	if payload != nil {
		body, err = json.Marshal(payload)
		if err != nil {
			return empty(), permanent("request_invalid", "request is invalid")
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	if payload != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	authorization := "Basic " + base64.StdEncoding.EncodeToString([]byte(config(c, "email")+"/token:"+token))
	secretHeaders := map[string][]string{"Authorization": {authorization}}
	if requestRef != "" && write {
		headers["Idempotency-Key"] = []string{requestRef}
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: baseURL(c) + path, Headers: headers, SecretHeaders: secretHeaders, Body: body, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		return empty(), transportFailure(write, "network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	output := Response{}
	valid := len(response.Body) == 0 || json.Unmarshal(response.Body, &output) == nil
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code := clean(output["error"])
		if code == "" {
			code = "http_" + strconv.Itoa(response.StatusCode)
		}
		cause := fmt.Errorf("Zendesk returned HTTP %d", response.StatusCode)
		if response.StatusCode == 429 {
			return connector.TypedResult[Response]{Output: output, ResponseRef: ref}, connector.RetryableError("zendesk."+code, cause)
		}
		if response.StatusCode >= 500 {
			return connector.TypedResult[Response]{Output: output, ResponseRef: ref}, transportFailure(write, code, cause)
		}
		return connector.TypedResult[Response]{Output: output, ResponseRef: ref}, connector.PermanentError("zendesk."+code, cause)
	}
	if !valid {
		return connector.TypedResult[Response]{ResponseRef: ref}, transportFailure(write, "response_invalid", errors.New("Zendesk response is invalid JSON"))
	}
	for _, key := range []string{"ticket", "user"} {
		if item, ok := output[key].(map[string]any); ok {
			if id := jsonID(item["id"]); id != "" {
				ref = "zendesk:" + key + ":" + id
				break
			}
		}
	}
	return connector.TypedResult[Response]{Output: output, ResponseRef: ref}, nil
}
func jsonID(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case float64:
		return strconv.FormatInt(int64(typed), 10)
	case json.Number:
		return typed.String()
	default:
		return ""
	}
}
func config(c connector.Connection, key string) string { return clean(c.Config[key]) }
func clean(v any) string {
	result := strings.TrimSpace(fmt.Sprint(v))
	if result == "<nil>" {
		return ""
	}
	return result
}
func baseURL(c connector.Connection) string {
	if value := strings.TrimRight(config(c, "base_url"), "/"); value != "" {
		return value
	}
	return "https://" + config(c, "subdomain") + ".zendesk.com"
}
func loopback(host string) bool {
	return host == "localhost" || strings.HasPrefix(host, "127.") || host == "::1"
}
func empty() connector.TypedResult[Response] { return connector.TypedResult[Response]{} }
func permanent(suffix, message string) error {
	return connector.PermanentError("zendesk."+suffix, errors.New(message))
}
func transportFailure(write bool, suffix string, cause error) error {
	if write {
		return connector.UncertainError("zendesk."+suffix, cause)
	}
	return connector.RetryableError("zendesk."+suffix, cause)
}
