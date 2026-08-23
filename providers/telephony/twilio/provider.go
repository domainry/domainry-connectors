// Package twilio implements the official Twilio telephony Provider.
package twilio

import (
	"context"
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
	ConnectorKey         = "telephony"
	ProviderKey          = "twilio"
	defaultBaseURL       = "https://api.twilio.com"
	responseLimit  int64 = 4 << 20
)

type Response map[string]any
type SendSMSInput struct {
	To   string `json:"to"`
	From string `json:"from,omitempty"`
	Body string `json:"body"`
}

var (
	SendSMS        = operation[SendSMSInput]("send_sms", "7182e865f86096f212d4ec8bf86fce67a19efe3e6dac7b726cabb53afa3d4343", connector.EffectWrite, connector.IdempotencyNone)
	TestConnection = operation[struct{}]("test_connection", "c71765c363077d5a07897a82d99be9ebdb751eafd4772b8e3356e1503fc11077", connector.EffectRead, connector.IdempotencyNatural)
)

func operation[I any](key, hash string, effect connector.OperationEffect, idempotency connector.IdempotencyStrategy) connector.CallOperation[I, Response] {
	return connector.CallOperation[I, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: key, ContractSHA256: hash, Reliability: connector.ReliabilityContract{Effect: effect, Idempotency: connector.IdempotencyContract{Strategy: idempotency}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Twilio transport is required")
	}
	p := &provider{transport: transport}
	send, err := connector.BindCall(SendSMS, p.sendSMS)
	if err != nil {
		return nil, err
	}
	test, err := connector.BindCall(TestConnection, p.test)
	if err != nil {
		return nil, err
	}
	adapter, err := connector.NewProvider(schema(), send, test)
	if err != nil {
		return nil, err
	}
	p.Adapter = adapter
	return p, nil
}
func schema() connector.ProviderSchema {
	min, max := float64(1), float64(120)
	text := func(key, name string) connector.ConfigField {
		return connector.ConfigField{Key: key, Name: name, Type: connector.ConfigFieldText}
	}
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{text("account_id", "Account ID"), {Key: "from_number", Name: "From number", Type: connector.ConfigFieldText, Required: true}, text("webhook_url", "Webhook URL"), text("default_country", "Default country"), text("default_owner_queue", "Default owner queue"), text("callback_workflow_key", "Callback workflow key"), text("callback_sla_hours", "Callback SLA hours"), text("recording_archive_policy", "Recording archive policy"), text("use_cases", "Use cases"), {Key: "base_url", Name: "Twilio API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://api.twilio.com"`)}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &min, Max: &max}}}, SecretFields: []connector.SecretField{secret("account_sid", "Account SID", connector.SecretCredentialIdentifier), secret("auth_token", "Auth token", connector.SecretCredentialBasicAuthPassword)}}
}
func secret(key, name string, kind connector.SecretCredentialKind) connector.SecretField {
	return connector.SecretField{Key: key, Name: name, Required: true, CredentialKind: kind, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}
}
func (p *provider) ValidateConfig(c connector.Connection) error {
	if config(c, "from_number") == "" {
		return permanent("from_number_required", "from_number is required")
	}
	endpoint, err := url.Parse(baseURL(c))
	if err != nil || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || (endpoint.Scheme != "https" && !(endpoint.Scheme == "http" && loopback(endpoint.Hostname()))) {
		return permanent("endpoint_invalid", "HTTPS or loopback HTTP endpoint is required")
	}
	return nil
}
func (p *provider) test(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	sid := strings.TrimSpace(r.Secrets["account_sid"])
	if sid == "" {
		return empty(), permanent("account_sid_required", "resolved account SID is required")
	}
	return p.execute(ctx, r.Connection, r.Secrets, r.RequestRef, http.MethodGet, "/2010-04-01/Accounts/"+url.PathEscape(sid)+".json", nil, false)
}
func (p *provider) TestConnection(ctx context.Context, r connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.test(ctx, connector.TypedRequest[struct{}]{Connection: r.Connection, Secrets: r.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	raw, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: raw}, nil
}
func (p *provider) sendSMS(ctx context.Context, r connector.TypedRequest[SendSMSInput]) (connector.TypedResult[Response], error) {
	to, from, body := strings.TrimSpace(r.Input.To), strings.TrimSpace(r.Input.From), strings.TrimSpace(r.Input.Body)
	if from == "" {
		from = config(r.Connection, "from_number")
	}
	if to == "" || from == "" || body == "" {
		return empty(), permanent("message_invalid", "to, from and body are required")
	}
	sid := strings.TrimSpace(r.Secrets["account_sid"])
	if sid == "" {
		return empty(), permanent("account_sid_required", "resolved account SID is required")
	}
	form := url.Values{"To": {to}, "From": {from}, "Body": {body}}
	if callback := config(r.Connection, "webhook_url"); callback != "" {
		form.Set("StatusCallback", callback)
	}
	return p.execute(ctx, r.Connection, r.Secrets, r.RequestRef, http.MethodPost, "/2010-04-01/Accounts/"+url.PathEscape(sid)+"/Messages.json", form, true)
}
func (p *provider) execute(ctx context.Context, c connector.Connection, secrets map[string]string, requestRef, method, path string, form url.Values, write bool) (connector.TypedResult[Response], error) {
	if err := p.ValidateConfig(c); err != nil {
		return empty(), err
	}
	sid, token := strings.TrimSpace(secrets["account_sid"]), strings.TrimSpace(secrets["auth_token"])
	if sid == "" || token == "" {
		return empty(), permanent("credentials_required", "resolved account SID and auth token are required")
	}
	body := []byte{}
	headers := map[string][]string{"Accept": {"application/json"}}
	if form != nil {
		body = []byte(form.Encode())
		headers["Content-Type"] = []string{"application/x-www-form-urlencoded"}
	}
	if requestRef != "" && write {
		headers["Idempotency-Key"] = []string{requestRef}
	}
	auth := "Basic " + base64.StdEncoding.EncodeToString([]byte(sid+":"+token))
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: baseURL(c) + path, Headers: headers, SecretHeaders: map[string][]string{"Authorization": {auth}}, Body: body, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		return empty(), transportFailure(write, "network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	output := Response{}
	valid := len(response.Body) == 0 || json.Unmarshal(response.Body, &output) == nil
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("Twilio returned HTTP %d", response.StatusCode)
		code := "http_" + strconv.Itoa(response.StatusCode)
		if response.StatusCode == 429 {
			return connector.TypedResult[Response]{Output: output, ResponseRef: ref}, connector.RetryableError("twilio."+code, cause)
		}
		if response.StatusCode >= 500 {
			return connector.TypedResult[Response]{Output: output, ResponseRef: ref}, transportFailure(write, code, cause)
		}
		return connector.TypedResult[Response]{Output: output, ResponseRef: ref}, connector.PermanentError("twilio."+code, cause)
	}
	if !valid {
		return connector.TypedResult[Response]{ResponseRef: ref}, transportFailure(write, "response_invalid", errors.New("Twilio response is invalid JSON"))
	}
	if sid := clean(output["sid"]); sid != "" {
		ref = "twilio:" + sid
	}
	return connector.TypedResult[Response]{Output: output, ResponseRef: ref}, nil
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
	return defaultBaseURL
}
func loopback(host string) bool {
	return host == "localhost" || strings.HasPrefix(host, "127.") || host == "::1"
}
func empty() connector.TypedResult[Response] { return connector.TypedResult[Response]{} }
func permanent(suffix, message string) error {
	return connector.PermanentError("twilio."+suffix, errors.New(message))
}
func transportFailure(write bool, suffix string, cause error) error {
	if write {
		return connector.UncertainError("twilio."+suffix, cause)
	}
	return connector.RetryableError("twilio."+suffix, cause)
}
