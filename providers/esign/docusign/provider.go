// Package docusign implements the official Docusign eSignature Provider.
package docusign

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/internal/oauth2"
)

const (
	ConnectorKey    = "esign"
	ProviderKey     = "docusign"
	defaultTokenURL = "https://account.docusign.com/oauth/token"
)

type CreateEnvelopeInput struct {
	Input map[string]any `json:"input"`
}
type EnvelopeInput struct {
	EnvelopeID string `json:"envelope_id"`
}
type RecipientViewInput struct {
	EnvelopeID string         `json:"envelope_id"`
	Input      map[string]any `json:"input"`
}
type VoidEnvelopeInput struct {
	EnvelopeID string `json:"envelope_id"`
	Reason     string `json:"reason"`
}
type ListEnvelopeStatusChangesInput struct {
	FromDate string `json:"from_date"`
	Cursor   string `json:"cursor,omitempty"`
}

var (
	CreateEnvelope            = writeOp[CreateEnvelopeInput]("create_envelope", "cffe2007f4352b8542e199f6578f5476b0a0aa7a7bc3ccf3875916e90da8b663")
	CreateRecipientView       = writeOp[RecipientViewInput]("create_recipient_view", "0defebfe26cb6649fb00973fd883fc7a9bf9a20ef911570d206975a49190b045")
	GetEnvelope               = readOp[EnvelopeInput]("get_envelope", "d7dbda5c4ae2787e050cdf5935aed6a8d429a2ebddc93643dcd991aca972d267")
	ListEnvelopeStatusChanges = readOp[ListEnvelopeStatusChangesInput]("list_envelope_status_changes", "f5fd79d5a3011418986efdf1216f47099c8bd45956cab5201b5a62f4c3cbbd6e")
	SendEnvelope              = writeOp[EnvelopeInput]("send_envelope", "4936ba0cd6632815d0548c127cfcb36411edbe98990641559161dfec8b4221df")
	TestConnection            = readOp[struct{}]("test_connection", "973cfc9b2671622a791ab84975fb9aa853730640a998307ae0f66f38847ce6fa")
	VoidEnvelope              = writeOp[VoidEnvelopeInput]("void_envelope", "49b989e409f53805a2135308785ebe4c8170fc16044805aafde6c32f875dcd9f")
)

func readOp[I any](key, hash string) connector.CallOperation[I, map[string]any] {
	return operation[I](key, hash, connector.EffectRead)
}
func writeOp[I any](key, hash string) connector.CallOperation[I, map[string]any] {
	return operation[I](key, hash, connector.EffectWrite)
}
func operation[I any](key, hash string, effect connector.OperationEffect) connector.CallOperation[I, map[string]any] {
	strategy := connector.IdempotencyNone
	if effect == connector.EffectRead {
		strategy = connector.IdempotencyNatural
	}
	return connector.CallOperation[I, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: key, ContractSHA256: hash, Reliability: connector.ReliabilityContract{Effect: effect, Idempotency: connector.IdempotencyContract{Strategy: strategy}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Docusign transport is required")
	}
	p := &provider{transport: transport}
	operations := make([]connector.BoundOperation, 0, 7)
	bind := func(bound connector.BoundOperation, err error) error {
		operations = append(operations, bound)
		return err
	}
	if err := bind(connector.BindCall(CreateEnvelope, p.createEnvelope)); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(CreateRecipientView, p.createRecipientView)); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(GetEnvelope, p.getEnvelope)); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(ListEnvelopeStatusChanges, p.listChanges)); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(SendEnvelope, p.sendEnvelope)); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(TestConnection, p.testConnection)); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(VoidEnvelope, p.voidEnvelope)); err != nil {
		return nil, err
	}
	adapter, err := connector.NewProvider(schema(), operations...)
	if err != nil {
		return nil, err
	}
	p.Adapter = adapter
	return p, nil
}
func schema() connector.ProviderSchema {
	min, max := float64(1), float64(120)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{
		{Key: "base_url", Name: "eSignature REST API base URL", Type: connector.ConfigFieldText, Required: true}, {Key: "account_id", Name: "Docusign account ID", Type: connector.ConfigFieldText, Required: true},
		{Key: "token_url", Name: "OAuth token URL", Type: connector.ConfigFieldText, Default: json.RawMessage(`"https://account.docusign.com/oauth/token"`)}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &min, Max: &max}},
	}, SecretFields: []connector.SecretField{
		secret("access_token", "OAuth access token", true, connector.SecretCredentialBearerToken, connector.SecretRotationOAuthRefresh), secret("refresh_token", "OAuth refresh token", false, connector.SecretCredentialRefreshToken, connector.SecretRotationOAuthRefresh),
		secret("client_id", "Integration key", false, connector.SecretCredentialIdentifier, connector.SecretRotationManual), secret("client_secret", "OAuth client secret", false, connector.SecretCredentialOAuthClientSecret, connector.SecretRotationManual), secret("webhook_secret", "Connect HMAC key", false, connector.SecretCredentialSigningSecret, connector.SecretRotationManual),
	}}
}
func secret(key, name string, required bool, kind connector.SecretCredentialKind, rotation connector.SecretRotationPolicy) connector.SecretField {
	return connector.SecretField{Key: key, Name: name, Required: required, CredentialKind: kind, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: rotation, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	if !validEndpoint(config(connection, "base_url", "")) {
		return permanent("endpoint_invalid", "valid Docusign API base URL is required")
	}
	if config(connection, "account_id", "") == "" {
		return permanent("account_id_required", "account_id is required")
	}
	if !validEndpoint(config(connection, "token_url", defaultTokenURL)) {
		return permanent("token_url_invalid", "valid OAuth token URL is required")
	}
	timeout := intValue(connection.Config["timeout_seconds"], 30)
	if timeout < 1 || timeout > 120 {
		return permanent("timeout_invalid", "timeout_seconds must be between 1 and 120")
	}
	return nil
}
func validEndpoint(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && parsed.Host != "" && parsed.User == nil && parsed.RawQuery == "" && parsed.Fragment == "" && (parsed.Scheme == "https" || (parsed.Scheme == "http" && isLoopback(parsed.Hostname())))
}

func (p *provider) createEnvelope(ctx context.Context, r connector.TypedRequest[CreateEnvelopeInput]) (connector.TypedResult[map[string]any], error) {
	if len(r.Input.Input) == 0 {
		return empty(), permanent("input_required", "envelope input is required")
	}
	return p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodPost, envelopeBase(r.Connection), nil, r.Input.Input, true)
}
func (p *provider) getEnvelope(ctx context.Context, r connector.TypedRequest[EnvelopeInput]) (connector.TypedResult[map[string]any], error) {
	id, err := envelopeID(r.Input.EnvelopeID)
	if err != nil {
		return empty(), err
	}
	return p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodGet, envelopeBase(r.Connection)+"/"+url.PathEscape(id), nil, nil, false)
}
func (p *provider) sendEnvelope(ctx context.Context, r connector.TypedRequest[EnvelopeInput]) (connector.TypedResult[map[string]any], error) {
	id, err := envelopeID(r.Input.EnvelopeID)
	if err != nil {
		return empty(), err
	}
	return p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodPut, envelopeBase(r.Connection)+"/"+url.PathEscape(id), nil, map[string]any{"status": "sent"}, true)
}
func (p *provider) voidEnvelope(ctx context.Context, r connector.TypedRequest[VoidEnvelopeInput]) (connector.TypedResult[map[string]any], error) {
	id, err := envelopeID(r.Input.EnvelopeID)
	if err != nil || strings.TrimSpace(r.Input.Reason) == "" {
		return empty(), permanent("void_fields_required", "envelope_id and reason are required")
	}
	return p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodPut, envelopeBase(r.Connection)+"/"+url.PathEscape(id), nil, map[string]any{"status": "voided", "voidedReason": strings.TrimSpace(r.Input.Reason)}, true)
}
func (p *provider) createRecipientView(ctx context.Context, r connector.TypedRequest[RecipientViewInput]) (connector.TypedResult[map[string]any], error) {
	id, err := envelopeID(r.Input.EnvelopeID)
	if err != nil || len(r.Input.Input) == 0 {
		return empty(), permanent("recipient_view_fields_required", "envelope_id and recipient view input are required")
	}
	return p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodPost, envelopeBase(r.Connection)+"/"+url.PathEscape(id)+"/views/recipient", nil, r.Input.Input, true)
}
func (p *provider) listChanges(ctx context.Context, r connector.TypedRequest[ListEnvelopeStatusChangesInput]) (connector.TypedResult[map[string]any], error) {
	from := strings.TrimSpace(r.Input.FromDate)
	if from == "" {
		return empty(), permanent("from_date_required", "from_date is required")
	}
	query := url.Values{"from_date": {from}}
	if cursor := strings.TrimSpace(r.Input.Cursor); cursor != "" {
		query.Set("start_position", cursor)
	}
	return p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodGet, envelopeBase(r.Connection), query, nil, false)
}
func (p *provider) testConnection(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[map[string]any], error) {
	return p.executeWithRefresh(ctx, r.Connection, r.Secrets, http.MethodGet, envelopeBase(r.Connection), nil, nil, false)
}
func (p *provider) TestConnection(ctx context.Context, r connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.testConnection(ctx, connector.TypedRequest[struct{}]{Connection: r.Connection, Secrets: r.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details, SecretUpdates: result.SecretUpdates}, nil
}

func (p *provider) executeWithRefresh(ctx context.Context, connection connector.Connection, secrets map[string]string, method, path string, query url.Values, body map[string]any, write bool) (connector.TypedResult[map[string]any], error) {
	if err := p.ValidateConfig(connection); err != nil {
		return empty(), err
	}
	token := strings.TrimSpace(secrets["access_token"])
	if token == "" {
		return empty(), permanent("access_token_required", "access token is required")
	}
	result, status, err := p.execute(ctx, connection, token, method, path, query, body, write)
	if err == nil || status != http.StatusUnauthorized {
		return result, err
	}
	if strings.TrimSpace(secrets["refresh_token"]) == "" || strings.TrimSpace(secrets["client_id"]) == "" || strings.TrimSpace(secrets["client_secret"]) == "" {
		return result, err
	}
	updated, refreshErr := oauth2.Refresh(ctx, p.transport, oauth2.RefreshRequest{Endpoint: config(connection, "token_url", defaultTokenURL), RefreshToken: secrets["refresh_token"], ClientID: secrets["client_id"], ClientSecret: secrets["client_secret"], ClientAuthentication: oauth2.ClientAuthenticationBasic, ErrorPrefix: "docusign"})
	if refreshErr != nil {
		return connector.TypedResult[map[string]any]{ResponseRef: "oauth:refresh_failed"}, refreshErr
	}
	result, _, err = p.execute(ctx, connection, updated.AccessToken, method, path, query, body, write)
	result.SecretUpdates = map[string]string{"access_token": updated.AccessToken}
	if updated.RefreshToken != "" {
		result.SecretUpdates["refresh_token"] = updated.RefreshToken
	}
	return result, err
}
func (p *provider) execute(ctx context.Context, connection connector.Connection, token, method, path string, query url.Values, body map[string]any, write bool) (connector.TypedResult[map[string]any], int, error) {
	var raw []byte
	var err error
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return empty(), 0, permanent("request_invalid", "request body is invalid")
		}
	}
	endpoint := strings.TrimRight(config(connection, "base_url", ""), "/") + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint, Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, Body: raw, MaxResponseBytes: 4 << 20})
	if err != nil {
		if write {
			return empty(), 0, connector.UncertainError("docusign.delivery_uncertain", err)
		}
		return empty(), 0, connector.RetryableError("docusign.network_error", err)
	}
	payload := map[string]any{}
	if len(response.Body) > 0 && json.Unmarshal(response.Body, &payload) != nil {
		if write {
			return empty(), response.StatusCode, connector.UncertainError("docusign.response_uncertain", errors.New("Docusign response is invalid"))
		}
		return empty(), response.StatusCode, connector.RetryableError("docusign.response_invalid", errors.New("Docusign response is invalid"))
	}
	ref := fmt.Sprintf("http:%d", response.StatusCode)
	if id, _ := payload["envelopeId"].(string); strings.TrimSpace(id) != "" {
		ref = "docusign:" + strings.TrimSpace(id)
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, response.StatusCode, nil
	}
	code, _ := payload["errorCode"].(string)
	if code == "" {
		code = fmt.Sprintf("http_%d", response.StatusCode)
	}
	cause := errors.New("Docusign request failed")
	if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
		return connector.TypedResult[map[string]any]{ResponseRef: ref}, response.StatusCode, connector.RetryableError("docusign."+strings.ToLower(code), cause)
	}
	return connector.TypedResult[map[string]any]{ResponseRef: ref}, response.StatusCode, connector.PermanentError("docusign."+strings.ToLower(code), cause)
}

func (p *provider) VerifyWebhook(ctx context.Context, request connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	if err := ctx.Err(); err != nil {
		return connector.VerifiedWebhook{}, err
	}
	secret := strings.TrimSpace(request.Secrets["webhook_secret"])
	if secret == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_secret_required", "resolved webhook secret is required")
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(request.Body)
	expected := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	valid := false
	for i := 1; i <= 3; i++ {
		if hmac.Equal([]byte(expected), []byte(header(request.Headers, "X-DocuSign-Signature-"+strconv.Itoa(i)))) {
			valid = true
			break
		}
	}
	if !valid {
		return connector.VerifiedWebhook{}, permanent("webhook_signature_invalid", "Docusign webhook signature does not match")
	}
	payload := map[string]any{}
	if json.Unmarshal(request.Body, &payload) != nil {
		return connector.VerifiedWebhook{}, permanent("webhook_payload_invalid", "Docusign webhook payload is invalid")
	}
	data, _ := payload["data"].(map[string]any)
	id, _ := data["envelopeId"].(string)
	event, _ := payload["event"].(string)
	id, event = strings.TrimSpace(id), strings.TrimSpace(event)
	if id == "" || event == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_identity_missing", "Docusign webhook identity is missing")
	}
	return connector.VerifiedWebhook{EventType: event, ExternalID: id + ":" + event, Payload: append(json.RawMessage(nil), request.Body...), Security: &connector.WebhookSecurityEvidence{SignatureVerified: true}, ExternalIdentity: &connector.WebhookExternalIdentity{Subject: id, SubjectType: "docusign_envelope"}}, nil
}
func envelopeBase(connection connector.Connection) string {
	return "/v2.1/accounts/" + url.PathEscape(config(connection, "account_id", "")) + "/envelopes"
}
func envelopeID(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", permanent("envelope_id_required", "envelope_id is required")
	}
	return value, nil
}
func empty() connector.TypedResult[map[string]any] { return connector.TypedResult[map[string]any]{} }
func config(connection connector.Connection, key, fallback string) string {
	value, _ := connection.Config[key].(string)
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return fallback
}
func intValue(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		return typed
	case float64:
		return int(typed)
	case json.Number:
		parsed, err := strconv.Atoi(typed.String())
		if err == nil {
			return parsed
		}
	}
	return fallback
}
func isLoopback(host string) bool {
	return strings.EqualFold(host, "localhost") || (net.ParseIP(host) != nil && net.ParseIP(host).IsLoopback())
}
func header(headers map[string][]string, name string) string {
	for key, values := range headers {
		if strings.EqualFold(key, name) && len(values) > 0 {
			return strings.TrimSpace(values[0])
		}
	}
	return ""
}
func permanent(code, message string) error {
	return connector.PermanentError("docusign."+code, errors.New(message))
}
