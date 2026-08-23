// Package microsoft implements the official Microsoft 365 synchronization Provider.
package microsoft

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
	"github.com/domainry/domainry-connectors/internal/oauth2"
)

const (
	ConnectorKey              = "microsoft_365"
	ProviderKey               = "microsoft"
	defaultGraphBaseURL       = "https://graph.microsoft.com/v1.0"
	defaultScope              = "https://graph.microsoft.com/.default offline_access"
	defaultTimeout            = 30
	maximumTimeout            = 300
	responseLimit       int64 = 4 << 20
)

type SyncInput struct {
	Limit     int    `json:"limit,omitempty"`
	SkipToken string `json:"skip_token,omitempty"`
}
type Response map[string]any

var (
	SyncCalendar         = readOperation("sync_calendar", "6810952a3c33307bd1ccbc77db25e8bca1414438f617dd1802e13612903fb18e")
	SyncContacts         = readOperation("sync_contacts", "5d65856b321e10b13702e0b6026ab5ed98ef1489a51b643ccb22c5147c279f7f")
	SyncOneDriveFileRefs = readOperation("sync_onedrive_file_refs", "6559e67f2b17f63138e83fe87f743dbf40f3ac62bbaa1e5e5fb4d7cb24581528")
	SyncOutlookMail      = readOperation("sync_outlook_mail", "eb7187b4d91d9ef0cf586eb1a2a5158536fd04c31030df469097e81616dd7f92")
	TestConnection       = connector.CallOperation[struct{}, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection", ContractSHA256: "7aaec32fcb96ac020946ade5744e637b848ed7d16e5277bc1055fae8574c45d2", Reliability: readReliability()}
)

func readOperation(key, hash string) connector.CallOperation[SyncInput, Response] {
	return connector.CallOperation[SyncInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: key, ContractSHA256: hash, Reliability: readReliability()}
}
func readReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Microsoft 365 transport is required")
	}
	p := &provider{transport: transport}
	bindings := []func() (connector.BoundOperation, error){func() (connector.BoundOperation, error) {
		return connector.BindCall(SyncCalendar, p.sync("/me/events"))
	}, func() (connector.BoundOperation, error) {
		return connector.BindCall(SyncContacts, p.sync("/me/contacts"))
	}, func() (connector.BoundOperation, error) {
		return connector.BindCall(SyncOneDriveFileRefs, p.sync("/me/drive/root/children"))
	}, func() (connector.BoundOperation, error) {
		return connector.BindCall(SyncOutlookMail, p.sync("/me/messages"))
	}, func() (connector.BoundOperation, error) { return connector.BindCall(TestConnection, p.test) }}
	operations := make([]connector.BoundOperation, 0, len(bindings))
	for _, bind := range bindings {
		op, err := bind()
		if err != nil {
			return nil, err
		}
		operations = append(operations, op)
	}
	adapter, err := connector.NewProvider(schema(), operations...)
	if err != nil {
		return nil, err
	}
	p.Adapter = adapter
	return p, nil
}
func schema() connector.ProviderSchema {
	minimum, maximum := float64(1), float64(maximumTimeout)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "graph_base_url", Name: "Microsoft Graph Base URL", Type: connector.ConfigFieldText, Default: json.RawMessage(`"https://graph.microsoft.com/v1.0"`)}, {Key: "token_url", Name: "OAuth Token URL", Type: connector.ConfigFieldText}, {Key: "tenant_id", Name: "Tenant ID", Type: connector.ConfigFieldText, Required: true}, {Key: "scope", Name: "OAuth Scope", Type: connector.ConfigFieldText, Default: json.RawMessage(`"https://graph.microsoft.com/.default offline_access"`)}, {Key: "timeout_seconds", Name: "Timeout Seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}}}, SecretFields: []connector.SecretField{secret("access_token", "Access Token", true, connector.SecretCredentialBearerToken, connector.SecretRotationOAuthRefresh), secret("refresh_token", "Refresh Token", false, connector.SecretCredentialRefreshToken, connector.SecretRotationOAuthRefresh), secret("client_id", "OAuth Client ID", false, connector.SecretCredentialIdentifier, connector.SecretRotationManual), secret("client_secret", "OAuth Client Secret", false, connector.SecretCredentialOAuthClientSecret, connector.SecretRotationManual)}}
}
func secret(key, name string, required bool, kind connector.SecretCredentialKind, rotation connector.SecretRotationPolicy) connector.SecretField {
	return connector.SecretField{Key: key, Name: name, Required: required, CredentialKind: kind, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: rotation, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	if config(connection, "tenant_id") == "" {
		return permanent("tenant_required", "tenant_id is required")
	}
	for _, raw := range []string{graphBase(connection), tokenURL(connection)} {
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && loopback(parsed.Hostname()))) {
			return permanent("endpoint_invalid", "HTTPS or loopback HTTP endpoints are required")
		}
	}
	timeout := integer(connection.Config["timeout_seconds"], defaultTimeout)
	if timeout < 1 || timeout > maximumTimeout {
		return permanent("timeout_invalid", "timeout_seconds is invalid")
	}
	return nil
}
func (p *provider) sync(path string) connector.CallHandler[SyncInput, Response] {
	return func(ctx context.Context, request connector.TypedRequest[SyncInput]) (connector.TypedResult[Response], error) {
		if request.Input.Limit < 0 || request.Input.Limit > 1000 {
			return empty(), permanent("limit_invalid", "limit must be between 0 and 1000")
		}
		limit := request.Input.Limit
		if limit == 0 {
			limit = 100
		}
		query := url.Values{"$top": {strconv.Itoa(limit)}}
		if token := strings.TrimSpace(request.Input.SkipToken); token != "" {
			query.Set("$skiptoken", token)
		}
		return p.executeWithRefresh(ctx, request.Connection, request.Secrets, graphBase(request.Connection)+path, query)
	}
}
func (p *provider) test(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	return p.executeWithRefresh(ctx, request.Connection, request.Secrets, graphBase(request.Connection)+"/me", nil)
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.test(ctx, connector.TypedRequest[struct{}]{Connection: request.Connection, Secrets: request.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details, SecretUpdates: result.SecretUpdates}, nil
}
func (p *provider) executeWithRefresh(ctx context.Context, connection connector.Connection, secrets map[string]string, endpoint string, query url.Values) (connector.TypedResult[Response], error) {
	token := strings.TrimSpace(secrets["access_token"])
	if token == "" {
		return empty(), permanent("access_token_required", "resolved access token is required")
	}
	result, status, err := p.execute(ctx, connection, endpoint, query, token)
	if status != http.StatusUnauthorized {
		return result, err
	}
	refresh, clientID := strings.TrimSpace(secrets["refresh_token"]), strings.TrimSpace(secrets["client_id"])
	if refresh == "" || clientID == "" {
		return result, err
	}
	updated, refreshErr := oauth2.Refresh(ctx, p.transport, oauth2.RefreshRequest{Endpoint: tokenURL(connection), RefreshToken: refresh, ClientID: clientID, ClientSecret: strings.TrimSpace(secrets["client_secret"]), ClientSecretOptional: true, Scope: configDefault(connection, "scope", defaultScope), ClientAuthentication: oauth2.ClientAuthenticationForm, ErrorPrefix: "microsoft.oauth"})
	if refreshErr != nil {
		return connector.TypedResult[Response]{ResponseRef: "oauth:refresh_failed"}, refreshErr
	}
	result, _, err = p.execute(ctx, connection, endpoint, query, updated.AccessToken)
	result.SecretUpdates = map[string]string{"access_token": updated.AccessToken}
	if updated.RefreshToken != "" {
		result.SecretUpdates["refresh_token"] = updated.RefreshToken
	}
	return result, err
}
func (p *provider) execute(ctx context.Context, connection connector.Connection, endpoint string, query url.Values, token string) (connector.TypedResult[Response], int, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return empty(), 0, err
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return empty(), 0, permanent("endpoint_invalid", "endpoint is invalid")
	}
	parsed.RawQuery = query.Encode()
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodGet, URL: parsed.String(), Headers: map[string][]string{"Accept": {"application/json"}}, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		return empty(), 0, connector.RetryableError("microsoft.network_error", transportErr)
	}
	status, ref := response.StatusCode, "http:"+strconv.Itoa(response.StatusCode)
	payload := Response{}
	valid := len(response.Body) == 0 || json.Unmarshal(response.Body, &payload) == nil
	if status < 200 || status >= 300 {
		cause := fmt.Errorf("Microsoft Graph returned HTTP %d", status)
		code := "microsoft.http_" + strconv.Itoa(status)
		if status == http.StatusUnauthorized {
			return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, status, connector.PermanentError(code, cause)
		}
		if status == http.StatusTooManyRequests || status == http.StatusRequestTimeout || status >= 500 {
			return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, status, connector.RetryableError(code, cause)
		}
		return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, status, connector.PermanentError(code, cause)
	}
	if !valid {
		return connector.TypedResult[Response]{ResponseRef: ref}, status, connector.RetryableError("microsoft.response_invalid", errors.New("Microsoft Graph response is invalid JSON"))
	}
	return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, status, nil
}
func graphBase(connection connector.Connection) string {
	return strings.TrimRight(configDefault(connection, "graph_base_url", defaultGraphBaseURL), "/")
}
func tokenURL(connection connector.Connection) string {
	if value := config(connection, "token_url"); value != "" {
		return value
	}
	return "https://login.microsoftonline.com/" + url.PathEscape(config(connection, "tenant_id")) + "/oauth2/v2.0/token"
}
func config(connection connector.Connection, key string) string {
	value, _ := connection.Config[key].(string)
	return strings.TrimSpace(value)
}
func configDefault(connection connector.Connection, key, fallback string) string {
	if value := config(connection, key); value != "" {
		return value
	}
	return fallback
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
func loopback(host string) bool              { return host == "localhost" || host == "127.0.0.1" || host == "::1" }
func empty() connector.TypedResult[Response] { return connector.TypedResult[Response]{} }
func permanent(code, message string) error {
	return connector.PermanentError("microsoft."+code, errors.New(message))
}
