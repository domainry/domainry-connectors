package microsoftteams

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
	"github.com/domainry/domainry-connectors/internal/notificationmessage"
)

const (
	defaultGraphBase = "https://graph.microsoft.com/v1.0"
	responseLimit    = 4 << 20
)

type provider struct {
	connector.Adapter
	transport   connector.Transport
	providerKey string
}

func New(transport connector.Transport, identity Identity) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Microsoft Teams transport is required")
	}
	if identity.ConnectorKey == "" || identity.ProviderKey == "" || identity.SendContractSHA256 == "" || identity.TestContractSHA256 == "" {
		return nil, errors.New("Microsoft Teams Provider identity is required")
	}
	sendOperation := connector.EnqueueOperation[SendMessageInput]{ConnectorKey: identity.ConnectorKey, ProviderKey: identity.ProviderKey, Key: "send_message", ContractSHA256: identity.SendContractSHA256, Reliability: writeReliability()}
	testOperation := connector.CallOperation[struct{}, Response]{ConnectorKey: identity.ConnectorKey, ProviderKey: identity.ProviderKey, Key: "test_connection", ContractSHA256: identity.TestContractSHA256, Reliability: readReliability()}
	p := &provider{transport: transport, providerKey: identity.ProviderKey}
	send, err := connector.BindEnqueueDelivery(sendOperation, p.sendMessage)
	if err != nil {
		return nil, err
	}
	test, err := connector.BindCall(testOperation, p.callTestConnection)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(identity), send, test)
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

func schema(identity Identity) connector.ProviderSchema {
	minimum, maximum := float64(1), float64(60)
	return connector.ProviderSchema{ConnectorKey: identity.ConnectorKey, ProviderKey: identity.ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{
		{Key: "target_type", Name: "Microsoft Teams target type", Type: connector.ConfigFieldSelect, Required: true, Validation: connector.ConfigValidation{Options: []string{"chat", "channel"}}},
		{Key: "team_id", Name: "Microsoft Teams team ID", Type: connector.ConfigFieldText},
		{Key: "graph_base_url", Name: "Microsoft Graph API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://graph.microsoft.com/v1.0"`)},
		{Key: "notification_jwks_url", Name: "Graph notification JWKS URL", Type: connector.ConfigFieldText},
		{Key: "notification_audience", Name: "Graph notification audience", Type: connector.ConfigFieldText},
		{Key: "notification_issuer", Name: "Graph notification issuer", Type: connector.ConfigFieldText},
		{Key: "encryption_certificate_id", Name: "Graph encryption certificate ID", Type: connector.ConfigFieldText},
		{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`15`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}},
	}, SecretFields: []connector.SecretField{
		{Key: "access_token", Name: "Microsoft delegated OAuth access token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationOAuthRefresh, ExpiryPolicy: connector.SecretExpiryRequired, TestRequirement: connector.SecretTestWhenBound},
		{Key: "client_state", Name: "Graph webhook client state", CredentialKind: connector.SecretCredentialSigningSecret, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound},
		{Key: "encryption_private_key", Name: "Graph notification encryption private key", CredentialKind: connector.SecretCredentialPrivateKey, MaterialFormat: connector.SecretMaterialPEM, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound},
	}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(graphBase(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return permanent("endpoint_invalid", "valid Microsoft Graph endpoint is required")
	}
	if !(parsed.Scheme == "http" && isLoopback(parsed.Hostname())) && (parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), "graph.microsoft.com") || strings.TrimRight(parsed.EscapedPath(), "/") != "/v1.0") {
		return permanent("endpoint_invalid", "official Microsoft Graph v1.0 endpoint or loopback HTTP is required")
	}
	switch config(connection, "target_type", "") {
	case "chat":
	case "channel":
		if config(connection, "team_id", "") == "" {
			return permanent("team_id_required", "team_id is required for channel targets")
		}
	default:
		return permanent("target_type_invalid", "target_type must be chat or channel")
	}
	if seconds := intValue(connection.Config["timeout_seconds"], 15); seconds < 1 || seconds > 60 {
		return permanent("timeout_invalid", "timeout_seconds must be between 1 and 60")
	}
	return nil
}

func (p *provider) sendMessage(ctx context.Context, request connector.TypedRequest[SendMessageInput]) (connector.DeliveryResult, error) {
	recipient, message := strings.TrimSpace(request.Input.Recipient), first(request.Input.Message, request.Input.Text)
	if recipient == "" || message == "" {
		return connector.DeliveryResult{}, permanent("message_fields_required", "recipient and message are required")
	}
	path, err := messagePath(request.Connection, recipient)
	if err != nil {
		return connector.DeliveryResult{}, err
	}
	payload, compileErr := notificationmessage.ResolveProviderPayload(p.providerKey, request.Input.NotificationContent, request.Input.ProviderPayload)
	if compileErr != nil {
		return connector.DeliveryResult{}, permanent("notification_content_invalid", compileErr.Error())
	}
	if len(payload) == 0 {
		payload = map[string]any{"body": map[string]any{"contentType": "text", "content": message}}
	}
	result, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodPost, path, payload, true)
	if err != nil {
		return connector.DeliveryResult{ResponseRef: ref}, err
	}
	messageID := mapString(result, "id")
	if messageID == "" {
		return connector.DeliveryResult{ResponseRef: ref}, connector.UncertainError("microsoft_teams.message_response_invalid", errors.New("Graph response lacks message id"))
	}
	return connector.DeliveryResult{ResponseRef: "teams:" + messageID}, nil
}

func (p *provider) callTestConnection(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, "/me", nil, false)
	return connector.TypedResult[Response]{Output: Response{"connected": err == nil, "user_id": payload["id"], "display_name": payload["displayName"]}, ResponseRef: ref}, err
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.callTestConnection(ctx, connector.TypedRequest[struct{}]{Connection: request.Connection, Secrets: request.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, method, path string, body map[string]any, write bool) (Response, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", err
	}
	token := strings.TrimSpace(secrets["access_token"])
	if token == "" {
		return nil, "", permanent("access_token_required", "resolved Microsoft delegated access token is required")
	}
	var raw []byte
	var err error
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return nil, "", permanent("request_invalid", "Microsoft Graph request body is invalid")
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json; charset=utf-8"}
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: graphBase(connection) + path, Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, Body: raw, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		if write {
			return nil, "", connector.UncertainError("microsoft_teams.network_error", transportErr)
		}
		return nil, "", connector.RetryableError("microsoft_teams.network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := Response{}
	valid := len(response.Body) == 0 || json.Unmarshal(response.Body, &payload) == nil
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("Microsoft Graph returned HTTP %d", response.StatusCode)
		code := "microsoft_teams.http_" + strconv.Itoa(response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests {
			return payload, ref, connector.RetryableError(code, cause)
		}
		if response.StatusCode == http.StatusRequestTimeout || response.StatusCode >= 500 {
			if write {
				return payload, ref, connector.UncertainError(code, cause)
			}
			return payload, ref, connector.RetryableError(code, cause)
		}
		return payload, ref, connector.PermanentError(code, cause)
	}
	if !valid {
		if write {
			return nil, ref, connector.UncertainError("microsoft_teams.response_invalid", errors.New("Microsoft Graph response is invalid JSON"))
		}
		return nil, ref, permanent("response_invalid", "Microsoft Graph response is invalid JSON")
	}
	return payload, ref, nil
}

func messagePath(connection connector.Connection, recipient string) (string, error) {
	switch config(connection, "target_type", "") {
	case "chat":
		return "/chats/" + url.PathEscape(recipient) + "/messages", nil
	case "channel":
		teamID := config(connection, "team_id", "")
		if teamID == "" {
			return "", permanent("team_id_required", "team_id is required for channel targets")
		}
		return "/teams/" + url.PathEscape(teamID) + "/channels/" + url.PathEscape(recipient) + "/messages", nil
	default:
		return "", permanent("target_type_invalid", "target_type must be chat or channel")
	}
}
func graphBase(connection connector.Connection) string {
	return strings.TrimRight(config(connection, "graph_base_url", defaultGraphBase), "/")
}
func config(connection connector.Connection, key, fallback string) string {
	if value := mapString(connection.Config, key); value != "" {
		return value
	}
	return fallback
}
func mapString(values map[string]any, key string) string {
	if values == nil || values[key] == nil {
		return ""
	}
	value := strings.TrimSpace(fmt.Sprint(values[key]))
	if value == "<nil>" {
		return ""
	}
	return value
}
func intValue(value any, fallback int) int {
	if value == nil {
		return fallback
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(fmt.Sprint(value)))
	if err != nil {
		return 0
	}
	return parsed
}
func first(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
func cloneMap(source map[string]any) map[string]any {
	if len(source) == 0 {
		return nil
	}
	target := make(map[string]any, len(source))
	for key, value := range source {
		target[key] = value
	}
	return target
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
func permanent(code, message string) error {
	return connector.PermanentError("microsoft_teams."+code, errors.New(message))
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
