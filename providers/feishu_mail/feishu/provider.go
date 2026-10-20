// Package feishumail implements the official Feishu Mail provider.
package feishu

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
	"github.com/domainry/domainry-connector-sdk/mail"
	"github.com/domainry/domainry-connector-sdk/mailattachment"
	"github.com/domainry/domainry-connector-sdk/mailwrite"
	internalfeishu "github.com/domainry/domainry-connectors/internal/feishu"
)

const (
	ConnectorKey              = "feishu_mail"
	ProviderKey               = "feishu"
	defaultBaseURL            = "https://open.feishu.cn"
	defaultOAuthBaseURL       = "https://accounts.feishu.cn"
	responseLimit       int64 = 1 << 20
)

type Response map[string]any

var (
	MailList = connector.CallOperation[mail.PageRequest, mail.MessagesPage]{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: mail.ListOperationKey,
		ContractSHA256: mail.OperationSHA256(mail.ListOperationKey), Reliability: readReliability(),
	}
	MailSearch = connector.CallOperation[mail.SearchRequest, mail.MessagesPage]{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: mail.SearchOperationKey,
		ContractSHA256: mail.OperationSHA256(mail.SearchOperationKey), Reliability: readReliability(),
	}
	MailRead = connector.CallOperation[mail.ReadRequest, mail.Message]{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: mail.ReadOperationKey,
		ContractSHA256: mail.OperationSHA256(mail.ReadOperationKey), Reliability: readReliability(),
	}
	MailSend = connector.CallOperation[mailwrite.SendRequest, mailwrite.Result]{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: mailwrite.SendOperationKey,
		ContractSHA256: mailwrite.OperationSHA256(mailwrite.SendOperationKey), Reliability: writeReliability(),
	}
	MailReply = connector.CallOperation[mailwrite.ReplyRequest, mailwrite.Result]{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: mailwrite.ReplyOperationKey,
		ContractSHA256: mailwrite.OperationSHA256(mailwrite.ReplyOperationKey), Reliability: writeReliability(),
	}
	MailAttachmentDownload = connector.CallOperation[mailattachment.DownloadRequest, mailattachment.DownloadResult]{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: mailattachment.DownloadOperationKey,
		ContractSHA256: mailattachment.OperationSHA256(mailattachment.DownloadOperationKey), Reliability: readReliability(),
	}
	TestConnection = connector.CallOperation[struct{}, Response]{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection",
		ContractSHA256: "9edf1791ea327a92b41ac8e6eb4e1d55ee1db59b12a2110e0428b73060bc4810", Reliability: readReliability(),
	}
)

type provider struct {
	connector.Adapter
	transport connector.Transport
	now       func() time.Time
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Feishu Mail transport is required")
	}
	p := &provider{transport: transport, now: time.Now}
	list, err := connector.BindCall(MailList, p.mailList)
	if err != nil {
		return nil, err
	}
	search, err := connector.BindCall(MailSearch, p.mailSearch)
	if err != nil {
		return nil, err
	}
	read, err := connector.BindCall(MailRead, p.mailRead)
	if err != nil {
		return nil, err
	}
	send, err := connector.BindCall(MailSend, p.mailSend)
	if err != nil {
		return nil, err
	}
	reply, err := connector.BindCall(MailReply, p.mailReply)
	if err != nil {
		return nil, err
	}
	attachment, err := connector.BindCall(MailAttachmentDownload, p.mailAttachmentDownload)
	if err != nil {
		return nil, err
	}
	test, err := connector.BindCall(TestConnection, p.callTestConnection)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), list, search, read, send, reply, attachment, test)
	if err != nil {
		return nil, err
	}
	p.Adapter = bound
	return p, nil
}

func schema() connector.ProviderSchema {
	minimum, maximum := float64(1), float64(120)
	reconcileMinimum, reconcileMaximum := float64(300), float64(86400)
	lookupMinimum, lookupMaximum := float64(1), float64(100)
	secret := func(key, name string, kind connector.SecretCredentialKind, rotation connector.SecretRotationPolicy) connector.SecretField {
		return connector.SecretField{Key: key, Name: name, CredentialKind: kind, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: rotation, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}
	}
	return connector.ProviderSchema{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0",
		ConfigFields: []connector.ConfigField{
			{Key: "mailbox_id", Name: "Feishu mailbox address or me", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"me"`)},
			{Key: "base_url", Name: "Feishu API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://open.feishu.cn"`)},
			{Key: "oauth_base_url", Name: "Feishu OAuth base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://accounts.feishu.cn"`)},
			{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}},
			{Key: "mail_ingest_enabled", Name: "Enable mailbox synchronization", Type: connector.ConfigFieldBoolean, Default: json.RawMessage(`true`)},
			{Key: "mail_subscription_enabled", Name: "Enable new-mail event subscription", Type: connector.ConfigFieldBoolean, Default: json.RawMessage(`true`)},
			{Key: "mail_reconcile_seconds", Name: "Inbox and sent reconciliation interval", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`900`), Validation: connector.ConfigValidation{Min: &reconcileMinimum, Max: &reconcileMaximum}},
			{Key: "mail_write_reconcile_pages", Name: "Maximum sent-mail pages per write reconciliation", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`5`), Validation: connector.ConfigValidation{Min: &lookupMinimum, Max: &lookupMaximum}},
		},
		SecretFields: []connector.SecretField{
			secret("access_token", "User access token", connector.SecretCredentialBearerToken, connector.SecretRotationOAuthRefresh),
			secret("refresh_token", "User refresh token", connector.SecretCredentialRefreshToken, connector.SecretRotationOAuthRefresh),
			secret("client_id", "OAuth client ID", connector.SecretCredentialIdentifier, connector.SecretRotationManual),
			secret("client_secret", "OAuth client secret", connector.SecretCredentialOAuthClientSecret, connector.SecretRotationManual),
			secret("encrypt_key", "Feishu event encrypt key", connector.SecretCredentialGeneric, connector.SecretRotationManual),
			secret("verification_token", "Feishu event verification token", connector.SecretCredentialSigningSecret, connector.SecretRotationManual),
		},
	}
}

func readReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

func writeReliability() connector.ReliabilityContract {
	// Both send operations use a deterministic RFC Message-ID. Feishu rejects a
	// repeated Message-ID with 1236005, so the identity is natural rather than a
	// provider key with a fabricated retention period. dedupe_key is sent too,
	// but Feishu does not publish how long that field is retained.
	return connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationProviderLookup, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	if connection.ConnectorKey != ConnectorKey || connection.ProviderKey != ProviderKey {
		return permanent("connection_mismatch", "connection does not match Feishu Mail")
	}
	for _, endpoint := range []struct{ value, host string }{{baseURL(connection), "open.feishu.cn"}, {oauthBaseURL(connection), "accounts.feishu.cn"}} {
		parsed, err := url.Parse(endpoint.value)
		if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" || parsed.RawQuery != "" || (!(parsed.Scheme == "http" && isLoopback(parsed.Hostname())) && (parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), endpoint.host))) {
			return permanent("endpoint_invalid", "official Feishu endpoints or loopback HTTP are required")
		}
	}
	if mailboxID(connection) == "" {
		return permanent("mailbox_required", "mailbox_id is required")
	}
	if seconds := configInt(connection.Config, "mail_reconcile_seconds", 900); seconds < 300 || seconds > 86400 {
		return permanent("reconcile_interval_invalid", "mail reconciliation interval must be between 300 and 86400 seconds")
	}
	if pages := configInt(connection.Config, "mail_write_reconcile_pages", 5); pages < 1 || pages > 100 {
		return permanent("write_reconcile_pages_invalid", "mail write reconciliation pages must be between 1 and 100")
	}
	return nil
}

func (p *provider) callTestConnection(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	session := p.newAPISession(request.Connection, request.Secrets)
	result, err := session.call(ctx, http.MethodGet, messagePath(request.Connection), url.Values{"page_size": {"1"}, "folder_id": {"INBOX"}}, nil, false)
	return connector.TypedResult[Response]{Output: result.Output, ResponseRef: result.ResponseRef, SecretUpdates: cloneStrings(session.updates)}, err
}

func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	session := p.newAPISession(request.Connection, request.Secrets)
	result, err := session.call(ctx, http.MethodGet, messagePath(request.Connection), url.Values{"page_size": {"1"}, "folder_id": {"INBOX"}}, nil, false)
	if err != nil {
		return connector.TestConnectionResult{SecretUpdates: cloneStrings(session.updates)}, err
	}
	detail := map[string]any{"mailbox_id": mailboxID(request.Connection), "provider": ProviderKey, "response_ref": result.ResponseRef}
	if strings.TrimSpace(request.Secrets["refresh_token"]) != "" {
		userResult, userErr := session.call(ctx, http.MethodGet, "/open-apis/authen/v1/user_info", nil, nil, false)
		if userErr != nil {
			return connector.TestConnectionResult{SecretUpdates: cloneStrings(session.updates)}, userErr
		}
		user := nested(userResult.Output, "data")
		openID, unionID := mapString(user, "open_id"), mapString(user, "union_id")
		subject := unionID
		if subject == "" {
			subject = openID
		}
		if subject == "" || openID == "" {
			return connector.TestConnectionResult{SecretUpdates: cloneStrings(session.updates)}, permanent("provider_account_invalid", "Feishu user profile lacks a stable identity")
		}
		routes := []map[string]string{{"kind": "open_id", "value": openID}}
		if unionID != "" && unionID != openID {
			routes = append(routes, map[string]string{"kind": "union_id", "value": unionID})
		}
		if userID := mapString(user, "user_id"); userID != "" {
			routes = append(routes, map[string]string{"kind": "user_id", "value": userID})
		}
		if email := strings.ToLower(mapString(user, "email")); email != "" {
			routes = append(routes, map[string]string{"kind": "email", "value": email})
		}
		detail["provider_account"] = map[string]any{"subject": subject, "routes": routes}
	}
	details, err := json.Marshal(detail)
	if err != nil {
		return connector.TestConnectionResult{SecretUpdates: cloneStrings(session.updates)}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details, SecretUpdates: cloneStrings(session.updates)}, nil
}

type apiSession struct {
	provider   *provider
	connection connector.Connection
	secrets    map[string]string
	token      string
	updates    map[string]string
}

func (p *provider) newAPISession(connection connector.Connection, secrets map[string]string) *apiSession {
	return &apiSession{provider: p, connection: connection, secrets: cloneStrings(secrets), updates: map[string]string{}}
}

func (s *apiSession) accessToken(ctx context.Context) (string, error) {
	if s.token != "" {
		return s.token, nil
	}
	token := strings.TrimSpace(s.secrets["access_token"])
	if token == "" {
		return "", permanent("credentials_required", "a personal Feishu user access token is required")
	}
	s.token = token
	return token, nil
}

func (s *apiSession) call(ctx context.Context, method, path string, query url.Values, body any, write bool) (connector.TypedResult[Response], error) {
	token, err := s.accessToken(ctx)
	if err != nil {
		return connector.TypedResult[Response]{SecretUpdates: cloneStrings(s.updates)}, err
	}
	output, ref, err := s.provider.execute(ctx, s.connection, token, method, path, query, body, write)
	if err == nil || !accessTokenRejected(err) || strings.TrimSpace(s.secrets["refresh_token"]) == "" {
		return connector.TypedResult[Response]{Output: output, ResponseRef: ref, SecretUpdates: cloneStrings(s.updates)}, err
	}
	updates, refreshErr := s.provider.refreshUserToken(ctx, s.connection, s.secrets)
	if refreshErr != nil {
		return connector.TypedResult[Response]{ResponseRef: "oauth:refresh_failed", SecretUpdates: cloneStrings(s.updates)}, refreshErr
	}
	for key, value := range updates {
		s.secrets[key], s.updates[key] = value, value
	}
	s.token = updates["access_token"]
	output, ref, err = s.provider.execute(ctx, s.connection, s.token, method, path, query, body, write)
	return connector.TypedResult[Response]{Output: output, ResponseRef: ref, SecretUpdates: cloneStrings(s.updates)}, err
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, token, method, path string, query url.Values, body any, write bool) (Response, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", err
	}
	endpoint, err := url.Parse(baseURL(connection) + path)
	if err != nil {
		return nil, "", permanent("request_invalid", "request URL is invalid")
	}
	endpoint.RawQuery = query.Encode()
	var raw []byte
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return nil, "", permanent("request_invalid", "request body is invalid")
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json; charset=utf-8"}
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint.String(), Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, Body: raw, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		if write {
			return nil, "", connector.UncertainError("feishu_mail.network_error", transportErr)
		}
		return nil, "", connector.RetryableError("feishu_mail.network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	output := Response{}
	validJSON := len(response.Body) == 0 || json.Unmarshal(response.Body, &output) == nil
	if internalfeishu.IsRateLimitedResponse(response.StatusCode, response.Body) {
		return output, ref, internalfeishu.RateLimitError("feishu_mail")
	}
	if validJSON {
		if code := intValue(output, "code"); code != 0 {
			if code == 99991679 {
				return output, ref, permanent("oauth.user_authorization_required", "Feishu requires the user to authorize the requested mailbox scope")
			}
			machine := "feishu_mail.provider_code_" + strconv.Itoa(code)
			cause := fmt.Errorf("provider returned code %d", code)
			if write && (response.StatusCode == http.StatusRequestTimeout || response.StatusCode >= 500) {
				return output, ref, connector.UncertainError(machine, cause)
			}
			if response.StatusCode == http.StatusRequestTimeout || response.StatusCode >= 500 {
				return output, ref, connector.RetryableError(machine, cause)
			}
			return output, ref, connector.PermanentError(machine, cause)
		}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 || !validJSON {
		code := "feishu_mail.http_" + strconv.Itoa(response.StatusCode)
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		if !validJSON {
			code, cause = "feishu_mail.response_invalid", errors.New("provider response is invalid JSON")
		}
		if write && (response.StatusCode == http.StatusRequestTimeout || response.StatusCode >= 500 || !validJSON) {
			return output, ref, connector.UncertainError(code, cause)
		}
		if response.StatusCode == http.StatusRequestTimeout || response.StatusCode >= 500 {
			return output, ref, connector.RetryableError(code, cause)
		}
		return output, ref, connector.PermanentError(code, cause)
	}
	return output, ref, nil
}

func messagePath(connection connector.Connection) string {
	return "/open-apis/mail/v1/user_mailboxes/" + url.PathEscape(mailboxID(connection)) + "/messages"
}

func mailboxID(connection connector.Connection) string { return config(connection, "mailbox_id", "me") }
func baseURL(connection connector.Connection) string {
	return strings.TrimRight(config(connection, "base_url", defaultBaseURL), "/")
}
func oauthBaseURL(connection connector.Connection) string {
	return strings.TrimRight(config(connection, "oauth_base_url", defaultOAuthBaseURL), "/")
}
func config(connection connector.Connection, key, fallback string) string {
	if value := strings.TrimSpace(fmt.Sprint(connection.Config[key])); value != "" && value != "<nil>" {
		return value
	}
	return fallback
}
func configBool(values map[string]any, key string, fallback bool) bool {
	value, ok := values[key]
	if !ok {
		return fallback
	}
	result, ok := value.(bool)
	return ok && result
}
func configInt(values map[string]any, key string, fallback int) int {
	value, ok := values[key]
	if !ok {
		return fallback
	}
	switch current := value.(type) {
	case float64:
		return int(current)
	case int:
		return current
	case json.Number:
		parsed, _ := strconv.Atoi(current.String())
		return parsed
	}
	return 0
}
func nested(input map[string]any, keys ...string) map[string]any {
	current := input
	for _, key := range keys {
		next, _ := current[key].(map[string]any)
		if next == nil {
			return map[string]any{}
		}
		current = next
	}
	return current
}
func mapString(input map[string]any, key string) string {
	value, _ := input[key].(string)
	return strings.TrimSpace(value)
}
func intValue(values map[string]any, key string) int {
	return configInt(values, key, 0)
}
func cloneStrings(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
func permanent(code, message string) error {
	return connector.PermanentError("feishu_mail."+code, errors.New(message))
}
func accessTokenRejected(err error) bool {
	code, ok := connector.ProviderErrorCodeOf(err)
	if !ok {
		return false
	}
	if code == "feishu_mail.http_401" {
		return true
	}
	for value := 99991661; value <= 99991668; value++ {
		if code == "feishu_mail.provider_code_"+strconv.Itoa(value) {
			return true
		}
	}
	return code == "feishu_mail.provider_code_99991677"
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
