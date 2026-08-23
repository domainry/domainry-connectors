// Package oidc implements the official OpenID Connect Provider.
package oidc

import (
	"context"
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
	ConnectorKey        = "sso"
	ProviderKey         = "oidc"
	responseLimit int64 = 1 << 20
)

type Response map[string]any
type ListUsersInput struct{}

var (
	ListUsers      = operation[ListUsersInput]("list_users", "66b1c57c2f4d62ed19a5f4f99f6b8f5fcec849358864e77f3e4d42b909b1246f")
	TestConnection = operation[struct{}]("test_connection", "8b4f574c082bb27e303f29179aefd5329fd2b63d33163d5f41f2f0397057c6b4")
)

func operation[I any](key, hash string) connector.CallOperation[I, Response] {
	return connector.CallOperation[I, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: key, ContractSHA256: hash, Reliability: connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("OIDC transport is required")
	}
	p := &provider{transport: transport}
	users, err := connector.BindCall(ListUsers, p.listUsers)
	if err != nil {
		return nil, err
	}
	test, err := connector.BindCall(TestConnection, p.test)
	if err != nil {
		return nil, err
	}
	adapter, err := connector.NewProvider(schema(), users, test)
	if err != nil {
		return nil, err
	}
	p.Adapter = adapter
	return p, nil
}
func schema() connector.ProviderSchema {
	min, max := float64(1), float64(30)
	field := func(key, en, zh string, required bool) connector.ConfigField {
		return connector.ConfigField{Key: key, Name: en, Type: connector.ConfigFieldText, Required: required, I18n: i18n(en, zh)}
	}
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{field("issuer", "Issuer URL", "签发方地址", true), field("client_id", "Client ID", "客户端 ID", true), field("redirect_url", "Redirect URL", "回调地址", true), field("scope", "Scopes", "授权范围", false), field("discovery_url", "Discovery URL", "发现文档地址", false), field("list_users_url", "List Users URL", "用户列表地址", false), {Key: "timeout_seconds", Name: "Timeout Seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`10`), Validation: connector.ConfigValidation{Min: &min, Max: &max}, I18n: i18n("Timeout Seconds", "超时秒数")}}, SecretFields: []connector.SecretField{secret("client_secret", "Client Secret", "客户端密钥", connector.SecretCredentialOAuthClientSecret, true), secret("access_token", "Directory Access Token", "目录访问令牌", connector.SecretCredentialBearerToken, false)}}
}
func i18n(en, zh string) map[string]connector.FieldLocalization {
	return map[string]connector.FieldLocalization{"en-US": {Name: en}, "zh-CN": {Name: zh}}
}
func secret(key, en, zh string, kind connector.SecretCredentialKind, required bool) connector.SecretField {
	return connector.SecretField{Key: key, Name: en, I18n: i18n(en, zh), Required: required, CredentialKind: kind, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}
}
func (p *provider) ValidateConfig(c connector.Connection) error {
	if _, err := secureURL(config(c, "issuer")); err != nil {
		return permanent("issuer_invalid", "issuer must be HTTPS or loopback HTTP")
	}
	if config(c, "client_id") == "" {
		return permanent("client_id_required", "client_id is required")
	}
	if _, err := secureURL(config(c, "redirect_url")); err != nil {
		return permanent("redirect_url_invalid", "redirect_url must be HTTPS or loopback HTTP")
	}
	return nil
}
func (p *provider) test(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	endpoint, err := discoveryURL(r.Connection)
	if err != nil {
		return empty(), err
	}
	result, err := p.get(ctx, r.Connection, nil, endpoint)
	if err != nil {
		return result, err
	}
	for _, key := range []string{"authorization_endpoint", "token_endpoint", "jwks_uri"} {
		if clean(result.Output[key]) == "" {
			return result, permanent("discovery_invalid", "discovery document is incomplete")
		}
	}
	return connector.TypedResult[Response]{Output: Response{"connected": true, "issuer": result.Output["issuer"]}, ResponseRef: result.ResponseRef}, nil
}
func (p *provider) TestConnection(ctx context.Context, r connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.test(ctx, connector.TypedRequest[struct{}]{Connection: r.Connection, Secrets: r.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	raw, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: raw}, nil
}
func (p *provider) listUsers(ctx context.Context, r connector.TypedRequest[ListUsersInput]) (connector.TypedResult[Response], error) {
	endpoint, err := usersURL(r.Connection)
	if err != nil {
		return empty(), err
	}
	token := strings.TrimSpace(r.Secrets["access_token"])
	if token == "" {
		return empty(), permanent("access_token_required", "resolved directory access token is required")
	}
	result, err := p.get(ctx, r.Connection, map[string][]string{"Authorization": {"Bearer " + token}}, endpoint)
	if err != nil {
		return result, err
	}
	users, ok := result.Output["users"].([]any)
	if !ok {
		return result, permanent("users_response_invalid", "users response is invalid")
	}
	return connector.TypedResult[Response]{Output: Response{"users": users}, ResponseRef: result.ResponseRef}, nil
}
func (p *provider) get(ctx context.Context, c connector.Connection, secretHeaders map[string][]string, endpoint string) (connector.TypedResult[Response], error) {
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodGet, URL: endpoint, Headers: map[string][]string{"Accept": {"application/json"}, "User-Agent": {"domainry-sso-oidc/1.0"}}, SecretHeaders: secretHeaders, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		return empty(), connector.RetryableError("oidc.network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return connector.TypedResult[Response]{ResponseRef: ref}, connector.RetryableError("oidc.http_status_"+strconv.Itoa(response.StatusCode), fmt.Errorf("OIDC endpoint returned HTTP %d", response.StatusCode))
	}
	payload := Response{}
	if len(response.Body) == 0 || json.Unmarshal(response.Body, &payload) != nil {
		return connector.TypedResult[Response]{ResponseRef: ref}, connector.RetryableError("oidc.response_invalid", errors.New("OIDC response is invalid JSON"))
	}
	return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, nil
}
func discoveryURL(c connector.Connection) (string, error) {
	if endpoint := config(c, "discovery_url"); endpoint != "" {
		return resolve(c, endpoint)
	}
	issuer := strings.TrimRight(config(c, "issuer"), "/")
	if issuer == "" {
		return "", permanent("issuer_required", "issuer is required")
	}
	return resolve(c, issuer+"/.well-known/openid-configuration")
}
func usersURL(c connector.Connection) (string, error) {
	endpoint := config(c, "list_users_url")
	if endpoint == "" {
		return "", permanent("list_users_endpoint_required", "list_users_url is required")
	}
	return resolve(c, endpoint)
}
func resolve(c connector.Connection, endpoint string) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err == nil && parsed.IsAbs() {
		return checked(parsed)
	}
	base, err := url.Parse(config(c, "issuer"))
	if err != nil || base.Host == "" {
		return "", permanent("endpoint_invalid", "endpoint is invalid")
	}
	relative, err := url.Parse(endpoint)
	if err != nil {
		return "", permanent("endpoint_invalid", "endpoint is invalid")
	}
	return checked(base.ResolveReference(relative))
}
func checked(parsed *url.URL) (string, error) {
	if parsed.User != nil {
		return "", permanent("endpoint_invalid", "endpoint is invalid")
	}
	if _, err := secureURL(parsed.String()); err != nil {
		return "", permanent("endpoint_invalid", "endpoint must be HTTPS or loopback HTTP")
	}
	return parsed.String(), nil
}
func secureURL(value string) (*url.URL, error) {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && loopback(parsed.Hostname()))) {
		return nil, errors.New("invalid URL")
	}
	return parsed, nil
}
func config(c connector.Connection, key string) string { return clean(c.Config[key]) }
func clean(v any) string {
	result := strings.TrimSpace(fmt.Sprint(v))
	if result == "<nil>" {
		return ""
	}
	return result
}
func loopback(host string) bool {
	return host == "localhost" || strings.HasPrefix(host, "127.") || host == "::1"
}
func empty() connector.TypedResult[Response] { return connector.TypedResult[Response]{} }
func permanent(suffix, message string) error {
	return connector.PermanentError("oidc."+suffix, errors.New(message))
}
