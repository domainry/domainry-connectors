// Package llmproxy adapts the llm-proxy public web endpoints. It owns neither
// network clients nor credentials, and does not import another Provider.
package llmproxy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/web"
)

const (
	ConnectorKey     = "web"
	ProviderKey      = "llm_proxy"
	searchPath       = "/tool/web_search"
	fetchPath        = "/tool/web_fetch_jina"
	maxRequestBytes  = 16 << 10
	maxResponseBytes = 4 << 20
)

// Natural idempotency describes the read effect, not upstream billing or a
// deduplication guarantee. This Provider never retries or reports retryable
// errors: an explicit new request can incur another service charge.
func readReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{
		Effect:         connector.EffectRead,
		Idempotency:    connector.IdempotencyContract{Strategy: connector.IdempotencyNatural},
		Reconciliation: connector.ReconciliationNone,
		Compensation:   connector.CompensationContract{Mode: connector.CompensationNone},
	}
}

var Search = connector.CallOperation[web.SearchRequest, web.SearchResult]{
	ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "web_search",
	ContractSHA256: web.OperationSHA256("web_search"), Reliability: readReliability(),
}
var Fetch = connector.CallOperation[web.FetchRequest, web.Page]{
	ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "web_fetch",
	ContractSHA256: web.OperationSHA256("web_fetch"), Reliability: readReliability(),
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

// New performs no I/O. There is no free service probe in this protocol, so this
// Provider deliberately does not implement ConnectionTester.
func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("web llm-proxy transport is required")
	}
	p := &provider{transport: transport}
	search, err := connector.BindCall(Search, p.search)
	if err != nil {
		return nil, err
	}
	fetch, err := connector.BindCall(Fetch, p.fetch)
	if err != nil {
		return nil, err
	}
	p.Adapter, err = connector.NewProvider(schema(), search, fetch)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	_, err := settings(connection)
	return err
}

// Empty OAuth scope alternatives do not waive owner, connection or tool
// authorization. The service uses a host-injected Passport Bearer token.
func (p *provider) OAuthOperationScopes(key string) ([][]string, bool) {
	switch key {
	case Search.Key, Fetch.Key:
		return [][]string{{}}, true
	default:
		return nil, false
	}
}

func (p *provider) post(ctx context.Context, config configuration, path string, secrets map[string]string, input any) ([]byte, error) {
	token := secrets["api_token"]
	if token == "" || len(token) > 16<<10 || !utf8.ValidString(token) || strings.ContainsFunc(token, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
		return nil, permanent("credential_invalid", "a resolved raw Passport token is required")
	}
	body, err := json.Marshal(input)
	if err != nil || len(body) > maxRequestBytes {
		return nil, permanent("request_invalid", "web request exceeds service limits")
	}
	requestCtx, cancel := context.WithTimeout(ctx, config.timeout)
	defer cancel()
	if requestCtx.Err() != nil {
		return nil, permanent("request_cancelled", "web request cancelled before dispatch")
	}
	response, err := p.transport.RoundTripHTTP(requestCtx, connector.HTTPRequest{
		Method: http.MethodPost, URL: config.origin + path, Body: body, MaxResponseBytes: maxResponseBytes,
		Headers:       map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/json"}},
		SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}},
	})
	if err != nil {
		// Do not retain a transport cause: it may contain private credentials or
		// remote content. Cancellation does not prove remote work or billing stopped.
		return nil, uncertain("network_error", "web service response was not received")
	}
	if response.StatusCode != http.StatusOK {
		return nil, statusError(response.StatusCode)
	}
	if len(response.Body) > maxResponseBytes || !utf8.Valid(response.Body) {
		return nil, invalidResponse()
	}
	return response.Body, nil
}

func statusError(status int) error {
	switch status {
	case http.StatusUnauthorized:
		return permanent("unauthorized", "web service authentication failed")
	case http.StatusForbidden:
		return permanent("forbidden", "web service access was denied")
	case http.StatusBadRequest, http.StatusRequestEntityTooLarge, http.StatusUnprocessableEntity:
		return permanent("request_rejected", "web service rejected the request")
	case http.StatusNotFound, http.StatusMethodNotAllowed:
		return permanent("endpoint_unavailable", "web service endpoint is unavailable")
	case http.StatusTooManyRequests:
		return permanent("rate_limited", "web service rate limited this request")
	default:
		return uncertain("upstream_error", "web service did not return a successful response")
	}
}

func permanent(code, message string) error {
	return connector.PermanentError("llm_proxy_web."+code, errors.New(message))
}
func uncertain(code, message string) error {
	return connector.UncertainError("llm_proxy_web."+code, errors.New(message))
}
func invalidResponse() error {
	return uncertain("response_invalid", "web service response contract is invalid")
}
func responseRef() string { return "http:" + strconv.Itoa(http.StatusOK) }

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.OAuthOperationScopeProvider = (*provider)(nil)
