// Package comexstat implements the official Brazilian Comex Stat Provider.
package comexstat

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	ConnectorKey          = "trade_statistics"
	ProviderKey           = "comex_stat"
	officialBaseURL       = "https://api-comexstat.mdic.gov.br"
	responseLimit   int64 = 4 << 20
)

var (
	ncmPattern    = regexp.MustCompile(`^[0-9]{8}$`)
	periodPattern = regexp.MustCompile(`^[0-9]{4}-(0[1-9]|1[0-2])$`)
	codePattern   = regexp.MustCompile(`^[0-9]{2,7}$`)
)

var QueryImportStatistics = connector.CallOperation[QueryImportStatisticsInput, QueryImportStatisticsOutput]{
	ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "query_import_statistics",
	ContractSHA256: "9b4f3fc0f5f205ae880eb1fa360561c594617a3bb5c725edcf26b2a603c06501",
	Reliability: connector.ReliabilityContract{
		Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural},
		Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone},
	},
}

type QueryImportStatisticsInput struct {
	NCM        string `json:"ncm"`
	PeriodFrom string `json:"period_from"`
	PeriodTo   string `json:"period_to"`
	UFCode     string `json:"uf_code"`
	URFCode    string `json:"urf_code"`
	ViaCode    string `json:"via_code"`
}

type QueryImportStatisticsOutput struct {
	Result Evidence `json:"result"`
}

type Evidence struct {
	Provider               string         `json:"provider"`
	SourceType             string         `json:"source_type"`
	Request                map[string]any `json:"request"`
	RequestSHA256          string         `json:"request_sha256"`
	QueriedAt              string         `json:"queried_at"`
	RawResponseBase64      string         `json:"raw_response_base64"`
	RawResponseSHA256      string         `json:"raw_response_sha256"`
	RawResponseBytes       int            `json:"raw_response_bytes"`
	ResponseRef            string         `json:"response_ref"`
	ResponseStatus         int            `json:"response_status"`
	Publication            map[string]any `json:"publication"`
	PublicationResponseRef string         `json:"publication_response_ref"`
	ProviderPayload        map[string]any `json:"provider_payload"`
}

type provider struct {
	connector.Adapter
	transport connector.Transport
	now       func() time.Time
}

// New constructs the Provider without performing network access.
func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Comex Stat transport is required")
	}
	implementation := &provider{transport: transport, now: time.Now}
	operation, err := connector.BindCall(QueryImportStatistics, implementation.queryImportStatistics)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), operation)
	if err != nil {
		return nil, err
	}
	implementation.Adapter = bound
	return implementation, nil
}

func schema() connector.ProviderSchema {
	minimum, maximum := float64(1), float64(90)
	return connector.ProviderSchema{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0",
		StartupActivation: connector.StartupActivationDefaultSafe,
		ConfigFields: []connector.ConfigField{
			{Key: "base_url", Name: "Official API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://api-comexstat.mdic.gov.br"`), Validation: connector.ConfigValidation{MaxLength: 2048, Pattern: `^https?://`}},
			{Key: "language", Name: "Response language", Type: connector.ConfigFieldSelect, Required: true, Default: json.RawMessage(`"pt"`), Validation: connector.ConfigValidation{Options: []string{"pt", "en", "es"}}},
			{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}},
		},
	}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(baseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return connector.PermanentError("comex_stat.endpoint_invalid", errors.New("HTTPS or loopback HTTP endpoint is required"))
	}
	if parsed.Scheme == "https" && !strings.EqualFold(parsed.Hostname(), "api-comexstat.mdic.gov.br") {
		return connector.PermanentError("comex_stat.official_endpoint_required", errors.New("official Comex Stat endpoint is required"))
	}
	language := responseLanguage(connection)
	if language != "pt" && language != "en" && language != "es" {
		return connector.PermanentError("comex_stat.language_invalid", errors.New("language must be pt, en, or es"))
	}
	return nil
}

func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	publication, ref, err := p.publication(ctx, request.Connection)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, err := json.Marshal(map[string]any{"publication": publication, "response_ref": ref})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) queryImportStatistics(ctx context.Context, request connector.TypedRequest[QueryImportStatisticsInput]) (connector.TypedResult[QueryImportStatisticsOutput], error) {
	canonical, err := canonicalRequest(request.Input)
	if err != nil {
		return connector.TypedResult[QueryImportStatisticsOutput]{}, err
	}
	body, err := json.Marshal(canonical)
	if err != nil {
		return connector.TypedResult[QueryImportStatisticsOutput]{}, connector.PermanentError("comex_stat.request_invalid", err)
	}
	endpoint := baseURL(request.Connection) + "/general?language=" + url.QueryEscape(responseLanguage(request.Connection))
	queriedAt := p.now().UTC()
	response, ref, payload, err := p.requestJSON(ctx, connector.HTTPRequest{
		Method: http.MethodPost, URL: endpoint, Body: body, MaxResponseBytes: responseLimit,
		Headers: map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/json"}},
	}, "comex_stat.response_invalid")
	if err != nil {
		return connector.TypedResult[QueryImportStatisticsOutput]{ResponseRef: ref}, err
	}
	publication, publicationRef, err := p.publication(ctx, request.Connection)
	if err != nil {
		return connector.TypedResult[QueryImportStatisticsOutput]{ResponseRef: ref}, err
	}
	requestDigest, responseDigest := sha256.Sum256(body), sha256.Sum256(response.Body)
	evidence := Evidence{
		Provider: "comex_stat", SourceType: "official_api", Request: canonical,
		RequestSHA256: "sha256:" + hex.EncodeToString(requestDigest[:]), QueriedAt: queriedAt.Format(time.RFC3339Nano),
		RawResponseBase64: base64.StdEncoding.EncodeToString(response.Body), RawResponseSHA256: "sha256:" + hex.EncodeToString(responseDigest[:]),
		RawResponseBytes: len(response.Body), ResponseRef: ref, ResponseStatus: response.StatusCode,
		Publication: publication, PublicationResponseRef: publicationRef, ProviderPayload: payload,
	}
	return connector.TypedResult[QueryImportStatisticsOutput]{Output: QueryImportStatisticsOutput{Result: evidence}, ResponseRef: ref}, nil
}

func (p *provider) publication(ctx context.Context, connection connector.Connection) (map[string]any, string, error) {
	endpoint := baseURL(connection) + "/general/dates/updated?language=" + url.QueryEscape(responseLanguage(connection))
	_, ref, payload, err := p.requestJSON(ctx, connector.HTTPRequest{Method: http.MethodGet, URL: endpoint, MaxResponseBytes: responseLimit}, "comex_stat.publication_invalid")
	return payload, ref, err
}

func (p *provider) requestJSON(ctx context.Context, request connector.HTTPRequest, invalidCode string) (connector.HTTPResponse, string, map[string]any, error) {
	response, err := p.transport.RoundTripHTTP(ctx, request)
	if err != nil {
		return connector.HTTPResponse{}, "", nil, connector.RetryableError("comex_stat.network_error", err)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	if int64(len(response.Body)) > responseLimit {
		return response, ref, nil, connector.PermanentError(invalidCode, errors.New("response exceeds size limit"))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		code := providerErrorCode(response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			return response, ref, nil, connector.RetryableError(code, cause)
		}
		return response, ref, nil, connector.PermanentError(code, cause)
	}
	payload := map[string]any{}
	if err := json.Unmarshal(response.Body, &payload); err != nil {
		return response, ref, nil, connector.PermanentError(invalidCode, err)
	}
	return response, ref, payload, nil
}

func canonicalRequest(input QueryImportStatisticsInput) (map[string]any, error) {
	ncm, from, to := strings.TrimSpace(input.NCM), strings.TrimSpace(input.PeriodFrom), strings.TrimSpace(input.PeriodTo)
	uf, urf, via := strings.TrimSpace(input.UFCode), strings.TrimSpace(input.URFCode), strings.TrimSpace(input.ViaCode)
	if !ncmPattern.MatchString(ncm) {
		return nil, connector.PermanentError("comex_stat.ncm_invalid", errors.New("NCM must contain eight digits"))
	}
	if !periodPattern.MatchString(from) || !periodPattern.MatchString(to) || from > to {
		return nil, connector.PermanentError("comex_stat.period_invalid", errors.New("period is invalid"))
	}
	if !codePattern.MatchString(uf) || !codePattern.MatchString(urf) || !codePattern.MatchString(via) {
		return nil, connector.PermanentError("comex_stat.filter_invalid", errors.New("UF, URF, and via codes must be numeric"))
	}
	return map[string]any{
		"flow": "import", "monthDetail": true, "period": map[string]any{"from": from, "to": to},
		"filters": []any{
			map[string]any{"filter": "ncm", "values": []any{ncm}}, map[string]any{"filter": "state", "values": []any{uf}},
			map[string]any{"filter": "urf", "values": []any{urf}}, map[string]any{"filter": "via", "values": []any{via}},
		},
		"details": []any{"ncm", "state", "urf", "via"},
		"metrics": []any{"metricFOB", "metricKG", "metricFreight", "metricInsurance", "metricCIF"},
	}, nil
}

func providerErrorCode(status int) string {
	switch status {
	case http.StatusTooManyRequests:
		return "comex_stat.rate_limited"
	case http.StatusForbidden, http.StatusUnauthorized:
		return "comex_stat.access_denied"
	default:
		return "comex_stat.http_" + strconv.Itoa(status)
	}
}

func baseURL(connection connector.Connection) string {
	if value := strings.TrimRight(configString(connection.Config, "base_url"), "/"); value != "" {
		return value
	}
	return officialBaseURL
}

func responseLanguage(connection connector.Connection) string {
	if value := strings.ToLower(configString(connection.Config, "language")); value != "" {
		return value
	}
	return "pt"
}

func configString(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return strings.TrimSpace(value)
}

func isLoopback(host string) bool {
	host = strings.TrimSpace(strings.ToLower(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
