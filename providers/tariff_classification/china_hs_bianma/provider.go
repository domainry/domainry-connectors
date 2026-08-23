// Package chinahsbianma implements the optional China HS Bianma auxiliary Provider.
package chinahsbianma

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	ConnectorKey    = "tariff_classification"
	ProviderKey     = "china_hs_bianma"
	officialBaseURL = "https://hs-bianma.com"
)

var (
	candidatePattern = regexp.MustCompile(`(?is)<div\s+class=["']list["'][^>]*>\s*<div[^>]*>\s*([0-9]{10})\s*</div>\s*<div[^>]*>.*?<b[^>]*>(.*?)</b>`)
	tagPattern       = regexp.MustCompile(`(?s)<[^>]+>`)
)

var SearchCandidates = connector.CallOperation[SearchCandidatesInput, SearchCandidatesOutput]{
	ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "search_china_hs_candidates",
	ContractSHA256: "84e27b750eb9744679510ad3302b3b906705a7f186fbcb6bd127e0191b7c3bd7",
	Reliability: connector.ReliabilityContract{
		Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural},
		Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone},
	},
}

type SearchCandidatesInput struct {
	Query string `json:"query"`
}

type Candidate struct {
	Code         string `json:"code"`
	Description  string `json:"description"`
	Jurisdiction string `json:"jurisdiction"`
	Digits       int    `json:"digits"`
}

type SearchCandidatesOutput struct {
	Result SearchResult `json:"result"`
}

type SearchResult struct {
	RetrievalStatus           string      `json:"retrieval_status"`
	Provider                  string      `json:"provider"`
	SourceType                string      `json:"source_type"`
	SourceTrust               string      `json:"source_trust"`
	SourceJurisdiction        string      `json:"source_jurisdiction"`
	CodeSystem                string      `json:"code_system"`
	Query                     string      `json:"query"`
	Candidates                []Candidate `json:"candidates"`
	RequestSHA256             string      `json:"request_sha256"`
	RequestBytes              int         `json:"request_bytes"`
	RawResponseSHA256         string      `json:"raw_response_sha256"`
	RawResponseBytes          int         `json:"raw_response_bytes"`
	ResponseStatus            int         `json:"response_status"`
	ResponseRef               string      `json:"response_ref"`
	ManualFallbackRequired    bool        `json:"manual_fallback_required"`
	IsBrazilNCM               bool        `json:"is_brazil_ncm"`
	FinalClassification       bool        `json:"final_classification"`
	HumanConfirmationRequired bool        `json:"human_confirmation_required"`
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

// New constructs the Provider without performing network access.
func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("China HS Bianma transport is required")
	}
	implementation := &provider{transport: transport}
	operation, err := connector.BindCall(SearchCandidates, implementation.searchCandidates)
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
	minimumTimeout, maximumTimeout := float64(1), float64(60)
	minimumBytes, maximumBytes := float64(65536), float64(4194304)
	return connector.ProviderSchema{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0",
		StartupActivation: connector.StartupActivationDefaultSafe,
		ConfigFields: []connector.ConfigField{
			{Key: "base_url", Name: "Source origin", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://hs-bianma.com"`), Validation: connector.ConfigValidation{MaxLength: 2048, Pattern: `^https?://`}},
			{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`20`), Validation: connector.ConfigValidation{Min: &minimumTimeout, Max: &maximumTimeout}},
			{Key: "max_response_bytes", Name: "Maximum response bytes", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`1048576`), Validation: connector.ConfigValidation{Min: &minimumBytes, Max: &maximumBytes}},
		},
	}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(baseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return connector.PermanentError("tariff_classification.china_hs_bianma.endpoint_invalid", errors.New("source endpoint is invalid"))
	}
	if parsed.Scheme == "https" && !strings.EqualFold(parsed.Hostname(), "hs-bianma.com") {
		return connector.PermanentError("tariff_classification.china_hs_bianma.source_endpoint_required", errors.New("official source endpoint is required"))
	}
	return nil
}

func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	raw, status, ref, err := p.fetch(ctx, request.Connection, baseURL(request.Connection)+"/")
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	if !htmlLooksRecognized(raw) {
		return connector.TestConnectionResult{}, connector.PermanentError("tariff_classification.china_hs_bianma.site_changed", errors.New("source HTML is not recognized"))
	}
	details, err := json.Marshal(map[string]any{
		"connected": true, "candidate_success": false, "source_type": "untrusted_optional_html", "source_jurisdiction": "CN",
		"response_status": status, "response_ref": ref, "raw_response_sha256": sha256Value(raw), "raw_response_bytes": len(raw),
	})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) searchCandidates(ctx context.Context, request connector.TypedRequest[SearchCandidatesInput]) (connector.TypedResult[SearchCandidatesOutput], error) {
	query := strings.TrimSpace(request.Input.Query)
	if query == "" || len([]rune(query)) > 500 {
		return connector.TypedResult[SearchCandidatesOutput]{}, connector.PermanentError("tariff_classification.china_hs_bianma.query_invalid", errors.New("query must contain between 1 and 500 characters"))
	}
	endpoint := baseURL(request.Connection) + "/search/index?ser=" + url.QueryEscape(query)
	raw, status, ref, err := p.fetch(ctx, request.Connection, endpoint)
	if err != nil {
		return connector.TypedResult[SearchCandidatesOutput]{ResponseRef: ref}, err
	}
	if !htmlLooksRecognized(raw) {
		return connector.TypedResult[SearchCandidatesOutput]{ResponseRef: ref}, connector.PermanentError("tariff_classification.china_hs_bianma.site_changed", errors.New("source HTML is not recognized"))
	}
	candidates := parseCandidates(raw)
	retrievalStatus := "live"
	manualFallback := false
	if len(candidates) == 0 {
		retrievalStatus, manualFallback = "no_result", true
	}
	requestEvidence := []byte("query=" + query)
	result := SearchResult{
		RetrievalStatus: retrievalStatus, Provider: "china_hs_bianma", SourceType: "untrusted_optional_html",
		SourceTrust: "untrusted_optional_auxiliary", SourceJurisdiction: "CN", CodeSystem: "china_customs_10",
		Query: query, Candidates: candidates, RequestSHA256: sha256Value(requestEvidence), RequestBytes: len(requestEvidence),
		RawResponseSHA256: sha256Value(raw), RawResponseBytes: len(raw), ResponseStatus: status, ResponseRef: ref,
		ManualFallbackRequired: manualFallback, IsBrazilNCM: false, FinalClassification: false, HumanConfirmationRequired: true,
	}
	return connector.TypedResult[SearchCandidatesOutput]{Output: SearchCandidatesOutput{Result: result}, ResponseRef: ref}, nil
}

func (p *provider) fetch(ctx context.Context, connection connector.Connection, endpoint string) ([]byte, int, string, error) {
	limit := int64(configInt(connection.Config, 1<<20, "max_response_bytes"))
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{
		Method: http.MethodGet, URL: endpoint, Headers: map[string][]string{"Accept": {"text/html,application/xhtml+xml"}}, MaxResponseBytes: limit,
	})
	if err != nil {
		return nil, 0, "", connector.RetryableError("tariff_classification.china_hs_bianma.network_error", err)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	if int64(len(response.Body)) > limit {
		return nil, response.StatusCode, ref, connector.PermanentError("tariff_classification.china_hs_bianma.response_too_large", errors.New("response exceeds configured size limit"))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code := "tariff_classification.china_hs_bianma.http_" + strconv.Itoa(response.StatusCode)
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests {
			return nil, response.StatusCode, ref, connector.RetryableError("tariff_classification.china_hs_bianma.rate_limited", cause)
		}
		if response.StatusCode >= 500 {
			return nil, response.StatusCode, ref, connector.RetryableError(code, cause)
		}
		return nil, response.StatusCode, ref, connector.PermanentError(code, cause)
	}
	return response.Body, response.StatusCode, ref, nil
}

func htmlLooksRecognized(raw []byte) bool {
	value := strings.ToLower(string(raw))
	return strings.Contains(value, `<form action="/search/index"`) || strings.Contains(value, `<form action='/search/index'`) || strings.Contains(value, "hs编码查询结果")
}

func parseCandidates(raw []byte) []Candidate {
	matches := candidatePattern.FindAllSubmatch(raw, -1)
	byCode := make(map[string]string, len(matches))
	for _, match := range matches {
		code := string(match[1])
		description := strings.Join(strings.Fields(html.UnescapeString(tagPattern.ReplaceAllString(string(match[2]), " "))), " ")
		if description != "" {
			byCode[code] = description
		}
	}
	codes := make([]string, 0, len(byCode))
	for code := range byCode {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	result := make([]Candidate, 0, len(codes))
	for _, code := range codes {
		result = append(result, Candidate{Code: code, Description: byCode[code], Jurisdiction: "CN", Digits: 10})
	}
	return result
}

func baseURL(connection connector.Connection) string {
	if value := strings.TrimRight(configString(connection.Config, "base_url"), "/"); value != "" {
		return value
	}
	return officialBaseURL
}

func sha256Value(raw []byte) string {
	digest := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func configString(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return strings.TrimSpace(value)
}

func configInt(values map[string]any, fallback int, key string) int {
	switch value := values[key].(type) {
	case int:
		return value
	case float64:
		return int(value)
	case json.Number:
		parsed, err := strconv.Atoi(value.String())
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func isLoopback(host string) bool {
	host = strings.TrimSpace(strings.ToLower(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
