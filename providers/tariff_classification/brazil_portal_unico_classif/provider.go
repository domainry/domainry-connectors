// Package brazilportalunicoclassif implements the official Brazil Portal Unico Classif Provider.
package brazilportalunicoclassif

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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
	ProviderKey     = "brazil_portal_unico_classif"
	officialBaseURL = "https://portalunico.siscomex.gov.br"
)

var (
	hs6Pattern  = regexp.MustCompile(`^[0-9]{6}$`)
	datePattern = regexp.MustCompile(`(?i)^Vigente em ([0-9]{2})/([0-9]{2})/([0-9]{4})$`)
)

var MatchCandidates = connector.CallOperation[MatchCandidatesInput, MatchCandidatesOutput]{
	ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "match_brazil_ncm_candidates",
	ContractSHA256: "5eadec2c2f6e65ec2912008800b8ab71122b2b2390c2dbfc9ffd5b8597581850",
	Reliability: connector.ReliabilityContract{
		Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural},
		Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone},
	},
}

type MatchCandidatesInput struct {
	HS6               string         `json:"hs6"`
	ProductAttributes map[string]any `json:"product_attributes,omitempty"`
}

type MatchCandidatesOutput struct {
	Result map[string]any `json:"result"`
}

type catalog struct {
	LastUpdated   string         `json:"Data_Ultima_Atualizacao_NCM"`
	LegalAct      string         `json:"Ato"`
	Nomenclatures []nomenclature `json:"Nomenclaturas"`
}

type nomenclature struct {
	Code             string `json:"Codigo"`
	Description      string `json:"Descricao"`
	EffectiveFrom    string `json:"Data_Inicio"`
	EffectiveTo      string `json:"Data_Fim"`
	InitialActType   string `json:"Tipo_Ato_Ini"`
	InitialActNumber string `json:"Numero_Ato_Ini"`
	InitialActYear   string `json:"Ano_Ato_Ini"`
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

// New constructs the Provider without performing network access.
func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Brazil Portal Unico Classif transport is required")
	}
	implementation := &provider{transport: transport}
	operation, err := connector.BindCall(MatchCandidates, implementation.matchCandidates)
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
	minimumTimeout, maximumTimeout := float64(1), float64(90)
	minimumBytes, maximumBytes := float64(1048576), float64(16777216)
	return connector.ProviderSchema{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0",
		StartupActivation: connector.StartupActivationDefaultSafe,
		ConfigFields: []connector.ConfigField{
			{Key: "base_url", Name: "Official Classif origin", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://portalunico.siscomex.gov.br"`), Validation: connector.ConfigValidation{MaxLength: 2048, Pattern: `^https?://`}},
			{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimumTimeout, Max: &maximumTimeout}},
			{Key: "max_response_bytes", Name: "Maximum response bytes", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`8388608`), Validation: connector.ConfigValidation{Min: &minimumBytes, Max: &maximumBytes}},
		},
	}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(baseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return connector.PermanentError("tariff_classification.brazil_portal_unico_classif.endpoint_invalid", errors.New("official catalog endpoint is invalid"))
	}
	if parsed.Scheme == "https" && !strings.EqualFold(parsed.Hostname(), "portalunico.siscomex.gov.br") {
		return connector.PermanentError("tariff_classification.brazil_portal_unico_classif.official_endpoint_required", errors.New("official Portal Unico endpoint is required"))
	}
	return nil
}

func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	catalog, raw, status, ref, headers, err := p.downloadCatalog(ctx, request.Connection)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, err := json.Marshal(map[string]any{
		"connected": true, "candidate_success": false, "source_type": "official_public_catalog", "source_jurisdiction": "BR",
		"official_catalog_version": catalog.LastUpdated, "official_legal_act": catalog.LegalAct, "catalog_entry_count": len(catalog.Nomenclatures),
		"response_status": status, "response_ref": ref, "raw_response_sha256": sha256Value(raw), "raw_response_bytes": len(raw),
		"content_disposition": firstHeader(headers, "Content-Disposition"),
	})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) matchCandidates(ctx context.Context, request connector.TypedRequest[MatchCandidatesInput]) (connector.TypedResult[MatchCandidatesOutput], error) {
	hs6 := strings.TrimSpace(request.Input.HS6)
	if !hs6Pattern.MatchString(hs6) {
		return connector.TypedResult[MatchCandidatesOutput]{}, connector.PermanentError("tariff_classification.brazil_portal_unico_classif.hs6_invalid", errors.New("HS6 must contain six digits"))
	}
	attributes, err := productAttributes(request.Input.ProductAttributes)
	if err != nil {
		return connector.TypedResult[MatchCandidatesOutput]{}, err
	}
	catalog, raw, status, ref, headers, err := p.downloadCatalog(ctx, request.Connection)
	if err != nil {
		return connector.TypedResult[MatchCandidatesOutput]{ResponseRef: ref}, err
	}
	candidates, attributeMatches := candidates(catalog.Nomenclatures, hs6, attributes)
	retrievalStatus := "live"
	manualFallback := false
	if len(candidates) == 0 {
		retrievalStatus, manualFallback = "no_result", true
	}
	canonicalRequest, err := json.Marshal(map[string]any{"hs6": hs6, "product_attributes": attributes})
	if err != nil {
		return connector.TypedResult[MatchCandidatesOutput]{ResponseRef: ref}, err
	}
	result := map[string]any{
		"retrieval_status": retrievalStatus, "provider": "brazil_portal_unico_classif", "source_type": "official_public_catalog",
		"source_trust": "official_current_public_catalog", "source_jurisdiction": "BR", "code_system": "brazil_ncm_8",
		"hs6": hs6, "product_attributes": attributes, "candidates": candidates, "attribute_matched_candidates": attributeMatches,
		"attribute_match_is_non_excluding": true, "official_catalog_version": catalog.LastUpdated,
		"official_catalog_effective_date": effectiveDate(catalog.LastUpdated), "official_legal_act": catalog.LegalAct,
		"catalog_entry_count": len(catalog.Nomenclatures), "request_sha256": sha256Value(canonicalRequest), "request_bytes": len(canonicalRequest),
		"raw_response_sha256": sha256Value(raw), "raw_response_bytes": len(raw), "response_status": status, "response_ref": ref,
		"content_disposition": firstHeader(headers, "Content-Disposition"), "manual_fallback_required": manualFallback,
		"final_classification": false, "human_confirmation_required": true,
	}
	return connector.TypedResult[MatchCandidatesOutput]{Output: MatchCandidatesOutput{Result: result}, ResponseRef: ref}, nil
}

func (p *provider) downloadCatalog(ctx context.Context, connection connector.Connection) (catalog, []byte, int, string, map[string][]string, error) {
	endpoint := baseURL(connection) + "/classif/api/publico/nomenclatura/download/json?perfil=PUBLICO"
	limit := int64(configInt(connection.Config, 8<<20, "max_response_bytes"))
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodGet, URL: endpoint, Headers: map[string][]string{"Accept": {"application/json"}}, MaxResponseBytes: limit})
	if err != nil {
		return catalog{}, nil, 0, "", nil, connector.RetryableError("tariff_classification.brazil_portal_unico_classif.network_error", err)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	if int64(len(response.Body)) > limit {
		return catalog{}, nil, response.StatusCode, ref, response.Headers, connector.PermanentError("tariff_classification.brazil_portal_unico_classif.response_too_large", errors.New("response exceeds configured size limit"))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code := "tariff_classification.brazil_portal_unico_classif.http_" + strconv.Itoa(response.StatusCode)
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests {
			return catalog{}, response.Body, response.StatusCode, ref, response.Headers, connector.RetryableError("tariff_classification.brazil_portal_unico_classif.rate_limited", cause)
		}
		if response.StatusCode >= 500 {
			return catalog{}, response.Body, response.StatusCode, ref, response.Headers, connector.RetryableError(code, cause)
		}
		return catalog{}, response.Body, response.StatusCode, ref, response.Headers, connector.PermanentError(code, cause)
	}
	var decoded catalog
	if err := json.Unmarshal(response.Body, &decoded); err != nil || strings.TrimSpace(decoded.LastUpdated) == "" || strings.TrimSpace(decoded.LegalAct) == "" || len(decoded.Nomenclatures) == 0 {
		return catalog{}, response.Body, response.StatusCode, ref, response.Headers, connector.PermanentError("tariff_classification.brazil_portal_unico_classif.catalog_shape_changed", errors.New("official catalog shape is invalid"))
	}
	return decoded, response.Body, response.StatusCode, ref, response.Headers, nil
}

func productAttributes(object map[string]any) (map[string]string, error) {
	result := make(map[string]string, len(object))
	for key, raw := range object {
		name, value := strings.TrimSpace(key), strings.TrimSpace(fmt.Sprint(raw))
		if name == "" || value == "" || len([]rune(name)) > 100 || len([]rune(value)) > 500 {
			return nil, connector.PermanentError("tariff_classification.brazil_portal_unico_classif.product_attributes_invalid", errors.New("product attributes are invalid"))
		}
		result[name] = value
	}
	return result, nil
}

func candidates(entries []nomenclature, hs6 string, attributes map[string]string) ([]map[string]any, []map[string]any) {
	result, matches := []map[string]any{}, []map[string]any{}
	for _, entry := range entries {
		code := digitsOnly(entry.Code)
		if len(code) != 8 || !strings.HasPrefix(code, hs6) {
			continue
		}
		matched, unmatched := attributeEvidence(entry.Description, attributes)
		candidate := map[string]any{
			"code": code, "formatted_code": entry.Code, "description": strings.TrimSpace(entry.Description),
			"effective_from": entry.EffectiveFrom, "effective_to": entry.EffectiveTo,
			"initial_legal_act":  map[string]any{"type": entry.InitialActType, "number": entry.InitialActNumber, "year": entry.InitialActYear},
			"matched_attributes": matched, "unmatched_attributes": unmatched, "all_attributes_matched": len(unmatched) == 0,
		}
		result = append(result, candidate)
		if len(unmatched) == 0 {
			matches = append(matches, candidate)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i]["code"].(string) < result[j]["code"].(string) })
	sort.Slice(matches, func(i, j int) bool { return matches[i]["code"].(string) < matches[j]["code"].(string) })
	return result, matches
}

func attributeEvidence(description string, attributes map[string]string) ([]string, []string) {
	keys := make([]string, 0, len(attributes))
	for key := range attributes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	normalized := normalizeText(description)
	matched, unmatched := []string{}, []string{}
	for _, key := range keys {
		if strings.Contains(normalized, normalizeText(attributes[key])) {
			matched = append(matched, key)
		} else {
			unmatched = append(unmatched, key)
		}
	}
	return matched, unmatched
}

func normalizeText(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.NewReplacer("á", "a", "à", "a", "â", "a", "ã", "a", "ä", "a", "é", "e", "è", "e", "ê", "e", "ë", "e", "í", "i", "ì", "i", "î", "i", "ï", "i", "ó", "o", "ò", "o", "ô", "o", "õ", "o", "ö", "o", "ú", "u", "ù", "u", "û", "u", "ü", "u", "ç", "c").Replace(value)
	return strings.Join(strings.Fields(value), " ")
}

func digitsOnly(value string) string {
	var builder strings.Builder
	for _, current := range value {
		if current >= '0' && current <= '9' {
			builder.WriteRune(current)
		}
	}
	return builder.String()
}

func effectiveDate(value string) string {
	match := datePattern.FindStringSubmatch(strings.TrimSpace(value))
	if len(match) != 4 {
		return ""
	}
	return match[3] + "-" + match[2] + "-" + match[1]
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

func firstHeader(headers map[string][]string, key string) string {
	for current, values := range headers {
		if strings.EqualFold(current, key) && len(values) > 0 {
			return values[0]
		}
	}
	return ""
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
