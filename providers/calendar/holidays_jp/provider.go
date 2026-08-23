// Package holidaysjp implements the official Holidays JP calendar Provider.
package holidaysjp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	ConnectorKey        = "calendar"
	ProviderKey         = "holidays_jp"
	responseLimit int64 = 2 << 20
)

var ListFactors = connector.CallOperation[ListFactorsInput, ListFactorsOutput]{
	ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_factors",
	ContractSHA256: "baac11fb6b727ea609ed4d7ab70221c2cef8048ef1df2d5e9d00c31ce505228c",
	Reliability: connector.ReliabilityContract{
		Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural},
		Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone},
	},
}

type ListFactorsInput struct {
	Locale    string `json:"locale"`
	Region    string `json:"region"`
	StartDate string `json:"start_date"`
	EndDate   string `json:"end_date"`
}

type Factor struct {
	Date   string `json:"date"`
	Name   string `json:"name"`
	Kind   string `json:"kind"`
	Locale string `json:"locale"`
	Region string `json:"region"`
	Impact string `json:"impact"`
}

type ListFactorsOutput struct {
	Factors        []Factor `json:"factors"`
	SourceTime     string   `json:"source_time"`
	SourceVersion  string   `json:"source_version"`
	ProviderStatus string   `json:"provider_status"`
}

type cachedDataset struct {
	expires             time.Time
	values              map[string]string
	version, sourceTime string
}

type provider struct {
	connector.Adapter
	transport connector.Transport
	mu        sync.Mutex
	cache     map[string]cachedDataset
	now       func() time.Time
}

// New constructs the Provider without performing network access.
func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Holidays JP transport is required")
	}
	implementation := &provider{transport: transport, cache: map[string]cachedDataset{}, now: time.Now}
	operation, err := connector.BindCall(ListFactors, implementation.listFactors)
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
	minTimeout, maxTimeout, minCache, maxCache := float64(1), float64(60), float64(0), float64(86400)
	return connector.ProviderSchema{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0",
		StartupActivation: connector.StartupActivationDefaultSafe,
		ConfigFields: []connector.ConfigField{
			{Key: "base_url", Name: "Dataset URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://holidays-jp.github.io/api/v1/date.json"`), Validation: connector.ConfigValidation{MaxLength: 2048, Pattern: `^https?://`}},
			{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`15`), Validation: connector.ConfigValidation{Min: &minTimeout, Max: &maxTimeout}},
			{Key: "cache_ttl_seconds", Name: "Cache TTL seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`3600`), Validation: connector.ConfigValidation{Min: &minCache, Max: &maxCache}},
		},
	}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	_, err := endpoint(connection.Config)
	return err
}

func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	values, version, _, status, ref, err := p.dataset(ctx, request.Connection, request.Principal)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, err := json.Marshal(map[string]any{"provider_status": status, "source_version": version, "record_count": len(values), "response_ref": ref})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) listFactors(ctx context.Context, request connector.TypedRequest[ListFactorsInput]) (connector.TypedResult[ListFactorsOutput], error) {
	locale, region, start, end, err := normalizeRequest(request.Input)
	if err != nil {
		return connector.TypedResult[ListFactorsOutput]{}, err
	}
	values, version, sourceTime, status, ref, err := p.dataset(ctx, request.Connection, request.Principal)
	if err != nil {
		return connector.TypedResult[ListFactorsOutput]{ResponseRef: ref}, err
	}
	keys := make([]string, 0, len(values))
	for date := range values {
		if date >= start && date <= end {
			keys = append(keys, date)
		}
	}
	sort.Strings(keys)
	factors := make([]Factor, 0, len(keys))
	for _, date := range keys {
		factors = append(factors, Factor{Date: date, Name: values[date], Kind: "public_holiday", Locale: locale, Region: region, Impact: "business_closure"})
	}
	return connector.TypedResult[ListFactorsOutput]{Output: ListFactorsOutput{Factors: factors, SourceTime: sourceTime, SourceVersion: version, ProviderStatus: status}, ResponseRef: ref}, nil
}

func (p *provider) dataset(ctx context.Context, connection connector.Connection, principal connector.Principal) (map[string]string, string, string, string, string, error) {
	remote, err := endpoint(connection.Config)
	if err != nil {
		return nil, "", "", "", "", err
	}
	cacheKey := principal.WorkspaceID + "|" + connection.Key + "|" + remote.String()
	ttl := time.Duration(configInt(connection.Config, 3600, "cache_ttl_seconds")) * time.Second
	if ttl > 0 {
		p.mu.Lock()
		item, ok := p.cache[cacheKey]
		p.mu.Unlock()
		if ok && p.now().Before(item.expires) {
			return cloneDataset(item.values), item.version, item.sourceTime, "cache_hit", "cache:" + item.version, nil
		}
	}
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodGet, URL: remote.String(), MaxResponseBytes: responseLimit})
	if err != nil {
		return nil, "", "", "", "", connector.RetryableError("calendar.unavailable", err)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	if response.StatusCode == http.StatusTooManyRequests {
		return nil, "", "", "", ref, connector.RetryableError("calendar.rate_limited", errors.New("provider rate limited request"))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		if response.StatusCode >= 500 {
			return nil, "", "", "", ref, connector.RetryableError("calendar.unavailable", cause)
		}
		return nil, "", "", "", ref, connector.PermanentError("calendar.provider_rejected", cause)
	}
	if int64(len(response.Body)) > responseLimit {
		return nil, "", "", "", ref, connector.PermanentError("calendar.response_invalid", errors.New("response exceeds size limit"))
	}
	values := map[string]string{}
	if err := json.Unmarshal(response.Body, &values); err != nil {
		return nil, "", "", "", ref, connector.PermanentError("calendar.response_invalid", err)
	}
	hash := sha256.Sum256(response.Body)
	version := "sha256:" + hex.EncodeToString(hash[:])
	sourceTime := p.now().UTC().Format(time.RFC3339)
	if modified := firstHeader(response.Headers, "Last-Modified"); modified != "" {
		sourceTime = modified
	}
	if ttl > 0 {
		p.mu.Lock()
		p.cache[cacheKey] = cachedDataset{expires: p.now().Add(ttl), values: cloneDataset(values), version: version, sourceTime: sourceTime}
		p.mu.Unlock()
	}
	return values, version, sourceTime, "live", ref, nil
}

func endpoint(config map[string]any) (*url.URL, error) {
	raw := strings.TrimSpace(configString(config, "base_url"))
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return nil, connector.PermanentError("calendar.endpoint_invalid", errors.New("HTTPS or loopback HTTP endpoint is required"))
	}
	return parsed, nil
}

func normalizeRequest(input ListFactorsInput) (string, string, string, string, error) {
	locale := strings.TrimSpace(input.Locale)
	region := strings.ToUpper(strings.TrimSpace(input.Region))
	from, fromErr := time.Parse("2006-01-02", input.StartDate)
	to, toErr := time.Parse("2006-01-02", input.EndDate)
	if locale == "" || region != "JP" || fromErr != nil || toErr != nil || to.Before(from) || to.Sub(from) > 366*24*time.Hour {
		return "", "", "", "", connector.PermanentError("calendar.request_invalid", errors.New("calendar factor request is invalid"))
	}
	return locale, region, input.StartDate, input.EndDate, nil
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

func firstHeader(headers map[string][]string, key string) string {
	for current, values := range headers {
		if strings.EqualFold(current, key) && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}

func cloneDataset(values map[string]string) map[string]string {
	clone := make(map[string]string, len(values))
	for key, value := range values {
		clone[key] = value
	}
	return clone
}

func isLoopback(host string) bool {
	host = strings.TrimSpace(strings.ToLower(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
