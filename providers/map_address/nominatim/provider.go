// Package nominatim implements the official self-hosted Nominatim map-address Provider.
package nominatim

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
)

const (
	ConnectorKey = "map_address"
	ProviderKey  = "nominatim"

	responseLimit int64 = 2 << 20
)

var SearchAddress = connector.CallOperation[SearchAddressInput, SearchAddressOutput]{
	ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "search_address",
	ContractSHA256: "574d53285e7ad9f9af2ac2ee3a9288b8e032ab3db54885d8bbf6bd654e52cfb2",
	Reliability:    readReliability(),
}

var ReverseGeocode = connector.CallOperation[ReverseGeocodeInput, ReverseGeocodeOutput]{
	ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "reverse_geocode",
	ContractSHA256: "9c4b5701bd8e698437d72f1788fa480214a121d70b6907cd695f7c87bac38600",
	Reliability:    readReliability(),
}

type SearchAddressInput struct {
	Query        string `json:"query"`
	Limit        int    `json:"limit,omitempty"`
	CountryCodes string `json:"country_codes,omitempty"`
}

type SearchAddressOutput struct {
	Results []map[string]any `json:"results"`
}

type ReverseGeocodeInput struct {
	Latitude  string `json:"latitude"`
	Longitude string `json:"longitude"`
	Zoom      int    `json:"zoom,omitempty"`
}

type ReverseGeocodeOutput struct {
	Result map[string]any `json:"result"`
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

// New constructs the Provider without performing network access.
func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Nominatim transport is required")
	}
	implementation := &provider{transport: transport}
	search, err := connector.BindCall(SearchAddress, implementation.searchAddress)
	if err != nil {
		return nil, err
	}
	reverse, err := connector.BindCall(ReverseGeocode, implementation.reverseGeocode)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), search, reverse)
	if err != nil {
		return nil, err
	}
	implementation.Adapter = bound
	return implementation, nil
}

func schema() connector.ProviderSchema {
	minimum, maximum := float64(1), float64(120)
	return connector.ProviderSchema{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0",
		ConfigFields: []connector.ConfigField{
			{Key: "base_url", Name: "Nominatim base URL", Description: "Base URL of a self-hosted or privately selected Nominatim service.", Type: connector.ConfigFieldText, Required: true, Validation: connector.ConfigValidation{MaxLength: 2048, Pattern: `^https?://`}},
			{Key: "user_agent", Name: "Application User-Agent", Type: connector.ConfigFieldText, Required: true, Validation: connector.ConfigValidation{MinLength: 1, MaxLength: 512}},
			{Key: "locale", Name: "Response locale", Type: connector.ConfigFieldText, Default: json.RawMessage(`"en-US"`), Validation: connector.ConfigValidation{MaxLength: 64}},
			{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`15`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}},
		},
	}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	endpoint, err := endpointURL(connection.Config)
	if err != nil {
		return err
	}
	if strings.EqualFold(endpoint.Hostname(), "nominatim.openstreetmap.org") {
		return connector.PermanentError("nominatim.public_endpoint_forbidden", errors.New("public OpenStreetMap Nominatim endpoint is not permitted"))
	}
	userAgent := strings.TrimSpace(configString(connection.Config, "user_agent"))
	if userAgent == "" || strings.HasPrefix(strings.ToLower(userAgent), "go-http-client") {
		return connector.PermanentError("nominatim.user_agent_required", errors.New("an application-specific User-Agent is required"))
	}
	return nil
}

func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	payload, ref, err := p.request(ctx, request.Connection, "/status", url.Values{"format": {"json"}}, false)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, err := json.Marshal(map[string]any{"connected": true, "response_ref": ref, "status": payload})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) searchAddress(ctx context.Context, request connector.TypedRequest[SearchAddressInput]) (connector.TypedResult[SearchAddressOutput], error) {
	query := strings.TrimSpace(request.Input.Query)
	if query == "" {
		return connector.TypedResult[SearchAddressOutput]{}, connector.PermanentError("nominatim.query_required", errors.New("query is required"))
	}
	limit := request.Input.Limit
	if limit == 0 {
		limit = 10
	}
	if limit < 1 || limit > 50 {
		return connector.TypedResult[SearchAddressOutput]{}, connector.PermanentError("nominatim.limit_invalid", errors.New("limit must be between 1 and 50"))
	}
	values := url.Values{"format": {"jsonv2"}, "addressdetails": {"1"}, "q": {query}, "limit": {strconv.Itoa(limit)}}
	if countries := strings.ToLower(strings.TrimSpace(request.Input.CountryCodes)); countries != "" {
		values.Set("countrycodes", countries)
	}
	payload, ref, err := p.request(ctx, request.Connection, "/search", values, true)
	if err != nil {
		return connector.TypedResult[SearchAddressOutput]{ResponseRef: ref}, err
	}
	results, ok := payload.([]map[string]any)
	if !ok {
		return connector.TypedResult[SearchAddressOutput]{ResponseRef: ref}, connector.PermanentError("nominatim.response_invalid", errors.New("search response is not a list"))
	}
	return connector.TypedResult[SearchAddressOutput]{Output: SearchAddressOutput{Results: results}, ResponseRef: ref}, nil
}

func (p *provider) reverseGeocode(ctx context.Context, request connector.TypedRequest[ReverseGeocodeInput]) (connector.TypedResult[ReverseGeocodeOutput], error) {
	latitude, longitude := strings.TrimSpace(request.Input.Latitude), strings.TrimSpace(request.Input.Longitude)
	if latitude == "" || longitude == "" {
		return connector.TypedResult[ReverseGeocodeOutput]{}, connector.PermanentError("nominatim.coordinates_required", errors.New("latitude and longitude are required"))
	}
	zoom := request.Input.Zoom
	if zoom == 0 {
		zoom = 18
	}
	if zoom < 0 || zoom > 18 {
		return connector.TypedResult[ReverseGeocodeOutput]{}, connector.PermanentError("nominatim.zoom_invalid", errors.New("zoom must be between 0 and 18"))
	}
	values := url.Values{"format": {"jsonv2"}, "addressdetails": {"1"}, "lat": {latitude}, "lon": {longitude}, "zoom": {strconv.Itoa(zoom)}}
	payload, ref, err := p.request(ctx, request.Connection, "/reverse", values, false)
	if err != nil {
		return connector.TypedResult[ReverseGeocodeOutput]{ResponseRef: ref}, err
	}
	result, ok := payload.(map[string]any)
	if !ok {
		return connector.TypedResult[ReverseGeocodeOutput]{ResponseRef: ref}, connector.PermanentError("nominatim.response_invalid", errors.New("reverse response is not an object"))
	}
	return connector.TypedResult[ReverseGeocodeOutput]{Output: ReverseGeocodeOutput{Result: result}, ResponseRef: ref}, nil
}

func (p *provider) request(ctx context.Context, connection connector.Connection, path string, query url.Values, list bool) (any, string, error) {
	base, err := endpointURL(connection.Config)
	if err != nil {
		return nil, "", err
	}
	endpoint := *base
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + path
	endpoint.RawQuery = query.Encode()
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{
		Method: http.MethodGet, URL: endpoint.String(), MaxResponseBytes: responseLimit,
		Headers: map[string][]string{"Accept": {"application/json"}, "User-Agent": {configString(connection.Config, "user_agent")}, "Accept-Language": {configString(connection.Config, "locale")}},
	})
	if err != nil {
		return nil, "", connector.RetryableError("nominatim.network_error", err)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	if int64(len(response.Body)) > responseLimit {
		return nil, ref, connector.PermanentError("nominatim.response_invalid", errors.New("response exceeds size limit"))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			return nil, ref, connector.RetryableError("nominatim.http_"+strconv.Itoa(response.StatusCode), cause)
		}
		return nil, ref, connector.PermanentError("nominatim.http_"+strconv.Itoa(response.StatusCode), cause)
	}
	if list {
		items := []map[string]any{}
		if err := json.Unmarshal(response.Body, &items); err != nil {
			return nil, ref, connector.PermanentError("nominatim.response_invalid", err)
		}
		return items, ref, nil
	}
	object := map[string]any{}
	if len(strings.TrimSpace(string(response.Body))) > 0 {
		if err := json.Unmarshal(response.Body, &object); err != nil {
			return nil, ref, connector.PermanentError("nominatim.response_invalid", err)
		}
	}
	return object, ref, nil
}

func endpointURL(config map[string]any) (*url.URL, error) {
	raw := strings.TrimRight(strings.TrimSpace(configString(config, "base_url")), "/")
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return nil, connector.PermanentError("nominatim.endpoint_invalid", errors.New("HTTPS or loopback HTTP endpoint is required"))
	}
	return parsed, nil
}

func configString(config map[string]any, key string) string {
	value, _ := config[key].(string)
	return strings.TrimSpace(value)
}

func isLoopback(host string) bool {
	host = strings.TrimSpace(strings.ToLower(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

func readReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{
		Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural},
		Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone},
	}
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
