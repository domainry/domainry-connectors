// Package openmeteo implements the official Open-Meteo weather Provider.
package openmeteo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

const responseLimit int64 = 2 << 20

const (
	ConnectorKey = "weather"
	ProviderKey  = "open_meteo"
)

var GetDaily = connector.CallOperation[GetDailyInput, GetDailyOutput]{
	ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "get_daily",
	ContractSHA256: "65073ca1eb88e5161ba84d107734b1c611cae21cd0ea487290fb4cdc539b9378",
	Reliability: connector.ReliabilityContract{
		Effect:         connector.EffectRead,
		Idempotency:    connector.IdempotencyContract{Strategy: connector.IdempotencyNatural},
		Reconciliation: connector.ReconciliationNone,
		Compensation:   connector.CompensationContract{Mode: connector.CompensationNone},
	},
}

type GetDailyInput struct {
	Latitude  *float64 `json:"latitude,omitempty"`
	Longitude *float64 `json:"longitude,omitempty"`
	Location  string   `json:"location,omitempty"`
	StartDate string   `json:"start_date"`
	EndDate   string   `json:"end_date"`
	Timezone  string   `json:"timezone,omitempty"`
}

type GetDailyOutput struct {
	Location       Location `json:"location"`
	Days           []Day    `json:"days"`
	SourceTime     string   `json:"source_time"`
	SourceVersion  string   `json:"source_version"`
	ProviderStatus string   `json:"provider_status"`
}

type Location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Timezone  string  `json:"timezone"`
}

type Day struct {
	Date            string   `json:"date"`
	WeatherCode     *float64 `json:"weather_code"`
	TemperatureMaxC *float64 `json:"temperature_max_c"`
	TemperatureMinC *float64 `json:"temperature_min_c"`
	PrecipitationMM *float64 `json:"precipitation_mm"`
	RainMM          *float64 `json:"rain_mm"`
	SnowfallCM      *float64 `json:"snowfall_cm"`
}

type cachedResult struct {
	expires time.Time
	output  GetDailyOutput
	ref     string
}

type provider struct {
	connector.Adapter
	transport connector.Transport
	mu        sync.Mutex
	cache     map[string]cachedResult
	now       func() time.Time
}

// New constructs the Provider without performing network access.
func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Open-Meteo transport is required")
	}
	implementation := &provider{transport: transport, cache: map[string]cachedResult{}, now: time.Now}
	operation, err := connector.BindCall(GetDaily, implementation.getDaily)
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
	minimumCache, maximumCache := float64(0), float64(3600)
	return connector.ProviderSchema{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0",
		StartupActivation: connector.StartupActivationDefaultSafe,
		ConfigFields: []connector.ConfigField{
			{Key: "base_url", Name: "Base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://api.open-meteo.com"`), Validation: connector.ConfigValidation{MaxLength: 2048, Pattern: `^https?://`}},
			{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`15`), Validation: connector.ConfigValidation{Min: &minimumTimeout, Max: &maximumTimeout}},
			{Key: "cache_ttl_seconds", Name: "Cache TTL seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`300`), Validation: connector.ConfigValidation{Min: &minimumCache, Max: &maximumCache}},
		},
	}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	_, err := endpoint(connection.Config)
	return err
}

func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	today := p.now().UTC().Format("2006-01-02")
	output, _, err := p.fetch(ctx, connector.TypedRequest[GetDailyInput]{
		Connection: request.Connection,
		Principal:  request.Principal,
		Input: GetDailyInput{
			Latitude: float64Pointer(35.6762), Longitude: float64Pointer(139.6503),
			StartDate: today, EndDate: today, Timezone: "Asia/Tokyo",
		},
	})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, err := json.Marshal(map[string]string{"provider_status": output.ProviderStatus})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) getDaily(ctx context.Context, request connector.TypedRequest[GetDailyInput]) (connector.TypedResult[GetDailyOutput], error) {
	output, ref, err := p.fetch(ctx, request)
	return connector.TypedResult[GetDailyOutput]{Output: output, ResponseRef: ref}, err
}

func (p *provider) fetch(ctx context.Context, request connector.TypedRequest[GetDailyInput]) (GetDailyOutput, string, error) {
	latitude, longitude, start, end, timezone, err := normalizeInput(request.Input)
	if err != nil {
		return GetDailyOutput{}, "", err
	}
	cacheKey := strings.Join([]string{request.Principal.WorkspaceID, request.Connection.Key, strconv.FormatFloat(latitude, 'f', 6, 64), strconv.FormatFloat(longitude, 'f', 6, 64), start, end, timezone}, "|")
	ttl := time.Duration(configInt(request.Connection.Config, 300, "cache_ttl_seconds")) * time.Second
	if ttl > 0 {
		p.mu.Lock()
		cached, ok := p.cache[cacheKey]
		p.mu.Unlock()
		if ok && p.now().Before(cached.expires) {
			output := cached.output
			output.Days = append([]Day(nil), output.Days...)
			output.ProviderStatus = "cache_hit"
			return output, cached.ref, nil
		}
	}
	base, err := endpoint(request.Connection.Config)
	if err != nil {
		return GetDailyOutput{}, "", err
	}
	remote := *base
	remote.Path = strings.TrimRight(remote.Path, "/") + "/v1/forecast"
	query := remote.Query()
	query.Set("latitude", strconv.FormatFloat(latitude, 'f', 6, 64))
	query.Set("longitude", strconv.FormatFloat(longitude, 'f', 6, 64))
	query.Set("start_date", start)
	query.Set("end_date", end)
	query.Set("timezone", timezone)
	query.Set("daily", "weather_code,temperature_2m_max,temperature_2m_min,precipitation_sum,rain_sum,snowfall_sum")
	remote.RawQuery = query.Encode()
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodGet, URL: remote.String(), MaxResponseBytes: responseLimit})
	if err != nil {
		return GetDailyOutput{}, "", connector.RetryableError("weather.unavailable", err)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	if response.StatusCode == http.StatusTooManyRequests {
		return GetDailyOutput{}, ref, connector.RetryableError("weather.rate_limited", errors.New("provider rate limited request"))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		if response.StatusCode >= 500 {
			return GetDailyOutput{}, ref, connector.RetryableError("weather.unavailable", cause)
		}
		return GetDailyOutput{}, ref, connector.PermanentError("weather.provider_rejected", cause)
	}
	output, err := decodeResponse(response.Body, p.now())
	if err != nil {
		return GetDailyOutput{}, ref, connector.PermanentError("weather.response_invalid", err)
	}
	if ttl > 0 {
		p.mu.Lock()
		p.cache[cacheKey] = cachedResult{expires: p.now().Add(ttl), output: output, ref: ref}
		p.mu.Unlock()
	}
	return output, ref, nil
}

func endpoint(config map[string]any) (*url.URL, error) {
	raw := strings.TrimRight(configString(config, "base_url"), "/")
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return nil, connector.PermanentError("weather.endpoint_invalid", errors.New("HTTPS or loopback HTTP endpoint is required"))
	}
	return parsed, nil
}

func normalizeInput(input GetDailyInput) (float64, float64, string, string, string, error) {
	var latitude, longitude float64
	validCoordinates := input.Latitude != nil && input.Longitude != nil
	if validCoordinates {
		latitude, longitude = *input.Latitude, *input.Longitude
	} else if parts := strings.Split(strings.TrimSpace(input.Location), ","); len(parts) == 2 {
		var latErr, lonErr error
		latitude, latErr = strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
		longitude, lonErr = strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
		validCoordinates = latErr == nil && lonErr == nil
	}
	startDate, startErr := time.Parse("2006-01-02", input.StartDate)
	endDate, endErr := time.Parse("2006-01-02", input.EndDate)
	if !validCoordinates || latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180 {
		return 0, 0, "", "", "", connector.PermanentError("weather.location_invalid", errors.New("latitude or longitude is invalid"))
	}
	if startErr != nil || endErr != nil || endDate.Before(startDate) || endDate.Sub(startDate) > 31*24*time.Hour {
		return 0, 0, "", "", "", connector.PermanentError("weather.date_range_invalid", errors.New("date range is invalid"))
	}
	timezone := strings.TrimSpace(input.Timezone)
	if timezone == "" {
		timezone = "UTC"
	}
	return latitude, longitude, input.StartDate, input.EndDate, timezone, nil
}

func decodeResponse(raw []byte, observedAt time.Time) (GetDailyOutput, error) {
	var wire struct {
		Latitude  float64 `json:"latitude"`
		Longitude float64 `json:"longitude"`
		Timezone  string  `json:"timezone"`
		Daily     struct {
			Time             []string  `json:"time"`
			WeatherCode      []float64 `json:"weather_code"`
			TemperatureMax   []float64 `json:"temperature_2m_max"`
			TemperatureMin   []float64 `json:"temperature_2m_min"`
			PrecipitationSum []float64 `json:"precipitation_sum"`
			RainSum          []float64 `json:"rain_sum"`
			SnowfallSum      []float64 `json:"snowfall_sum"`
		} `json:"daily"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil || len(wire.Daily.Time) == 0 {
		return GetDailyOutput{}, errors.New("Open-Meteo response is invalid")
	}
	days := make([]Day, 0, len(wire.Daily.Time))
	for index, date := range wire.Daily.Time {
		days = append(days, Day{Date: date, WeatherCode: at(wire.Daily.WeatherCode, index), TemperatureMaxC: at(wire.Daily.TemperatureMax, index), TemperatureMinC: at(wire.Daily.TemperatureMin, index), PrecipitationMM: at(wire.Daily.PrecipitationSum, index), RainMM: at(wire.Daily.RainSum, index), SnowfallCM: at(wire.Daily.SnowfallSum, index)})
	}
	return GetDailyOutput{Location: Location{Latitude: wire.Latitude, Longitude: wire.Longitude, Timezone: wire.Timezone}, Days: days, SourceTime: observedAt.UTC().Format(time.RFC3339), SourceVersion: "open-meteo-v1", ProviderStatus: "live"}, nil
}

func at(values []float64, index int) *float64 {
	if index < 0 || index >= len(values) {
		return nil
	}
	value := values[index]
	return &value
}

func configString(values map[string]any, key string) string {
	return strings.TrimSpace(fmt.Sprint(values[key]))
}

func configInt(values map[string]any, fallback int, key string) int {
	value, err := strconv.Atoi(configString(values, key))
	if err != nil {
		return fallback
	}
	return value
}

func isLoopback(host string) bool {
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

func float64Pointer(value float64) *float64 { return &value }

var (
	_ connector.ConfigValidator  = (*provider)(nil)
	_ connector.ConnectionTester = (*provider)(nil)
)
