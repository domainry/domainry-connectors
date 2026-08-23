// Package feishu contains private Feishu protocol kernels shared by official
// Providers. It is not part of the third-party Connector API.
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

	connector "github.com/domainry/domainry-connector-sdk"
)

const tokenResponseLimit int64 = 1 << 20

type TenantTokenRequest struct {
	Endpoint    string
	AppID       string
	AppSecret   string
	ErrorPrefix string
}

type TenantToken struct {
	AccessToken      string
	ExpiresInSeconds int
}

// FetchTenantToken obtains an internal-app tenant token while keeping both
// application credentials in Runtime-only JSON fields.
func FetchTenantToken(ctx context.Context, transport connector.Transport, request TenantTokenRequest) (TenantToken, error) {
	if transport == nil {
		return TenantToken{}, errors.New("Feishu transport is required")
	}
	prefix := strings.TrimSpace(request.ErrorPrefix)
	if prefix == "" {
		prefix = "feishu"
	}
	parsed, parseErr := url.Parse(strings.TrimSpace(request.Endpoint))
	if parseErr != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) || strings.TrimSpace(request.AppID) == "" || strings.TrimSpace(request.AppSecret) == "" {
		return TenantToken{}, connector.PermanentError(prefix+".token_configuration_invalid", errors.New("valid Feishu token endpoint, app ID, and app secret are required"))
	}
	response, err := transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodPost, URL: strings.TrimSpace(request.Endpoint), Headers: map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/json; charset=utf-8"}}, SecretJSON: map[string]string{"app_id": strings.TrimSpace(request.AppID), "app_secret": strings.TrimSpace(request.AppSecret)}, Body: []byte(`{}`), MaxResponseBytes: tokenResponseLimit})
	if err != nil {
		return TenantToken{}, connector.RetryableError(prefix+".token_network_error", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("Feishu token endpoint returned HTTP %d", response.StatusCode)
		code := prefix + ".token_http_" + strconv.Itoa(response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			return TenantToken{}, connector.RetryableError(code, cause)
		}
		return TenantToken{}, connector.PermanentError(code, cause)
	}
	var payload struct {
		Code        int    `json:"code"`
		Message     string `json:"msg"`
		AccessToken string `json:"tenant_access_token"`
		Expires     int    `json:"expire"`
	}
	if err = json.Unmarshal(response.Body, &payload); err != nil {
		return TenantToken{}, connector.PermanentError(prefix+".token_response_invalid", err)
	}
	if payload.Code != 0 {
		return TenantToken{}, connector.PermanentError(prefix+".token_rejected", fmt.Errorf("Feishu token endpoint returned code %d", payload.Code))
	}
	if strings.TrimSpace(payload.AccessToken) == "" {
		return TenantToken{}, connector.PermanentError(prefix+".token_response_invalid", errors.New("Feishu token response lacks tenant_access_token"))
	}
	return TenantToken{AccessToken: strings.TrimSpace(payload.AccessToken), ExpiresInSeconds: payload.Expires}, nil
}

func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
