// Package googleplayconsole implements the official Google Play Console review Provider.
package googleplayconsole

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
	ConnectorKey         = "app_review"
	ProviderKey          = "google_play_console"
	defaultBaseURL       = "https://androidpublisher.googleapis.com/androidpublisher/v3"
	responseLimit  int64 = 4 << 20
)

var ListReviews = connector.CallOperation[ListReviewsInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_reviews", ContractSHA256: "1f66d52b94c81cfeeae5c04361222593c8f6061d0a67b73e3116295b0dcd5975", Reliability: readReliability()}
var ReplyReview = connector.CallOperation[ReplyReviewInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "reply_review", ContractSHA256: "09151a2a48bca2386217e16f8d3b23458421687fedab5ad05b9392b84fd82c8b", Reliability: connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNone}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}}

type ListReviewsInput struct {
	PageSize            int    `json:"page_size,omitempty"`
	After               string `json:"after,omitempty"`
	TranslationLanguage string `json:"translation_language,omitempty"`
}
type ReplyReviewInput struct {
	ReviewID  string `json:"review_id"`
	ReplyText string `json:"reply_text"`
}
type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Google Play Console transport is required")
	}
	p := &provider{transport: transport}
	list, err := connector.BindCall(ListReviews, p.listReviews)
	if err != nil {
		return nil, err
	}
	reply, err := connector.BindCall(ReplyReview, p.replyReview)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), list, reply)
	if err != nil {
		return nil, err
	}
	p.Adapter = bound
	return p, nil
}
func readReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}
func schema() connector.ProviderSchema {
	min, max := float64(1), float64(120)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "package_name", Name: "Android package name", Type: connector.ConfigFieldText, Required: true}, {Key: "base_url", Name: "Google Play API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://androidpublisher.googleapis.com/androidpublisher/v3"`)}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &min, Max: &max}}}, SecretFields: []connector.SecretField{{Key: "access_token", Name: "OAuth access token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	if configString(connection.Config, "package_name") == "" {
		return connector.PermanentError("google_play.package_name_required", errors.New("Android package name is required"))
	}
	parsed, err := url.Parse(baseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return connector.PermanentError("google_play.endpoint_invalid", errors.New("HTTPS or loopback HTTP endpoint is required"))
	}
	return nil
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, reviewsPath(request.Connection), url.Values{"maxResults": {"1"}}, nil, false)
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, err := json.Marshal(map[string]any{"reviews": payload["reviews"], "response_ref": ref})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}
func (p *provider) listReviews(ctx context.Context, request connector.TypedRequest[ListReviewsInput]) (connector.TypedResult[map[string]any], error) {
	size := request.Input.PageSize
	if size == 0 {
		size = 100
	}
	if size < 1 || size > 100 {
		return connector.TypedResult[map[string]any]{}, connector.PermanentError("google_play.page_size_invalid", errors.New("page size must be between 1 and 100"))
	}
	query := url.Values{"maxResults": {strconv.Itoa(size)}}
	if value := strings.TrimSpace(request.Input.After); value != "" {
		query.Set("token", value)
	}
	if value := strings.TrimSpace(request.Input.TranslationLanguage); value != "" {
		query.Set("translationLanguage", value)
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, reviewsPath(request.Connection), query, nil, false)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) replyReview(ctx context.Context, request connector.TypedRequest[ReplyReviewInput]) (connector.TypedResult[map[string]any], error) {
	id, reply := strings.TrimSpace(request.Input.ReviewID), strings.TrimSpace(request.Input.ReplyText)
	if id == "" || reply == "" {
		return connector.TypedResult[map[string]any]{}, connector.PermanentError("google_play.reply_required", errors.New("review ID and reply text are required"))
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodPost, reviewsPath(request.Connection)+"/"+url.PathEscape(id)+":reply", nil, map[string]any{"replyText": reply}, true)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, method, path string, query url.Values, body map[string]any, write bool) (map[string]any, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", err
	}
	token := strings.TrimSpace(secrets["access_token"])
	if token == "" {
		return nil, "", connector.PermanentError("google_play.access_token_required", errors.New("resolved access token is required"))
	}
	endpoint, err := url.Parse(baseURL(connection) + path)
	if err != nil {
		return nil, "", connector.PermanentError("google_play.request_invalid", err)
	}
	endpoint.RawQuery = query.Encode()
	var raw []byte
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return nil, "", connector.PermanentError("google_play.request_invalid", err)
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint.String(), Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, Body: raw, MaxResponseBytes: responseLimit})
	if err != nil {
		if write {
			return nil, "", connector.UncertainError("google_play.write_outcome_uncertain", err)
		}
		return nil, "", connector.RetryableError("google_play.network_error", err)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := map[string]any{}
	if json.Unmarshal(response.Body, &payload) != nil {
		return nil, ref, connector.PermanentError("google_play.response_invalid", errors.New("provider response is invalid JSON"))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code, cause := "google_play.http_"+strconv.Itoa(response.StatusCode), fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests {
			return payload, ref, connector.RetryableError(code, cause)
		}
		if response.StatusCode >= 500 {
			if write {
				return payload, ref, connector.UncertainError("google_play.write_outcome_uncertain", cause)
			}
			return payload, ref, connector.RetryableError(code, cause)
		}
		return payload, ref, connector.PermanentError(code, cause)
	}
	if id := configString(payload, "reviewId"); id != "" {
		ref = "google_play:review:" + id
	}
	return payload, ref, nil
}
func reviewsPath(connection connector.Connection) string {
	return "/applications/" + url.PathEscape(configString(connection.Config, "package_name")) + "/reviews"
}
func baseURL(connection connector.Connection) string {
	if value := strings.TrimRight(configString(connection.Config, "base_url"), "/"); value != "" {
		return value
	}
	return defaultBaseURL
}
func configString(values map[string]any, key string) string {
	if values == nil || values[key] == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(values[key]))
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
