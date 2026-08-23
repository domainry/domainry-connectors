// Package talentlms implements the official TalentLMS Provider.
package talentlms

import (
	"context"
	"encoding/base64"
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
	ConnectorKey        = "lms"
	ProviderKey         = "talentlms"
	responseLimit int64 = 4 << 20
)

var ListCourses = connector.CallOperation[ListCoursesInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_courses", ContractSHA256: "c94745f0431bcceb8943b07c11c30d9305fb715566fe048add3580b42696bd75", Reliability: readReliability()}
var SearchUsers = connector.CallOperation[SearchUsersInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "search_users", ContractSHA256: "260d267d7d6a6852d49f86e45599135a6b4cd1cbf08b04e46ed9d91ec165a4eb", Reliability: readReliability()}
var ListEnrolledUsers = connector.CallOperation[ListEnrolledUsersInput, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_enrolled_users", ContractSHA256: "c36f2ee1bac3092d5df2f0a763007ae68d81c60c4890f9898bfc5d1e899f4d1b", Reliability: readReliability()}

type ListCoursesInput struct{}
type SearchCriterion struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
type SearchUsersInput struct {
	Criteria []SearchCriterion `json:"criteria,omitempty"`
}
type ListEnrolledUsersInput struct {
	CourseID string `json:"course_id"`
}
type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("TalentLMS transport is required")
	}
	p := &provider{transport: transport}
	courses, err := connector.BindCall(ListCourses, p.listCourses)
	if err != nil {
		return nil, err
	}
	users, err := connector.BindCall(SearchUsers, p.searchUsers)
	if err != nil {
		return nil, err
	}
	enrolled, err := connector.BindCall(ListEnrolledUsers, p.listEnrolledUsers)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), courses, users, enrolled)
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
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "domain_url", Name: "TalentLMS domain URL", Type: connector.ConfigFieldText, Required: true}, {Key: "api_version", Name: "API version", Type: connector.ConfigFieldSelect, Required: true, Default: json.RawMessage(`"v1"`), Validation: connector.ConfigValidation{Options: []string{"v1"}}}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &min, Max: &max}}}, SecretFields: []connector.SecretField{{Key: "api_key", Name: "API key", Required: true, CredentialKind: connector.SecretCredentialAPIKey, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(domainURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return connector.PermanentError("talentlms.domain_url_invalid", errors.New("HTTPS or loopback HTTP endpoint is required"))
	}
	version := configString(connection.Config, "api_version")
	if version != "" && version != "v1" {
		return connector.PermanentError("talentlms.api_version_unsupported", errors.New("only API v1 is supported"))
	}
	return nil
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	payload, ref, err := p.get(ctx, request.Connection, request.Secrets, "/siteinfo")
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, err := json.Marshal(map[string]any{"site": payload, "response_ref": ref})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}
func (p *provider) listCourses(ctx context.Context, request connector.TypedRequest[ListCoursesInput]) (connector.TypedResult[map[string]any], error) {
	payload, ref, err := p.get(ctx, request.Connection, request.Secrets, "/courses")
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) searchUsers(ctx context.Context, request connector.TypedRequest[SearchUsersInput]) (connector.TypedResult[map[string]any], error) {
	if len(request.Input.Criteria) > 0 {
		return connector.TypedResult[map[string]any]{}, connector.PermanentError("talentlms.criteria_unsupported", errors.New("TalentLMS v1 list users does not support the generic LMS criteria contract"))
	}
	payload, ref, err := p.get(ctx, request.Connection, request.Secrets, "/users")
	if err == nil {
		payload = map[string]any{"users": payload["items"]}
	}
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) listEnrolledUsers(ctx context.Context, request connector.TypedRequest[ListEnrolledUsersInput]) (connector.TypedResult[map[string]any], error) {
	id := strings.TrimSpace(request.Input.CourseID)
	if id == "" {
		return connector.TypedResult[map[string]any]{}, connector.PermanentError("talentlms.course_id_required", errors.New("course ID is required"))
	}
	payload, ref, err := p.get(ctx, request.Connection, request.Secrets, "/courses/id:"+url.PathEscape(id))
	if err == nil {
		payload = map[string]any{"items": payload["users"], "course": payload}
	}
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) get(ctx context.Context, connection connector.Connection, secrets map[string]string, path string) (map[string]any, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", err
	}
	key := strings.TrimSpace(secrets["api_key"])
	if key == "" {
		return nil, "", connector.PermanentError("talentlms.api_key_required", errors.New("resolved API key is required"))
	}
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodGet, URL: baseURL(connection) + path, Headers: map[string][]string{"Accept": {"application/json"}}, SecretHeaders: map[string][]string{"Authorization": {"Basic " + base64.StdEncoding.EncodeToString([]byte(key+":"))}}, MaxResponseBytes: responseLimit})
	if err != nil {
		return nil, "", connector.RetryableError("talentlms.network_error", err)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	var decoded any
	if len(response.Body) > 0 && json.Unmarshal(response.Body, &decoded) != nil {
		return nil, ref, connector.PermanentError("talentlms.response_invalid", errors.New("provider response is invalid JSON"))
	}
	payload, ok := decoded.(map[string]any)
	if !ok {
		if items, arrayOK := decoded.([]any); arrayOK {
			payload = map[string]any{"items": items}
		} else {
			payload = map[string]any{}
		}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code, cause := "talentlms.http_"+strconv.Itoa(response.StatusCode), fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			return payload, ref, connector.RetryableError(code, cause)
		}
		return payload, ref, connector.PermanentError(code, cause)
	}
	return payload, ref, nil
}
func domainURL(connection connector.Connection) string {
	return strings.TrimRight(configString(connection.Config, "domain_url"), "/")
}
func baseURL(connection connector.Connection) string { return domainURL(connection) + "/api/v1" }
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
