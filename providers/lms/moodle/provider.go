// Package moodle implements the official Moodle LMS Provider.
package moodle

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
	ConnectorKey        = "lms"
	ProviderKey         = "moodle"
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
		return nil, errors.New("Moodle transport is required")
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
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "base_url", Name: "Moodle site URL", Type: connector.ConfigFieldText, Required: true}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &min, Max: &max}}}, SecretFields: []connector.SecretField{{Key: "api_token", Name: "Web service token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(baseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return connector.PermanentError("moodle.endpoint_invalid", errors.New("HTTPS or loopback HTTP endpoint is required"))
	}
	return nil
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, "core_webservice_get_site_info", nil)
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
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, "core_course_get_courses", nil)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) searchUsers(ctx context.Context, request connector.TypedRequest[SearchUsersInput]) (connector.TypedResult[map[string]any], error) {
	criteria := request.Input.Criteria
	if len(criteria) == 0 {
		criteria = []SearchCriterion{{Key: "email", Value: "%"}}
	}
	values := url.Values{}
	for index, criterion := range criteria {
		key, value := strings.TrimSpace(criterion.Key), strings.TrimSpace(criterion.Value)
		if key == "" || value == "" {
			return connector.TypedResult[map[string]any]{}, connector.PermanentError("moodle.criteria_invalid", errors.New("each search criterion requires key and value"))
		}
		values.Set(fmt.Sprintf("criteria[%d][key]", index), key)
		values.Set(fmt.Sprintf("criteria[%d][value]", index), value)
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, "core_user_get_users", values)
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) listEnrolledUsers(ctx context.Context, request connector.TypedRequest[ListEnrolledUsersInput]) (connector.TypedResult[map[string]any], error) {
	id := strings.TrimSpace(request.Input.CourseID)
	if id == "" {
		return connector.TypedResult[map[string]any]{}, connector.PermanentError("moodle.course_id_required", errors.New("course ID is required"))
	}
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, "core_enrol_get_enrolled_users", url.Values{"courseid": {id}})
	return connector.TypedResult[map[string]any]{Output: payload, ResponseRef: ref}, err
}
func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, function string, values url.Values) (map[string]any, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", err
	}
	token := strings.TrimSpace(secrets["api_token"])
	if token == "" {
		return nil, "", connector.PermanentError("moodle.api_token_required", errors.New("resolved web service token is required"))
	}
	if values == nil {
		values = url.Values{}
	}
	values.Set("wsfunction", function)
	values.Set("moodlewsrestformat", "json")
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodPost, URL: baseURL(connection) + "/webservice/rest/server.php", Headers: map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/x-www-form-urlencoded"}}, SecretForm: map[string]string{"wstoken": token}, Body: []byte(values.Encode()), MaxResponseBytes: responseLimit})
	if err != nil {
		return nil, "", connector.RetryableError("moodle.network_error", err)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	var decoded any
	if len(response.Body) > 0 && json.Unmarshal(response.Body, &decoded) != nil {
		return nil, ref, connector.PermanentError("moodle.response_invalid", errors.New("provider response is invalid JSON"))
	}
	payload, ok := decoded.(map[string]any)
	if !ok {
		if items, arrayOK := decoded.([]any); arrayOK {
			payload = map[string]any{"items": items}
		} else {
			payload = map[string]any{}
		}
	}
	if rawCode := configString(payload, "errorcode"); rawCode != "" {
		return payload, ref, connector.PermanentError("moodle.provider_"+codeToken(rawCode), errors.New("Moodle rejected the request"))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code, cause := "moodle.http_"+strconv.Itoa(response.StatusCode), fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			return payload, ref, connector.RetryableError(code, cause)
		}
		return payload, ref, connector.PermanentError(code, cause)
	}
	if userID := configString(payload, "userid"); userID != "" {
		ref = "moodle:user:" + userID
	}
	return payload, ref, nil
}
func baseURL(connection connector.Connection) string {
	return strings.TrimRight(configString(connection.Config, "base_url"), "/")
}
func configString(values map[string]any, key string) string {
	if values == nil || values[key] == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(values[key]))
}
func codeToken(value string) string {
	var result strings.Builder
	for _, current := range strings.ToLower(strings.TrimSpace(value)) {
		if current >= 'a' && current <= 'z' || current >= '0' && current <= '9' {
			result.WriteRune(current)
		} else if result.Len() > 0 {
			result.WriteByte('_')
		}
	}
	token := strings.Trim(result.String(), "_")
	if token == "" {
		return "unknown"
	}
	return token
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
