// Package linear implements the official Linear delivery-operations Provider.
package linear

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	ConnectorKey    = "delivery_operations"
	ProviderKey     = "linear"
	defaultEndpoint = "https://api.linear.app/graphql"
	responseLimit   = 4 << 20
)

type ListProjectsInput struct {
	PageSize      int    `json:"page_size,omitempty"`
	MaxResults    int    `json:"maxResults,omitempty"`
	Cursor        string `json:"cursor,omitempty"`
	NextPageToken string `json:"nextPageToken,omitempty"`
}
type ListItemsInput struct {
	PageSize       int    `json:"page_size,omitempty"`
	MaxResults     int    `json:"maxResults,omitempty"`
	Cursor         string `json:"cursor,omitempty"`
	NextPageToken  string `json:"nextPageToken,omitempty"`
	ProviderFilter any    `json:"provider_filter,omitempty"`
}
type CreateItemInput struct {
	Fields map[string]any `json:"fields"`
}
type UpdateItemInput struct {
	ItemKey string         `json:"item_key"`
	Fields  map[string]any `json:"fields"`
}
type TransitionItemInput struct {
	ItemKey      string `json:"item_key"`
	TransitionID string `json:"transition_id"`
}
type Response map[string]any

var (
	ListDeliveryProjects   = connector.CallOperation[ListProjectsInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_delivery_projects", ContractSHA256: "044bd4d279359354f6fe310a889682bc10ce2b4b9f5e6e58cf89a4f5a66e202c", Reliability: readReliability()}
	ListDeliveryItems      = connector.CallOperation[ListItemsInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_delivery_items", ContractSHA256: "b3503478e21cbb172628db3b0e0e4eb10afeb50a0c7451f067369dc4fbadb4a6", Reliability: readReliability()}
	CreateDeliveryItem     = connector.CallOperation[CreateItemInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "create_delivery_item", ContractSHA256: "9a097cf1ba8cda7547fc06582abe5babfc4a4f7a7b58d8d824fc436fdc36cbd6", Reliability: writeReliability()}
	UpdateDeliveryItem     = connector.CallOperation[UpdateItemInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "update_delivery_item", ContractSHA256: "b076f0651614ba18b76b0a70c89e7672495ba0bb0b52f23014e353f7c1666630", Reliability: writeReliability()}
	TransitionDeliveryItem = connector.CallOperation[TransitionItemInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "transition_delivery_item", ContractSHA256: "5d455d95a0b8a58ce17bb61ff1f971ba648863c67de0c3b6bde8424f0e8d150e", Reliability: writeReliability()}
	TestConnection         = connector.CallOperation[struct{}, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection", ContractSHA256: "e105356cce844443110d0fe5f4b4ab76bb3dc52bff4d7abf2147f02fffb0df9f", Reliability: readReliability()}
)

func readReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}
func writeReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNone}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Linear transport is required")
	}
	p := &provider{transport: transport}
	projects, err := connector.BindCall(ListDeliveryProjects, p.listProjects)
	if err != nil {
		return nil, err
	}
	items, err := connector.BindCall(ListDeliveryItems, p.listItems)
	if err != nil {
		return nil, err
	}
	create, err := connector.BindCall(CreateDeliveryItem, p.createItem)
	if err != nil {
		return nil, err
	}
	update, err := connector.BindCall(UpdateDeliveryItem, p.updateItem)
	if err != nil {
		return nil, err
	}
	transition, err := connector.BindCall(TransitionDeliveryItem, p.transitionItem)
	if err != nil {
		return nil, err
	}
	test, err := connector.BindCall(TestConnection, p.callTestConnection)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), projects, items, create, update, transition, test)
	if err != nil {
		return nil, err
	}
	p.Adapter = bound
	return p, nil
}

func schema() connector.ProviderSchema {
	minimum, maximum := float64(1), float64(120)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "endpoint", Name: "Linear GraphQL endpoint", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://api.linear.app/graphql"`)}, {Key: "team_id", Name: "Default team ID", Type: connector.ConfigFieldText}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}}}, SecretFields: []connector.SecretField{{Key: "api_token", Name: "Linear API or OAuth token", Required: true, CredentialKind: connector.SecretCredentialAPIKey, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}, {Key: "webhook_secret", Name: "Webhook signing secret", CredentialKind: connector.SecretCredentialSigningSecret, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	parsed, err := url.Parse(endpoint(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return permanent("endpoint_invalid", "valid Linear GraphQL endpoint is required")
	}
	if !(parsed.Scheme == "http" && isLoopback(parsed.Hostname())) && (parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), "api.linear.app") || strings.TrimRight(parsed.EscapedPath(), "/") != "/graphql") {
		return permanent("endpoint_invalid", "official Linear GraphQL endpoint or loopback HTTP is required")
	}
	if seconds := intValue(connection.Config["timeout_seconds"], 30); seconds < 1 || seconds > 120 {
		return permanent("timeout_invalid", "timeout_seconds must be between 1 and 120")
	}
	return nil
}

func (p *provider) listProjects(ctx context.Context, request connector.TypedRequest[ListProjectsInput]) (connector.TypedResult[Response], error) {
	first := firstPositive(request.Input.PageSize, request.Input.MaxResults, 50)
	variables := map[string]any{"first": first, "after": nullable(firstString(request.Input.Cursor, request.Input.NextPageToken))}
	return p.execute(ctx, request.Connection, request.Secrets, `query Projects($first:Int,$after:String){projects(first:$first,after:$after){nodes{id name state progress updatedAt} pageInfo{hasNextPage endCursor}}}`, variables, false)
}
func (p *provider) listItems(ctx context.Context, request connector.TypedRequest[ListItemsInput]) (connector.TypedResult[Response], error) {
	first := firstPositive(request.Input.PageSize, request.Input.MaxResults, 50)
	variables := map[string]any{"first": first, "after": nullable(firstString(request.Input.Cursor, request.Input.NextPageToken)), "filter": nullable(request.Input.ProviderFilter)}
	return p.execute(ctx, request.Connection, request.Secrets, `query Issues($first:Int,$after:String,$filter:IssueFilter){issues(first:$first,after:$after,filter:$filter){nodes{id identifier title description priority state{id name} project{id name} assignee{id name} updatedAt} pageInfo{hasNextPage endCursor}}}`, variables, false)
}
func (p *provider) createItem(ctx context.Context, request connector.TypedRequest[CreateItemInput]) (connector.TypedResult[Response], error) {
	input := cloneMap(request.Input.Fields)
	if input == nil {
		input = map[string]any{}
	}
	if mapString(input, "teamId") == "" {
		if team := config(request.Connection, "team_id", ""); team != "" {
			input["teamId"] = team
		}
	}
	if mapString(input, "teamId") == "" || mapString(input, "title") == "" {
		return connector.TypedResult[Response]{}, permanent("input_required", "teamId and title are required")
	}
	return p.execute(ctx, request.Connection, request.Secrets, `mutation Create($input:IssueCreateInput!){issueCreate(input:$input){success issue{id identifier title state{id name}}}}`, map[string]any{"input": input}, true)
}
func (p *provider) updateItem(ctx context.Context, request connector.TypedRequest[UpdateItemInput]) (connector.TypedResult[Response], error) {
	if strings.TrimSpace(request.Input.ItemKey) == "" || request.Input.Fields == nil {
		return connector.TypedResult[Response]{}, permanent("update_fields_required", "item_key and fields are required")
	}
	return p.execute(ctx, request.Connection, request.Secrets, `mutation Update($id:String!,$input:IssueUpdateInput!){issueUpdate(id:$id,input:$input){success issue{id identifier title state{id name}}}}`, map[string]any{"id": request.Input.ItemKey, "input": cloneMap(request.Input.Fields)}, true)
}
func (p *provider) transitionItem(ctx context.Context, request connector.TypedRequest[TransitionItemInput]) (connector.TypedResult[Response], error) {
	if strings.TrimSpace(request.Input.ItemKey) == "" || strings.TrimSpace(request.Input.TransitionID) == "" {
		return connector.TypedResult[Response]{}, permanent("transition_fields_required", "item_key and transition_id are required")
	}
	return p.execute(ctx, request.Connection, request.Secrets, `mutation Transition($id:String!,$input:IssueUpdateInput!){issueUpdate(id:$id,input:$input){success issue{id identifier state{id name}}}}`, map[string]any{"id": request.Input.ItemKey, "input": map[string]any{"stateId": request.Input.TransitionID}}, true)
}
func (p *provider) callTestConnection(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	return p.execute(ctx, request.Connection, request.Secrets, `query Viewer { viewer { id name email } }`, nil, false)
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.callTestConnection(ctx, connector.TypedRequest[struct{}]{Connection: request.Connection, Secrets: request.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, query string, variables map[string]any, write bool) (connector.TypedResult[Response], error) {
	if err := p.ValidateConfig(connection); err != nil {
		return connector.TypedResult[Response]{}, err
	}
	token := strings.TrimSpace(secrets["api_token"])
	if token == "" {
		return connector.TypedResult[Response]{}, permanent("api_token_required", "resolved Linear API token is required")
	}
	raw, err := json.Marshal(map[string]any{"query": query, "variables": variables})
	if err != nil {
		return connector.TypedResult[Response]{}, permanent("request_invalid", "Linear GraphQL request is invalid")
	}
	authorization := token
	if !strings.HasPrefix(token, "lin_api_") {
		authorization = "Bearer " + token
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodPost, URL: endpoint(connection), Headers: map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/json"}}, SecretHeaders: map[string][]string{"Authorization": {authorization}}, Body: raw, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		if write {
			return connector.TypedResult[Response]{}, connector.UncertainError("linear.network_error", transportErr)
		}
		return connector.TypedResult[Response]{}, connector.RetryableError("linear.network_error", transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := Response{}
	valid := len(response.Body) == 0 || json.Unmarshal(response.Body, &payload) == nil
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("Linear returned HTTP %d", response.StatusCode)
		code := fmt.Sprintf("linear.http_%d", response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests {
			return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, connector.RetryableError(code, cause)
		}
		if response.StatusCode == http.StatusRequestTimeout || response.StatusCode >= 500 {
			if write {
				return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, connector.UncertainError(code, cause)
			}
			return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, connector.RetryableError(code, cause)
		}
		return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, connector.PermanentError(code, cause)
	}
	if !valid {
		if write {
			return connector.TypedResult[Response]{ResponseRef: ref}, connector.UncertainError("linear.response_invalid", errors.New("Linear response is invalid JSON"))
		}
		return connector.TypedResult[Response]{ResponseRef: ref}, permanent("response_invalid", "Linear response is invalid JSON")
	}
	if errorsList, ok := payload["errors"].([]any); ok && len(errorsList) > 0 {
		if graphQLErrorCode(errorsList) == "RATELIMITED" {
			return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, connector.RetryableError("linear.rate_limited", errors.New("Linear rate limited the request"))
		}
		return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, permanent("graphql_error", "Linear rejected the GraphQL operation")
	}
	if id := linearRef(payload); id != "" {
		ref = "linear:" + id
	}
	return connector.TypedResult[Response]{Output: payload, ResponseRef: ref}, nil
}

func (p *provider) VerifyWebhook(ctx context.Context, request connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	if err := ctx.Err(); err != nil {
		return connector.VerifiedWebhook{}, err
	}
	secret := strings.TrimSpace(request.Secrets["webhook_secret"])
	if secret == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_secret_required", "resolved Linear webhook secret is required")
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(request.Body)
	expected := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(strings.ToLower(headerValue(request.Headers, "Linear-Signature")))) {
		return connector.VerifiedWebhook{}, permanent("webhook_signature_invalid", "Linear webhook signature does not match")
	}
	payload := Response{}
	if json.Unmarshal(request.Body, &payload) != nil {
		return connector.VerifiedWebhook{}, permanent("webhook_payload_invalid", "Linear webhook payload is invalid JSON")
	}
	timestamp := int64(intValue(payload["webhookTimestamp"], 0))
	receivedAt := request.ReceivedAt
	if receivedAt.IsZero() {
		receivedAt = time.Now().UTC()
	}
	eventTime := time.UnixMilli(timestamp)
	if timestamp <= 0 || receivedAt.Sub(eventTime).Abs() > time.Minute {
		return connector.VerifiedWebhook{}, permanent("webhook_timestamp_invalid", "Linear webhook timestamp is outside tolerance")
	}
	action, eventType := strings.ToLower(mapString(payload, "action")), strings.ToLower(mapString(payload, "type"))
	data, _ := payload["data"].(map[string]any)
	subject := mapString(data, "id")
	delivery := headerValue(request.Headers, "Linear-Delivery")
	if action == "" || eventType == "" || subject == "" || delivery == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_identity_missing", "Linear webhook event identity is missing")
	}
	return connector.VerifiedWebhook{EventType: eventType + "." + action, ExternalID: delivery, ExternalIdentity: &connector.WebhookExternalIdentity{Subject: subject, SubjectType: eventType}, Payload: append(json.RawMessage(nil), request.Body...), Security: &connector.WebhookSecurityEvidence{SignatureVerified: true, EventTime: eventTime.UTC()}}, nil
}

func graphQLErrorCode(items []any) string {
	first := ""
	for _, raw := range items {
		item, _ := raw.(map[string]any)
		extensions, _ := item["extensions"].(map[string]any)
		if code := strings.ToUpper(mapString(extensions, "code")); code != "" {
			if code == "RATELIMITED" {
				return code
			}
			if first == "" {
				first = code
			}
		}
	}
	return first
}
func linearRef(payload map[string]any) string {
	data, _ := payload["data"].(map[string]any)
	for _, key := range []string{"issueCreate", "issueUpdate"} {
		mutation, _ := data[key].(map[string]any)
		issue, _ := mutation["issue"].(map[string]any)
		if identifier := mapString(issue, "identifier"); identifier != "" {
			return identifier
		}
		if id := mapString(issue, "id"); id != "" {
			return id
		}
	}
	return ""
}
func cloneMap(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	target := make(map[string]any, len(source))
	for key, value := range source {
		target[key] = value
	}
	return target
}
func nullable(value any) any {
	if value == nil || strings.TrimSpace(fmt.Sprint(value)) == "" {
		return nil
	}
	return value
}
func firstPositive(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}
func firstString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
func endpoint(connection connector.Connection) string {
	return config(connection, "endpoint", defaultEndpoint)
}
func config(connection connector.Connection, key, fallback string) string {
	if value := mapString(connection.Config, key); value != "" {
		return value
	}
	return fallback
}
func mapString(values map[string]any, key string) string {
	if values == nil {
		return ""
	}
	value, _ := values[key].(string)
	return strings.TrimSpace(value)
}
func intValue(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		result, err := strconv.Atoi(typed.String())
		if err == nil {
			return result
		}
	}
	return fallback
}
func headerValue(headers map[string][]string, key string) string {
	for candidate, values := range headers {
		if strings.EqualFold(candidate, key) && len(values) > 0 {
			return strings.TrimSpace(values[0])
		}
	}
	return ""
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
func permanent(code, message string) error {
	return connector.PermanentError("linear."+code, errors.New(message))
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
var _ connector.WebhookVerifier = (*provider)(nil)
