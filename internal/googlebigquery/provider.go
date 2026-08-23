// Package googlebigquery contains the private implementation shared by the
// public BigQuery database and analytics-warehouse Providers.
package googlebigquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"unicode"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	defaultBaseURL = "https://bigquery.googleapis.com"
	responseLimit  = 8 << 20
)

type Identity struct {
	ConnectorKey, ProviderKey                           string
	ListTablesHash, SampleQueryHash, TestConnectionHash string
}

type ListTablesInput struct {
	DatasetID string `json:"dataset_id,omitempty"`
	Schema    string `json:"schema,omitempty"`
	MaxRows   int    `json:"max_rows,omitempty"`
}
type SampleQueryInput struct {
	Query     string `json:"query"`
	DatasetID string `json:"dataset_id,omitempty"`
	Schema    string `json:"schema,omitempty"`
	MaxRows   int    `json:"max_rows,omitempty"`
}
type Response map[string]any

type provider struct {
	connector.Adapter
	transport connector.Transport
	identity  Identity
}

func New(identity Identity, transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("BigQuery transport is required")
	}
	if strings.TrimSpace(identity.ConnectorKey) == "" || strings.TrimSpace(identity.ProviderKey) == "" {
		return nil, errors.New("BigQuery Provider identity is required")
	}
	p := &provider{transport: transport, identity: identity}
	reliability := connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
	list := connector.CallOperation[ListTablesInput, Response]{ConnectorKey: identity.ConnectorKey, ProviderKey: identity.ProviderKey, Key: "list_tables", ContractSHA256: identity.ListTablesHash, Reliability: reliability}
	query := connector.CallOperation[SampleQueryInput, Response]{ConnectorKey: identity.ConnectorKey, ProviderKey: identity.ProviderKey, Key: "sample_query", ContractSHA256: identity.SampleQueryHash, Reliability: reliability}
	test := connector.CallOperation[struct{}, Response]{ConnectorKey: identity.ConnectorKey, ProviderKey: identity.ProviderKey, Key: "test_connection", ContractSHA256: identity.TestConnectionHash, Reliability: reliability}
	listBound, err := connector.BindCall(list, p.listTables)
	if err != nil {
		return nil, err
	}
	queryBound, err := connector.BindCall(query, p.sampleQuery)
	if err != nil {
		return nil, err
	}
	testBound, err := connector.BindCall(test, p.callTestConnection)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(p.schema(), listBound, queryBound, testBound)
	if err != nil {
		return nil, err
	}
	p.Adapter = bound
	return p, nil
}

func (p *provider) schema() connector.ProviderSchema {
	minRows, maxRows, minTimeout, maxTimeout := float64(1), float64(1000), float64(1), float64(300)
	return connector.ProviderSchema{ConnectorKey: p.identity.ConnectorKey, ProviderKey: p.identity.ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{
		{Key: "project_id", Name: "Google Cloud project ID", Type: connector.ConfigFieldText, Required: true},
		{Key: "dataset_id", Name: "Default BigQuery dataset ID", Type: connector.ConfigFieldText},
		{Key: "location", Name: "BigQuery job location", Type: connector.ConfigFieldText},
		{Key: "base_url", Name: "BigQuery API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://bigquery.googleapis.com"`)},
		{Key: "readonly", Name: "Read only", Type: connector.ConfigFieldBoolean, Required: true, Default: json.RawMessage(`true`)},
		{Key: "max_rows", Name: "Maximum rows", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`100`), Validation: connector.ConfigValidation{Min: &minRows, Max: &maxRows}},
		{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minTimeout, Max: &maxTimeout}},
	}, SecretFields: []connector.SecretField{{Key: "access_token", Name: "Google OAuth access token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationOAuthRefresh, ExpiryPolicy: connector.SecretExpiryRequired, TestRequirement: connector.SecretTestWhenBound}}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	if projectID(connection) == "" {
		return p.permanent("project_id_required", "project_id is required")
	}
	if value, exists := connection.Config["readonly"]; exists && !boolValue(value, true) {
		return p.permanent("readonly_required", "readonly must be true")
	}
	if timeout := intValue(connection.Config["timeout_seconds"], 30); timeout < 1 || timeout > 300 {
		return p.permanent("timeout_invalid", "timeout_seconds must be between 1 and 300")
	}
	parsed, err := url.Parse(baseURL(connection))
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return p.permanent("endpoint_invalid", "valid BigQuery endpoint is required")
	}
	if parsed.Scheme == "http" && isLoopback(parsed.Hostname()) {
		return nil
	}
	if parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), "bigquery.googleapis.com") || (parsed.EscapedPath() != "" && parsed.EscapedPath() != "/") {
		return p.permanent("endpoint_invalid", "official BigQuery endpoint or loopback HTTP is required")
	}
	return nil
}

func (p *provider) listTables(ctx context.Context, request connector.TypedRequest[ListTablesInput]) (connector.TypedResult[Response], error) {
	dataset := first(request.Input.DatasetID, request.Input.Schema, config(request.Connection, "dataset_id", ""), config(request.Connection, "schema", ""))
	if dataset == "" {
		return connector.TypedResult[Response]{}, p.permanent("dataset_id_required", "dataset_id is required")
	}
	limit, err := p.maxRows(request.Connection, request.Input.MaxRows)
	if err != nil {
		return connector.TypedResult[Response]{}, err
	}
	path := "/bigquery/v2/projects/" + url.PathEscape(projectID(request.Connection)) + "/datasets/" + url.PathEscape(dataset) + "/tables"
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, path, url.Values{"maxResults": {strconv.Itoa(limit)}}, nil)
	if err != nil {
		return connector.TypedResult[Response]{ResponseRef: ref}, err
	}
	rows := make([]any, 0)
	for _, item := range anySlice(payload["tables"]) {
		table, _ := item.(map[string]any)
		reference, _ := table["tableReference"].(map[string]any)
		rows = append(rows, map[string]any{"project_id": reference["projectId"], "dataset_id": reference["datasetId"], "table_id": reference["tableId"], "type": table["type"]})
	}
	return connector.TypedResult[Response]{Output: Response{"columns": []string{"project_id", "dataset_id", "table_id", "type"}, "rows": rows, "row_count": len(rows), "truncated": payload["nextPageToken"] != nil}, ResponseRef: "bigquery:tables:" + dataset}, nil
}

func (p *provider) sampleQuery(ctx context.Context, request connector.TypedRequest[SampleQueryInput]) (connector.TypedResult[Response], error) {
	if code := validateReadOnlySQL(request.Input.Query); code != "" {
		return connector.TypedResult[Response]{}, p.permanent(code, "query must be one safe read-only statement")
	}
	limit, err := p.maxRows(request.Connection, request.Input.MaxRows)
	if err != nil {
		return connector.TypedResult[Response]{}, err
	}
	body := map[string]any{"query": strings.TrimSpace(request.Input.Query), "useLegacySql": false, "maxResults": limit, "timeoutMs": intValue(request.Connection.Config["timeout_seconds"], 30) * 1000}
	if dataset := first(request.Input.DatasetID, request.Input.Schema, config(request.Connection, "dataset_id", ""), config(request.Connection, "schema", "")); dataset != "" {
		body["defaultDataset"] = map[string]any{"projectId": projectID(request.Connection), "datasetId": dataset}
	}
	if location := config(request.Connection, "location", ""); location != "" {
		body["location"] = location
	}
	path := "/bigquery/v2/projects/" + url.PathEscape(projectID(request.Connection)) + "/queries"
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodPost, path, nil, body)
	if err != nil {
		return connector.TypedResult[Response]{ResponseRef: ref}, err
	}
	if complete, ok := payload["jobComplete"].(bool); ok && !complete {
		return connector.TypedResult[Response]{ResponseRef: ref}, connector.RetryableError(p.code("query_not_complete"), errors.New("BigQuery query job is not complete"))
	}
	columns := queryColumns(payload)
	rows := queryRows(payload, columns)
	return connector.TypedResult[Response]{Output: Response{"columns": columns, "rows": rows, "row_count": len(rows), "truncated": payload["pageToken"] != nil}, ResponseRef: "bigquery:rows:" + strconv.Itoa(len(rows))}, nil
}

func (p *provider) callTestConnection(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	path := "/bigquery/v2/projects/" + url.PathEscape(projectID(request.Connection)) + "/datasets"
	payload, ref, err := p.execute(ctx, request.Connection, request.Secrets, http.MethodGet, path, url.Values{"maxResults": {"1"}}, nil)
	if err != nil {
		return connector.TypedResult[Response]{ResponseRef: ref}, err
	}
	return connector.TypedResult[Response]{Output: Response{"connected": true, "project_id": projectID(request.Connection), "dataset_count": len(anySlice(payload["datasets"])), "readonly": true}, ResponseRef: "bigquery:" + projectID(request.Connection)}, nil
}

func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.callTestConnection(ctx, connector.TypedRequest[struct{}]{Connection: request.Connection, Secrets: request.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, method, path string, query url.Values, body map[string]any) (Response, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", err
	}
	token := strings.TrimSpace(secrets["access_token"])
	if token == "" {
		return nil, "", p.permanent("access_token_required", "resolved Google OAuth access token is required")
	}
	endpoint, err := url.Parse(baseURL(connection) + path)
	if err != nil {
		return nil, "", p.permanent("request_invalid", "BigQuery request URL is invalid")
	}
	endpoint.RawQuery = query.Encode()
	var raw []byte
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return nil, "", p.permanent("request_invalid", "BigQuery request body is invalid")
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}, "User-Agent": {"domainry-bigquery-connector/1.0"}}
	if body != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint.String(), Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, Body: raw, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		return nil, "", connector.RetryableError(p.code("network_error"), transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := Response{}
	if len(response.Body) > 0 && json.Unmarshal(response.Body, &payload) != nil {
		return nil, ref, p.permanent("response_invalid", "BigQuery response is invalid JSON")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		code := p.code("http_" + strconv.Itoa(response.StatusCode))
		if response.StatusCode == http.StatusRequestTimeout || response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			return payload, ref, connector.RetryableError(code, cause)
		}
		return payload, ref, connector.PermanentError(code, cause)
	}
	return payload, ref, nil
}

func (p *provider) maxRows(connection connector.Connection, input int) (int, error) {
	value := input
	if value == 0 {
		value = intValue(connection.Config["max_rows"], 100)
	}
	if value < 1 || value > 1000 {
		return 0, p.permanent("max_rows_invalid", "max_rows must be between 1 and 1000")
	}
	return value, nil
}
func (p *provider) code(suffix string) string {
	return p.identity.ConnectorKey + "." + p.identity.ProviderKey + "." + suffix
}
func (p *provider) permanent(suffix, message string) error {
	return connector.PermanentError(p.code(suffix), errors.New(message))
}
func projectID(connection connector.Connection) string {
	return first(config(connection, "project_id", ""), config(connection, "database", ""))
}
func baseURL(connection connector.Connection) string {
	if value := strings.TrimRight(config(connection, "base_url", ""), "/"); value != "" {
		return value
	}
	return defaultBaseURL
}
func config(connection connector.Connection, key, fallback string) string {
	if value := mapString(connection.Config, key); value != "" {
		return value
	}
	return fallback
}
func mapString(values map[string]any, key string) string {
	if values[key] == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(values[key]))
}
func first(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
func intValue(value any, fallback int) int {
	if value == nil {
		return fallback
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(fmt.Sprint(value)))
	if err != nil {
		return 0
	}
	return parsed
}
func boolValue(value any, fallback bool) bool {
	if value == nil {
		return fallback
	}
	parsed, err := strconv.ParseBool(strings.TrimSpace(fmt.Sprint(value)))
	if err != nil {
		return false
	}
	return parsed
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
func anySlice(value any) []any { items, _ := value.([]any); return items }
func queryColumns(payload map[string]any) []string {
	schema, _ := payload["schema"].(map[string]any)
	fields := anySlice(schema["fields"])
	columns := make([]string, 0, len(fields))
	for _, item := range fields {
		field, ok := item.(map[string]any)
		if ok && mapString(field, "name") != "" {
			columns = append(columns, mapString(field, "name"))
		}
	}
	return columns
}
func queryRows(payload map[string]any, columns []string) []any {
	result := make([]any, 0)
	for _, item := range anySlice(payload["rows"]) {
		row, ok := item.(map[string]any)
		if !ok {
			continue
		}
		cells := anySlice(row["f"])
		mapped := map[string]any{}
		for i, column := range columns {
			if i < len(cells) {
				cell, _ := cells[i].(map[string]any)
				mapped[column] = cell["v"]
			}
		}
		result = append(result, mapped)
	}
	return result
}

func validateReadOnlySQL(query string) string {
	query = strings.TrimSpace(query)
	if query == "" || strings.ContainsRune(query, '\x00') || strings.Contains(query, "--") || strings.Contains(query, "/*") || strings.Contains(query, "*/") {
		return "query_unsafe"
	}
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	if strings.Contains(trimmed, ";") {
		return "multiple_statements_denied"
	}
	words := sqlWords(trimmed)
	if len(words) == 0 || (words[0] != "select" && words[0] != "with" && words[0] != "explain") {
		return "readonly_query_required"
	}
	denied := map[string]bool{"alter": true, "call": true, "create": true, "delete": true, "drop": true, "export": true, "grant": true, "insert": true, "load": true, "merge": true, "revoke": true, "truncate": true, "update": true}
	for _, word := range words {
		if denied[word] {
			return "write_keyword_denied"
		}
	}
	return ""
}
func sqlWords(query string) []string {
	words := []string{}
	var current strings.Builder
	quote := rune(0)
	flush := func() {
		if current.Len() > 0 {
			words = append(words, strings.ToLower(current.String()))
			current.Reset()
		}
	}
	for _, char := range query {
		if quote != 0 {
			if char == quote {
				quote = 0
			}
			continue
		}
		if char == '\'' || char == '"' || char == '`' {
			flush()
			quote = char
			continue
		}
		if unicode.IsLetter(char) || char == '_' {
			current.WriteRune(char)
		} else {
			flush()
		}
	}
	flush()
	return words
}
