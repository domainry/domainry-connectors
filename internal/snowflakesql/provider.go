// Package snowflakesql contains the private Snowflake SQL API implementation
// shared by the public database and analytics-warehouse Providers.
package snowflakesql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
	"unicode"

	connector "github.com/domainry/domainry-connector-sdk"
)

const responseLimit = 8 << 20

type Identity struct {
	ConnectorKey, ProviderKey                           string
	ListTablesHash, SampleQueryHash, TestConnectionHash string
}
type ListTablesInput struct {
	MaxRows int `json:"max_rows,omitempty"`
}
type SampleQueryInput struct {
	Query   string `json:"query"`
	MaxRows int    `json:"max_rows,omitempty"`
}
type Response map[string]any

type provider struct {
	connector.Adapter
	transport connector.Transport
	identity  Identity
}

func New(identity Identity, transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Snowflake transport is required")
	}
	if strings.TrimSpace(identity.ConnectorKey) == "" || strings.TrimSpace(identity.ProviderKey) == "" {
		return nil, errors.New("Snowflake Provider identity is required")
	}
	p := &provider{transport: transport, identity: identity}
	reliability := connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
	list := connector.CallOperation[ListTablesInput, Response]{ConnectorKey: identity.ConnectorKey, ProviderKey: identity.ProviderKey, Key: "list_tables", ContractSHA256: identity.ListTablesHash, Reliability: reliability}
	sample := connector.CallOperation[SampleQueryInput, Response]{ConnectorKey: identity.ConnectorKey, ProviderKey: identity.ProviderKey, Key: "sample_query", ContractSHA256: identity.SampleQueryHash, Reliability: reliability}
	test := connector.CallOperation[struct{}, Response]{ConnectorKey: identity.ConnectorKey, ProviderKey: identity.ProviderKey, Key: "test_connection", ContractSHA256: identity.TestConnectionHash, Reliability: reliability}
	a, err := connector.BindCall(list, p.listTables)
	if err != nil {
		return nil, err
	}
	b, err := connector.BindCall(sample, p.sampleQuery)
	if err != nil {
		return nil, err
	}
	c, err := connector.BindCall(test, p.callTestConnection)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(p.schema(), a, b, c)
	if err != nil {
		return nil, err
	}
	p.Adapter = bound
	return p, nil
}

func (p *provider) schema() connector.ProviderSchema {
	minRows, maxRows, minTimeout, maxTimeout := float64(1), float64(1000), float64(1), float64(300)
	return connector.ProviderSchema{ConnectorKey: p.identity.ConnectorKey, ProviderKey: p.identity.ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{
		{Key: "account", Name: "Snowflake account identifier", Type: connector.ConfigFieldText, Required: true}, {Key: "database", Name: "Database", Type: connector.ConfigFieldText}, {Key: "schema", Name: "Schema", Type: connector.ConfigFieldText}, {Key: "warehouse", Name: "Warehouse", Type: connector.ConfigFieldText}, {Key: "role", Name: "Role", Type: connector.ConfigFieldText}, {Key: "token_type", Name: "Authorization token type", Type: connector.ConfigFieldSelect, Validation: connector.ConfigValidation{Options: []string{"OAUTH", "KEYPAIR_JWT", "PROGRAMMATIC_ACCESS_TOKEN"}}}, {Key: "base_url", Name: "Snowflake SQL API base URL", Type: connector.ConfigFieldText}, {Key: "readonly", Name: "Read only", Type: connector.ConfigFieldBoolean, Required: true, Default: json.RawMessage(`true`)}, {Key: "max_rows", Name: "Maximum rows", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`100`), Validation: connector.ConfigValidation{Min: &minRows, Max: &maxRows}}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minTimeout, Max: &maxTimeout}},
	}, SecretFields: []connector.SecretField{{Key: "token", Name: "Snowflake authentication token or JWT", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	if account(connection) == "" && config(connection, "base_url", "") == "" {
		return p.permanent("account_required", "account is required when base_url is not set")
	}
	if value, exists := connection.Config["readonly"]; exists && !boolValue(value, true) {
		return p.permanent("readonly_required", "readonly must be true")
	}
	switch tokenType(connection) {
	case "", "OAUTH", "KEYPAIR_JWT", "PROGRAMMATIC_ACCESS_TOKEN":
	default:
		return p.permanent("token_type_invalid", "token_type is unsupported")
	}
	if timeout := intValue(connection.Config["timeout_seconds"], 30); timeout < 1 || timeout > 300 {
		return p.permanent("timeout_invalid", "timeout_seconds must be between 1 and 300")
	}
	_, err := p.endpoint(connection)
	return err
}

func (p *provider) listTables(ctx context.Context, request connector.TypedRequest[ListTablesInput]) (connector.TypedResult[Response], error) {
	return p.query(ctx, request.Connection, request.Secrets, "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_SCHEMA, TABLE_NAME", request.Input.MaxRows)
}
func (p *provider) sampleQuery(ctx context.Context, request connector.TypedRequest[SampleQueryInput]) (connector.TypedResult[Response], error) {
	if code := validateReadOnlySQL(request.Input.Query); code != "" {
		return connector.TypedResult[Response]{}, p.permanent(code, "query must be one safe read-only statement")
	}
	return p.query(ctx, request.Connection, request.Secrets, strings.TrimSpace(request.Input.Query), request.Input.MaxRows)
}
func (p *provider) query(ctx context.Context, connection connector.Connection, secrets map[string]string, statement string, inputLimit int) (connector.TypedResult[Response], error) {
	limit, err := p.maxRows(connection, inputLimit)
	if err != nil {
		return connector.TypedResult[Response]{}, err
	}
	payload, ref, err := p.executeStatement(ctx, connection, secrets, statement)
	if err != nil {
		return connector.TypedResult[Response]{ResponseRef: ref}, err
	}
	columns := resultColumns(payload)
	rows := resultRows(payload)
	truncated := len(rows) > limit || hasMorePartitions(payload)
	if len(rows) > limit {
		rows = rows[:limit]
	}
	return connector.TypedResult[Response]{Output: Response{"columns": columns, "rows": rows, "row_count": len(rows), "truncated": truncated}, ResponseRef: "snowflake:rows:" + strconv.Itoa(len(rows))}, nil
}
func (p *provider) callTestConnection(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	payload, ref, err := p.executeStatement(ctx, request.Connection, request.Secrets, "SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE()")
	if err != nil {
		return connector.TypedResult[Response]{ResponseRef: ref}, err
	}
	response := Response{"connected": true, "readonly": true}
	rows := resultRows(payload)
	if len(rows) > 0 {
		row, _ := rows[0].(map[string]any)
		response["account"], response["user"], response["role"] = row["CURRENT_ACCOUNT()"], row["CURRENT_USER()"], row["CURRENT_ROLE()"]
	}
	return connector.TypedResult[Response]{Output: response, ResponseRef: "snowflake:" + account(request.Connection)}, nil
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.callTestConnection(ctx, connector.TypedRequest[struct{}]{Connection: request.Connection, Secrets: request.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) executeStatement(ctx context.Context, connection connector.Connection, secrets map[string]string, statement string) (Response, string, error) {
	body := map[string]any{"statement": statement, "timeout": intValue(connection.Config["timeout_seconds"], 30)}
	for _, key := range []string{"database", "schema", "warehouse", "role"} {
		if value := config(connection, key, ""); value != "" {
			body[key] = value
		}
	}
	payload, status, ref, err := p.request(ctx, connection, secrets, http.MethodPost, "/api/v2/statements", body)
	if err != nil {
		return nil, ref, err
	}
	for status == http.StatusAccepted {
		statusPath, pathErr := p.statusURLPath(payload["statementStatusUrl"])
		if pathErr != nil {
			return nil, ref, pathErr
		}
		if err = waitForPoll(ctx); err != nil {
			return nil, ref, err
		}
		payload, status, ref, err = p.request(ctx, connection, secrets, http.MethodGet, statusPath, nil)
		if err != nil {
			return nil, ref, err
		}
	}
	return payload, ref, nil
}
func (p *provider) request(ctx context.Context, connection connector.Connection, secrets map[string]string, method, requestPath string, body map[string]any) (Response, int, string, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, 0, "", err
	}
	token := strings.TrimSpace(secrets["token"])
	if token == "" {
		return nil, 0, "", p.permanent("token_required", "resolved Snowflake authentication token is required")
	}
	base, err := p.endpoint(connection)
	if err != nil {
		return nil, 0, "", err
	}
	var raw []byte
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return nil, 0, "", p.permanent("request_invalid", "Snowflake request body is invalid")
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/json"}, "User-Agent": {"domainry-snowflake-connector/1.0"}}
	if value := tokenType(connection); value != "" {
		headers["X-Snowflake-Authorization-Token-Type"] = []string{value}
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: base + requestPath, Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, Body: raw, MaxResponseBytes: responseLimit})
	if transportErr != nil {
		return nil, 0, "", connector.RetryableError(p.code("network_error"), transportErr)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	payload := Response{}
	if len(response.Body) > 0 && json.Unmarshal(response.Body, &payload) != nil {
		return nil, response.StatusCode, ref, p.permanent("response_invalid", "Snowflake response is invalid JSON")
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusAccepted {
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		code := p.code("http_" + strconv.Itoa(response.StatusCode))
		if response.StatusCode == http.StatusRequestTimeout || response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			return payload, response.StatusCode, ref, connector.RetryableError(code, cause)
		}
		return payload, response.StatusCode, ref, connector.PermanentError(code, cause)
	}
	return payload, response.StatusCode, ref, nil
}

func (p *provider) endpoint(connection connector.Connection) (string, error) {
	raw := strings.TrimRight(config(connection, "base_url", ""), "/")
	if raw == "" {
		raw = "https://" + account(connection) + ".snowflakecomputing.com"
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return "", p.permanent("endpoint_invalid", "valid Snowflake endpoint is required")
	}
	if parsed.Scheme == "http" && isLoopback(parsed.Hostname()) {
		return raw, nil
	}
	host := strings.ToLower(parsed.Hostname())
	if parsed.Scheme != "https" || !strings.HasSuffix(host, ".snowflakecomputing.com") || host == "snowflakecomputing.com" || (parsed.EscapedPath() != "" && parsed.EscapedPath() != "/") {
		return "", p.permanent("endpoint_invalid", "official Snowflake endpoint or loopback HTTP is required")
	}
	return raw, nil
}
func (p *provider) statusURLPath(value any) (string, error) {
	raw := strings.TrimSpace(fmt.Sprint(value))
	parsed, err := url.Parse(raw)
	if err != nil || raw == "" || raw == "<nil>" || parsed.IsAbs() || parsed.Host != "" || !strings.HasPrefix(parsed.Path, "/api/v2/statements/") || path.Clean(parsed.Path) != parsed.Path {
		return "", p.permanent("status_url_invalid", "Snowflake statement status URL is invalid")
	}
	return parsed.RequestURI(), nil
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
func account(connection connector.Connection) string {
	return first(config(connection, "account", ""), config(connection, "host", ""))
}
func tokenType(connection connector.Connection) string {
	return strings.ToUpper(config(connection, "token_type", ""))
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
func waitForPoll(ctx context.Context) error {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func resultColumns(payload map[string]any) []string {
	metadata, _ := payload["resultSetMetaData"].(map[string]any)
	items, _ := metadata["rowType"].([]any)
	columns := make([]string, 0, len(items))
	for _, item := range items {
		column, ok := item.(map[string]any)
		if ok {
			columns = append(columns, mapString(column, "name"))
		}
	}
	return columns
}
func resultRows(payload map[string]any) []any {
	columns := resultColumns(payload)
	data, _ := payload["data"].([]any)
	rows := make([]any, 0, len(data))
	for _, item := range data {
		values, ok := item.([]any)
		if !ok {
			continue
		}
		row := map[string]any{}
		for i, column := range columns {
			if i < len(values) {
				row[column] = values[i]
			}
		}
		rows = append(rows, row)
	}
	return rows
}
func hasMorePartitions(payload map[string]any) bool {
	metadata, _ := payload["resultSetMetaData"].(map[string]any)
	partitions, _ := metadata["partitionInfo"].([]any)
	return len(partitions) > 1
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
	allowed := map[string]bool{"select": true, "with": true, "show": true, "describe": true, "explain": true}
	if len(words) == 0 || !allowed[words[0]] {
		return "readonly_query_required"
	}
	denied := map[string]bool{"alter": true, "call": true, "copy": true, "create": true, "delete": true, "drop": true, "execute": true, "grant": true, "insert": true, "merge": true, "put": true, "remove": true, "revoke": true, "truncate": true, "undrop": true, "update": true, "use": true}
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
