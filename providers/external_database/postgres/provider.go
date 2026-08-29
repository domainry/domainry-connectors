// Package postgres implements the official read-only PostgreSQL Provider.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/internal/databasesql"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	ConnectorKey = "external_database"
	ProviderKey  = "postgres"
)

type SampleQueryInput struct {
	Query   string `json:"query"`
	Args    []any  `json:"args,omitempty"`
	MaxRows int    `json:"max_rows,omitempty"`
}
type ListTablesInput struct {
	MaxRows int `json:"max_rows,omitempty"`
}

var (
	ListTables     = readOp[ListTablesInput]("list_tables", "d03d37a2abbe4b3ee52533969701dfe6cab3521c59307580d31363ef702a14fe")
	SampleQuery    = readOp[SampleQueryInput]("sample_query", "8cdaa65782d29c2bff21b1c26d6f895440443c2c280f9107180f9c1ffc9b2ba2")
	TestConnection = readOp[struct{}]("test_connection", "52de68ed6a73aefa755d209a067f2d0e9697d6840e1fda86cdf9be4ed764b853")
)

func readOp[I any](key, hash string) connector.CallOperation[I, map[string]any] {
	return connector.CallOperation[I, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: key, ContractSHA256: hash, Reliability: connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}}
}

type provider struct {
	connector.Adapter
	database *databasesql.Client
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("PostgreSQL transport is required")
	}
	database, err := databasesql.NewClient(transport, "pgx", ProviderKey)
	if err != nil {
		return nil, err
	}
	p := &provider{database: database}
	list, err := connector.BindCall(ListTables, p.list)
	if err != nil {
		return nil, err
	}
	sample, err := connector.BindCall(SampleQuery, p.sample)
	if err != nil {
		return nil, err
	}
	test, err := connector.BindCall(TestConnection, p.test)
	if err != nil {
		return nil, err
	}
	adapter, err := connector.NewProvider(schema(), list, sample, test)
	if err != nil {
		return nil, err
	}
	p.Adapter = adapter
	return p, nil
}
func schema() connector.ProviderSchema {
	minPort, maxPort, minRows, maxRows, minTimeout, maxTimeout := float64(1), float64(65535), float64(1), float64(1000), float64(1), float64(300)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "host", Name: "Host", Type: connector.ConfigFieldText}, {Key: "port", Name: "Port", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`5432`), Validation: connector.ConfigValidation{Min: &minPort, Max: &maxPort}}, {Key: "database", Name: "Database", Type: connector.ConfigFieldText}, {Key: "schema", Name: "Schema", Type: connector.ConfigFieldText}, {Key: "ssl_mode", Name: "SSL Mode", Type: connector.ConfigFieldSelect, Default: json.RawMessage(`"require"`), Validation: connector.ConfigValidation{Options: []string{"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}}}, {Key: "readonly", Name: "Read Only", Type: connector.ConfigFieldBoolean, Required: true, Default: json.RawMessage(`true`)}, {Key: "timeout_seconds", Name: "Timeout Seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`15`), Validation: connector.ConfigValidation{Min: &minTimeout, Max: &maxTimeout}}, {Key: "max_rows", Name: "Maximum Rows", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`100`), Validation: connector.ConfigValidation{Min: &minRows, Max: &maxRows}}}, SecretFields: []connector.SecretField{secret("connection_string", "Connection String"), secret("username", "Username"), secret("password", "Password")}}
}
func secret(key, name string) connector.SecretField {
	return connector.SecretField{Key: key, Name: name, CredentialKind: connector.SecretCredentialDatabasePassword, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	if !boolValue(connection.Config["readonly"], true) {
		return permanent("readonly_required", "readonly must remain enabled")
	}
	if ref(connection, "connection_string") != "" {
		return nil
	}
	if config(connection, "host") == "" || config(connection, "database") == "" {
		return permanent("host_database_required", "host and database are required")
	}
	if ref(connection, "username") == "" {
		return permanent("username_required", "username is required")
	}
	port := intValue(connection.Config["port"], 5432)
	if port < 1 || port > 65535 {
		return permanent("port_invalid", "port is invalid")
	}
	if !validSSL(configDefault(connection, "ssl_mode", "require")) {
		return permanent("ssl_mode_invalid", "ssl_mode is invalid")
	}
	return nil
}
func (p *provider) test(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[map[string]any], error) {
	dsn, err := connectionString(r.Connection, r.Secrets)
	if err != nil {
		return empty(), err
	}
	timeout := duration(r.Connection)
	if err = p.database.Ping(ctx, dsn, timeout); err != nil {
		return empty(), err
	}
	result, err := p.database.Query(ctx, dsn, "SELECT current_database(), current_user", nil, 1, timeout, "identity_failed")
	if err != nil {
		return empty(), err
	}
	database, user := "", ""
	if len(result.Rows) > 0 {
		if len(result.Rows[0]) > 0 {
			database = databasesql.Text(result.Rows[0][0])
		}
		if len(result.Rows[0]) > 1 {
			user = databasesql.Text(result.Rows[0][1])
		}
	}
	return connector.TypedResult[map[string]any]{Output: map[string]any{"connected": true, "database": database, "user": user, "readonly": true}, ResponseRef: "postgres:" + database}, nil
}
func (p *provider) TestConnection(ctx context.Context, r connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.test(ctx, connector.TypedRequest[struct{}]{Connection: r.Connection, Secrets: r.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}
func (p *provider) list(ctx context.Context, r connector.TypedRequest[ListTablesInput]) (connector.TypedResult[map[string]any], error) {
	return p.query(ctx, r.Connection, r.Secrets, "SELECT table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY table_schema, table_name", nil, r.Input.MaxRows)
}
func (p *provider) sample(ctx context.Context, r connector.TypedRequest[SampleQueryInput]) (connector.TypedResult[map[string]any], error) {
	query := strings.TrimSpace(r.Input.Query)
	if code := databasesql.ValidateReadOnly(query, postgresPolicy); code != "" {
		return empty(), permanent(code, "query violates read-only policy")
	}
	return p.query(ctx, r.Connection, r.Secrets, query, r.Input.Args, r.Input.MaxRows)
}
func (p *provider) query(ctx context.Context, connection connector.Connection, secrets map[string]string, statement string, args []any, requested int) (connector.TypedResult[map[string]any], error) {
	if err := p.ValidateConfig(connection); err != nil {
		return empty(), err
	}
	limit := requested
	if limit == 0 {
		limit = intValue(connection.Config["max_rows"], 100)
	}
	if limit < 1 || limit > 1000 {
		return empty(), permanent("max_rows_invalid", "max_rows must be between 1 and 1000")
	}
	dsn, err := connectionString(connection, secrets)
	if err != nil {
		return empty(), err
	}
	result, err := p.database.Query(ctx, dsn, statement, args, limit, duration(connection), "query_failed")
	if err != nil {
		return empty(), err
	}
	rows := databasesql.ProjectRows(result)
	return connector.TypedResult[map[string]any]{Output: map[string]any{"columns": result.Columns, "rows": rows, "row_count": len(rows), "truncated": result.Truncated}, ResponseRef: fmt.Sprintf("postgres:rows:%d", len(rows))}, nil
}
func connectionString(connection connector.Connection, secrets map[string]string) (string, error) {
	if raw := strings.TrimSpace(secrets["connection_string"]); raw != "" {
		return harden(raw)
	}
	username := strings.TrimSpace(secrets["username"])
	if username == "" {
		return "", permanent("username_required", "username is required")
	}
	host := config(connection, "host") + ":" + strconv.Itoa(intValue(connection.Config["port"], 5432))
	parsed := &url.URL{Scheme: "postgres", Host: host, Path: config(connection, "database")}
	if password := secrets["password"]; password != "" {
		parsed.User = url.UserPassword(username, password)
	} else {
		parsed.User = url.User(username)
	}
	query := url.Values{"sslmode": {configDefault(connection, "ssl_mode", "require")}, "default_transaction_read_only": {"on"}, "default_query_exec_mode": {"simple_protocol"}, "application_name": {"domainry-runtime"}}
	if schema := config(connection, "schema"); schema != "" {
		query.Set("search_path", schema)
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}
func harden(value string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil || parsed.Host == "" || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") {
		return "", permanent("connection_string_invalid", "connection string is invalid")
	}
	query := parsed.Query()
	mode := query.Get("sslmode")
	if mode == "" {
		mode = "require"
	}
	if !validSSL(mode) {
		return "", permanent("ssl_mode_invalid", "ssl_mode is invalid")
	}
	query.Set("sslmode", mode)
	query.Set("default_transaction_read_only", "on")
	query.Set("default_query_exec_mode", "simple_protocol")
	query.Set("application_name", "domainry-runtime")
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}
func validSSL(value string) bool {
	switch value {
	case "disable", "allow", "prefer", "require", "verify-ca", "verify-full":
		return true
	}
	return false
}

var postgresPolicy = databasesql.ReadOnlyPolicy{AllowedFirstKeywords: []string{"select", "with", "show", "explain"}, DeniedKeywords: []string{"alter", "analyze", "call", "cluster", "comment", "copy", "create", "delete", "do", "drop", "execute", "grant", "insert", "lock", "merge", "refresh", "reindex", "revoke", "set", "truncate", "update", "vacuum"}, QuotePairs: map[rune]rune{'\'': '\'', '"': '"'}}

func duration(connection connector.Connection) time.Duration {
	return time.Duration(intValue(connection.Config["timeout_seconds"], 15)) * time.Second
}
func empty() connector.TypedResult[map[string]any] { return connector.TypedResult[map[string]any]{} }
func config(connection connector.Connection, key string) string {
	value, _ := connection.Config[key].(string)
	return strings.TrimSpace(value)
}
func configDefault(connection connector.Connection, key, fallback string) string {
	if value := config(connection, key); value != "" {
		return value
	}
	return fallback
}
func ref(connection connector.Connection, key string) string {
	return strings.TrimSpace(connection.SecretRefs[key])
}
func intValue(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		return typed
	case float64:
		return int(typed)
	case json.Number:
		parsed, err := strconv.Atoi(typed.String())
		if err == nil {
			return parsed
		}
	}
	return fallback
}
func boolValue(value any, fallback bool) bool {
	typed, ok := value.(bool)
	if ok {
		return typed
	}
	return fallback
}
func permanent(code, message string) error {
	return connector.PermanentError("postgres."+code, errors.New(message))
}
