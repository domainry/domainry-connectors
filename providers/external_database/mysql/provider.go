// Package mysql implements the official read-only MySQL Provider.
package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/internal/databasesql"
	mysqldriver "github.com/go-sql-driver/mysql"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	ConnectorKey = "external_database"
	ProviderKey  = "mysql"
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
		return nil, errors.New("MySQL transport is required")
	}
	database, err := databasesql.NewClient(transport, "mysql", ProviderKey)
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
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "host", Name: "Host", Type: connector.ConfigFieldText}, {Key: "port", Name: "Port", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`3306`), Validation: connector.ConfigValidation{Min: &minPort, Max: &maxPort}}, {Key: "database", Name: "Database", Type: connector.ConfigFieldText}, {Key: "ssl_mode", Name: "SSL Mode", Type: connector.ConfigFieldSelect, Default: json.RawMessage(`"require"`), Validation: connector.ConfigValidation{Options: []string{"disable", "preferred", "require", "skip-verify"}}}, {Key: "readonly", Name: "Read Only", Type: connector.ConfigFieldBoolean, Required: true, Default: json.RawMessage(`true`)}, {Key: "max_rows", Name: "Maximum Rows", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`100`), Validation: connector.ConfigValidation{Min: &minRows, Max: &maxRows}}, {Key: "timeout_seconds", Name: "Timeout Seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`15`), Validation: connector.ConfigValidation{Min: &minTimeout, Max: &maxTimeout}}}, SecretFields: []connector.SecretField{secret("connection_string", "Connection String"), secret("username", "Username"), secret("password", "Password")}}
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
	if config(connection, "host") == "" || config(connection, "database") == "" || ref(connection, "username") == "" {
		return permanent("endpoint_credentials_required", "host, database, and username are required")
	}
	port := intValue(connection.Config["port"], 3306)
	if port < 1 || port > 65535 {
		return permanent("port_invalid", "port is invalid")
	}
	switch configDefault(connection, "ssl_mode", "require") {
	case "disable", "preferred", "require", "skip-verify":
	default:
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
	result, err := p.database.Query(ctx, dsn, "SELECT DATABASE(), CURRENT_USER()", nil, 1, timeout, "identity_failed")
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
	return connector.TypedResult[map[string]any]{Output: map[string]any{"connected": true, "database": database, "user": user, "readonly": true}, ResponseRef: "mysql:" + database}, nil
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
	return p.query(ctx, r.Connection, r.Secrets, "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM information_schema.tables WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME", nil, r.Input.MaxRows)
}
func (p *provider) sample(ctx context.Context, r connector.TypedRequest[SampleQueryInput]) (connector.TypedResult[map[string]any], error) {
	query := strings.TrimSpace(r.Input.Query)
	if code := databasesql.ValidateReadOnly(query, mysqlPolicy); code != "" {
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
	output := map[string]any{"columns": result.Columns, "rows": rows, "row_count": len(rows), "truncated": result.Truncated}
	return connector.TypedResult[map[string]any]{Output: output, ResponseRef: fmt.Sprintf("mysql:rows:%d", len(rows))}, nil
}
func connectionString(connection connector.Connection, secrets map[string]string) (string, error) {
	if raw := strings.TrimSpace(secrets["connection_string"]); raw != "" {
		parsed, err := mysqldriver.ParseDSN(raw)
		if err != nil {
			return "", permanent("connection_string_invalid", "connection string is invalid")
		}
		return harden(parsed, duration(connection)), nil
	}
	username := strings.TrimSpace(secrets["username"])
	if username == "" {
		return "", permanent("username_required", "username is required")
	}
	cfg := mysqldriver.NewConfig()
	cfg.User = username
	cfg.Passwd = secrets["password"]
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(config(connection, "host"), strconv.Itoa(intValue(connection.Config["port"], 3306)))
	cfg.DBName = config(connection, "database")
	cfg.TLSConfig = map[string]string{"disable": "false", "preferred": "preferred", "skip-verify": "skip-verify", "require": "true"}[configDefault(connection, "ssl_mode", "require")]
	return harden(cfg, duration(connection)), nil
}
func harden(cfg *mysqldriver.Config, timeout time.Duration) string {
	cfg.MultiStatements = false
	cfg.AllowAllFiles = false
	cfg.AllowCleartextPasswords = false
	cfg.ParseTime = true
	cfg.Timeout = timeout
	cfg.ReadTimeout = timeout
	cfg.WriteTimeout = timeout
	params := map[string]string{}
	for key, value := range cfg.Params {
		params[key] = value
	}
	params["transaction_read_only"] = "ON"
	params["sql_safe_updates"] = "1"
	cfg.Params = params
	cfg.Loc = time.UTC
	return cfg.FormatDSN()
}

var mysqlPolicy = databasesql.ReadOnlyPolicy{AllowedFirstKeywords: []string{"select", "with", "show", "describe", "explain"}, DeniedKeywords: []string{"alter", "analyze", "call", "create", "delete", "do", "drop", "execute", "grant", "insert", "load", "lock", "replace", "revoke", "set", "truncate", "unlock", "update"}, UnsafeFragments: []string{"#"}, QuotePairs: map[rune]rune{'\'': '\'', '"': '"', '`': '`'}, KeywordRules: []databasesql.KeywordRule{{Sequence: []string{"for", "update"}, Code: databasesql.LockingReadDenied}, {Sequence: []string{"for", "share"}, Code: databasesql.LockingReadDenied}}}

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
	return connector.PermanentError("mysql."+code, errors.New(message))
}
