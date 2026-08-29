// Package sqlserver implements the official read-only Microsoft SQL Server Provider.
package sqlserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/internal/databasesql"
)

const (
	ConnectorKey = "external_database"
	ProviderKey  = "sqlserver"
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
		return nil, errors.New("SQL Server transport is required")
	}
	database, err := databasesql.NewClient(transport, "sqlserver", ProviderKey)
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
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "host", Name: "Host", Type: connector.ConfigFieldText}, {Key: "port", Name: "Port", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`1433`), Validation: connector.ConfigValidation{Min: &minPort, Max: &maxPort}}, {Key: "database", Name: "Database", Type: connector.ConfigFieldText}, {Key: "encrypt", Name: "Encrypt", Type: connector.ConfigFieldBoolean, Default: json.RawMessage(`true`)}, {Key: "trust_server_certificate", Name: "Trust Server Certificate", Type: connector.ConfigFieldBoolean, Default: json.RawMessage(`false`)}, {Key: "readonly", Name: "Read Only", Type: connector.ConfigFieldBoolean, Required: true, Default: json.RawMessage(`true`)}, {Key: "max_rows", Name: "Maximum Rows", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`100`), Validation: connector.ConfigValidation{Min: &minRows, Max: &maxRows}}, {Key: "timeout_seconds", Name: "Timeout Seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`15`), Validation: connector.ConfigValidation{Min: &minTimeout, Max: &maxTimeout}}}, SecretFields: []connector.SecretField{secret("connection_string", "Connection String"), secret("username", "Username"), secret("password", "Password")}}
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
	port := intValue(connection.Config["port"], 1433)
	if port < 1 || port > 65535 {
		return permanent("port_invalid", "port is invalid")
	}
	return nil
}

func (p *provider) test(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[map[string]any], error) {
	dsn, err := connectionString(request.Connection, request.Secrets)
	if err != nil {
		return empty(), err
	}
	timeout := duration(request.Connection)
	if err = p.database.Ping(ctx, dsn, timeout); err != nil {
		return empty(), err
	}
	result, err := p.database.Query(ctx, dsn, "SELECT DB_NAME(), SYSTEM_USER", nil, 1, timeout, "identity_failed")
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
	return connector.TypedResult[map[string]any]{Output: map[string]any{"connected": true, "database": database, "user": user, "readonly": true}, ResponseRef: "sqlserver:" + database}, nil
}

func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.test(ctx, connector.TypedRequest[struct{}]{Connection: request.Connection, Secrets: request.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) list(ctx context.Context, request connector.TypedRequest[ListTablesInput]) (connector.TypedResult[map[string]any], error) {
	return p.query(ctx, request.Connection, request.Secrets, "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_SCHEMA, TABLE_NAME", nil, request.Input.MaxRows)
}

func (p *provider) sample(ctx context.Context, request connector.TypedRequest[SampleQueryInput]) (connector.TypedResult[map[string]any], error) {
	query := strings.TrimSpace(request.Input.Query)
	if code := databasesql.ValidateReadOnly(query, sqlServerPolicy); code != "" {
		return empty(), permanent(code, "query violates read-only policy")
	}
	return p.query(ctx, request.Connection, request.Secrets, query, request.Input.Args, request.Input.MaxRows)
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
	return connector.TypedResult[map[string]any]{Output: map[string]any{"columns": result.Columns, "rows": rows, "row_count": len(rows), "truncated": result.Truncated}, ResponseRef: fmt.Sprintf("sqlserver:rows:%d", len(rows))}, nil
}

func connectionString(connection connector.Connection, secrets map[string]string) (string, error) {
	if raw := strings.TrimSpace(secrets["connection_string"]); raw != "" {
		return harden(raw)
	}
	username := strings.TrimSpace(secrets["username"])
	if username == "" {
		return "", permanent("username_required", "username is required")
	}
	parsed := &url.URL{Scheme: "sqlserver", Host: net.JoinHostPort(config(connection, "host"), strconv.Itoa(intValue(connection.Config["port"], 1433)))}
	if password := secrets["password"]; password != "" {
		parsed.User = url.UserPassword(username, password)
	} else {
		parsed.User = url.User(username)
	}
	query := url.Values{"database": {config(connection, "database")}, "app name": {"domainry-runtime"}, "applicationintent": {"ReadOnly"}, "connection timeout": {strconv.Itoa(max(1, int(duration(connection).Seconds())))}}
	if boolValue(connection.Config["encrypt"], true) {
		query.Set("encrypt", "true")
	} else {
		query.Set("encrypt", "disable")
	}
	if boolValue(connection.Config["trust_server_certificate"], false) {
		query.Set("TrustServerCertificate", "true")
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func harden(value string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil || parsed.Host == "" || parsed.Scheme != "sqlserver" {
		return "", permanent("connection_string_invalid", "connection string is invalid")
	}
	query := parsed.Query()
	if strings.TrimSpace(query.Get("database")) == "" {
		return "", permanent("database_required", "database is required")
	}
	query.Set("applicationintent", "ReadOnly")
	query.Set("app name", "domainry-runtime")
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

var sqlServerPolicy = databasesql.ReadOnlyPolicy{
	AllowedFirstKeywords: []string{"select", "with"},
	DeniedKeywords:       []string{"alter", "backup", "create", "delete", "drop", "execute", "grant", "insert", "kill", "merge", "restore", "revoke", "truncate", "update"},
	QuotePairs:           map[rune]rune{'\'': '\'', '"': '"', '[': ']'},
	KeywordRules: []databasesql.KeywordRule{
		{Sequence: []string{"into"}, Code: databasesql.SelectIntoDenied},
		{Sequence: []string{"for", "update"}, Code: databasesql.LockingReadDenied},
	},
}

func duration(connection connector.Connection) time.Duration {
	return time.Duration(intValue(connection.Config["timeout_seconds"], 15)) * time.Second
}

func empty() connector.TypedResult[map[string]any] { return connector.TypedResult[map[string]any]{} }

func config(connection connector.Connection, key string) string {
	value, _ := connection.Config[key].(string)
	return strings.TrimSpace(value)
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
	return connector.PermanentError("sqlserver."+code, errors.New(message))
}
