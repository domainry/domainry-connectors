package postgres

import (
	"context"
	"encoding/json"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
	"strings"
	"testing"
)

type recordingTransport struct{ requests []connector.SQLRequest }

func (*recordingTransport) RoundTripHTTP(context.Context, connector.HTTPRequest) (connector.HTTPResponse, error) {
	return connector.HTTPResponse{}, errors.New("unexpected HTTP")
}
func (t *recordingTransport) ExecuteSQL(_ context.Context, r connector.SQLRequest) (connector.SQLResult, error) {
	t.requests = append(t.requests, r)
	if r.Operation == connector.SQLOperationPing {
		return connector.SQLResult{}, nil
	}
	if strings.Contains(r.Statement, "current_database()") {
		return connector.SQLResult{Columns: []string{"database", "user"}, Rows: [][]any{{"sales", "reporter"}}}, nil
	}
	return connector.SQLResult{Columns: []string{"id", "name"}, Rows: [][]any{{[]byte("1"), []byte("Alice")}}, Truncated: true}, nil
}
func TestRoutingAndDSNBoundary(t *testing.T) {
	tr := &recordingTransport{}
	adapter, err := New(tr)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	ops := []struct {
		key, hash string
		input     any
	}{{TestConnection.Key, TestConnection.ContractSHA256, struct{}{}}, {ListTables.Key, ListTables.ContractSHA256, ListTablesInput{MaxRows: 10}}, {SampleQuery.Key, SampleQuery.ContractSHA256, SampleQueryInput{Query: "SELECT id, name FROM accounts", MaxRows: 1}}}
	for _, op := range ops {
		raw, _ := json.Marshal(op.input)
		result, callErr := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: op.key, ContractSHA256: op.hash, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"username": "reporter", "password": "top-secret"}, Payload: raw})
		if callErr != nil || result.ResponseRef == "" {
			t.Fatalf("operation=%s result=%+v err=%v", op.key, result, callErr)
		}
	}
	for _, request := range tr.requests {
		encoded, _ := json.Marshal(request)
		if request.Driver != "pgx" || request.DSN == "" || strings.Contains(string(encoded), "top-secret") || strings.Contains(string(encoded), "reporter") {
			t.Fatalf("request=%+v json=%s", request, encoded)
		}
	}
}
func TestHardensConnectionStringAndRejectsWrites(t *testing.T) {
	dsn, err := harden("postgres://user:password@db.example.test/sales?sslmode=require&default_transaction_read_only=off")
	if err != nil || !strings.Contains(dsn, "default_transaction_read_only=on") {
		t.Fatalf("dsn=%s err=%v", dsn, err)
	}
	adapter, _ := New(&recordingTransport{})
	raw, _ := json.Marshal(SampleQueryInput{Query: "WITH changed AS (UPDATE users SET active=false RETURNING id) SELECT * FROM changed"})
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SampleQuery.Key, ContractSHA256: SampleQuery.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"username": "reporter", "password": "password"}, Payload: raw})
	if err == nil {
		t.Fatal("write query accepted")
	}
}
func validConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"host": "db.example.test", "port": 5432, "database": "sales", "schema": "public", "ssl_mode": "require", "readonly": true, "max_rows": 100, "timeout_seconds": 15}, SecretRefs: map[string]string{"username": "secret:user", "password": "secret:password"}}
}
