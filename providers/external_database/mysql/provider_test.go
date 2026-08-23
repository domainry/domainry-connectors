package mysql

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
	if strings.Contains(r.Statement, "DATABASE()") {
		return connector.SQLResult{Columns: []string{"database", "user"}, Rows: [][]any{{"sales", "reporter@%"}}}, nil
	}
	return connector.SQLResult{Columns: []string{"id", "name"}, Rows: [][]any{{[]byte("1"), []byte("Alice")}}, Truncated: true}, nil
}
func TestDescriptorRoutingAndDSNBoundary(t *testing.T) {
	tr := &recordingTransport{}
	adapter, err := New(tr)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	connection := validConnection()
	secrets := map[string]string{"username": "reporter", "password": "top-secret"}
	ops := []struct {
		key, hash string
		input     any
	}{{TestConnection.Key, TestConnection.ContractSHA256, struct{}{}}, {ListTables.Key, ListTables.ContractSHA256, ListTablesInput{MaxRows: 10}}, {SampleQuery.Key, SampleQuery.ContractSHA256, SampleQueryInput{Query: "SELECT id, name FROM accounts", MaxRows: 1}}}
	for _, op := range ops {
		raw, _ := json.Marshal(op.input)
		result, callErr := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: op.key, ContractSHA256: op.hash, Mode: connector.ModeCall, Connection: connection, Secrets: secrets, Payload: raw})
		if callErr != nil || result.ResponseRef == "" {
			t.Fatalf("operation=%s result=%+v err=%v", op.key, result, callErr)
		}
	}
	for _, request := range tr.requests {
		if request.DSN == "" {
			t.Fatal("missing DSN")
		}
		encoded, _ := json.Marshal(request)
		if strings.Contains(string(encoded), "top-secret") || strings.Contains(string(encoded), "reporter") {
			t.Fatalf("DSN leaked through JSON: %s", encoded)
		}
	}
}
func TestRejectsWritesAndWritableConfig(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	connection := validConnection()
	connection.Config["readonly"] = false
	if err := adapter.(connector.ConfigValidator).ValidateConfig(connection); err == nil {
		t.Fatal("writable connection accepted")
	}
	raw, _ := json.Marshal(SampleQueryInput{Query: "WITH removed AS (DELETE FROM users RETURNING id) SELECT * FROM removed"})
	_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SampleQuery.Key, ContractSHA256: SampleQuery.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"username": "reporter", "password": "password"}, Payload: raw})
	if err == nil {
		t.Fatal("write query accepted")
	}
}
func validConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"host": "db.example.test", "port": 3306, "database": "sales", "ssl_mode": "require", "readonly": true, "max_rows": 100, "timeout_seconds": 15}, SecretRefs: map[string]string{"username": "secret:user", "password": "secret:password"}}
}
