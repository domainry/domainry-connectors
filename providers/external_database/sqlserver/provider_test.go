package sqlserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
)

type recordingTransport struct {
	requests []connector.SQLRequest
}

func (*recordingTransport) RoundTripHTTP(context.Context, connector.HTTPRequest) (connector.HTTPResponse, error) {
	return connector.HTTPResponse{}, errors.New("unexpected HTTP")
}

func (transport *recordingTransport) ExecuteSQL(_ context.Context, request connector.SQLRequest) (connector.SQLResult, error) {
	transport.requests = append(transport.requests, request)
	if request.Operation == connector.SQLOperationPing {
		return connector.SQLResult{}, nil
	}
	if strings.Contains(request.Statement, "DB_NAME()") {
		return connector.SQLResult{Columns: []string{"database", "user"}, Rows: [][]any{{"sales", "reporter"}}}, nil
	}
	return connector.SQLResult{Columns: []string{"id", "name"}, Rows: [][]any{{[]byte("1"), []byte("Alice")}}, Truncated: true}, nil
}

func TestRoutingAndDSNBoundary(t *testing.T) {
	transport := &recordingTransport{}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	operations := []struct {
		key, hash string
		input     any
	}{{TestConnection.Key, TestConnection.ContractSHA256, struct{}{}}, {ListTables.Key, ListTables.ContractSHA256, ListTablesInput{MaxRows: 10}}, {SampleQuery.Key, SampleQuery.ContractSHA256, SampleQueryInput{Query: "SELECT id, name FROM accounts", MaxRows: 1}}}
	for _, operation := range operations {
		payload, _ := json.Marshal(operation.input)
		result, callErr := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: operation.key, ContractSHA256: operation.hash, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"username": "reporter", "password": "top-secret"}, Payload: payload})
		if callErr != nil || result.ResponseRef == "" {
			t.Fatalf("operation=%s result=%+v err=%v", operation.key, result, callErr)
		}
	}
	for _, request := range transport.requests {
		encoded, _ := json.Marshal(request)
		if request.Driver != "sqlserver" || request.DSN == "" || strings.Contains(string(encoded), "top-secret") || strings.Contains(string(encoded), "reporter") {
			t.Fatalf("request=%+v json=%s", request, encoded)
		}
	}
}

func TestConnectionStringHardeningAndReadOnlyPolicy(t *testing.T) {
	dsn, err := harden("sqlserver://reporter:secret@db.example.test:1433?database=sales&applicationintent=ReadWrite")
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := url.Parse(dsn)
	if parsed.Query().Get("applicationintent") != "ReadOnly" || parsed.Query().Get("app name") != "domainry-runtime" {
		t.Fatalf("hardened query=%v", parsed.Query())
	}
	for _, query := range []string{"DELETE FROM orders", "SELECT * INTO copied_orders FROM orders", "WITH doomed AS (SELECT id FROM orders) UPDATE orders SET id=1", "SELECT 1; SELECT 2"} {
		adapter, _ := New(&recordingTransport{})
		payload, _ := json.Marshal(SampleQueryInput{Query: query})
		_, callErr := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SampleQuery.Key, ContractSHA256: SampleQuery.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"username": "reporter", "password": "password"}, Payload: payload})
		if callErr == nil {
			t.Fatalf("accepted unsafe query %q", query)
		}
	}
}

func TestRejectsInvalidConnectionStringsAndMissingDatabase(t *testing.T) {
	for _, dsn := range []string{"postgres://db.example.test/sales", "sqlserver://db.example.test:1433"} {
		if _, err := harden(dsn); err == nil {
			t.Fatalf("accepted DSN %q", dsn)
		}
	}
}

func validConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"host": "db.example.test", "port": 1433, "database": "sales", "encrypt": true, "trust_server_certificate": false, "readonly": true, "max_rows": 100, "timeout_seconds": 15}, SecretRefs: map[string]string{"username": "secret:user", "password": "secret:password"}}
}
