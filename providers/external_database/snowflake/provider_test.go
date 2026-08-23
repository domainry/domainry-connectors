package snowflake

import (
	"context"
	"encoding/json"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
	"net/http"
	"strings"
	"testing"
)

type recordingTransport struct {
	requests  []connector.HTTPRequest
	responses []connector.HTTPResponse
	errors    []error
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, request)
	i := len(t.requests) - 1
	var response connector.HTTPResponse
	if i < len(t.responses) {
		response = t.responses[i]
	}
	var err error
	if i < len(t.errors) {
		err = t.errors[i]
	}
	return response, err
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestDescriptorCredentialAndEndpointBoundary(t *testing.T) {
	adapter, err := New(&recordingTransport{})
	if err != nil {
		t.Fatal(err)
	}
	if err = contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	d := adapter.Descriptor()
	if d.ConnectorKey != ConnectorKey || d.ProviderKey != ProviderKey || len(d.Operations) != 3 || d.SecretFields[0].Key != "token" || d.SecretFields[0].CredentialKind != connector.SecretCredentialBearerToken {
		t.Fatalf("descriptor=%+v", d)
	}
	validator := adapter.(connector.ConfigValidator)
	for _, connection := range []connector.Connection{snowflakeConnection("https://acme-org.snowflakecomputing.com"), snowflakeConnection("https://xy12345.eu-central-1.snowflakecomputing.com"), snowflakeConnection("http://localhost:8080")} {
		if err = validator.ValidateConfig(connection); err != nil {
			t.Fatalf("valid=%v err=%v", connection.Config, err)
		}
	}
	for _, endpoint := range []string{"https://snowflakecomputing.com", "https://snowflakecomputing.com.evil.test", "http://acme.snowflakecomputing.com", "https://acme.example.com", "https://acme.snowflakecomputing.com/custom"} {
		if err = validator.ValidateConfig(snowflakeConnection(endpoint)); err == nil {
			t.Fatalf("invalid accepted=%s", endpoint)
		}
	}
	for _, tokenType := range []string{"OAUTH", "KEYPAIR_JWT", "PROGRAMMATIC_ACCESS_TOKEN", ""} {
		connection := snowflakeConnection("http://localhost")
		connection.Config["token_type"] = tokenType
		if err = validator.ValidateConfig(connection); err != nil {
			t.Fatalf("token type=%q err=%v", tokenType, err)
		}
	}
	connection := snowflakeConnection("http://localhost")
	connection.Config["token_type"] = "PASSWORD"
	if err = validator.ValidateConfig(connection); err == nil {
		t.Fatal("invalid token type accepted")
	}
}

func TestStatementsPollingResultsAndRuntimeOnlyToken(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: 200, Body: snowflakeResult([]string{"CURRENT_ACCOUNT()", "CURRENT_USER()", "CURRENT_ROLE()"}, [][]any{{"ACME", "REPORTER", "READER"}}, false)}, {StatusCode: 200, Body: snowflakeResult([]string{"TABLE_NAME"}, [][]any{{"ORDERS"}, {"CUSTOMERS"}}, true)}, {StatusCode: http.StatusAccepted, Body: []byte(`{"statementStatusUrl":"/api/v2/statements/handle-1?partition=0"}`)}, {StatusCode: 200, Body: snowflakeResult([]string{"ID"}, [][]any{{"42"}}, false)}}}
	adapter, _ := New(transport)
	cases := []struct {
		key, hash string
		payload   any
	}{{TestConnection.Key, TestConnection.ContractSHA256, struct{}{}}, {ListTables.Key, ListTables.ContractSHA256, ListTablesInput{MaxRows: 1}}, {SampleQuery.Key, SampleQuery.ContractSHA256, SampleQueryInput{Query: "SELECT id FROM orders", MaxRows: 10}}}
	for _, tc := range cases {
		result, err := adapter.Call(t.Context(), call(tc.key, tc.hash, tc.payload))
		if err != nil {
			t.Fatalf("operation=%s result=%+v err=%v", tc.key, result, err)
		}
		if tc.key == ListTables.Key && (!strings.Contains(string(result.Payload), `"row_count":1`) || !strings.Contains(string(result.Payload), `"truncated":true`)) {
			t.Fatalf("list=%s", result.Payload)
		}
		if tc.key == SampleQuery.Key && result.ResponseRef != "snowflake:rows:1" {
			t.Fatalf("query=%+v", result)
		}
	}
	if len(transport.requests) != 4 || transport.requests[3].Method != http.MethodGet || !strings.HasSuffix(transport.requests[3].URL, "/api/v2/statements/handle-1?partition=0") {
		t.Fatalf("requests=%+v", transport.requests)
	}
	for _, request := range transport.requests {
		if request.SecretHeaders["Authorization"][0] != "Bearer snowflake-token" || strings.Contains(request.URL+string(request.Body), "snowflake-token") {
			t.Fatalf("token leaked=%+v", request)
		}
		if request.Headers["X-Snowflake-Authorization-Token-Type"][0] != "OAUTH" {
			t.Fatalf("headers=%v", request.Headers)
		}
	}
	body := string(transport.requests[0].Body)
	for _, expected := range []string{`"database":"SALES"`, `"schema":"PUBLIC"`, `"warehouse":"REPORTING"`, `"role":"READER"`} {
		if !strings.Contains(body, expected) {
			t.Fatalf("missing %s in %s", expected, body)
		}
	}
}

func TestSQLStatusURLValidationAndFailures(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	for _, query := range []string{"", "DELETE FROM orders", "WITH removed AS (DELETE FROM orders RETURNING id) SELECT * FROM removed", "SELECT 1; SELECT 2", "SELECT 1 -- comment"} {
		if _, err := adapter.Call(t.Context(), call(SampleQuery.Key, SampleQuery.ContractSHA256, SampleQueryInput{Query: query})); err == nil {
			t.Fatalf("unsafe accepted=%q", query)
		}
	}
	for _, query := range []string{"SHOW TABLES", "DESCRIBE TABLE orders", "SELECT 'drop' AS value", "WITH rows AS (SELECT 1) SELECT * FROM rows"} {
		current, _ := New(&recordingTransport{responses: []connector.HTTPResponse{{StatusCode: 200, Body: snowflakeResult(nil, nil, false)}}})
		if _, err := current.Call(t.Context(), call(SampleQuery.Key, SampleQuery.ContractSHA256, SampleQueryInput{Query: query})); err != nil {
			t.Fatalf("safe rejected=%q err=%v", query, err)
		}
	}
	for _, statusURL := range []string{"https://evil.test/api/v2/statements/x", "//evil.test/api/v2/statements/x", "/other/x", "/api/v2/statements/../admin", "/api/v2/statements/%2e%2e/admin"} {
		current, _ := New(&recordingTransport{responses: []connector.HTTPResponse{{StatusCode: http.StatusAccepted, Body: []byte(`{"statementStatusUrl":"` + statusURL + `"}`)}}})
		if _, err := current.Call(t.Context(), call(TestConnection.Key, TestConnection.ContractSHA256, struct{}{})); err == nil {
			t.Fatalf("status URL accepted=%s", statusURL)
		}
	}
	for _, tc := range []struct {
		name         string
		response     connector.HTTPResponse
		transportErr error
		want         connector.ErrorClassification
	}{{"network", connector.HTTPResponse{}, errors.New("reset"), connector.ErrorRetryable}, {"rate", connector.HTTPResponse{StatusCode: 429}, nil, connector.ErrorRetryable}, {"server", connector.HTTPResponse{StatusCode: 502}, nil, connector.ErrorRetryable}, {"rejection", connector.HTTPResponse{StatusCode: 400}, nil, connector.ErrorPermanent}, {"invalid", connector.HTTPResponse{StatusCode: 200, Body: []byte(`{`)}, nil, connector.ErrorPermanent}} {
		t.Run(tc.name, func(t *testing.T) {
			current, _ := New(&recordingTransport{responses: []connector.HTTPResponse{tc.response}, errors: []error{tc.transportErr}})
			_, err := current.Call(t.Context(), call(TestConnection.Key, TestConnection.ContractSHA256, struct{}{}))
			class, ok := connector.ErrorClassificationOf(err)
			if !ok || class != tc.want {
				t.Fatalf("err=%v class=%q want=%q", err, class, tc.want)
			}
		})
	}
}
func snowflakeConnection(endpoint string) connector.Connection {
	return connector.Connection{Config: map[string]any{"account": "acme-org", "base_url": endpoint, "database": "SALES", "schema": "PUBLIC", "warehouse": "REPORTING", "role": "READER", "token_type": "OAUTH", "readonly": true, "max_rows": 100, "timeout_seconds": 30}}
}
func call(key, hash string, payload any) connector.CallRequest {
	raw, _ := json.Marshal(payload)
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: key, ContractSHA256: hash, Mode: connector.ModeCall, Connection: snowflakeConnection("http://localhost:8080"), Secrets: map[string]string{"token": "snowflake-token"}, Payload: raw}
}
func snowflakeResult(columns []string, rows [][]any, more bool) []byte {
	rowType := make([]any, len(columns))
	for i, column := range columns {
		rowType[i] = map[string]any{"name": column}
	}
	partitions := []any{map[string]any{"rowCount": len(rows)}}
	if more {
		partitions = append(partitions, map[string]any{"rowCount": 1})
	}
	raw, _ := json.Marshal(map[string]any{"resultSetMetaData": map[string]any{"rowType": rowType, "partitionInfo": partitions}, "data": rows})
	return raw
}
