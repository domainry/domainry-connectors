package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
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

func TestDescriptorEndpointAndConfigBoundary(t *testing.T) {
	adapter, err := New(&recordingTransport{})
	if err != nil {
		t.Fatal(err)
	}
	if err = contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	descriptor := adapter.Descriptor()
	if descriptor.ConnectorKey != ConnectorKey || descriptor.ProviderKey != ProviderKey || len(descriptor.Operations) != 3 || descriptor.SecretFields[0].Key != "access_token" || descriptor.SecretFields[0].CredentialKind != connector.SecretCredentialBearerToken {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	validator := adapter.(connector.ConfigValidator)
	for _, endpoint := range []string{"https://bigquery.googleapis.com", "http://localhost:8080"} {
		if err = validator.ValidateConfig(connection(endpoint)); err != nil {
			t.Fatalf("valid endpoint=%s err=%v", endpoint, err)
		}
	}
	for _, endpoint := range []string{"https://bigquery.googleapis.com.evil.test", "http://bigquery.googleapis.com", "https://www.googleapis.com/bigquery/v2", "https://bigquery.googleapis.com/custom-prefix"} {
		if err = validator.ValidateConfig(connection(endpoint)); err == nil {
			t.Fatalf("invalid accepted=%s", endpoint)
		}
	}
	for _, config := range []map[string]any{{"readonly": true}, {"project_id": "p", "readonly": false}, {"project_id": "p", "readonly": true, "timeout_seconds": 301}} {
		if err = validator.ValidateConfig(connector.Connection{Config: config}); err == nil {
			t.Fatalf("invalid config accepted=%v", config)
		}
	}
}

func TestOperationsDecodeResultsAndKeepTokenRuntimeOnly(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: 200, Body: []byte(`{"datasets":[{}]}`)}, {StatusCode: 200, Body: []byte(`{"tables":[{"tableReference":{"projectId":"project-1","datasetId":"sales","tableId":"orders"},"type":"TABLE"}],"nextPageToken":"next"}`)}, {StatusCode: 200, Body: []byte(`{"jobComplete":true,"schema":{"fields":[{"name":"id"}]},"rows":[{"f":[{"v":"42"}]}]}`)}}}
	adapter, _ := New(transport)
	cases := []struct {
		key, hash string
		payload   any
	}{{TestConnection.Key, TestConnection.ContractSHA256, struct{}{}}, {ListTables.Key, ListTables.ContractSHA256, ListTablesInput{DatasetID: "sales", MaxRows: 25}}, {SampleQuery.Key, SampleQuery.ContractSHA256, SampleQueryInput{Query: "SELECT id FROM orders", DatasetID: "sales", MaxRows: 10}}}
	for _, tc := range cases {
		result, err := adapter.Call(t.Context(), request(tc.key, tc.hash, tc.payload))
		if err != nil {
			t.Fatalf("operation=%s err=%v", tc.key, err)
		}
		if tc.key == ListTables.Key && result.ResponseRef != "bigquery:tables:sales" {
			t.Fatalf("list result=%+v", result)
		}
		if tc.key == SampleQuery.Key && (!strings.Contains(string(result.Payload), `"id":"42"`) || result.ResponseRef != "bigquery:rows:1") {
			t.Fatalf("query result=%+v", result)
		}
	}
	for _, got := range transport.requests {
		if got.SecretHeaders["Authorization"][0] != "Bearer google-token" || strings.Contains(got.URL+string(got.Body), "google-token") {
			t.Fatalf("token leaked=%+v", got)
		}
	}
	if !strings.Contains(transport.requests[0].URL, "/bigquery/v2/projects/project-1/datasets?maxResults=1") {
		t.Fatalf("test URL=%s", transport.requests[0].URL)
	}
	if !strings.Contains(transport.requests[1].URL, "/datasets/sales/tables?maxResults=25") {
		t.Fatalf("list URL=%s", transport.requests[1].URL)
	}
	body := string(transport.requests[2].Body)
	if !strings.Contains(body, `"useLegacySql":false`) || !strings.Contains(body, `"defaultDataset":{"datasetId":"sales","projectId":"project-1"}`) {
		t.Fatalf("query body=%s", body)
	}
}

func TestReadOnlyPolicyValidationAndFailureClassification(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	for _, payload := range []SampleQueryInput{{}, {Query: "DELETE FROM orders"}, {Query: "WITH removed AS (DELETE FROM orders RETURNING id) SELECT * FROM removed"}, {Query: "SELECT 1; SELECT 2"}, {Query: "SELECT 1 -- comment"}} {
		if _, err := adapter.Call(t.Context(), request(SampleQuery.Key, SampleQuery.ContractSHA256, payload)); err == nil {
			t.Fatalf("unsafe accepted=%q", payload.Query)
		}
	}
	for _, payload := range []SampleQueryInput{{Query: "SELECT 'delete' AS word"}, {Query: "SELECT `update` FROM table"}, {Query: "WITH rows AS (SELECT 1) SELECT * FROM rows"}} {
		transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: 200, Body: []byte(`{"jobComplete":true}`)}}}
		current, _ := New(transport)
		if _, err := current.Call(t.Context(), request(SampleQuery.Key, SampleQuery.ContractSHA256, payload)); err != nil {
			t.Fatalf("safe rejected=%q err=%v", payload.Query, err)
		}
	}
	for _, tc := range []struct {
		name     string
		response connector.HTTPResponse
		err      error
		want     connector.ErrorClassification
	}{{"network", connector.HTTPResponse{}, errors.New("reset"), connector.ErrorRetryable}, {"rate", connector.HTTPResponse{StatusCode: http.StatusTooManyRequests}, nil, connector.ErrorRetryable}, {"server", connector.HTTPResponse{StatusCode: http.StatusBadGateway}, nil, connector.ErrorRetryable}, {"rejection", connector.HTTPResponse{StatusCode: http.StatusBadRequest}, nil, connector.ErrorPermanent}, {"invalid JSON", connector.HTTPResponse{StatusCode: 200, Body: []byte(`{`)}, nil, connector.ErrorPermanent}} {
		t.Run(tc.name, func(t *testing.T) {
			current, _ := New(&recordingTransport{responses: []connector.HTTPResponse{tc.response}, errors: []error{tc.err}})
			_, err := current.Call(t.Context(), request(TestConnection.Key, TestConnection.ContractSHA256, struct{}{}))
			class, ok := connector.ErrorClassificationOf(err)
			if !ok || class != tc.want {
				t.Fatalf("err=%v class=%q want=%q", err, class, tc.want)
			}
		})
	}
}

func connection(endpoint string) connector.Connection {
	return connector.Connection{Config: map[string]any{"project_id": "project-1", "dataset_id": "sales", "location": "US", "base_url": endpoint, "readonly": true, "max_rows": 100, "timeout_seconds": 30}}
}
func request(key, hash string, payload any) connector.CallRequest {
	raw, _ := json.Marshal(payload)
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: key, ContractSHA256: hash, Mode: connector.ModeCall, Connection: connection("http://localhost:8080"), Secrets: map[string]string{"access_token": "google-token"}, Payload: raw}
}
