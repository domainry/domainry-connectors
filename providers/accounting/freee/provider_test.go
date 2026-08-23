package freee

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
)

type recordingTransport struct {
	requests  []connector.HTTPRequest
	responses []connector.HTTPResponse
	err       error
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, request)
	if t.err != nil {
		return connector.HTTPResponse{}, t.err
	}
	response := t.responses[0]
	t.responses = t.responses[1:]
	return response, nil
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func journalInput() CreateJournalEntryInput {
	return CreateJournalEntryInput{BatchKey: "batch-1", EntryKey: "entry-1", TransactionDate: "2026-08-12", Currency: "JPY", Memo: "sale", Lines: []JournalLine{{Side: "debit", Amount: 300, AccountID: "101", TaxCode: "1", DepartmentID: "7"}, {Side: "credit", Amount: 300, AccountID: "202", TaxCode: "2", PartnerCode: "P-1"}}}
}

func invoke(t *testing.T, adapter connector.Adapter, connection connector.Connection, secrets map[string]string, input CreateJournalEntryInput) (connector.CallResult, error) {
	t.Helper()
	payload, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	return adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CreateJournalEntry.Key, ContractSHA256: CreateJournalEntry.ContractSHA256, Mode: connector.ModeEnqueue, Delivery: true, Connection: connection, Secrets: secrets, Payload: payload})
}

func TestCreateJournalEntryUsesRuntimeOnlyAuthorizationAndDeliveryMetadata(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: http.StatusCreated, Body: []byte(`{"manual_journal":{"id":123}}`)}}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	result, err := invoke(t, adapter, connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080"}}, map[string]string{"access_token": "access-secret", "company_id": "42"}, journalInput())
	if err != nil || result.ResponseRef != "freee:manual_journal:123" || len(result.Payload) != 0 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	request := transport.requests[0]
	if request.SecretHeaders["Authorization"][0] != "Bearer access-secret" || strings.Contains(request.URL+string(request.Body), "access-secret") {
		t.Fatalf("request leaked access token: %+v", request)
	}
	var payload map[string]any
	if err := json.Unmarshal(request.Body, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["company_id"] != float64(42) || len(payload["details"].([]any)) != 2 {
		t.Fatalf("payload=%+v", payload)
	}
}

func TestUnauthorizedRefreshUsesRuntimeOnlyFormAndRotatesSecrets(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{
		{StatusCode: http.StatusUnauthorized, Body: []byte(`{"errors":[{"type":"expired_access_token"}]}`)},
		{StatusCode: http.StatusOK, Body: []byte(`{"access_token":"new-access","refresh_token":"new-refresh"}`)},
		{StatusCode: http.StatusCreated, Body: []byte(`{"manual_journal":{"id":456}}`)},
	}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	result, err := invoke(t, adapter, connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080", "token_url": "http://localhost:8081/token"}}, map[string]string{"access_token": "old-access", "refresh_token": "refresh-secret", "client_id": "client-id", "client_secret": "client-secret", "company_id": "42"}, journalInput())
	if err != nil || result.SecretUpdates["access_token"] != "new-access" || result.SecretUpdates["refresh_token"] != "new-refresh" {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	refresh := transport.requests[1]
	if refresh.SecretForm["refresh_token"] != "refresh-secret" || refresh.SecretForm["client_secret"] != "client-secret" || strings.Contains(string(refresh.Body), "refresh-secret") {
		t.Fatalf("refresh=%+v", refresh)
	}
	if transport.requests[2].SecretHeaders["Authorization"][0] != "Bearer new-access" {
		t.Fatalf("retry=%+v", transport.requests[2])
	}
}

func TestWriteNetworkFailureIsUncertainAndConfigIsClosed(t *testing.T) {
	transport := &recordingTransport{err: errors.New("connection reset")}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	_, err = invoke(t, adapter, connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080"}}, map[string]string{"access_token": "access", "company_id": "42"}, journalInput())
	classification, ok := connector.ErrorClassificationOf(err)
	if !ok || classification != connector.ErrorUncertain {
		t.Fatalf("classification=%q/%v error=%v", classification, ok, err)
	}
	validator := adapter.(connector.ConfigValidator)
	if err := validator.ValidateConfig(connector.Connection{Config: map[string]any{"base_url": "http://remote.example"}}); err == nil {
		t.Fatal("remote plain HTTP endpoint accepted")
	}
	if got := adapter.Descriptor(); len(got.Operations) != 1 || got.Operations[0].Mode != connector.ModeEnqueue || len(got.SecretFields) != 5 {
		t.Fatalf("descriptor=%+v", got)
	}
}
