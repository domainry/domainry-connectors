package moneyforward

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
	return CreateJournalEntryInput{BatchKey: "batch", EntryKey: "entry", TransactionDate: "2026-08-12", Currency: "JPY", Memo: "sale", Lines: []JournalLine{{Side: "debit", Amount: 100, AccountID: "d1", TaxCode: "t1", Description: "first"}, {Side: "debit", Amount: 200, AccountID: "d2", TaxCode: "t2"}, {Side: "credit", Amount: 300, AccountID: "c1", TaxCode: "t3", PartnerCode: "P-1"}}}
}
func invoke(t *testing.T, adapter connector.Adapter, connection connector.Connection, secrets map[string]string) (connector.CallResult, error) {
	t.Helper()
	payload, _ := json.Marshal(journalInput())
	return adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CreateJournalEntry.Key, ContractSHA256: CreateJournalEntry.ContractSHA256, Mode: connector.ModeEnqueue, Delivery: true, Connection: connection, Secrets: secrets, Payload: payload})
}

func TestCreateJournalPairsLinesAndKeepsAuthorizationRuntimeOnly(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: http.StatusCreated, Body: []byte(`{"journal":{"id":"journal-1","transaction_id":"transaction-1"}}`)}}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	result, err := invoke(t, adapter, connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080"}}, map[string]string{"access_token": "access-secret"})
	if err != nil || result.ResponseRef != "money_forward:journal:journal-1" {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	request := transport.requests[0]
	if request.SecretHeaders["Authorization"][0] != "Bearer access-secret" || strings.Contains(request.URL+string(request.Body), "access-secret") {
		t.Fatalf("request=%+v", request)
	}
	var payload map[string]any
	_ = json.Unmarshal(request.Body, &payload)
	branches := payload["journal"].(map[string]any)["branches"].([]any)
	if len(branches) != 2 || branches[0].(map[string]any)["debitor"].(map[string]any)["value"] != float64(100) {
		t.Fatalf("payload=%+v", payload)
	}
}

func TestRefreshUsesRuntimeOnlyBasicAuthorizationAndRotatesSecrets(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: http.StatusUnauthorized, Body: []byte(`{"errors":[{"code":"MISSING_AUTHORIZATION"}]}`)}, {StatusCode: http.StatusOK, Body: []byte(`{"access_token":"new-access","refresh_token":"new-refresh"}`)}, {StatusCode: http.StatusCreated, Body: []byte(`{"journal":{"id":"journal-2"}}`)}}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	result, err := invoke(t, adapter, connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080", "token_url": "http://localhost:8081/token"}}, map[string]string{"access_token": "old", "refresh_token": "refresh-secret", "client_id": "client-id", "client_secret": "client-secret"})
	if err != nil || result.SecretUpdates["access_token"] != "new-access" {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	refresh := transport.requests[1]
	if len(refresh.SecretHeaders["Authorization"]) != 1 || !strings.HasPrefix(refresh.SecretHeaders["Authorization"][0], "Basic ") || strings.Contains(string(refresh.Body), "client-secret") {
		t.Fatalf("refresh=%+v", refresh)
	}
}

func TestWriteNetworkFailureIsUncertain(t *testing.T) {
	transport := &recordingTransport{err: errors.New("connection reset")}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	_, err = invoke(t, adapter, connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080"}}, map[string]string{"access_token": "access"})
	classification, ok := connector.ErrorClassificationOf(err)
	if !ok || classification != connector.ErrorUncertain {
		t.Fatalf("classification=%q/%v error=%v", classification, ok, err)
	}
	if got := adapter.Descriptor(); len(got.Operations) != 1 || got.Operations[0].Mode != connector.ModeEnqueue || len(got.SecretFields) != 4 {
		t.Fatalf("descriptor=%+v", got)
	}
}
