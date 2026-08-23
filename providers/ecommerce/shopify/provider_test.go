package shopify

import (
	"context"
	"encoding/json"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
	"testing"
)

type recordingTransport struct {
	requests  []connector.HTTPRequest
	responses []connector.HTTPResponse
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, r connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, r)
	return t.responses[len(t.requests)-1], nil
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func TestDescriptorAndQueries(t *testing.T) {
	responses := make([]connector.HTTPResponse, 4)
	for i := range responses {
		responses[i] = connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"data":{"shop":{"id":"gid://shopify/Shop/42"}}}`)}
	}
	tr := &recordingTransport{responses: responses}
	adapter, err := New(tr)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	calls := []connector.CallRequest{call(ListProducts.Key, ListProducts.ContractSHA256, ListInput{}), call(ListOrders.Key, ListOrders.ContractSHA256, ListInput{}), call(ListCustomers.Key, ListCustomers.ContractSHA256, ListInput{}), call(TestConnection.Key, TestConnection.ContractSHA256, struct{}{})}
	for _, request := range calls {
		result, callErr := adapter.Call(t.Context(), request)
		if callErr != nil {
			t.Fatal(callErr)
		}
		if request.OperationKey == TestConnection.Key && result.ResponseRef != "shopify:shop:42" {
			t.Fatalf("result=%+v", result)
		}
	}
}
func call(key, hash string, payload any) connector.CallRequest {
	raw, _ := json.Marshal(payload)
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: key, ContractSHA256: hash, Mode: connector.ModeCall, Connection: connector.Connection{Config: map[string]any{"shop_domain": "localhost:8080", "api_version": "2026-04"}}, Secrets: map[string]string{"access_token": "token"}, Payload: raw}
}
