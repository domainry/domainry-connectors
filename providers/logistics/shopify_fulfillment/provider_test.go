package shopifyfulfillment

import (
	"context"
	"encoding/json"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
	"testing"
)

type recordingTransport struct {
	responses []connector.HTTPResponse
	count     int
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, _ connector.HTTPRequest) (connector.HTTPResponse, error) {
	response := t.responses[t.count]
	t.count++
	return response, nil
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func TestDescriptorRoutingAndWriteClassification(t *testing.T) {
	responses := make([]connector.HTTPResponse, 5)
	for i := range responses {
		responses[i] = connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"data":{"fulfillmentCreate":{"fulfillment":{"id":"gid://shopify/Fulfillment/2"},"userErrors":[]}}}`)}
	}
	tr := &recordingTransport{responses: responses}
	adapter, err := New(tr)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	calls := []connector.CallRequest{call(ListFulfillmentOrders.Key, ListFulfillmentOrders.ContractSHA256, ListOrdersInput{OrderID: "1"}), call(CreateFulfillment.Key, CreateFulfillment.ContractSHA256, CreateFulfillmentInput{Input: map[string]any{"lineItemsByFulfillmentOrder": []any{map[string]any{"fulfillmentOrderId": "gid://shopify/FulfillmentOrder/1"}}}}), call(UpdateFulfillmentTracking.Key, UpdateFulfillmentTracking.ContractSHA256, UpdateTrackingInput{FulfillmentID: "2", Input: map[string]any{"number": "1"}}), call(CancelFulfillment.Key, CancelFulfillment.ContractSHA256, CancelFulfillmentInput{FulfillmentID: "2"}), call(TestConnection.Key, TestConnection.ContractSHA256, struct{}{})}
	for _, request := range calls {
		if _, err = adapter.Call(t.Context(), request); err != nil {
			t.Fatalf("operation=%s err=%v", request.OperationKey, err)
		}
	}
}
func call(key, hash string, payload any) connector.CallRequest {
	raw, _ := json.Marshal(payload)
	return connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: key, ContractSHA256: hash, Mode: connector.ModeCall, Connection: connector.Connection{Config: map[string]any{"shop_domain": "localhost:8080", "api_version": "2026-04"}}, Secrets: map[string]string{"access_token": "token"}, Payload: raw}
}
