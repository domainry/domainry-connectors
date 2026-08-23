package sfexpress

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
)

type recordingTransport struct {
	requests []connector.HTTPRequest
	respond  func(connector.HTTPRequest) (connector.HTTPResponse, error)
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, request)
	if t.respond != nil {
		return t.respond(request)
	}
	return sfResponse(map[string]any{"success": true, "msgData": map[string]any{"routeResps": []any{}}}), nil
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestProviderUsesTypedContractsAndRuntimeOnlySignature(t *testing.T) {
	transport := &recordingTransport{respond: func(request connector.HTTPRequest) (connector.HTTPResponse, error) {
		values, err := url.ParseQuery(string(request.Body))
		if err != nil {
			t.Fatal(err)
		}
		if values.Get("msgDigest") != "" || request.SecretForm["msgDigest"] != sfDigest(values.Get("msgData"), values.Get("timestamp"), "check-test") {
			t.Fatalf("request=%+v", request)
		}
		if values.Get("serviceCode") == createOrderServiceCode {
			return sfResponse(map[string]any{"success": true, "msgData": map[string]any{"waybillNo": "SF001"}}), nil
		}
		return sfResponse(map[string]any{"success": true, "msgData": map[string]any{"routeResps": []any{"route"}}}), nil
	}}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	p := adapter.(*provider)
	p.now = func() time.Time { return time.UnixMilli(1700000000000) }
	tests := []struct {
		operation, hash string
		mode            connector.OperationMode
		input           any
	}{
		{CreateShipment.Key, CreateShipment.ContractSHA256, connector.ModeCall, ProviderInput{Input: map[string]any{"orderId": "1"}}},
		{TrackShipment.Key, TrackShipment.ContractSHA256, connector.ModeCall, TrackShipmentInput{TrackingNumber: "SF001"}},
		{TestConnection.Key, TestConnection.ContractSHA256, connector.ModeCall, struct{}{}},
	}
	for _, test := range tests {
		payload, _ := json.Marshal(test.input)
		result, callErr := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: test.operation, ContractSHA256: test.hash, Mode: test.mode, RequestRef: "request-1", Connection: validConnection(), Secrets: map[string]string{"check_word": "check-test"}, Payload: payload})
		if callErr != nil || result.ResponseRef == "" {
			t.Fatalf("operation=%s result=%+v err=%v", test.operation, result, callErr)
		}
		request := transport.requests[len(transport.requests)-1]
		if request.MaxResponseBytes != responseLimit || request.SecretForm["msgDigest"] == "" {
			t.Fatalf("request=%+v", request)
		}
		encoded, _ := json.Marshal(request)
		if strings.Contains(string(encoded), "check-test") || strings.Contains(string(encoded), "msgDigest") {
			t.Fatalf("serialized request leaks secret material: %s", encoded)
		}
	}
}

func TestStartOperationPreservesDeliverySemantics(t *testing.T) {
	transport := &recordingTransport{respond: func(connector.HTTPRequest) (connector.HTTPResponse, error) {
		return sfResponse(map[string]any{"success": true, "msgData": map[string]any{"waybillNo": "SF-ASYNC"}}), nil
	}}
	adapter, _ := New(transport)
	payload, _ := json.Marshal(ProviderInput{Input: map[string]any{"orderId": "1"}})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: CreateShipmentOperation.Key, ContractSHA256: CreateShipmentOperation.ContractSHA256, Mode: connector.ModeStartOperation, Delivery: true, Connection: validConnection(), Secrets: map[string]string{"check_word": "check-test"}, Payload: payload})
	if err != nil || result.ResponseRef != "sf:SF-ASYNC" || len(result.Payload) != 0 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestValidationInputsAndFailureClassification(t *testing.T) {
	adapter, _ := New(&recordingTransport{})
	validator := adapter.(connector.ConfigValidator)
	for _, config := range []map[string]any{{"base_url": "http://remote.example", "partner_id": "p"}, {"base_url": "https://user:pass@example.com", "partner_id": "p"}, {"base_url": defaultBaseURL}, {"base_url": defaultBaseURL, "partner_id": "p", "timeout_seconds": 121}} {
		if validator.ValidateConfig(connector.Connection{Config: config}) == nil {
			t.Fatalf("accepted config=%v", config)
		}
	}
	tests := []struct {
		name, operation, hash string
		input                 any
		response              connector.HTTPResponse
		transportErr          error
		class                 connector.ErrorClassification
	}{
		{"write network", CreateShipment.Key, CreateShipment.ContractSHA256, ProviderInput{Input: map[string]any{"order": 1}}, connector.HTTPResponse{}, errors.New("reset"), connector.ErrorUncertain},
		{"write server", CreateShipment.Key, CreateShipment.ContractSHA256, ProviderInput{Input: map[string]any{"order": 1}}, connector.HTTPResponse{StatusCode: 503, Body: []byte(`{}`)}, nil, connector.ErrorUncertain},
		{"read network", TrackShipment.Key, TrackShipment.ContractSHA256, TrackShipmentInput{TrackingNumber: "SF1"}, connector.HTTPResponse{}, errors.New("reset"), connector.ErrorRetryable},
		{"rate limited", CreateShipment.Key, CreateShipment.ContractSHA256, ProviderInput{Input: map[string]any{"order": 1}}, connector.HTTPResponse{StatusCode: 429, Body: []byte(`{}`)}, nil, connector.ErrorRetryable},
		{"rejected", TrackShipment.Key, TrackShipment.ContractSHA256, TrackShipmentInput{TrackingNumber: "SF1"}, connector.HTTPResponse{StatusCode: 400, Body: []byte(`{"apiResultCode":"A2001"}`)}, nil, connector.ErrorPermanent},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transport := &recordingTransport{respond: func(connector.HTTPRequest) (connector.HTTPResponse, error) { return test.response, test.transportErr }}
			adapter, _ := New(transport)
			payload, _ := json.Marshal(test.input)
			_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: test.operation, ContractSHA256: test.hash, Mode: connector.ModeCall, Connection: validConnection(), Secrets: map[string]string{"check_word": "check-test"}, Payload: payload})
			classification, ok := connector.ErrorClassificationOf(err)
			if !ok || classification != test.class {
				t.Fatalf("classification=%q err=%v", classification, err)
			}
		})
	}
}

func TestNormalizationVariants(t *testing.T) {
	if _, _, err := normalizeSFResponse(map[string]any{"apiResultCode": "A1000", "apiResultData": "{"}, normalizeTracking); err == nil {
		t.Fatal("invalid response accepted")
	}
	if _, _, err := normalizeCreateShipment(map[string]any{}); err == nil {
		t.Fatal("missing waybill accepted")
	}
	output, ref, err := normalizeCreateShipment(map[string]any{"waybillNoInfoList": []any{map[string]any{"waybillNo": "SF2"}}, "documents": []any{"label"}})
	if err != nil || ref != "sf:SF2" || output["documents"] == nil {
		t.Fatalf("output=%v ref=%q err=%v", output, ref, err)
	}
}

func sfResponse(result map[string]any) connector.HTTPResponse {
	raw, _ := json.Marshal(result)
	body, _ := json.Marshal(map[string]any{"apiResultCode": "A1000", "apiResultData": string(raw)})
	return connector.HTTPResponse{StatusCode: http.StatusOK, Body: body}
}
func validConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"base_url": "http://127.0.0.1:8080/service", "partner_id": "partner-test", "test_tracking_number": "SF001", "timeout_seconds": 30}, SecretRefs: map[string]string{"check_word": "secret:check"}}
}
