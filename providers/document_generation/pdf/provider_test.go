package pdf

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
	"testing"
)

type transport struct{}

func (*transport) RoundTripHTTP(context.Context, connector.HTTPRequest) (connector.HTTPResponse, error) {
	return connector.HTTPResponse{}, errors.New("unexpected HTTP")
}
func (*transport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func TestRenderUnicodePDFAndSignature(t *testing.T) {
	adapter, err := New(&transport{})
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	payload, _ := json.Marshal(RenderInput{Title: "客户账单", Content: "订单已确认\nAmount: 120", Filename: "billing/2026"})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: RenderPDF.Key, ContractSHA256: RenderPDF.ContractSHA256, Mode: connector.ModeCall, Connection: connector.Connection{Config: map[string]any{"header": "Invoice", "footer": "Page 1", "watermark": "DRAFT"}, SecretRefs: map[string]string{"signature_secret": "secret:sign"}}, Secrets: map[string]string{"signature_secret": "private-signing-value"}, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	var output Response
	if json.Unmarshal(result.Payload, &output) != nil {
		t.Fatal("invalid output")
	}
	raw, err := base64.StdEncoding.DecodeString(output["document_base64"].(string))
	if err != nil || !bytes.HasPrefix(raw, []byte("%PDF-1.7")) || !bytes.Contains(raw, []byte("%%EOF")) {
		t.Fatalf("invalid PDF bytes=%d err=%v", len(raw), err)
	}
	if output["filename"] != "billing_2026.pdf" || output["signature_hmac_sha256"] == "" || result.ResponseRef == "" {
		t.Fatalf("output=%+v result=%+v", output, result)
	}
}
func TestValidationAndCancellation(t *testing.T) {
	adapter, _ := New(&transport{})
	connection := connector.Connection{Config: map[string]any{"max_content_bytes": 0}}
	if adapter.(connector.ConfigValidator).ValidateConfig(connection) == nil {
		t.Fatal("accepted zero content limit")
	}
	payload, _ := json.Marshal(RenderInput{Title: "Title", Content: "too long"})
	_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: RenderPDF.Key, ContractSHA256: RenderPDF.ContractSHA256, Mode: connector.ModeCall, Connection: connector.Connection{Config: map[string]any{"max_content_bytes": 4}}, Payload: payload})
	if err == nil {
		t.Fatal("accepted oversized content")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = adapter.Call(ctx, connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: RenderPDF.Key, ContractSHA256: RenderPDF.ContractSHA256, Mode: connector.ModeCall, Payload: payload})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
}
