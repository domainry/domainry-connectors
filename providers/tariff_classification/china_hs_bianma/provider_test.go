package chinahsbianma

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
	requests []connector.HTTPRequest
	response connector.HTTPResponse
	err      error
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, request)
	return t.response, t.err
}

func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestProviderPreservesUntrustedAuxiliaryBoundaryAndEvidence(t *testing.T) {
	raw := `<!doctype html><form action="/search/index" method="get"></form>
<div class="list"><div>8481804090</div><div><b>其他阀门</b></div></div>
<div class="list"><div>8481300000</div><div><b>止回&amp;阀</b></div></div>`
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(raw)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	connection := connector.Connection{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Config: map[string]any{"base_url": "http://127.0.0.1:8080"}}
	payload, _ := json.Marshal(SearchCandidatesInput{Query: "工业阀门"})
	call, err := adapter.Call(t.Context(), connector.CallRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SearchCandidates.Key,
		ContractSHA256: SearchCandidates.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	var output SearchCandidatesOutput
	if err := json.Unmarshal(call.Payload, &output); err != nil {
		t.Fatal(err)
	}
	result := output.Result
	if result.RetrievalStatus != "live" || result.SourceTrust != "untrusted_optional_auxiliary" || result.IsBrazilNCM || result.FinalClassification || !result.HumanConfirmationRequired {
		t.Fatalf("boundary=%+v", result)
	}
	if len(result.Candidates) != 2 || result.Candidates[0].Code != "8481300000" || result.Candidates[0].Description != "止回&阀" || result.Candidates[1].Code != "8481804090" {
		t.Fatalf("candidates=%+v", result.Candidates)
	}
	if result.RequestBytes != len([]byte("query=工业阀门")) || result.RawResponseBytes != len(raw) || !strings.HasPrefix(result.RequestSHA256, "sha256:") || !strings.HasPrefix(result.RawResponseSHA256, "sha256:") {
		t.Fatalf("evidence=%+v", result)
	}
	if len(transport.requests) != 1 || !strings.Contains(transport.requests[0].URL, "/search/index?ser=") || transport.requests[0].MaxResponseBytes != 1<<20 {
		t.Fatalf("request=%+v", transport.requests)
	}
	probe, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: connection})
	if err != nil || !probe.Connected {
		t.Fatalf("probe=%+v error=%v", probe, err)
	}
	if got := adapter.Descriptor(); got.StartupActivation != connector.StartupActivationDefaultSafe || len(got.Operations) != 1 {
		t.Fatalf("descriptor=%+v", got)
	}
}

func TestProviderMakesNoResultAndFailureClassesExplicit(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`<html><form action="/search/index"></form><title>无 的HS编码查询结果</title></html>`)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	connection := connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080", "max_response_bytes": 65536}}
	payload, _ := json.Marshal(SearchCandidatesInput{Query: "无结果"})
	call, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SearchCandidates.Key, ContractSHA256: SearchCandidates.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	var output SearchCandidatesOutput
	if err := json.Unmarshal(call.Payload, &output); err != nil || output.Result.RetrievalStatus != "no_result" || !output.Result.ManualFallbackRequired || len(output.Result.Candidates) != 0 {
		t.Fatalf("output=%+v error=%v", output, err)
	}
	for _, endpoint := range []string{"https://example.com", "http://remote.example", "https://hs-bianma.com?x=1"} {
		if err := adapter.(connector.ConfigValidator).ValidateConfig(connector.Connection{Config: map[string]any{"base_url": endpoint}}); err == nil {
			t.Fatalf("endpoint %q accepted", endpoint)
		}
	}
	transport.response = connector.HTTPResponse{StatusCode: http.StatusTooManyRequests}
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SearchCandidates.Key, ContractSHA256: SearchCandidates.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Payload: payload})
	classification, ok := connector.ErrorClassificationOf(err)
	code, coded := connector.ProviderErrorCodeOf(err)
	if !ok || classification != connector.ErrorRetryable || !coded || code != "tariff_classification.china_hs_bianma.rate_limited" {
		t.Fatalf("classification=%q/%v code=%q/%v error=%v", classification, ok, code, coded, err)
	}
	transport.response = connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(strings.Repeat("x", 65537))}
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SearchCandidates.Key, ContractSHA256: SearchCandidates.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Payload: payload})
	if code, _ := connector.ProviderErrorCodeOf(err); code != "tariff_classification.china_hs_bianma.response_too_large" {
		t.Fatalf("large response error=%v", err)
	}
}
