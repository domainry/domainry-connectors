package brazilportalunicoclassif

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
)

const catalogFixture = `{"Data_Ultima_Atualizacao_NCM":"Vigente em 11/08/2026","Ato":"Resolução Gecex nº 926/2026","Nomenclaturas":[{"Codigo":"8481.80.21","Descricao":"Válvulas de esfera, de aço","Data_Inicio":"01/04/2022","Data_Fim":"31/12/9999","Tipo_Ato_Ini":"Res Camex","Numero_Ato_Ini":"272","Ano_Ato_Ini":"2021"},{"Codigo":"8481.80.99","Descricao":"Outros dispositivos","Data_Inicio":"01/04/2022","Data_Fim":"31/12/9999","Tipo_Ato_Ini":"Res Camex","Numero_Ato_Ini":"272","Ano_Ato_Ini":"2021"},{"Codigo":"8481.90.90","Descricao":"Outras partes","Data_Inicio":"01/04/2022","Data_Fim":"31/12/9999","Tipo_Ato_Ini":"Res Camex","Numero_Ato_Ini":"272","Ano_Ato_Ini":"2021"}]}`

type recordingTransport struct {
	requests []connector.HTTPRequest
	response connector.HTTPResponse
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, request)
	return t.response, nil
}

func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestProviderReturnsAllHS6CandidatesAndNonExcludingAttributeEvidence(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{
		StatusCode: http.StatusOK, Body: []byte(catalogFixture),
		Headers: map[string][]string{"Content-Disposition": {`attachment;filename=Tabela_NCM_Vigente_20260811.json`}},
	}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	connection := connector.Connection{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Config: map[string]any{"base_url": "http://localhost:8080"}}
	payload, _ := json.Marshal(MatchCandidatesInput{HS6: "848180", ProductAttributes: map[string]any{"material": "aço", "type": "esfera"}})
	call, err := adapter.Call(t.Context(), connector.CallRequest{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: MatchCandidates.Key,
		ContractSHA256: MatchCandidates.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	var output MatchCandidatesOutput
	if err := json.Unmarshal(call.Payload, &output); err != nil {
		t.Fatal(err)
	}
	result := output.Result
	if result["retrieval_status"] != "live" || result["source_trust"] != "official_current_public_catalog" || result["code_system"] != "brazil_ncm_8" || result["final_classification"] != false || result["human_confirmation_required"] != true {
		t.Fatalf("boundary=%+v", result)
	}
	candidates := result["candidates"].([]any)
	matches := result["attribute_matched_candidates"].([]any)
	if len(candidates) != 2 || candidates[0].(map[string]any)["code"] != "84818021" || candidates[1].(map[string]any)["code"] != "84818099" || len(matches) != 1 {
		t.Fatalf("candidates=%+v matches=%+v", candidates, matches)
	}
	if result["official_catalog_version"] != "Vigente em 11/08/2026" || result["official_catalog_effective_date"] != "2026-08-11" || result["official_legal_act"] != "Resolução Gecex nº 926/2026" {
		t.Fatalf("catalog=%+v", result)
	}
	if result["raw_response_bytes"].(float64) != float64(len(catalogFixture)) || !strings.HasPrefix(result["request_sha256"].(string), "sha256:") || !strings.HasPrefix(result["raw_response_sha256"].(string), "sha256:") {
		t.Fatalf("evidence=%+v", result)
	}
	if len(transport.requests) != 1 || !strings.Contains(transport.requests[0].URL, "perfil=PUBLICO") || transport.requests[0].MaxResponseBytes != 8<<20 {
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

func TestProviderMakesNoResultAndRemoteFailuresExplicit(t *testing.T) {
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(catalogFixture)}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	connection := connector.Connection{Config: map[string]any{"base_url": "http://127.0.0.1:8080", "max_response_bytes": 1048576}}
	payload, _ := json.Marshal(MatchCandidatesInput{HS6: "999999"})
	call, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: MatchCandidates.Key, ContractSHA256: MatchCandidates.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	var output MatchCandidatesOutput
	if err := json.Unmarshal(call.Payload, &output); err != nil || output.Result["retrieval_status"] != "no_result" || output.Result["manual_fallback_required"] != true {
		t.Fatalf("output=%+v error=%v", output, err)
	}
	for _, endpoint := range []string{"https://example.com", "http://remote.example", "https://portalunico.siscomex.gov.br?x=1"} {
		if err := adapter.(connector.ConfigValidator).ValidateConfig(connector.Connection{Config: map[string]any{"base_url": endpoint}}); err == nil {
			t.Fatalf("endpoint %q accepted", endpoint)
		}
	}
	transport.response = connector.HTTPResponse{StatusCode: http.StatusTooManyRequests}
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: MatchCandidates.Key, ContractSHA256: MatchCandidates.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Payload: payload})
	classification, ok := connector.ErrorClassificationOf(err)
	code, coded := connector.ProviderErrorCodeOf(err)
	if !ok || classification != connector.ErrorRetryable || !coded || code != "tariff_classification.brazil_portal_unico_classif.rate_limited" {
		t.Fatalf("classification=%q/%v code=%q/%v error=%v", classification, ok, code, coded, err)
	}
	transport.response = connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{}`)}
	_, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: MatchCandidates.Key, ContractSHA256: MatchCandidates.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Payload: payload})
	if code, _ := connector.ProviderErrorCodeOf(err); code != "tariff_classification.brazil_portal_unico_classif.catalog_shape_changed" {
		t.Fatalf("shape error=%v", err)
	}
}
