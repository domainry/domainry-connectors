package httpapi

import (
	"encoding/json"
	"reflect"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
)

const analysisFixtureDigest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func analysisFixtureItem() AnalysisTableCatalogItem {
	return AnalysisTableCatalogItem{
		DatasetKey: "table_0123456789abcdef0123456789abcdef", DocID: "doc-1", TableRef: "tbl_fixture_0",
		Name: "Revenue", Sheet: "Revenue", DefinitionVersion: analysisFixtureDigest,
		DataVersion: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Generation:  "gen_current", DocVersion: 2, RowCount: 2, Complete: true,
		Columns: []AnalysisTableColumn{{Key: "amount", Name: "Amount", Type: "decimal", Precision: 18, Scale: 2}, {Key: "region", Name: "Region", Type: "text"}},
	}
}

func analysisRequest(op connector.OperationDescriptor, input any) connector.CallRequest {
	r := request(op, input)
	r.Connection.Config["analysis_document_ids"] = []string{"doc-2", "doc-1", "doc-1"}
	return r
}

func TestAnalysisTableOperationsUseTrustedDocumentsAndValidateEveryPage(t *testing.T) {
	item := analysisFixtureItem()
	catalogBody, _ := json.Marshal(map[string]any{"err_code": 0, "data": map[string]any{"tables": []AnalysisTableCatalogItem{item}}})
	transport := &recordingTransport{response: connector.HTTPResponse{StatusCode: 200, Body: catalogBody}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Call(t.Context(), analysisRequest(CatalogAnalysisTables.Descriptor(), AnalysisTableCatalogInput{}))
	if err != nil {
		t.Fatal(err)
	}
	var catalog AnalysisTableCatalogOutput
	if json.Unmarshal(result.Payload, &catalog) != nil || catalog.Provider != ProviderKey || len(catalog.Tables) != 1 {
		t.Fatalf("catalog=%s", result.Payload)
	}
	var sent map[string]any
	if json.Unmarshal(transport.requests[0].Body, &sent) != nil || !reflect.DeepEqual(sent["document_ids"], []any{"doc-1", "doc-2"}) || sent["team_id"] != "team" || sent["kb_id"] != "bcri" {
		t.Fatalf("catalog request=%s", transport.requests[0].Body)
	}

	amount, east, next := "10.25", "east", 1
	read := AnalysisTableReadOutput{
		AnalysisTableCatalogItem: item, Fields: []string{"amount", "region"}, ContentSHA256: analysisFixtureDigest,
		Offset: 0, Rows: [][]*string{{&amount, &east}}, NextOffset: &next,
	}
	readBody, _ := json.Marshal(map[string]any{"err_code": 0, "data": read})
	transport.response = connector.HTTPResponse{StatusCode: 200, Body: readBody}
	input := AnalysisTableReadInput{
		DocID: item.DocID, DatasetKey: item.DatasetKey, Generation: item.Generation,
		DefinitionVersion: item.DefinitionVersion, DataVersion: item.DataVersion,
		Fields: []string{"amount", "region"}, Offset: 0, Limit: 1,
	}
	result, err = adapter.Call(t.Context(), analysisRequest(ReadAnalysisTable.Descriptor(), input))
	if err != nil {
		t.Fatal(err)
	}
	var output AnalysisTableReadOutput
	if json.Unmarshal(result.Payload, &output) != nil || len(output.Rows) != 1 || output.Provider != ProviderKey || output.KBID != "bcri" {
		t.Fatalf("read=%s", result.Payload)
	}
	if transport.requests[1].URL != "https://kb.example.com/v1/kb/analysis/tables/read" {
		t.Fatalf("read URL=%q", transport.requests[1].URL)
	}

	read.Rows[0] = read.Rows[0][:1]
	readBody, _ = json.Marshal(map[string]any{"err_code": 0, "data": read})
	transport.response.Body = readBody
	if _, err = adapter.Call(t.Context(), analysisRequest(ReadAnalysisTable.Descriptor(), input)); err == nil {
		t.Fatal("invalid row width accepted")
	}
}

func TestAnalysisTableOperationsRejectUnconfiguredOrUnlistedDocuments(t *testing.T) {
	adapter, err := New(&recordingTransport{})
	if err != nil {
		t.Fatal(err)
	}
	r := request(CatalogAnalysisTables.Descriptor(), AnalysisTableCatalogInput{})
	if _, err = adapter.Call(t.Context(), r); err == nil {
		t.Fatal("catalog accepted without trusted documents")
	}
	r = analysisRequest(ReadAnalysisTable.Descriptor(), AnalysisTableReadInput{
		DocID: "doc-private", DatasetKey: "table_0123456789abcdef0123456789abcdef",
		Generation: "gen_current", DefinitionVersion: analysisFixtureDigest,
		DataVersion: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Fields:      []string{"amount"}, Limit: 1,
	})
	if _, err = adapter.Call(t.Context(), r); err == nil {
		t.Fatal("unlisted document accepted")
	}
}
