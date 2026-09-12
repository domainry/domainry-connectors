package httpapi

import (
	"context"
	"encoding/json"
	"regexp"
	"slices"
	"sort"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
)

var CatalogAnalysisTables = connector.CallOperation[AnalysisTableCatalogInput, AnalysisTableCatalogOutput]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "analysis_table_catalog", ContractSHA256: "a4cc8ab7e1c7f78a2c17995dece68922d90dfb19e6b1dc5017199074be29b266", Reliability: readReliability()}
var ReadAnalysisTable = connector.CallOperation[AnalysisTableReadInput, AnalysisTableReadOutput]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "analysis_table_read", ContractSHA256: "292b1dbe23ad36bdbaad40fb4e25af395f7f7ba4aee9630503849d428e09efa0", Reliability: readReliability()}

type AnalysisTableCatalogInput struct{}

type AnalysisTableColumn struct {
	Key       string `json:"key"`
	Name      string `json:"name"`
	Type      string `json:"type"`
	Unit      string `json:"unit"`
	Precision int    `json:"precision,omitempty"`
	Scale     int    `json:"scale,omitempty"`
}

type AnalysisTableCatalogItem struct {
	DatasetKey        string                `json:"dataset_key"`
	DocID             string                `json:"doc_id"`
	TableRef          string                `json:"table_ref"`
	Name              string                `json:"name"`
	Sheet             string                `json:"sheet"`
	DefinitionVersion string                `json:"definition_version"`
	DataVersion       string                `json:"data_version"`
	Generation        string                `json:"generation"`
	DocVersion        int                   `json:"doc_version"`
	RowCount          int64                 `json:"row_count"`
	Complete          bool                  `json:"complete"`
	Columns           []AnalysisTableColumn `json:"columns"`
}

type AnalysisTableCatalogOutput struct {
	Provider string                     `json:"provider"`
	KBID     string                     `json:"kb_id"`
	Tables   []AnalysisTableCatalogItem `json:"tables"`
}

type AnalysisTableReadInput struct {
	DocID             string   `json:"doc_id"`
	DatasetKey        string   `json:"dataset_key"`
	Generation        string   `json:"generation"`
	DefinitionVersion string   `json:"definition_version"`
	DataVersion       string   `json:"data_version"`
	Fields            []string `json:"fields"`
	Offset            int      `json:"offset"`
	Limit             int      `json:"limit"`
}

type AnalysisTableReadOutput struct {
	Provider string `json:"provider"`
	KBID     string `json:"kb_id"`
	AnalysisTableCatalogItem
	Fields        []string    `json:"fields"`
	ContentSHA256 string      `json:"content_sha256"`
	Offset        int         `json:"offset"`
	Rows          [][]*string `json:"rows"`
	NextOffset    *int        `json:"next_offset"`
}

var analysisIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
var analysisDigest = regexp.MustCompile(`^[0-9a-f]{64}$`)

func analysisDocumentIDs(connection connector.Connection) ([]string, error) {
	value, ok := connection.Config["analysis_document_ids"]
	if !ok {
		return nil, permanent("analysis_unavailable")
	}
	var ids []string
	switch values := value.(type) {
	case []string:
		ids = slices.Clone(values)
	case []any:
		ids = make([]string, len(values))
		for index, value := range values {
			var valid bool
			ids[index], valid = value.(string)
			if !valid {
				return nil, permanent("request_invalid")
			}
		}
	default:
		return nil, permanent("request_invalid")
	}
	if len(ids) < 1 || len(ids) > 50 {
		return nil, permanent("request_invalid")
	}
	for _, id := range ids {
		if !validText(id, 256) {
			return nil, permanent("request_invalid")
		}
	}
	sort.Strings(ids)
	return slices.Compact(ids), nil
}

func decodeAnalysisData(raw json.RawMessage, target any) error {
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if json.Unmarshal(raw, &envelope) != nil || len(envelope.Data) == 0 || string(envelope.Data) == "null" || json.Unmarshal(envelope.Data, target) != nil {
		return permanent("response_invalid")
	}
	return nil
}

func validAnalysisCatalogItem(item AnalysisTableCatalogItem) bool {
	if !analysisIdentifier.MatchString(item.DatasetKey) || !validText(item.DocID, 256) || !validText(item.TableRef, 256) || !validText(item.Name, 256) || !validText(item.Generation, 256) || !analysisDigest.MatchString(item.DefinitionVersion) || !analysisDigest.MatchString(item.DataVersion) || item.DocVersion < 1 || item.RowCount < 0 || item.RowCount > 100000 || !item.Complete || len(item.Columns) < 1 || len(item.Columns) > 256 {
		return false
	}
	seen := map[string]bool{}
	for _, column := range item.Columns {
		if !analysisIdentifier.MatchString(column.Key) || seen[column.Key] || len(column.Name) > 256 || len(column.Unit) > 64 || !map[string]bool{"text": true, "boolean": true, "integer": true, "decimal": true, "number": true, "date": true, "datetime": true}[column.Type] || column.Precision < 0 || column.Scale < 0 {
			return false
		}
		seen[column.Key] = true
	}
	return true
}

func (p *provider) catalogAnalysisTables(ctx context.Context, r connector.TypedRequest[AnalysisTableCatalogInput]) (connector.TypedResult[AnalysisTableCatalogOutput], error) {
	var out connector.TypedResult[AnalysisTableCatalogOutput]
	documents, err := analysisDocumentIDs(r.Connection)
	if err != nil {
		return out, err
	}
	result, err := p.request(ctx, r.Connection, r.Secrets, r.Principal, "/v1/kb/analysis/tables/catalog", "", map[string]any{"document_ids": documents})
	if err != nil {
		return out, err
	}
	var data struct {
		Tables []AnalysisTableCatalogItem `json:"tables"`
	}
	if err := decodeAnalysisData(result.Output.Result, &data); err != nil || len(data.Tables) > 256 {
		return out, permanent("response_invalid")
	}
	seen := map[string]bool{}
	for _, item := range data.Tables {
		if !validAnalysisCatalogItem(item) || seen[item.DatasetKey] || !slices.Contains(documents, item.DocID) {
			return out, permanent("response_invalid")
		}
		seen[item.DatasetKey] = true
	}
	_, _, kb, _ := settings(r.Connection)
	out.ResponseRef = result.ResponseRef
	out.Output = AnalysisTableCatalogOutput{Provider: ProviderKey, KBID: kb, Tables: data.Tables}
	if out.Output.Tables == nil {
		out.Output.Tables = []AnalysisTableCatalogItem{}
	}
	return out, nil
}

func validAnalysisReadInput(in AnalysisTableReadInput) bool {
	if !validText(in.DocID, 256) || !analysisIdentifier.MatchString(in.DatasetKey) || !validText(in.Generation, 256) || !analysisDigest.MatchString(in.DefinitionVersion) || !analysisDigest.MatchString(in.DataVersion) || in.Offset < 0 || in.Limit < 0 || in.Limit > 500 || len(in.Fields) < 1 || len(in.Fields) > 256 || !sort.StringsAreSorted(in.Fields) {
		return false
	}
	for index, field := range in.Fields {
		if !analysisIdentifier.MatchString(field) || index > 0 && field == in.Fields[index-1] {
			return false
		}
	}
	return true
}

func (p *provider) readAnalysisTable(ctx context.Context, r connector.TypedRequest[AnalysisTableReadInput]) (connector.TypedResult[AnalysisTableReadOutput], error) {
	var out connector.TypedResult[AnalysisTableReadOutput]
	documents, err := analysisDocumentIDs(r.Connection)
	if err != nil {
		return out, err
	}
	if !validAnalysisReadInput(r.Input) || !slices.Contains(documents, r.Input.DocID) {
		return out, permanent("request_invalid")
	}
	result, err := p.request(ctx, r.Connection, r.Secrets, r.Principal, "/v1/kb/analysis/tables/read", "", map[string]any{
		"doc_id": r.Input.DocID, "dataset_key": r.Input.DatasetKey,
		"generation": r.Input.Generation, "definition_version": r.Input.DefinitionVersion,
		"data_version": r.Input.DataVersion, "fields": r.Input.Fields,
		"offset": r.Input.Offset, "limit": r.Input.Limit,
	})
	if err != nil {
		return out, err
	}
	var data AnalysisTableReadOutput
	if err := decodeAnalysisData(result.Output.Result, &data); err != nil || !validAnalysisCatalogItem(data.AnalysisTableCatalogItem) || data.DocID != r.Input.DocID || data.DatasetKey != r.Input.DatasetKey || data.Generation != r.Input.Generation || data.DefinitionVersion != r.Input.DefinitionVersion || data.DataVersion != r.Input.DataVersion || data.Offset != r.Input.Offset || !slices.Equal(data.Fields, r.Input.Fields) || !analysisDigest.MatchString(data.ContentSHA256) || len(data.Rows) > r.Input.Limit {
		return out, permanent("response_invalid")
	}
	for _, row := range data.Rows {
		if len(row) != len(data.Fields) {
			return out, permanent("response_invalid")
		}
		for _, cell := range row {
			if cell != nil && (len(*cell) > 16<<10 || strings.ContainsRune(*cell, 0)) {
				return out, permanent("response_invalid")
			}
		}
	}
	if data.NextOffset != nil && (*data.NextOffset <= data.Offset || *data.NextOffset != data.Offset+len(data.Rows) || *data.NextOffset > int(data.RowCount)) || data.NextOffset == nil && data.Offset+len(data.Rows) < int(data.RowCount) && r.Input.Limit > 0 {
		return out, permanent("response_invalid")
	}
	_, _, kb, _ := settings(r.Connection)
	data.Provider, data.KBID = ProviderKey, kb
	out.ResponseRef, out.Output = result.ResponseRef, data
	return out, nil
}
