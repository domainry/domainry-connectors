package httpapi

import contract "github.com/domainry/domainry-connector-sdk/providers/knowledge_base/http_api"

const ConnectorKey = contract.ConnectorKey
const ProviderKey = contract.ProviderKey
const MaxDocumentBytes = contract.MaxDocumentBytes

type SearchInput = contract.SearchInput
type FetchInput = contract.FetchInput
type Output = contract.Output
type DocumentInput = contract.DocumentInput
type PutDocumentInput = contract.PutDocumentInput
type DocumentStatusOutput = contract.DocumentStatusOutput
type AnalysisTableCatalogInput = contract.AnalysisTableCatalogInput
type AnalysisTableColumn = contract.AnalysisTableColumn
type AnalysisTableCatalogItem = contract.AnalysisTableCatalogItem
type AnalysisTableCatalogOutput = contract.AnalysisTableCatalogOutput
type AnalysisTableReadInput = contract.AnalysisTableReadInput
type AnalysisTableReadOutput = contract.AnalysisTableReadOutput

var Search = contract.Search
var Fetch = contract.Fetch
var PutDocument = contract.PutDocument
var DeleteDocument = contract.DeleteDocument
var DocumentStatus = contract.DocumentStatus
var CatalogAnalysisTables = contract.CatalogAnalysisTables
var ReadAnalysisTable = contract.ReadAnalysisTable
