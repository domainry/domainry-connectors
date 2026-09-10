package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
)

const MaxDocumentBytes = 16 << 20

// Writes report the upstream acknowledgement, not indexing completion. The
// API has no verified idempotency/fencing or atomic ACL update contract, so the
// host must serialize document generations and reconcile uncertain writes.
var PutDocument = connector.CallOperation[PutDocumentInput, Output]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "put_document", ContractSHA256: "ada514ca5026f94fbe6191208dcd185567e2124d0fa04caba9d0ef8795d0f3c1", Reliability: documentWriteReliability()}
var DeleteDocument = connector.CallOperation[DocumentInput, Output]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "delete_document", ContractSHA256: "4729fefbfaf34173e5f8d651216f66c293091af7a4f72240004b1557e49054b6", Reliability: documentWriteReliability()}
var DocumentStatus = connector.CallOperation[DocumentInput, DocumentStatusOutput]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "document_status", ContractSHA256: "7b5608d5de92711c8069d7f0df8e41c789175584f5cd94dbe6e1e714331eb9cb", Reliability: readReliability()}

type DocumentInput struct {
	DocID string `json:"doc_id"`
}

// Content is base64 in SDK JSON and original bytes in the HTTP request. A file
// path, remote KB, ACL, or credential is never accepted as operation input.
type PutDocumentInput struct {
	DocID    string `json:"doc_id"`
	Filename string `json:"filename"`
	Content  []byte `json:"content"`
}

type DocumentStatusOutput struct {
	Provider    string `json:"provider"`
	KBID        string `json:"kb_id"`
	DocID       string `json:"doc_id"`
	Exists      bool   `json:"exists"`                 // Present in the current permission scope, not a global existence oracle.
	IndexStatus string `json:"index_status,omitempty"` // Upstream status, not an invented ready state.
}

func documentWriteReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNone}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

func (p *provider) documentWrite(ctx context.Context, connection connector.Connection, secrets map[string]string, principal connector.Principal, method, id, filename string, content []byte) (connector.TypedResult[Output], error) {
	var zero connector.TypedResult[Output]
	if !principal.IsAuthenticated || !validText(principal.UserID, 255) || connection.WorkspaceID == "" || principal.WorkspaceID != connection.WorkspaceID {
		return zero, permanent("access_denied")
	}
	base, _, kb, err := settings(connection)
	if err != nil {
		return zero, err
	}
	if kb == "." || kb == ".." || strings.ContainsAny(kb, "/\\") || !validText(id, 4096) {
		return zero, permanent("request_invalid")
	}
	token := strings.TrimSpace(secrets["api_key"])
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return zero, permanent("access_denied")
	}
	query := url.Values{"doc_id": {id}}
	if method == http.MethodPost {
		if !validText(filename, 255) || filename == "." || filename == ".." || strings.ContainsAny(filename, "/\\\r\n") || len(content) < 1 || len(content) > MaxDocumentBytes {
			return zero, permanent("request_invalid")
		}
		query.Set("filename", filename)
	}
	return p.exchange(ctx, method, base+"/v1/kb/kbs/"+url.PathEscape(kb)+"/documents?"+query.Encode(), "application/octet-stream", content, token, kb)
}

func (p *provider) putDocument(ctx context.Context, r connector.TypedRequest[PutDocumentInput]) (connector.TypedResult[Output], error) {
	return p.documentWrite(ctx, r.Connection, r.Secrets, r.Principal, http.MethodPost, r.Input.DocID, r.Input.Filename, r.Input.Content)
}

func (p *provider) deleteDocument(ctx context.Context, r connector.TypedRequest[DocumentInput]) (connector.TypedResult[Output], error) {
	return p.documentWrite(ctx, r.Connection, r.Secrets, r.Principal, http.MethodDelete, r.Input.DocID, "", nil)
}

func (p *provider) documentStatus(ctx context.Context, r connector.TypedRequest[DocumentInput]) (connector.TypedResult[DocumentStatusOutput], error) {
	var out connector.TypedResult[DocumentStatusOutput]
	if !validText(r.Input.DocID, 4096) {
		return out, permanent("request_invalid")
	}
	result, err := p.request(ctx, r.Connection, r.Secrets, r.Principal, "/v1/kb/fetch", "metadata", map[string]any{"doc_id": r.Input.DocID, "include_content": false, "live": map[string]any{"enabled": false}})
	if code, _ := connector.ProviderErrorCodeOf(err); code == "knowledge_api.not_found" {
		_, _, kb, _ := settings(r.Connection)
		out.ResponseRef = result.ResponseRef
		out.Output = DocumentStatusOutput{Provider: ProviderKey, KBID: kb, DocID: r.Input.DocID, Exists: false}
		return out, nil
	}
	if err != nil {
		return out, err
	}
	var envelope struct {
		Data struct {
			DocID  string `json:"doc_id"`
			Status string `json:"status"`
		} `json:"data"`
	}
	if json.Unmarshal(result.Output.Result, &envelope) != nil || !validText(envelope.Data.Status, 64) || envelope.Data.DocID != "" && envelope.Data.DocID != r.Input.DocID {
		return out, permanent("response_invalid")
	}
	out.ResponseRef = result.ResponseRef
	out.Output = DocumentStatusOutput{Provider: ProviderKey, KBID: result.Output.KBID, DocID: r.Input.DocID, Exists: true, IndexStatus: envelope.Data.Status}
	return out, nil
}
