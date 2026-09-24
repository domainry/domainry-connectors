package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
)

var documentWriteIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
var documentWriteFilenamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9 ._-]{0,254}$`)

func (p *provider) documentWrite(ctx context.Context, connection connector.Connection, secrets map[string]string, principal connector.Principal, method, id, filename string, content []byte, requestRef string) (connector.TypedResult[Output], error) {
	var zero connector.TypedResult[Output]
	if !principal.IsAuthenticated || !validText(principal.UserID, 255) || connection.WorkspaceID == "" || principal.WorkspaceID != connection.WorkspaceID {
		return zero, permanent("access_denied")
	}
	base, _, kb, err := settings(connection)
	if err != nil {
		return zero, err
	}
	if kb == "." || kb == ".." || strings.ContainsAny(kb, "/\\") || !documentWriteIDPattern.MatchString(id) {
		return zero, permanent("request_invalid")
	}
	token := strings.TrimSpace(secrets["api_key"])
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return zero, permanent("access_denied")
	}
	query := url.Values{"doc_id": {id}}
	var headers map[string][]string
	if method == http.MethodPost {
		if !documentWriteFilenamePattern.MatchString(filename) || strings.TrimSpace(filename) != filename || len(content) < 1 || len(content) > MaxDocumentBytes {
			return zero, permanent("request_invalid")
		}
		query.Set("filename", filename)
		headers, err = documentPermissionHeaders(connection, requestRef)
		if err != nil {
			return zero, err
		}
	}
	return p.exchangeHeaders(ctx, method, base+"/v1/kb/kbs/"+url.PathEscape(kb)+"/documents?"+query.Encode(), "application/octet-stream", content, token, kb, headers)
}

func (p *provider) putDocument(ctx context.Context, r connector.TypedRequest[PutDocumentInput]) (connector.TypedResult[Output], error) {
	return p.documentWrite(ctx, r.Connection, r.Secrets, r.Principal, http.MethodPost, r.Input.DocID, r.Input.Filename, r.Input.Content, r.RequestRef)
}

func (p *provider) deleteDocument(ctx context.Context, r connector.TypedRequest[DocumentInput]) (connector.TypedResult[Output], error) {
	return p.documentWrite(ctx, r.Connection, r.Secrets, r.Principal, http.MethodDelete, r.Input.DocID, "", nil, "")
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
