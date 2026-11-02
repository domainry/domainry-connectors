package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	connector "github.com/domainry/domainry-connector-sdk"
)

var documentWriteIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
var documentWriteFilenamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9 ._-]{0,254}$`)

func validDocumentSource(source *PutDocumentSource) bool {
	if source == nil {
		return true
	}
	if source.Provider != "verdent_knowledge" {
		return false
	}
	// A Verdent Knowledge file belongs to the managed source even when it is
	// not a page projection. Page metadata is therefore optional as one atomic
	// group: either no page fields are present, or every page field is valid.
	if source.Title == "" && source.PageID == "" && source.PageVersion == 0 && source.LibraryKind == "" && source.ContentType == "" {
		return true
	}
	if !documentWriteIDPattern.MatchString(source.PageID) ||
		source.PageVersion < 1 || source.LibraryKind != "personal" && source.LibraryKind != "shared" ||
		!validText(source.ContentType, 64) || !utf8.ValidString(source.Title) || len(source.Title) > 512 ||
		strings.TrimSpace(source.Title) != source.Title {
		return false
	}
	return !strings.ContainsFunc(source.Title, func(value rune) bool {
		return unicode.IsControl(value) || value == unicode.ReplacementChar
	})
}

func (p *provider) documentWrite(ctx context.Context, connection connector.Connection, secrets map[string]string, principal connector.Principal, method, id, filename string, source *PutDocumentSource, content io.Reader, contentLength int64, contentSHA256, requestRef string) (connector.TypedResult[Output], error) {
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
		maximum := MaximumInlineDocumentBytesForFilename(filename)
		if !documentWriteFilenamePattern.MatchString(filename) || strings.TrimSpace(filename) != filename || !validDocumentSource(source) || content == nil || contentLength < 1 || maximum < 1 || contentLength > maximum || !connector.ValidSHA256(contentSHA256) {
			return zero, permanent("request_invalid")
		}
		query.Set("filename", filename)
		if source != nil {
			query.Set("source", source.Provider)
			if source.PageID != "" {
				query.Set("title", source.Title)
				query.Set("page_id", source.PageID)
				query.Set("page_version", strconv.FormatInt(source.PageVersion, 10))
				query.Set("library_kind", source.LibraryKind)
				query.Set("content_type", source.ContentType)
			}
		}
		headers, err = documentPermissionHeaders(connection, requestRef)
		if err != nil {
			return zero, err
		}
	}
	if method == http.MethodPost {
		return p.exchangeContentHeaders(ctx, method, base+"/v1/kb/kbs/"+url.PathEscape(kb)+"/documents?"+query.Encode(), "application/octet-stream", content, contentLength, contentSHA256, token, kb, headers)
	}
	return p.exchangeHeaders(ctx, method, base+"/v1/kb/kbs/"+url.PathEscape(kb)+"/documents?"+query.Encode(), "application/octet-stream", nil, token, kb, headers)
}

func (p *provider) putDocument(ctx context.Context, r connector.TypedRequest[PutDocumentInput]) (connector.TypedResult[Output], error) {
	if r.Input.Bytes != r.ContentLength || r.Input.SHA256 != r.ContentSHA256 || r.ContentType != "application/octet-stream" {
		return connector.TypedResult[Output]{}, permanent("request_invalid")
	}
	return p.documentWrite(ctx, r.Connection, r.Secrets, r.Principal, http.MethodPost, r.Input.DocID, r.Input.Filename, r.Input.Source, r.Content, r.ContentLength, r.ContentSHA256, r.RequestRef)
}

func (p *provider) deleteDocument(ctx context.Context, r connector.TypedRequest[DocumentInput]) (connector.TypedResult[Output], error) {
	return p.documentWrite(ctx, r.Connection, r.Secrets, r.Principal, http.MethodDelete, r.Input.DocID, "", nil, nil, 0, "", "")
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
