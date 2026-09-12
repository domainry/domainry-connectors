// Package httpapi implements workspace-scoped knowledge retrieval and document management.
package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	ConnectorKey  = "knowledge_base"
	ProviderKey   = "http_api"
	responseLimit = 512 * 1024
)

var Search = connector.CallOperation[SearchInput, Output]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "search", ContractSHA256: "573e693a2b63cd71a5f0937f5d1be49124e36d312de707160a5524ec00ab6c6a", Reliability: readReliability()}
var Fetch = connector.CallOperation[FetchInput, Output]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "fetch", ContractSHA256: "31256a5720fc482b2fd0cd0b259936a58f8bc903c712a66f567c934f42a6b80e", Reliability: readReliability()}

type SearchInput struct {
	Query         string `json:"query"`
	TopK          int    `json:"top_k,omitempty"`
	ResultContent string `json:"result_content,omitempty"`
}
type FetchInput struct {
	DocID         string `json:"doc_id"`
	ResultContent string `json:"result_content,omitempty"`
}

// The console documents requests, not a response schema. Preserve upstream
// JSON and sources without guessing snippet/title/full-text field names.
type Output struct {
	Provider string          `json:"provider"`
	KBID     string          `json:"kb_id"`
	Result   json.RawMessage `json:"result"`
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("knowledge transport is required")
	}
	p := &provider{transport: transport}
	search, err := connector.BindCall(Search, p.search)
	if err != nil {
		return nil, err
	}
	fetch, err := connector.BindCall(Fetch, p.fetch)
	if err != nil {
		return nil, err
	}
	put, err := connector.BindCall(PutDocument, p.putDocument)
	if err != nil {
		return nil, err
	}
	remove, err := connector.BindCall(DeleteDocument, p.deleteDocument)
	if err != nil {
		return nil, err
	}
	status, err := connector.BindCall(DocumentStatus, p.documentStatus)
	if err != nil {
		return nil, err
	}
	catalogTables, err := connector.BindCall(CatalogAnalysisTables, p.catalogAnalysisTables)
	if err != nil {
		return nil, err
	}
	readTable, err := connector.BindCall(ReadAnalysisTable, p.readAnalysisTable)
	if err != nil {
		return nil, err
	}
	p.Adapter, err = connector.NewProvider(schema(), search, fetch, put, remove, status, catalogTables, readTable)
	return p, err
}

func readReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

func schema() connector.ProviderSchema {
	return connector.ProviderSchema{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.4.0",
		ConfigFields: []connector.ConfigField{
			{Key: "base_url", Name: "API origin", Type: connector.ConfigFieldText, Required: true, Validation: connector.ConfigValidation{MaxLength: 2048}},
			{Key: "team_id", Name: "Knowledge service team ID", Type: connector.ConfigFieldText, Required: true, Validation: connector.ConfigValidation{MaxLength: 256}},
			{Key: "kb_id", Name: "Knowledge base ID", Type: connector.ConfigFieldText, Required: true, Validation: connector.ConfigValidation{MaxLength: 256}},
			{Key: "permission_ids_by_user", Name: "Server-managed document access by user", Type: connector.ConfigFieldJSON},
			{Key: "document_permission_ids", Name: "Server-managed upload document ACL", Type: connector.ConfigFieldJSON},
			{Key: "analysis_document_ids", Name: "Server-managed structured documents available for analysis", Type: connector.ConfigFieldJSON},
		},
		SecretFields: []connector.SecretField{{Key: "api_key", Name: "Knowledge service API key", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestOptional}},
	}
}

func permanent(code string) error { return connector.PermanentError("knowledge_api."+code, nil) }
func retryable(code string) error { return connector.RetryableError("knowledge_api."+code, nil) }
func validText(value string, limit int) bool {
	return len(value) <= limit && strings.TrimSpace(value) != "" && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

func settings(connection connector.Connection) (string, string, string, error) {
	base, _ := connection.Config["base_url"].(string)
	base = strings.TrimSpace(base)
	if base == "" {
		return "", "", "", permanent("request_invalid")
	}
	if len(base) > 2048 {
		return "", "", "", permanent("request_invalid")
	}
	u, err := url.Parse(base)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(base, "#") || (u.Path != "" && u.Path != "/") {
		return "", "", "", permanent("request_invalid")
	}
	ip := net.ParseIP(u.Hostname())
	loopback := u.Hostname() == "localhost" || ip != nil && ip.IsLoopback()
	if u.Scheme != "https" && !(u.Scheme == "http" && loopback) {
		return "", "", "", permanent("request_invalid")
	}
	team, _ := connection.Config["team_id"].(string)
	kb, _ := connection.Config["kb_id"].(string)
	if !validText(team, 256) || !validText(kb, 256) {
		return "", "", "", permanent("request_invalid")
	}
	return strings.TrimRight(base, "/"), team, kb, nil
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	_, _, _, err := settings(connection)
	if err != nil {
		return err
	}
	_, err = permissions(connection, "")
	if err == nil {
		_, _, err = documentPermissionHeader(connection)
	}
	if err == nil {
		if _, present := connection.Config["analysis_document_ids"]; present {
			_, err = analysisDocumentIDs(connection)
		}
	}
	return err
}

// Permission IDs are connection policy, never operation input. The host owns
// this mapping and derives Principal from its authenticated request context.
func permissions(connection connector.Connection, userID string) ([]string, error) {
	value, ok := connection.Config["permission_ids_by_user"]
	if !ok {
		return nil, nil
	}
	raw, err := json.Marshal(value)
	if err != nil || len(raw) > 65536 {
		return nil, permanent("access_denied")
	}
	var mapping map[string][]string
	if json.Unmarshal(raw, &mapping) != nil || mapping == nil {
		return nil, permanent("access_denied")
	}
	for user, ids := range mapping {
		if !validText(user, 255) || len(ids) > 100 {
			return nil, permanent("access_denied")
		}
		for _, id := range ids {
			if !validText(id, 256) {
				return nil, permanent("access_denied")
			}
		}
	}
	return mapping[userID], nil
}

func (p *provider) search(ctx context.Context, r connector.TypedRequest[SearchInput]) (connector.TypedResult[Output], error) {
	if !validText(r.Input.Query, 65536) {
		return connector.TypedResult[Output]{}, permanent("request_invalid")
	}
	topK := r.Input.TopK
	if topK == 0 {
		topK = 5
	}
	if topK < 1 || topK > 20 {
		return connector.TypedResult[Output]{}, permanent("request_invalid")
	}
	body := map[string]any{"query": r.Input.Query, "top_k": topK}
	if r.Input.ResultContent == "metadata" {
		body["result_content"] = "metadata"
	}
	return p.request(ctx, r.Connection, r.Secrets, r.Principal, "/v1/kb/search", r.Input.ResultContent, body)
}

func (p *provider) fetch(ctx context.Context, r connector.TypedRequest[FetchInput]) (connector.TypedResult[Output], error) {
	if !validText(r.Input.DocID, 4096) {
		return connector.TypedResult[Output]{}, permanent("request_invalid")
	}
	body := map[string]any{"doc_id": r.Input.DocID}
	if r.Input.ResultContent == "metadata" {
		body["include_content"] = false
		body["live"] = map[string]any{"enabled": false}
	}
	return p.request(ctx, r.Connection, r.Secrets, r.Principal, "/v1/kb/fetch", r.Input.ResultContent, body)
}

func (p *provider) request(ctx context.Context, connection connector.Connection, secrets map[string]string, principal connector.Principal, path, resultContent string, body map[string]any) (connector.TypedResult[Output], error) {
	var result connector.TypedResult[Output]
	if !principal.IsAuthenticated || !validText(principal.UserID, 255) || connection.WorkspaceID == "" || principal.WorkspaceID != connection.WorkspaceID {
		return result, permanent("access_denied")
	}
	base, team, kb, err := settings(connection)
	if err != nil {
		return result, err
	}
	ids, err := permissions(connection, principal.UserID)
	if err != nil {
		return result, err
	}
	token := strings.TrimSpace(secrets["api_key"])
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return result, permanent("access_denied")
	}
	if resultContent != "" && resultContent != "metadata" {
		return result, permanent("request_invalid")
	}
	body["team_id"], body["kb_id"] = team, kb
	if len(ids) > 0 {
		body["permission_ids"] = ids
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return result, permanent("request_invalid")
	}
	return p.exchange(ctx, http.MethodPost, base+path, "application/json", raw, token, kb)
}

func (p *provider) exchange(ctx context.Context, method, target, contentType string, body []byte, token, kb string) (connector.TypedResult[Output], error) {
	return p.exchangeHeaders(ctx, method, target, contentType, body, token, kb, nil)
}

func (p *provider) exchangeHeaders(ctx context.Context, method, target, contentType string, body []byte, token, kb string, extra map[string][]string) (connector.TypedResult[Output], error) {
	var result connector.TypedResult[Output]
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	headers := map[string][]string{"Content-Type": {contentType}, "Accept": {"application/json"}}
	for name, values := range extra {
		headers[name] = append([]string(nil), values...)
	}
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{
		Method: method, URL: target, Body: body, MaxResponseBytes: responseLimit,
		Headers:       headers,
		SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}},
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return result, context.Canceled
		}
		var network net.Error
		if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &network) && network.Timeout() {
			return result, retryable("timeout")
		}
		return result, retryable("network")
	}
	result.ResponseRef = "http:" + strconv.Itoa(response.StatusCode)
	if response.StatusCode/100 != 2 {
		return result, statusError(response.StatusCode)
	}
	if len(response.Body) > responseLimit {
		return result, permanent("response_invalid")
	}
	raw := bytes.TrimSpace(response.Body)
	if len(raw) == 0 || !utf8.Valid(raw) || !json.Valid(raw) || (raw[0] != '{' && raw[0] != '[') {
		return result, permanent("response_invalid")
	}
	var envelope map[string]json.RawMessage
	if raw[0] == '{' && json.Unmarshal(raw, &envelope) == nil {
		if len(envelope["error"]) > 0 && string(envelope["error"]) != "null" {
			return result, permanent("failed")
		}
		// The live service can return HTTP 200 for a business failure. Its
		// err_code is an integer; 0 is success and observed 1004 is a missing
		// document. Preserve raw success data and never expose err_msg text.
		if code, present := envelope["err_code"]; present {
			status, parseErr := strconv.ParseInt(string(bytes.TrimSpace(code)), 10, 64)
			if parseErr != nil {
				return result, permanent("response_invalid")
			}
			if status == 1004 {
				return result, permanent("not_found")
			}
			if status == 409 {
				return result, permanent("source_changed")
			}
			if status != 0 {
				return result, permanent("failed")
			}
		}
	}
	result.Output = Output{Provider: ProviderKey, KBID: kb, Result: append(json.RawMessage(nil), raw...)}
	return result, nil
}

func statusError(status int) error {
	switch status {
	case 401, 403:
		return permanent("access_denied")
	case 402:
		return permanent("quota_exhausted")
	case 429:
		return retryable("rate_limited")
	case 408, 504:
		return retryable("timeout")
	case 400, 404, 422:
		return permanent("request_invalid")
	default:
		if status >= 500 {
			return retryable("unavailable")
		}
		return permanent("failed")
	}
}

var _ connector.ConfigValidator = (*provider)(nil)
