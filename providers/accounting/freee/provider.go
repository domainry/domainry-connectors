// Package freee implements the official freee Accounting Provider.
package freee

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/internal/accounting/journal"
	internaloauth2 "github.com/domainry/domainry-connectors/internal/oauth2"
)

const (
	ConnectorKey          = "accounting"
	ProviderKey           = "freee"
	defaultBaseURL        = "https://api.freee.co.jp"
	defaultTokenURL       = "https://accounts.secure.freee.co.jp/public_api/token"
	responseLimit   int64 = 4 << 20
)

var CreateJournalEntry = connector.EnqueueOperation[CreateJournalEntryInput]{
	ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "create_journal_entry",
	ContractSHA256: "0f26937182a10d05c71591466024e86216688cdd1006d94edc8f0f32b93825da",
	Reliability:    connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNone}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}},
}

type CreateJournalEntryInput struct {
	BatchKey        string        `json:"batch_key"`
	EntryKey        string        `json:"entry_key"`
	TransactionDate string        `json:"transaction_date"`
	Currency        string        `json:"currency"`
	Memo            string        `json:"memo,omitempty"`
	Lines           []JournalLine `json:"lines"`
}

type JournalLine struct {
	Side         string `json:"side"`
	Amount       int64  `json:"amount"`
	AccountID    string `json:"account_id"`
	TaxCode      string `json:"tax_code"`
	DepartmentID string `json:"department_id,omitempty"`
	SubAccountID string `json:"sub_account_id,omitempty"`
	PartnerCode  string `json:"partner_code,omitempty"`
	Description  string `json:"description,omitempty"`
	InvoiceKind  string `json:"invoice_kind,omitempty"`
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("freee transport is required")
	}
	p := &provider{transport: transport}
	delivery, err := connector.BindEnqueueDelivery(CreateJournalEntry, p.createJournalEntry)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), delivery)
	if err != nil {
		return nil, err
	}
	p.Adapter = bound
	return p, nil
}

func schema() connector.ProviderSchema {
	minimum, maximum := float64(1), float64(300)
	secret := func(key, name string, kind connector.SecretCredentialKind, required bool, rotation connector.SecretRotationPolicy) connector.SecretField {
		return connector.SecretField{Key: key, Name: name, Required: required, CredentialKind: kind, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: rotation, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}
	}
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{
		{Key: "base_url", Name: "freee API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://api.freee.co.jp"`)},
		{Key: "token_url", Name: "freee OAuth token URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://accounts.secure.freee.co.jp/public_api/token"`)},
		{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}},
	}, SecretFields: []connector.SecretField{
		secret("access_token", "OAuth access token", connector.SecretCredentialBearerToken, true, connector.SecretRotationOAuthRefresh),
		secret("refresh_token", "OAuth refresh token", connector.SecretCredentialRefreshToken, false, connector.SecretRotationOAuthRefresh),
		secret("client_id", "OAuth client ID", connector.SecretCredentialIdentifier, false, connector.SecretRotationOAuthRefresh),
		secret("client_secret", "OAuth client secret", connector.SecretCredentialOAuthClientSecret, false, connector.SecretRotationOAuthRefresh),
		secret("company_id", "Selected freee company ID", connector.SecretCredentialIdentifier, true, connector.SecretRotationManual),
	}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	for _, endpoint := range []string{config(connection, "base_url", defaultBaseURL), config(connection, "token_url", defaultTokenURL)} {
		parsed, err := url.Parse(endpoint)
		if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
			return connector.PermanentError("freee.endpoint_invalid", errors.New("HTTPS or loopback HTTP endpoints are required"))
		}
	}
	return nil
}

func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	companyID, err := positiveID(request.Secrets["company_id"])
	if err != nil {
		return connector.TestConnectionResult{}, connector.PermanentError("freee.company_id_invalid", err)
	}
	payload, ref, updates, err := p.executeWithRefresh(ctx, request.Connection, request.Secrets, http.MethodGet, "/api/1/companies/"+strconv.FormatInt(companyID, 10), nil, false)
	if err != nil {
		return connector.TestConnectionResult{SecretUpdates: updates}, err
	}
	details, err := json.Marshal(map[string]any{"provider_payload": payload, "response_ref": ref})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	return connector.TestConnectionResult{Connected: true, Details: details, SecretUpdates: updates}, nil
}

func (p *provider) createJournalEntry(ctx context.Context, request connector.TypedRequest[CreateJournalEntryInput]) (connector.DeliveryResult, error) {
	entry, err := journal.Validate(toJournal(request.Input))
	if err != nil {
		return connector.DeliveryResult{}, err
	}
	companyID, err := positiveID(request.Secrets["company_id"])
	if err != nil {
		return connector.DeliveryResult{}, connector.PermanentError("freee.company_id_invalid", err)
	}
	details := make([]any, 0, len(entry.Lines))
	for _, line := range entry.Lines {
		accountID, accountErr := positiveID(line.AccountID)
		taxCode, taxErr := nonNegativeID(line.TaxCode)
		if accountErr != nil || taxErr != nil {
			return connector.DeliveryResult{}, connector.PermanentError("freee.line_identity_invalid", errors.New("line account or tax identity is invalid"))
		}
		detail := map[string]any{"entry_side": line.Side, "amount": line.Amount, "account_item_id": accountID, "tax_code": taxCode}
		if line.DepartmentID != "" {
			sectionID, sectionErr := positiveID(line.DepartmentID)
			if sectionErr != nil {
				return connector.DeliveryResult{}, connector.PermanentError("freee.line_identity_invalid", sectionErr)
			}
			detail["section_id"] = sectionID
		}
		optional(detail, "partner_code", line.PartnerCode)
		optional(detail, "description", line.Description)
		details = append(details, detail)
	}
	payload := map[string]any{"company_id": companyID, "issue_date": entry.TransactionDate, "details": details}
	response, ref, updates, err := p.executeWithRefresh(ctx, request.Connection, request.Secrets, http.MethodPost, "/api/1/manual_journals", payload, true)
	if err != nil {
		return connector.DeliveryResult{ResponseRef: ref, SecretUpdates: updates}, err
	}
	item, ok := response["manual_journal"].(map[string]any)
	id := mapString(item, "id")
	if !ok || id == "" {
		return connector.DeliveryResult{ResponseRef: ref, SecretUpdates: updates}, connector.UncertainError("freee.response_invalid", errors.New("journal creation response lacks an identity"))
	}
	return connector.DeliveryResult{ResponseRef: "freee:manual_journal:" + id, SecretUpdates: updates}, nil
}

func (p *provider) executeWithRefresh(ctx context.Context, connection connector.Connection, secrets map[string]string, method, path string, payload map[string]any, write bool) (map[string]any, string, map[string]string, error) {
	token := strings.TrimSpace(secrets["access_token"])
	if token == "" {
		return nil, "", nil, connector.PermanentError("freee.access_token_required", errors.New("resolved access token is required"))
	}
	result, ref, status, err := p.execute(ctx, connection, method, path, payload, token, write)
	if status != http.StatusUnauthorized {
		return result, ref, nil, err
	}
	refreshed, refreshErr := internaloauth2.Refresh(ctx, p.transport, internaloauth2.RefreshRequest{Endpoint: config(connection, "token_url", defaultTokenURL), RefreshToken: secrets["refresh_token"], ClientID: secrets["client_id"], ClientSecret: secrets["client_secret"], ClientAuthentication: internaloauth2.ClientAuthenticationForm, ErrorPrefix: "freee.oauth"})
	if refreshErr != nil {
		return nil, "oauth:refresh_failed", nil, refreshErr
	}
	updates := map[string]string{"access_token": refreshed.AccessToken}
	if refreshed.RefreshToken != "" {
		updates["refresh_token"] = refreshed.RefreshToken
	}
	result, ref, _, err = p.execute(ctx, connection, method, path, payload, refreshed.AccessToken, write)
	return result, ref, updates, err
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, method, path string, payload map[string]any, token string, write bool) (map[string]any, string, int, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", 0, err
	}
	var body []byte
	if payload != nil {
		var err error
		body, err = json.Marshal(payload)
		if err != nil {
			return nil, "", 0, connector.PermanentError("freee.request_invalid", err)
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	if payload != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: strings.TrimRight(config(connection, "base_url", defaultBaseURL), "/") + path, Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, Body: body, MaxResponseBytes: responseLimit})
	if err != nil {
		if write {
			return nil, "", 0, connector.UncertainError("freee.network_outcome_unknown", err)
		}
		return nil, "", 0, connector.RetryableError("freee.network_error", err)
	}
	ref := "http:" + strconv.Itoa(response.StatusCode)
	result := map[string]any{}
	_ = json.Unmarshal(response.Body, &result)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code := freeeErrorCode(result, response.StatusCode)
		cause := fmt.Errorf("provider returned HTTP %d", response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests {
			return result, ref, response.StatusCode, connector.RetryableError(code, cause)
		}
		if response.StatusCode >= 500 {
			if write {
				return result, ref, response.StatusCode, connector.UncertainError(code, cause)
			}
			return result, ref, response.StatusCode, connector.RetryableError(code, cause)
		}
		return result, ref, response.StatusCode, connector.PermanentError(code, cause)
	}
	if len(result) == 0 {
		return nil, ref, response.StatusCode, connector.PermanentError("freee.response_invalid", errors.New("provider response is invalid JSON"))
	}
	return result, responseRef(result, ref), response.StatusCode, nil
}

func toJournal(input CreateJournalEntryInput) journal.Entry {
	lines := make([]journal.Line, len(input.Lines))
	for index, line := range input.Lines {
		lines[index] = journal.Line{Side: line.Side, Amount: line.Amount, AccountID: line.AccountID, TaxCode: line.TaxCode, DepartmentID: line.DepartmentID, SubAccountID: line.SubAccountID, PartnerCode: line.PartnerCode, Description: line.Description, InvoiceKind: line.InvoiceKind}
	}
	return journal.Entry{BatchKey: input.BatchKey, EntryKey: input.EntryKey, TransactionDate: input.TransactionDate, Currency: input.Currency, Memo: input.Memo, Lines: lines}
}
func positiveID(value string) (int64, error) {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || parsed < 1 {
		return 0, errors.New("positive numeric identity is required")
	}
	return parsed, nil
}
func nonNegativeID(value string) (int64, error) {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || parsed < 0 {
		return 0, errors.New("non-negative numeric identity is required")
	}
	return parsed, nil
}
func optional(target map[string]any, key, value string) {
	if value = strings.TrimSpace(value); value != "" {
		target[key] = value
	}
}
func config(connection connector.Connection, key, fallback string) string {
	if value := mapString(connection.Config, key); value != "" {
		return value
	}
	return fallback
}
func mapString(values map[string]any, key string) string {
	value := values[key]
	if value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}
func responseRef(payload map[string]any, fallback string) string {
	for _, key := range []string{"manual_journal", "company"} {
		if item, ok := payload[key].(map[string]any); ok {
			if id := mapString(item, "id"); id != "" {
				return "freee:" + key + ":" + id
			}
		}
	}
	return fallback
}
func freeeErrorCode(payload map[string]any, status int) string {
	if values, ok := payload["errors"].([]any); ok && len(values) > 0 {
		if item, ok := values[0].(map[string]any); ok {
			if code := mapString(item, "code"); code != "" {
				return "freee." + code
			}
			if code := mapString(item, "type"); code != "" {
				return "freee." + code
			}
		}
	}
	return "freee.http_" + strconv.Itoa(status)
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
