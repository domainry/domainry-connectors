// Package quickbooks implements the official QuickBooks Online Accounting Provider.
package quickbooks

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
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
	ProviderKey           = "quickbooks"
	defaultBaseURL        = "https://quickbooks.api.intuit.com"
	defaultTokenURL       = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
	responseLimit   int64 = 4 << 20
)

type QueryInput struct {
	ChangedSince string `json:"changed_since,omitempty"`
	Limit        int    `json:"limit,omitempty"`
}
type CDCInput struct {
	Entities     string `json:"entities"`
	ChangedSince string `json:"changed_since"`
}
type JSONObject map[string]any
type JournalInput struct {
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

var (
	ListCustomers      = call[QueryInput]("list_customers", "a8550753aaebce829648374815ca4d02c545ce18b7220e4b56af770357a01c0a", readReliability())
	ListInvoices       = call[QueryInput]("list_invoices", "0912055bdf0133dad49b17d626ecd82271a8c3948d533124ad8b0fcbc8a4cff0", readReliability())
	ListPayments       = call[QueryInput]("list_payments", "a052a8cf7cc3716527843b5af451a1ddc90fb4297bed4ab8dd99a145b6113c3a", readReliability())
	ListAccounts       = call[QueryInput]("list_accounts", "795ed0b64be558e7da9f2705dd208b35b8d1b4a1cec2398e840816ff2ad18150", readReliability())
	ListTaxCodes       = call[QueryInput]("list_tax_codes", "c05c58a112ea80c051e02e558f709cefa269ac479621a3c9aa61dfe5b6919949", readReliability())
	CreateCustomer     = call[JSONObject]("create_customer", "fcd6995a11b35e2e2056085ac5871be40a8ca51bb15ddc167e530f8d2be40f14", writeReliability())
	UpdateCustomer     = call[JSONObject]("update_customer", "a4ed8b11ec4107e6f2e736005724b7fd25dbff282545c98c3f955990c03d075f", writeReliability())
	CreateInvoice      = call[JSONObject]("create_invoice", "cd7ef89b653202b85b0ef26a2d92f1ab7a99785d8b0602c67c330351084e06c6", writeReliability())
	UpdateInvoice      = call[JSONObject]("update_invoice", "379f1662d9ef860602d0bccb9be3fa95f5a30687128dc6db3e5a43f1428cce0a", writeReliability())
	CreatePayment      = call[JSONObject]("create_payment", "6636da69abfc7ec2dd5628672d3a93c2e823d1ca9e18e6b9bbde96d50f614ba0", writeReliability())
	CreateJournalEntry = connector.EnqueueOperation[JournalInput]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "create_journal_entry", ContractSHA256: "85fe2779166332efc7b3ffc11ce6ba01c167929370cdd1bd096d81fd3701a0b4", Reliability: writeReliability()}
	CDCSync            = call[CDCInput]("cdc_sync", "b25eda2f8884aa97351fd25f5cded1e9ccf5fa475a98250d542ad2912f47414e", readReliability())
	TestConnection     = call[struct{}]("test_connection", "c8225d47c872988fd3b3056de9599379ece193e2525a0603fd33b29299e84d96", readReliability())
)

func call[I any](key, hash string, reliability connector.ReliabilityContract) connector.CallOperation[I, JSONObject] {
	return connector.CallOperation[I, JSONObject]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: key, ContractSHA256: hash, Reliability: reliability}
}
func readReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}
func writeReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNone}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("QuickBooks transport is required")
	}
	p := &provider{transport: transport}
	operations := make([]connector.BoundOperation, 0, 13)
	bind := func(operation connector.BoundOperation, err error) error {
		if err != nil {
			return err
		}
		operations = append(operations, operation)
		return nil
	}
	if err := bind(connector.BindCall(ListCustomers, p.query("Customer"))); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(ListInvoices, p.query("Invoice"))); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(ListPayments, p.query("Payment"))); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(ListAccounts, p.query("Account"))); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(ListTaxCodes, p.query("TaxCode"))); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(CreateCustomer, p.mutate("customer", false, "DisplayName"))); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(UpdateCustomer, p.mutate("customer", true))); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(CreateInvoice, p.mutate("invoice", false, "CustomerRef", "Line"))); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(UpdateInvoice, p.mutate("invoice", true))); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(CreatePayment, p.mutate("payment", false, "CustomerRef", "TotalAmt"))); err != nil {
		return nil, err
	}
	if err := bind(connector.BindEnqueueDelivery(CreateJournalEntry, p.createJournalEntry)); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(CDCSync, p.cdcSync)); err != nil {
		return nil, err
	}
	if err := bind(connector.BindCall(TestConnection, p.callTestConnection)); err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), operations...)
	if err != nil {
		return nil, err
	}
	p.Adapter = bound
	return p, nil
}

func schema() connector.ProviderSchema {
	minimum, maximum := float64(1), float64(300)
	secret := func(key, name string, kind connector.SecretCredentialKind, required bool) connector.SecretField {
		return connector.SecretField{Key: key, Name: name, Required: required, CredentialKind: kind, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationOAuthRefresh, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}
	}
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{
		{Key: "company_id", Name: "Realm company ID", Type: connector.ConfigFieldText, Required: true},
		{Key: "base_url", Name: "QuickBooks API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://quickbooks.api.intuit.com"`)},
		{Key: "token_url", Name: "Intuit OAuth token URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"`)},
		{Key: "minor_version", Name: "API minor version", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`75`)},
		{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}},
	}, SecretFields: []connector.SecretField{
		secret("access_token", "OAuth access token", connector.SecretCredentialBearerToken, true), secret("refresh_token", "OAuth refresh token", connector.SecretCredentialRefreshToken, false), secret("client_id", "OAuth client ID", connector.SecretCredentialIdentifier, false), secret("client_secret", "OAuth client secret", connector.SecretCredentialOAuthClientSecret, false), secret("webhook_verifier_token", "Webhook verifier token", connector.SecretCredentialSigningSecret, false),
	}}
}

func (p *provider) ValidateConfig(connection connector.Connection) error {
	if config(connection, "company_id", "") == "" {
		return permanent("company_id_required", "realm company ID is required")
	}
	if !officialEndpoint(config(connection, "base_url", defaultBaseURL), "quickbooks.api.intuit.com", "sandbox-quickbooks.api.intuit.com") {
		return permanent("endpoint_invalid", "official QuickBooks API endpoint or loopback HTTP is required")
	}
	if !officialEndpoint(config(connection, "token_url", defaultTokenURL), "oauth.platform.intuit.com") {
		return permanent("token_endpoint_invalid", "official Intuit OAuth endpoint or loopback HTTP is required")
	}
	return nil
}

func (p *provider) query(entity string) connector.CallHandler[QueryInput, JSONObject] {
	return func(ctx context.Context, request connector.TypedRequest[QueryInput]) (connector.TypedResult[JSONObject], error) {
		limit := request.Input.Limit
		if limit == 0 {
			limit = 100
		}
		if limit < 1 || limit > 1000 {
			return connector.TypedResult[JSONObject]{}, permanent("limit_invalid", "limit must be between 1 and 1000")
		}
		statement := "select * from " + entity
		if changed := strings.TrimSpace(request.Input.ChangedSince); changed != "" {
			statement += " where MetaData.LastUpdatedTime >= '" + strings.ReplaceAll(changed, "'", "''") + "'"
		}
		statement += fmt.Sprintf(" maxresults %d", limit)
		return p.call(ctx, request.Connection, request.Secrets, request.RequestRef, http.MethodGet, companyPath(request.Connection, "/query"), url.Values{"query": {statement}}, nil, false)
	}
}

func (p *provider) mutate(entity string, update bool, required ...string) connector.CallHandler[JSONObject, JSONObject] {
	return func(ctx context.Context, request connector.TypedRequest[JSONObject]) (connector.TypedResult[JSONObject], error) {
		for _, key := range required {
			if request.Input[key] == nil {
				return connector.TypedResult[JSONObject]{}, permanent("input_invalid", key+" is required")
			}
		}
		if update && (mapString(request.Input, "Id") == "" || mapString(request.Input, "SyncToken") == "") {
			return connector.TypedResult[JSONObject]{}, permanent("update_identity_required", "Id and SyncToken are required")
		}
		return p.call(ctx, request.Connection, request.Secrets, request.RequestRef, http.MethodPost, companyPath(request.Connection, "/"+entity), nil, request.Input, true)
	}
}

func (p *provider) cdcSync(ctx context.Context, request connector.TypedRequest[CDCInput]) (connector.TypedResult[JSONObject], error) {
	entities, changed := strings.TrimSpace(request.Input.Entities), strings.TrimSpace(request.Input.ChangedSince)
	if entities == "" || changed == "" {
		return connector.TypedResult[JSONObject]{}, permanent("cdc_fields_required", "entities and changed_since are required")
	}
	return p.call(ctx, request.Connection, request.Secrets, request.RequestRef, http.MethodGet, companyPath(request.Connection, "/cdc"), url.Values{"entities": {entities}, "changedSince": {changed}}, nil, false)
}

func (p *provider) createJournalEntry(ctx context.Context, request connector.TypedRequest[JournalInput]) (connector.DeliveryResult, error) {
	entry, err := journal.Validate(toJournal(request.Input))
	if err != nil {
		return connector.DeliveryResult{}, err
	}
	lines := make([]any, 0, len(entry.Lines))
	for _, line := range entry.Lines {
		posting := "Credit"
		if line.Side == "debit" {
			posting = "Debit"
		}
		detail := map[string]any{"PostingType": posting, "AccountRef": map[string]any{"value": line.AccountID}}
		if line.DepartmentID != "" {
			detail["DepartmentRef"] = map[string]any{"value": line.DepartmentID}
		}
		item := map[string]any{"Amount": line.Amount, "DetailType": "JournalEntryLineDetail", "JournalEntryLineDetail": detail}
		if line.Description != "" {
			item["Description"] = line.Description
		}
		lines = append(lines, item)
	}
	payload := JSONObject{"TxnDate": entry.TransactionDate, "Line": lines}
	if entry.Memo != "" {
		payload["PrivateNote"] = entry.Memo
	}
	result, err := p.call(ctx, request.Connection, request.Secrets, request.RequestRef, http.MethodPost, companyPath(request.Connection, "/journalentry"), nil, payload, true)
	delivery := connector.DeliveryResult{ResponseRef: result.ResponseRef, SecretUpdates: result.SecretUpdates}
	if err != nil {
		return delivery, err
	}
	item, _ := result.Output["JournalEntry"].(map[string]any)
	id := mapString(item, "Id")
	if id == "" {
		return delivery, connector.UncertainError("quickbooks.response_invalid", errors.New("journal response lacks an identity"))
	}
	delivery.ResponseRef = "quickbooks:journal_entry:" + id
	return delivery, nil
}

func (p *provider) callTestConnection(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[JSONObject], error) {
	return p.call(ctx, request.Connection, request.Secrets, request.RequestRef, http.MethodGet, companyPath(request.Connection, "/companyinfo/"+url.PathEscape(companyID(request.Connection))), nil, nil, false)
}
func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.call(ctx, request.Connection, request.Secrets, "", http.MethodGet, companyPath(request.Connection, "/companyinfo/"+url.PathEscape(companyID(request.Connection))), nil, nil, false)
	if err != nil {
		return connector.TestConnectionResult{SecretUpdates: result.SecretUpdates}, err
	}
	details, marshalErr := json.Marshal(result.Output)
	if marshalErr != nil {
		return connector.TestConnectionResult{}, marshalErr
	}
	return connector.TestConnectionResult{Connected: true, Details: details, SecretUpdates: result.SecretUpdates}, nil
}

func (p *provider) call(ctx context.Context, connection connector.Connection, secrets map[string]string, requestRef, method, path string, query url.Values, payload JSONObject, write bool) (connector.TypedResult[JSONObject], error) {
	result, ref, updates, err := p.executeWithRefresh(ctx, connection, secrets, requestRef, method, path, query, payload, write)
	return connector.TypedResult[JSONObject]{Output: result, ResponseRef: ref, SecretUpdates: updates}, err
}

func (p *provider) executeWithRefresh(ctx context.Context, connection connector.Connection, secrets map[string]string, requestRef, method, path string, query url.Values, payload JSONObject, write bool) (JSONObject, string, map[string]string, error) {
	token := strings.TrimSpace(secrets["access_token"])
	if token == "" {
		return nil, "", nil, permanent("access_token_required", "resolved access token is required")
	}
	result, ref, status, err := p.execute(ctx, connection, requestRef, method, path, query, payload, token, write)
	if status != http.StatusUnauthorized {
		return result, ref, nil, err
	}
	refreshed, refreshErr := internaloauth2.Refresh(ctx, p.transport, internaloauth2.RefreshRequest{Endpoint: config(connection, "token_url", defaultTokenURL), RefreshToken: secrets["refresh_token"], ClientID: secrets["client_id"], ClientSecret: secrets["client_secret"], ClientAuthentication: internaloauth2.ClientAuthenticationBasic, ErrorPrefix: "quickbooks.oauth"})
	if refreshErr != nil {
		return nil, "oauth:refresh_failed", nil, refreshErr
	}
	updates := map[string]string{"access_token": refreshed.AccessToken}
	if refreshed.RefreshToken != "" {
		updates["refresh_token"] = refreshed.RefreshToken
	}
	result, ref, _, err = p.execute(ctx, connection, requestRef, method, path, query, payload, refreshed.AccessToken, write)
	return result, ref, updates, err
}

func (p *provider) execute(ctx context.Context, connection connector.Connection, requestRef, method, path string, query url.Values, payload JSONObject, token string, write bool) (JSONObject, string, int, error) {
	if err := p.ValidateConfig(connection); err != nil {
		return nil, "", 0, err
	}
	query = cloneValues(query)
	query.Set("minorversion", strconv.Itoa(configInt(connection, "minor_version", 75)))
	if method == http.MethodPost && strings.TrimSpace(requestRef) != "" {
		query.Set("requestid", strings.TrimSpace(requestRef))
	}
	endpoint := strings.TrimRight(config(connection, "base_url", defaultBaseURL), "/") + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	var body []byte
	var err error
	if payload != nil {
		body, err = json.Marshal(payload)
		if err != nil {
			return nil, "", 0, connector.PermanentError("quickbooks.request_invalid", err)
		}
	}
	headers := map[string][]string{"Accept": {"application/json"}}
	if payload != nil {
		headers["Content-Type"] = []string{"application/json"}
	}
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: method, URL: endpoint, Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"Bearer " + token}}, Body: body, MaxResponseBytes: responseLimit})
	if err != nil {
		if write {
			return nil, "", 0, connector.UncertainError("quickbooks.network_outcome_unknown", err)
		}
		return nil, "", 0, connector.RetryableError("quickbooks.network_error", err)
	}
	ref, result := "http:"+strconv.Itoa(response.StatusCode), JSONObject{}
	_ = json.Unmarshal(response.Body, &result)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code, cause := errorCode(result, response.StatusCode), fmt.Errorf("provider returned HTTP %d", response.StatusCode)
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
		if write {
			return nil, ref, response.StatusCode, connector.UncertainError("quickbooks.response_invalid", errors.New("provider response is invalid JSON"))
		}
		return nil, ref, response.StatusCode, permanent("response_invalid", "provider response is invalid JSON")
	}
	return result, responseRef(result, ref), response.StatusCode, nil
}

func (p *provider) VerifyWebhook(ctx context.Context, request connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	if err := ctx.Err(); err != nil {
		return connector.VerifiedWebhook{}, err
	}
	secret := strings.TrimSpace(request.Secrets["webhook_verifier_token"])
	if secret == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_secret_required", "resolved webhook verifier token is required")
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(request.Body)
	expected := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(header(request.Headers, "intuit-signature"))) {
		return connector.VerifiedWebhook{}, permanent("webhook_signature_invalid", "webhook signature is invalid")
	}
	payload := JSONObject{}
	if json.Unmarshal(request.Body, &payload) != nil {
		return connector.VerifiedWebhook{}, permanent("webhook_payload_invalid", "webhook payload is invalid JSON")
	}
	notifications, _ := payload["eventNotifications"].([]any)
	for _, raw := range notifications {
		notification, _ := raw.(map[string]any)
		realm := mapString(notification, "realmId")
		if expectedRealm := companyID(request.Connection); expectedRealm != "" && realm != expectedRealm {
			continue
		}
		data, _ := notification["dataChangeEvent"].(map[string]any)
		entities, _ := data["entities"].([]any)
		for _, rawEntity := range entities {
			entity, _ := rawEntity.(map[string]any)
			name, id, operation := mapString(entity, "name"), mapString(entity, "id"), strings.ToLower(mapString(entity, "operation"))
			if name == "" || id == "" || operation == "" {
				continue
			}
			externalID := strings.Join([]string{realm, name, id, mapString(entity, "lastUpdated")}, ":")
			return connector.VerifiedWebhook{EventType: "data_change." + strings.ToLower(name) + "." + operation, ExternalID: externalID, Payload: append(json.RawMessage(nil), request.Body...), Security: &connector.WebhookSecurityEvidence{SignatureVerified: true}, ExternalIdentity: &connector.WebhookExternalIdentity{Subject: id, SubjectType: strings.ToLower(name), Group: realm}}, nil
		}
	}
	return connector.VerifiedWebhook{}, permanent("webhook_identity_missing", "webhook event identity is missing")
}

func toJournal(input JournalInput) journal.Entry {
	lines := make([]journal.Line, len(input.Lines))
	for i, line := range input.Lines {
		lines[i] = journal.Line{Side: line.Side, Amount: line.Amount, AccountID: line.AccountID, TaxCode: line.TaxCode, DepartmentID: line.DepartmentID, SubAccountID: line.SubAccountID, PartnerCode: line.PartnerCode, Description: line.Description, InvoiceKind: line.InvoiceKind}
	}
	return journal.Entry{BatchKey: input.BatchKey, EntryKey: input.EntryKey, TransactionDate: input.TransactionDate, Currency: input.Currency, Memo: input.Memo, Lines: lines}
}
func companyID(connection connector.Connection) string { return config(connection, "company_id", "") }
func companyPath(connection connector.Connection, suffix string) string {
	return "/v3/company/" + url.PathEscape(companyID(connection)) + suffix
}
func config(connection connector.Connection, key, fallback string) string {
	if value := mapString(connection.Config, key); value != "" {
		return value
	}
	return fallback
}
func configInt(connection connector.Connection, key string, fallback int) int {
	value, err := strconv.Atoi(config(connection, key, ""))
	if err != nil {
		return fallback
	}
	return value
}
func mapString(values map[string]any, key string) string {
	if values[key] == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(values[key]))
}
func cloneValues(source url.Values) url.Values {
	result := url.Values{}
	for key, values := range source {
		result[key] = append([]string(nil), values...)
	}
	return result
}
func responseRef(payload JSONObject, fallback string) string {
	for _, key := range []string{"Customer", "Invoice", "Payment", "JournalEntry", "CompanyInfo"} {
		item, _ := payload[key].(map[string]any)
		if id := mapString(item, "Id"); id != "" {
			return "quickbooks:" + strings.ToLower(key) + ":" + id
		}
	}
	return fallback
}
func errorCode(payload JSONObject, status int) string {
	fault, _ := payload["Fault"].(map[string]any)
	list, _ := fault["Error"].([]any)
	if len(list) > 0 {
		item, _ := list[0].(map[string]any)
		if code := mapString(item, "code"); code != "" {
			return "quickbooks.provider_" + code
		}
	}
	return "quickbooks.http_" + strconv.Itoa(status)
}
func header(headers map[string][]string, key string) string {
	for current, values := range headers {
		if strings.EqualFold(current, key) && len(values) > 0 {
			return strings.TrimSpace(values[0])
		}
	}
	return ""
}
func officialEndpoint(raw string, hosts ...string) bool {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return false
	}
	if parsed.Scheme == "http" && isLoopback(parsed.Hostname()) {
		return true
	}
	if parsed.Scheme != "https" {
		return false
	}
	for _, host := range hosts {
		if strings.EqualFold(parsed.Hostname(), host) {
			return true
		}
	}
	return false
}
func isLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
func permanent(code, message string) error {
	return connector.PermanentError("quickbooks."+code, errors.New(message))
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
var _ connector.WebhookVerifier = (*provider)(nil)
