// Package shopify implements the official Shopify ecommerce Provider.
package shopify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	connector "github.com/domainry/domainry-connector-sdk"
	shopifyprotocol "github.com/domainry/domainry-connectors/internal/shopify"
	"strings"
)

const (
	ConnectorKey = "ecommerce"
	ProviderKey  = "shopify"
)

type ListInput struct {
	PageSize int    `json:"page_size,omitempty"`
	Limit    int    `json:"limit,omitempty"`
	Cursor   string `json:"cursor,omitempty"`
	Query    string `json:"query,omitempty"`
}
type Response map[string]any

var (
	ListProducts   = connector.CallOperation[ListInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_products", ContractSHA256: "0d934939ee4152ac74aafce69825c9154a744b8856338ee02169ab0599c1c666", Reliability: readReliability()}
	ListOrders     = connector.CallOperation[ListInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_orders", ContractSHA256: "3a475ad243535c09802fe152f355871eafda77f644accfde7f8ac970f9e15be5", Reliability: readReliability()}
	ListCustomers  = connector.CallOperation[ListInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_customers", ContractSHA256: "ea5f5eb3f5d3bcb52c0005a2a9db6ed52ee6cbfc7c3b2b26ec92dde74bc1f1e2", Reliability: readReliability()}
	TestConnection = connector.CallOperation[struct{}, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection", ContractSHA256: "4149001f97eb3c94cc8f349e8b595521d0b50f769c8a6cc8cb0571925a07a219", Reliability: readReliability()}
)

func readReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Shopify transport is required")
	}
	p := &provider{transport: transport}
	products, err := connector.BindCall(ListProducts, p.listProducts)
	if err != nil {
		return nil, err
	}
	orders, err := connector.BindCall(ListOrders, p.listOrders)
	if err != nil {
		return nil, err
	}
	customers, err := connector.BindCall(ListCustomers, p.listCustomers)
	if err != nil {
		return nil, err
	}
	test, err := connector.BindCall(TestConnection, p.callTestConnection)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), products, orders, customers, test)
	if err != nil {
		return nil, err
	}
	p.Adapter = bound
	return p, nil
}
func schema() connector.ProviderSchema {
	min, max := float64(1), float64(120)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "shop_domain", Name: "Shopify shop domain", Type: connector.ConfigFieldText, Required: true}, {Key: "api_version", Name: "Shopify Admin API version", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"2026-04"`)}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`30`), Validation: connector.ConfigValidation{Min: &min, Max: &max}}}, SecretFields: []connector.SecretField{{Key: "access_token", Name: "Admin API access token", Required: true, CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}, {Key: "webhook_secret", Name: "Shopify app client secret", CredentialKind: connector.SecretCredentialSigningSecret, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}
func (p *provider) ValidateConfig(connection connector.Connection) error {
	if err := shopifyprotocol.Validate(protocolConfig(connection)); err != nil {
		return connector.PermanentError("shopify.endpoint_invalid", err)
	}
	if seconds := intValue(connection.Config["timeout_seconds"], 30); seconds < 1 || seconds > 120 {
		return connector.PermanentError("shopify.timeout_invalid", errors.New("timeout_seconds must be between 1 and 120"))
	}
	return nil
}
func (p *provider) listProducts(ctx context.Context, r connector.TypedRequest[ListInput]) (connector.TypedResult[Response], error) {
	return p.execute(ctx, r, `query Products($first:Int!,$after:String,$query:String){products(first:$first,after:$after,query:$query){nodes{id title handle status updatedAt variants(first:100){nodes{id sku price inventoryQuantity}}}pageInfo{hasNextPage endCursor}}}`)
}
func (p *provider) listOrders(ctx context.Context, r connector.TypedRequest[ListInput]) (connector.TypedResult[Response], error) {
	return p.execute(ctx, r, `query Orders($first:Int!,$after:String,$query:String){orders(first:$first,after:$after,query:$query){nodes{id name createdAt updatedAt displayFinancialStatus displayFulfillmentStatus totalPriceSet{shopMoney{amount currencyCode}} customer{id email}}pageInfo{hasNextPage endCursor}}}`)
}
func (p *provider) listCustomers(ctx context.Context, r connector.TypedRequest[ListInput]) (connector.TypedResult[Response], error) {
	return p.execute(ctx, r, `query Customers($first:Int!,$after:String,$query:String){customers(first:$first,after:$after,query:$query){nodes{id displayName email phone createdAt updatedAt numberOfOrders amountSpent{amount currencyCode}}pageInfo{hasNextPage endCursor}}}`)
}
func (p *provider) execute(ctx context.Context, r connector.TypedRequest[ListInput], query string) (connector.TypedResult[Response], error) {
	first := r.Input.PageSize
	if first <= 0 {
		first = r.Input.Limit
	}
	if first <= 0 {
		first = 50
	}
	variables := map[string]any{"first": first, "after": nullable(r.Input.Cursor), "query": nullable(r.Input.Query)}
	payload, ref, err := shopifyprotocol.Execute(ctx, p.transport, protocolConfig(r.Connection), r.Secrets["access_token"], query, variables, false)
	return connector.TypedResult[Response]{Output: Response(payload), ResponseRef: ref}, err
}
func (p *provider) callTestConnection(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	payload, ref, err := shopifyprotocol.Execute(ctx, p.transport, protocolConfig(r.Connection), r.Secrets["access_token"], `query Shop { shop { id name myshopifyDomain currencyCode } }`, nil, false)
	if err == nil {
		data, _ := payload["data"].(map[string]any)
		shop, _ := data["shop"].(map[string]any)
		if id, _ := shop["id"].(string); id != "" {
			ref = "shopify:shop:" + shopifyprotocol.IDFromGID(id)
		}
	}
	return connector.TypedResult[Response]{Output: Response(payload), ResponseRef: ref}, err
}
func (p *provider) TestConnection(ctx context.Context, r connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.callTestConnection(ctx, connector.TypedRequest[struct{}]{Connection: r.Connection, Secrets: r.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}
func (p *provider) VerifyWebhook(ctx context.Context, r connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	event, err := shopifyprotocol.VerifyWebhook(ctx, r)
	if err != nil {
		return connector.VerifiedWebhook{}, err
	}
	if event.DeliveryID == "" {
		return connector.VerifiedWebhook{}, connector.PermanentError("shopify.webhook_identity_missing", errors.New("Shopify webhook ID is missing"))
	}
	verified := connector.VerifiedWebhook{EventType: strings.ReplaceAll(event.Topic, "/", "."), ExternalID: event.DeliveryID, Payload: event.Raw, Security: &connector.WebhookSecurityEvidence{SignatureVerified: true}}
	if event.ShopDomain != "" {
		verified.ExternalIdentity = &connector.WebhookExternalIdentity{Subject: event.ShopDomain, SubjectType: "shopify_shop"}
	}
	return verified, nil
}
func protocolConfig(connection connector.Connection) shopifyprotocol.Config {
	return shopifyprotocol.Config{ShopDomain: config(connection, "shop_domain"), APIVersion: config(connection, "api_version")}
}
func config(connection connector.Connection, key string) string {
	value, _ := connection.Config[key].(string)
	return strings.TrimSpace(value)
}
func nullable(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}
func intValue(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		return typed
	case float64:
		return int(typed)
	case json.Number:
		var result int
		if _, err := fmt.Sscan(typed.String(), &result); err == nil {
			return result
		}
	}
	return fallback
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
var _ connector.WebhookVerifier = (*provider)(nil)
