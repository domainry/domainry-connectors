// Package shopifyfulfillment implements the official Shopify fulfillment logistics Provider.
package shopifyfulfillment

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
	ConnectorKey = "logistics"
	ProviderKey  = "shopify_fulfillment"
)

type ListOrdersInput struct {
	OrderID  string `json:"order_id"`
	PageSize int    `json:"page_size,omitempty"`
	Limit    int    `json:"limit,omitempty"`
	Cursor   string `json:"cursor,omitempty"`
}
type CreateFulfillmentInput struct {
	Input   map[string]any `json:"input"`
	Message string         `json:"message,omitempty"`
}
type UpdateTrackingInput struct {
	FulfillmentID  string         `json:"fulfillment_id"`
	Input          map[string]any `json:"input"`
	NotifyCustomer *bool          `json:"notify_customer,omitempty"`
}
type CancelFulfillmentInput struct {
	FulfillmentID string `json:"fulfillment_id"`
}
type Response map[string]any

var (
	ListFulfillmentOrders     = connector.CallOperation[ListOrdersInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_fulfillment_orders", ContractSHA256: "c796a62c2a0968d432d629c8e0dd2b4624e8422d2c47b20fca61f31e31261dbf", Reliability: readReliability()}
	CreateFulfillment         = connector.CallOperation[CreateFulfillmentInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "create_fulfillment", ContractSHA256: "7f0c7a1c961f6ddeb87cc0fb7509173c80ef3d9ce9881d09ac05a91fa63a4309", Reliability: writeReliability()}
	UpdateFulfillmentTracking = connector.CallOperation[UpdateTrackingInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "update_fulfillment_tracking", ContractSHA256: "7a0b2011f77672ad8e615bb50062e853179ad4650383fc625d44bbf59a1794c7", Reliability: writeReliability()}
	CancelFulfillment         = connector.CallOperation[CancelFulfillmentInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "cancel_fulfillment", ContractSHA256: "9415831e72591b5b6221b5aebc00decc85d45195eb3c6e6f071c0dba845e37d3", Reliability: writeReliability()}
	TestConnection            = connector.CallOperation[struct{}, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection", ContractSHA256: "a2879c297028e5fee189b49203b9a3f9c4c96182c7b68f928006edc6bd93408d", Reliability: readReliability()}
)

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
		return nil, errors.New("Shopify fulfillment transport is required")
	}
	p := &provider{transport: transport}
	list, err := connector.BindCall(ListFulfillmentOrders, p.list)
	if err != nil {
		return nil, err
	}
	create, err := connector.BindCall(CreateFulfillment, p.create)
	if err != nil {
		return nil, err
	}
	update, err := connector.BindCall(UpdateFulfillmentTracking, p.update)
	if err != nil {
		return nil, err
	}
	cancel, err := connector.BindCall(CancelFulfillment, p.cancel)
	if err != nil {
		return nil, err
	}
	test, err := connector.BindCall(TestConnection, p.callTestConnection)
	if err != nil {
		return nil, err
	}
	bound, err := connector.NewProvider(schema(), list, create, update, cancel, test)
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
		return connector.PermanentError("shopify_fulfillment.endpoint_invalid", err)
	}
	if seconds := intValue(connection.Config["timeout_seconds"], 30); seconds < 1 || seconds > 120 {
		return connector.PermanentError("shopify_fulfillment.timeout_invalid", errors.New("timeout_seconds must be between 1 and 120"))
	}
	return nil
}
func (p *provider) list(ctx context.Context, r connector.TypedRequest[ListOrdersInput]) (connector.TypedResult[Response], error) {
	if strings.TrimSpace(r.Input.OrderID) == "" {
		return connector.TypedResult[Response]{}, permanent("order_id_required", "order_id is required")
	}
	first := r.Input.PageSize
	if first <= 0 {
		first = r.Input.Limit
	}
	if first <= 0 {
		first = 50
	}
	return p.execute(ctx, r.Connection, r.Secrets, `query FulfillmentOrders($id:ID!,$first:Int!,$after:String){order(id:$id){id fulfillmentOrders(first:$first,after:$after){nodes{id status requestStatus fulfillAt assignedLocation{location{id name}} lineItems(first:100){nodes{id remainingQuantity totalQuantity}}} pageInfo{hasNextPage endCursor}}}}`, map[string]any{"id": shopifyprotocol.GID("Order", r.Input.OrderID), "first": first, "after": nullable(r.Input.Cursor)}, false)
}
func (p *provider) create(ctx context.Context, r connector.TypedRequest[CreateFulfillmentInput]) (connector.TypedResult[Response], error) {
	input := cloneMap(r.Input.Input)
	if input["lineItemsByFulfillmentOrder"] == nil {
		return connector.TypedResult[Response]{}, permanent("fulfillment_input_required", "lineItemsByFulfillmentOrder is required")
	}
	return p.execute(ctx, r.Connection, r.Secrets, `mutation CreateFulfillment($fulfillment:FulfillmentInput!,$message:String){fulfillmentCreate(fulfillment:$fulfillment,message:$message){fulfillment{id status trackingInfo{company number url}} userErrors{field message}}}`, map[string]any{"fulfillment": input, "message": nullable(r.Input.Message)}, true)
}
func (p *provider) update(ctx context.Context, r connector.TypedRequest[UpdateTrackingInput]) (connector.TypedResult[Response], error) {
	input := cloneMap(r.Input.Input)
	if strings.TrimSpace(r.Input.FulfillmentID) == "" || len(input) == 0 {
		return connector.TypedResult[Response]{}, permanent("tracking_fields_required", "fulfillment_id and tracking input are required")
	}
	return p.execute(ctx, r.Connection, r.Secrets, `mutation UpdateTracking($id:ID!,$trackingInfo:FulfillmentTrackingInput!,$notifyCustomer:Boolean){fulfillmentTrackingInfoUpdate(fulfillmentId:$id,trackingInfoInput:$trackingInfo,notifyCustomer:$notifyCustomer){fulfillment{id status trackingInfo{company number url}} userErrors{field message}}}`, map[string]any{"id": shopifyprotocol.GID("Fulfillment", r.Input.FulfillmentID), "trackingInfo": input, "notifyCustomer": r.Input.NotifyCustomer}, true)
}
func (p *provider) cancel(ctx context.Context, r connector.TypedRequest[CancelFulfillmentInput]) (connector.TypedResult[Response], error) {
	if strings.TrimSpace(r.Input.FulfillmentID) == "" {
		return connector.TypedResult[Response]{}, permanent("fulfillment_id_required", "fulfillment_id is required")
	}
	return p.execute(ctx, r.Connection, r.Secrets, `mutation CancelFulfillment($id:ID!){fulfillmentCancel(id:$id){fulfillment{id status} userErrors{field message}}}`, map[string]any{"id": shopifyprotocol.GID("Fulfillment", r.Input.FulfillmentID)}, true)
}
func (p *provider) execute(ctx context.Context, connection connector.Connection, secrets map[string]string, query string, variables map[string]any, write bool) (connector.TypedResult[Response], error) {
	payload, ref, err := shopifyprotocol.Execute(ctx, p.transport, protocolConfig(connection), secrets["access_token"], query, variables, write)
	if err != nil {
		return connector.TypedResult[Response]{Output: Response(payload), ResponseRef: ref}, err
	}
	if code := userError(payload); code != "" {
		return connector.TypedResult[Response]{Output: Response(payload), ResponseRef: ref}, connector.PermanentError("shopify_fulfillment.user_error", errors.New(code))
	}
	if id := fulfillmentID(payload); id != "" {
		ref = "shopify:" + shopifyprotocol.IDFromGID(id)
	}
	return connector.TypedResult[Response]{Output: Response(payload), ResponseRef: ref}, nil
}
func (p *provider) callTestConnection(ctx context.Context, r connector.TypedRequest[struct{}]) (connector.TypedResult[Response], error) {
	payload, ref, err := shopifyprotocol.Execute(ctx, p.transport, protocolConfig(r.Connection), r.Secrets["access_token"], `query Shop { shop { id name myshopifyDomain } }`, nil, false)
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
	externalID := event.DeliveryID
	if externalID == "" {
		externalID = numberString(event.Payload["id"])
		if externalID == "" {
			externalID = numberString(event.Payload["admin_graphql_api_id"])
		}
		if strings.HasPrefix(externalID, "gid://") {
			externalID = shopifyprotocol.IDFromGID(externalID)
		}
	}
	if externalID == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_identity_missing", "Shopify webhook identity is missing")
	}
	verified := connector.VerifiedWebhook{EventType: strings.ReplaceAll(event.Topic, "/", "."), ExternalID: externalID, Payload: event.Raw, Security: &connector.WebhookSecurityEvidence{SignatureVerified: true}}
	if event.ShopDomain != "" {
		verified.ExternalIdentity = &connector.WebhookExternalIdentity{Subject: event.ShopDomain, SubjectType: "shopify_shop"}
	}
	return verified, nil
}
func userError(payload map[string]any) string {
	data, _ := payload["data"].(map[string]any)
	for _, key := range []string{"fulfillmentCreate", "fulfillmentTrackingInfoUpdate", "fulfillmentCancel"} {
		mutation, _ := data[key].(map[string]any)
		items, _ := mutation["userErrors"].([]any)
		if len(items) > 0 {
			return fmt.Sprint(items[0])
		}
	}
	return ""
}
func fulfillmentID(payload map[string]any) string {
	data, _ := payload["data"].(map[string]any)
	for _, key := range []string{"fulfillmentCreate", "fulfillmentTrackingInfoUpdate", "fulfillmentCancel"} {
		mutation, _ := data[key].(map[string]any)
		fulfillment, _ := mutation["fulfillment"].(map[string]any)
		if id, _ := fulfillment["id"].(string); id != "" {
			return id
		}
	}
	return ""
}
func cloneMap(source map[string]any) map[string]any {
	target := map[string]any{}
	for key, value := range source {
		target[key] = value
	}
	return target
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
func numberString(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case float64:
		return fmt.Sprintf("%.0f", typed)
	}
	return ""
}
func intValue(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		return typed
	case float64:
		return int(typed)
	}
	return fallback
}
func permanent(code, message string) error {
	return connector.PermanentError("shopify_fulfillment."+code, errors.New(message))
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
var _ connector.WebhookVerifier = (*provider)(nil)
