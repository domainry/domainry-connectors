package stripe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
)

const defaultBaseURL = "https://api.stripe.com"

type provider struct {
	descriptor connector.ProviderDescriptor
	transport  connector.Transport
}

func (a *provider) Descriptor() connector.ProviderDescriptor { return a.descriptor }
func (a *provider) Call(ctx context.Context, req connector.CallRequest) (connector.CallResult, error) {
	if err := a.validateRequest(req); err != nil {
		return connector.CallResult{}, err
	}
	if req.OperationKey == "test_connection" {
		return a.callTestConnection(ctx, req)
	}
	return a.callOperation(ctx, req)
}

func (a *provider) validateRequest(request connector.CallRequest) error {
	if request.ConnectorKey != ConnectorKey || request.ProviderKey != ProviderKey {
		return connector.PermanentError("stripe.request_contract_invalid", errors.New("Provider identity does not match Stripe"))
	}
	for _, operation := range a.descriptor.Operations {
		if operation.Key != request.OperationKey {
			continue
		}
		if operation.ContractSHA256 != request.ContractSHA256 || operation.Mode != request.Mode {
			return connector.PermanentError("stripe.request_contract_invalid", errors.New("operation contract identity does not match"))
		}
		return nil
	}
	return connector.PermanentError("stripe.operation_unsupported", errors.New("operation is not declared"))
}

func (*provider) ValidateConfig(connection connector.Connection) error {
	baseURL := configString(connection.Config, "base_url")
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopbackHost(parsed.Hostname()))) {
		return connector.PermanentError("stripe.base_url_invalid", errors.New("HTTPS or loopback HTTP base URL is required"))
	}
	return nil
}

func isLoopbackHost(host string) bool {
	host = strings.TrimSpace(strings.ToLower(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

func (a *provider) callTestConnection(ctx context.Context, req connector.CallRequest) (connector.CallResult, error) {
	payload, ref, err := a.execute(ctx, req, http.MethodGet, "/v1/balance", nil)
	if err != nil {
		return connector.CallResult{ResponseRef: ref}, err
	}
	encoded, err := json.Marshal(map[string]any{"connected": true, "livemode": payload["livemode"]})
	if err != nil {
		return connector.CallResult{}, err
	}
	return connector.CallResult{Payload: encoded, ResponseRef: ref}, nil
}

func (a *provider) callOperation(ctx context.Context, req connector.CallRequest) (connector.CallResult, error) {
	input := map[string]any{}
	if err := json.Unmarshal(req.Payload, &input); err != nil {
		return connector.CallResult{}, connector.PermanentError("stripe.request_invalid", err)
	}
	values := url.Values{}
	path := ""
	method := http.MethodPost
	switch req.OperationKey {
	case "create_payment_intent":
		amount, err := positiveAmount(input, "amount")
		if err != nil {
			return connector.CallResult{}, err
		}
		currency := strings.ToLower(configString(input, "currency"))
		if currency == "" {
			currency = strings.ToLower(configString(req.Connection.Config, "currency"))
		}
		if currency == "" {
			return connector.CallResult{}, connector.PermanentError("stripe.currency_required", errors.New("currency is required"))
		}
		values.Set("amount", fmt.Sprint(amount))
		values.Set("currency", currency)
		copyQuery(values, input, "customer", "description", "payment_method")
		path = "/v1/payment_intents"
	case "process_card_payment":
		amount, err := positiveAmount(input, "amount")
		if err != nil {
			return connector.CallResult{}, err
		}
		currency := strings.ToLower(configString(input, "currency"))
		if currency == "" {
			return connector.CallResult{}, connector.PermanentError("stripe.currency_required", errors.New("currency is required"))
		}
		paymentMethod, err := requiredID(input, "payment_method")
		if err != nil {
			return connector.CallResult{}, err
		}
		values.Set("amount", fmt.Sprint(amount))
		values.Set("currency", currency)
		values.Set("payment_method", paymentMethod)
		values.Set("confirm", "true")
		copyQuery(values, input, "receipt_email", "description")
		path = "/v1/payment_intents"
	case "retrieve_payment_intent":
		paymentIntent, err := requiredID(input, "payment_intent")
		if err != nil {
			return connector.CallResult{}, err
		}
		method, path = http.MethodGet, "/v1/payment_intents/"+url.PathEscape(paymentIntent)
	case "create_refund":
		paymentIntent := configString(input, "payment_intent")
		if paymentIntent == "" {
			return connector.CallResult{}, connector.PermanentError("stripe.payment_intent_required", errors.New("payment_intent is required"))
		}
		values.Set("payment_intent", paymentIntent)
		if input["amount"] != nil {
			amount, err := positiveAmount(input, "amount")
			if err != nil {
				return connector.CallResult{}, err
			}
			values.Set("amount", fmt.Sprint(amount))
		}
		path = "/v1/refunds"
	case "confirm_payment_intent", "capture_payment_intent", "cancel_payment_intent":
		paymentIntent, err := requiredID(input, "payment_intent")
		if err != nil {
			return connector.CallResult{}, err
		}
		action := strings.TrimSuffix(req.OperationKey, "_payment_intent")
		copyQuery(values, input, "payment_method", "receipt_email", "amount_to_capture", "cancellation_reason")
		path = "/v1/payment_intents/" + url.PathEscape(paymentIntent) + "/" + action
	case "create_customer":
		copyQuery(values, input, "email", "name", "phone", "description")
		path = "/v1/customers"
	case "update_customer":
		customer, err := requiredID(input, "customer")
		if err != nil {
			return connector.CallResult{}, err
		}
		copyQuery(values, input, "email", "name", "phone", "description")
		path = "/v1/customers/" + url.PathEscape(customer)
	case "attach_payment_method":
		paymentMethod, err := requiredID(input, "payment_method")
		if err != nil {
			return connector.CallResult{}, err
		}
		customer, err := requiredID(input, "customer")
		if err != nil {
			return connector.CallResult{}, err
		}
		values.Set("customer", customer)
		path = "/v1/payment_methods/" + url.PathEscape(paymentMethod) + "/attach"
	case "detach_payment_method":
		paymentMethod, err := requiredID(input, "payment_method")
		if err != nil {
			return connector.CallResult{}, err
		}
		path = "/v1/payment_methods/" + url.PathEscape(paymentMethod) + "/detach"
	case "create_checkout_session":
		mode, price := configString(input, "mode"), configString(input, "price")
		if mode == "" || price == "" {
			return connector.CallResult{}, connector.PermanentError("stripe.checkout_fields_required", errors.New("mode and price are required"))
		}
		values.Set("mode", mode)
		values.Set("line_items[0][price]", price)
		values.Set("line_items[0][quantity]", fmt.Sprint(configInt(input, 1, "quantity")))
		copyQuery(values, input, "success_url", "cancel_url", "customer", "customer_email", "client_reference_id")
		path = "/v1/checkout/sessions"
	case "expire_checkout_session":
		session, err := requiredID(input, "session")
		if err != nil {
			return connector.CallResult{}, err
		}
		path = "/v1/checkout/sessions/" + url.PathEscape(session) + "/expire"
	case "create_subscription":
		customer, err := requiredID(input, "customer")
		if err != nil {
			return connector.CallResult{}, err
		}
		price, err := requiredID(input, "price")
		if err != nil {
			return connector.CallResult{}, err
		}
		values.Set("customer", customer)
		values.Set("items[0][price]", price)
		copyQuery(values, input, "default_payment_method", "trial_end")
		path = "/v1/subscriptions"
	case "update_subscription":
		subscription, err := requiredID(input, "subscription")
		if err != nil {
			return connector.CallResult{}, err
		}
		copyQuery(values, input, "default_payment_method", "cancel_at_period_end", "trial_end", "proration_behavior")
		path = "/v1/subscriptions/" + url.PathEscape(subscription)
	case "cancel_subscription":
		subscription, err := requiredID(input, "subscription")
		if err != nil {
			return connector.CallResult{}, err
		}
		method, path = http.MethodDelete, "/v1/subscriptions/"+url.PathEscape(subscription)
	case "create_invoice":
		customer, err := requiredID(input, "customer")
		if err != nil {
			return connector.CallResult{}, err
		}
		values.Set("customer", customer)
		copyQuery(values, input, "subscription", "description", "collection_method", "days_until_due")
		path = "/v1/invoices"
	case "create_invoice_item":
		customer, err := requiredID(input, "customer")
		if err != nil {
			return connector.CallResult{}, err
		}
		amount, err := positiveAmount(input, "amount")
		if err != nil {
			return connector.CallResult{}, err
		}
		currency := strings.ToLower(configString(input, "currency"))
		if currency == "" {
			currency = strings.ToLower(configString(req.Connection.Config, "currency"))
		}
		values.Set("customer", customer)
		values.Set("amount", fmt.Sprint(amount))
		values.Set("currency", currency)
		copyQuery(values, input, "invoice", "description")
		path = "/v1/invoiceitems"
	case "finalize_invoice", "pay_invoice", "void_invoice":
		invoice, err := requiredID(input, "invoice")
		if err != nil {
			return connector.CallResult{}, err
		}
		action := strings.TrimSuffix(req.OperationKey, "_invoice")
		path = "/v1/invoices/" + url.PathEscape(invoice) + "/" + action
	case "list_disputes":
		method = http.MethodGet
		copyQuery(values, input, "payment_intent", "charge", "starting_after", "ending_before", "limit")
		path = "/v1/disputes"
	case "update_dispute":
		dispute, err := requiredID(input, "dispute")
		if err != nil {
			return connector.CallResult{}, err
		}
		copyQuery(values, input, "evidence[customer_name]", "evidence[customer_email_address]", "evidence[product_description]", "submit")
		path = "/v1/disputes/" + url.PathEscape(dispute)
	case "list_balance_transactions":
		method = http.MethodGet
		copyQuery(values, input, "type", "payout", "source", "starting_after", "ending_before", "limit")
		path = "/v1/balance_transactions"
	case "list_refunds":
		method = http.MethodGet
		copyQuery(values, input, "payment_intent", "charge", "starting_after", "ending_before", "limit")
		path = "/v1/refunds"
	case "create_payout":
		amount, err := positiveAmount(input, "amount")
		if err != nil {
			return connector.CallResult{}, err
		}
		currency := strings.ToLower(configString(input, "currency"))
		if currency == "" {
			currency = strings.ToLower(configString(req.Connection.Config, "currency"))
		}
		values.Set("amount", fmt.Sprint(amount))
		values.Set("currency", currency)
		copyQuery(values, input, "description", "destination", "method")
		path = "/v1/payouts"
	case "list_payouts":
		method = http.MethodGet
		copyQuery(values, input, "status", "destination", "starting_after", "ending_before", "limit")
		path = "/v1/payouts"
	case "retrieve_connect_account":
		account, err := requiredID(input, "account")
		if err != nil {
			return connector.CallResult{}, err
		}
		method, path = http.MethodGet, "/v1/accounts/"+url.PathEscape(account)
	case "create_connect_account_link":
		account, err := requiredID(input, "account")
		if err != nil {
			return connector.CallResult{}, err
		}
		refreshURL, err := requiredID(input, "refresh_url")
		if err != nil {
			return connector.CallResult{}, err
		}
		returnURL, err := requiredID(input, "return_url")
		if err != nil {
			return connector.CallResult{}, err
		}
		values.Set("account", account)
		values.Set("refresh_url", refreshURL)
		values.Set("return_url", returnURL)
		values.Set("type", "account_onboarding")
		path = "/v1/account_links"
	default:
		return connector.CallResult{}, connector.PermanentError("stripe.operation_unsupported", errors.New("operation is unsupported"))
	}
	payload, ref, err := a.execute(ctx, req, method, path, values)
	if req.OperationKey == "retrieve_payment_intent" {
		payload = projectStripeFields(payload, "id", "status", "amount", "currency")
	}
	encoded, encodeErr := json.Marshal(payload)
	if encodeErr != nil {
		return connector.CallResult{}, connector.PermanentError("stripe.response_invalid", encodeErr)
	}
	return connector.CallResult{Payload: encoded, ResponseRef: ref}, err
}

func projectStripeFields(payload map[string]any, fields ...string) map[string]any {
	projected := make(map[string]any, len(fields))
	for _, field := range fields {
		if value, ok := payload[field]; ok {
			projected[field] = value
		}
	}
	return projected
}

func requiredID(input map[string]any, key string) (string, error) {
	value := configString(input, key)
	if value == "" {
		return "", connector.PermanentError("stripe."+key+"_required", fmt.Errorf("%s is required", key))
	}
	return value, nil
}

var _ connector.Adapter = (*provider)(nil)
var _ connector.ConfigValidator = (*provider)(nil)
