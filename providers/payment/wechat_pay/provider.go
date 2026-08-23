// Package wechatpay implements the official WeChat Pay Provider.
package wechatpay

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"net/url"
	"time"
)

const (
	ConnectorKey         = "payment"
	ProviderKey          = "wechat_pay"
	defaultBaseURL       = "https://api.mch.weixin.qq.com"
	defaultTimeout       = 15
	maximumTimeout       = 120
	responseLimit  int64 = 4 << 20
)

type Response map[string]any
type CreateNativePaymentOrderInput struct {
	MerchantOrderNo string `json:"merchant_order_no"`
	AmountMinor     int64  `json:"amount_minor"`
	Subject         string `json:"subject"`
	ExpiresAt       string `json:"expires_at,omitempty"`
	Currency        string `json:"currency,omitempty"`
}
type PaymentOrderInput struct {
	MerchantOrderNo string `json:"merchant_order_no"`
}
type CreatePaymentRefundInput struct {
	MerchantOrderNo   string `json:"merchant_order_no"`
	MerchantRefundNo  string `json:"merchant_refund_no"`
	RefundAmountMinor int64  `json:"refund_amount_minor"`
	TotalAmountMinor  int64  `json:"total_amount_minor"`
	Reason            string `json:"reason,omitempty"`
	Currency          string `json:"currency,omitempty"`
}
type QueryPaymentRefundInput struct {
	MerchantRefundNo string `json:"merchant_refund_no"`
}

var (
	ClosePaymentOrder        = connector.EnqueueOperation[PaymentOrderInput]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "close_payment_order", ContractSHA256: "f14e0fcb52024a8c9e892092cedc6507e82dd96e88042665a070f5060f2625de", Reliability: writeReliability()}
	CreateNativePaymentOrder = connector.EnqueueOperation[CreateNativePaymentOrderInput]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "create_native_payment_order", ContractSHA256: "daec256080e3012b8ce9fa55860490afc15816beb97804a4b178142bceb985b1", Reliability: writeReliability()}
	CreatePaymentRefund      = connector.EnqueueOperation[CreatePaymentRefundInput]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "create_payment_refund", ContractSHA256: "c996f9ba7ff9dd650043d08dba82d8b3f771ce1d24553837623ac061801a2ece", Reliability: writeReliability()}
	QueryPaymentOrder        = readOp[PaymentOrderInput]("query_payment_order", "014e6b5d4a4abd51ddd078d287e30f26fdc99a1fb28830cfa1b3a3391b17586d")
	QueryPaymentRefund       = readOp[QueryPaymentRefundInput]("query_payment_refund", "fd7709bce3675ef1a386beede661281e1a6063447dba66b5507c8f84336d4a82")
	TestConnection           = readOp[struct{}]("test_connection", "73084e236e350007d31eaf5ce0a27fbd7d2f5813ad8b779ff01d0f3af43c2d3c")
)

func readOp[I any](key, hash string) connector.CallOperation[I, Response] {
	return connector.CallOperation[I, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: key, ContractSHA256: hash, Reliability: readReliability()}
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
	now       func() time.Time
	nonce     func([]byte) (int, error)
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("WeChat Pay transport is required")
	}
	p := &provider{transport: transport, now: time.Now, nonce: rand.Read}
	bindings := []func() (connector.BoundOperation, error){func() (connector.BoundOperation, error) {
		return connector.BindEnqueueDelivery(ClosePaymentOrder, p.closePayment)
	}, func() (connector.BoundOperation, error) {
		return connector.BindEnqueueDelivery(CreateNativePaymentOrder, p.createPayment)
	}, func() (connector.BoundOperation, error) {
		return connector.BindEnqueueDelivery(CreatePaymentRefund, p.createRefund)
	}, func() (connector.BoundOperation, error) { return connector.BindCall(QueryPaymentOrder, p.queryPayment) }, func() (connector.BoundOperation, error) { return connector.BindCall(QueryPaymentRefund, p.queryRefund) }, func() (connector.BoundOperation, error) { return connector.BindCall(TestConnection, p.test) }}
	ops := make([]connector.BoundOperation, 0, len(bindings))
	for _, bind := range bindings {
		op, err := bind()
		if err != nil {
			return nil, err
		}
		ops = append(ops, op)
	}
	adapter, err := connector.NewProvider(schema(), ops...)
	if err != nil {
		return nil, err
	}
	p.Adapter = adapter
	return p, nil
}
func schema() connector.ProviderSchema {
	min, max := float64(1), float64(maximumTimeout)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{{Key: "base_url", Name: "WeChat Pay API base URL", Type: connector.ConfigFieldText, Required: true, Default: json.RawMessage(`"https://api.mch.weixin.qq.com"`)}, {Key: "app_id", Name: "App ID", Type: connector.ConfigFieldText, Required: true}, {Key: "merchant_id", Name: "Merchant ID", Type: connector.ConfigFieldText, Required: true}, {Key: "merchant_serial_no", Name: "Merchant certificate serial number", Type: connector.ConfigFieldText, Required: true}, {Key: "notify_url", Name: "Payment notification URL", Type: connector.ConfigFieldText, Required: true}, {Key: "refund_notify_url", Name: "Refund notification URL", Type: connector.ConfigFieldText}, {Key: "currency", Name: "Default currency", Type: connector.ConfigFieldText, Default: json.RawMessage(`"CNY"`)}, {Key: "webhook_tolerance_seconds", Name: "Webhook tolerance seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`300`)}, {Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`15`), Validation: connector.ConfigValidation{Min: &min, Max: &max}}}, SecretFields: []connector.SecretField{{Key: "merchant_private_key", Name: "Merchant RSA private key", Required: true, CredentialKind: connector.SecretCredentialPrivateKey, MaterialFormat: connector.SecretMaterialPEM, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}, {Key: "platform_public_key", Name: "WeChat Pay platform public key or certificate", Required: true, CredentialKind: connector.SecretCredentialCertificate, MaterialFormat: connector.SecretMaterialPEMOrReference, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}, {Key: "api_v3_key", Name: "WeChat Pay API v3 key", Required: true, CredentialKind: connector.SecretCredentialSigningSecret, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}}}
}
func (p *provider) ValidateConfig(c connector.Connection) error {
	parsed, err := url.Parse(configDefault(c, "base_url", defaultBaseURL))
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && loopback(parsed.Hostname()))) {
		return permanent("base_url_invalid", "HTTPS or loopback HTTP base URL is required")
	}
	for _, key := range []string{"app_id", "merchant_id", "merchant_serial_no", "notify_url"} {
		if config(c, key) == "" {
			return permanent(key+"_required", key+" is required")
		}
	}
	timeout := integer(c.Config["timeout_seconds"], defaultTimeout)
	if timeout < 1 || timeout > maximumTimeout {
		return permanent("timeout_invalid", "timeout_seconds is invalid")
	}
	return nil
}
