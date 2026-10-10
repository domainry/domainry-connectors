package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/internal/releaseverify"
	wechatpay "github.com/domainry/domainry-connectors/providers/payment/wechat_pay"
)

func main() {
	if err := verify(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("WeChat Pay controlled-test connector release verification passed")
}

func verify() error {
	if os.Getenv("WECHAT_PAY_CONTROLLED_TEST") != "1" {
		return errors.New("WECHAT_PAY_CONTROLLED_TEST=1 is required; use only a dedicated test merchant")
	}
	values := map[string]string{}
	for _, key := range []string{"WECHAT_PAY_APP_ID", "WECHAT_PAY_MERCHANT_ID", "WECHAT_PAY_MERCHANT_SERIAL_NO", "WECHAT_PAY_MERCHANT_PRIVATE_KEY", "WECHAT_PAY_PLATFORM_PUBLIC_KEY", "WECHAT_PAY_API_V3_KEY", "WECHAT_PAY_PAYER_OPENID", "WECHAT_PAY_NOTIFY_URL"} {
		values[key] = strings.TrimSpace(os.Getenv(key))
		if values[key] == "" {
			return fmt.Errorf("%s is required", key)
		}
	}
	adapter, err := wechatpay.New(releaseverify.NewHTTPTransport())
	if err != nil {
		return err
	}
	connection := connector.Connection{Config: map[string]any{"base_url": "https://api.mch.weixin.qq.com", "app_id": values["WECHAT_PAY_APP_ID"], "merchant_id": values["WECHAT_PAY_MERCHANT_ID"], "merchant_serial_no": values["WECHAT_PAY_MERCHANT_SERIAL_NO"], "notify_url": values["WECHAT_PAY_NOTIFY_URL"], "currency": "CNY", "timeout_seconds": 30}}
	secrets := map[string]string{"merchant_private_key": values["WECHAT_PAY_MERCHANT_PRIVATE_KEY"], "platform_public_key": values["WECHAT_PAY_PLATFORM_PUBLIC_KEY"], "api_v3_key": values["WECHAT_PAY_API_V3_KEY"]}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if _, err := adapter.(connector.ConnectionTester).TestConnection(ctx, connector.TestConnectionRequest{Connection: connection, Secrets: secrets}); err != nil {
		return fmt.Errorf("connection probe: %w", err)
	}
	order := fmt.Sprintf("domainry%d", time.Now().UnixNano())
	if err := callJSAPI(ctx, adapter, connection, secrets, order, values["WECHAT_PAY_PAYER_OPENID"]); err != nil {
		return fmt.Errorf("create controlled-test JSAPI order: %w", err)
	}
	defer func() {
		_ = call(context.Background(), adapter, wechatpay.ClosePaymentOrderNow.Key, wechatpay.ClosePaymentOrderNow.ContractSHA256, connector.ModeCall, connection, secrets, order+"-cleanup", wechatpay.PaymentOrderInput{MerchantOrderNo: order})
	}()
	if err := call(ctx, adapter, wechatpay.QueryPaymentOrder.Key, wechatpay.QueryPaymentOrder.ContractSHA256, connector.ModeCall, connection, secrets, order+"-query", wechatpay.PaymentOrderInput{MerchantOrderNo: order}); err != nil {
		return fmt.Errorf("query controlled-test order: %w", err)
	}
	if err := call(ctx, adapter, wechatpay.ClosePaymentOrderNow.Key, wechatpay.ClosePaymentOrderNow.ContractSHA256, connector.ModeCall, connection, secrets, order+"-close", wechatpay.PaymentOrderInput{MerchantOrderNo: order}); err != nil {
		return fmt.Errorf("close controlled-test order: %w", err)
	}
	return nil
}

func callJSAPI(ctx context.Context, adapter connector.Adapter, connection connector.Connection, secrets map[string]string, order, payerOpenID string) error {
	payload, err := json.Marshal(wechatpay.CreateJSAPIPaymentOrderInput{MerchantOrderNo: order, AmountMinor: 1, Subject: "Domainry JSAPI release verification", PayerOpenID: payerOpenID, ExpiresAt: time.Now().UTC().Add(10 * time.Minute).Format(time.RFC3339), Currency: "CNY"})
	if err != nil {
		return err
	}
	result, err := adapter.Call(ctx, connector.CallRequest{ConnectorKey: wechatpay.ConnectorKey, ProviderKey: wechatpay.ProviderKey, OperationKey: wechatpay.CreateJSAPIPaymentOrder.Key, ContractSHA256: wechatpay.CreateJSAPIPaymentOrder.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Secrets: secrets, RequestRef: order, Payload: payload})
	if err != nil {
		return err
	}
	var output map[string]any
	if json.Unmarshal(result.Payload, &output) != nil {
		return errors.New("JSAPI response is not valid JSON")
	}
	prepayID, prepayOK := output["prepay_id"].(string)
	paySign, signOK := output["pay_sign"].(string)
	packageValue, packageOK := output["package"].(string)
	if !prepayOK || strings.TrimSpace(prepayID) == "" || !signOK || strings.TrimSpace(paySign) == "" || !packageOK || packageValue != "prepay_id="+prepayID {
		return errors.New("JSAPI response is missing signed Mini Program payment parameters")
	}
	return nil
}

func call(ctx context.Context, adapter connector.Adapter, key, hash string, mode connector.OperationMode, connection connector.Connection, secrets map[string]string, ref string, input any) error {
	payload, err := json.Marshal(input)
	if err != nil {
		return err
	}
	_, err = adapter.Call(ctx, connector.CallRequest{ConnectorKey: wechatpay.ConnectorKey, ProviderKey: wechatpay.ProviderKey, OperationKey: key, ContractSHA256: hash, Mode: mode, Connection: connection, Secrets: secrets, RequestRef: ref, Delivery: mode == connector.ModeEnqueue, Payload: payload})
	return err
}
