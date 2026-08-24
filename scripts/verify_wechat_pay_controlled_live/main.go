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
	for _, key := range []string{"WECHAT_PAY_APP_ID", "WECHAT_PAY_MERCHANT_ID", "WECHAT_PAY_MERCHANT_SERIAL_NO", "WECHAT_PAY_MERCHANT_PRIVATE_KEY"} {
		values[key] = strings.TrimSpace(os.Getenv(key))
		if values[key] == "" {
			return fmt.Errorf("%s is required", key)
		}
	}
	adapter, err := wechatpay.New(releaseverify.NewHTTPTransport())
	if err != nil {
		return err
	}
	connection := connector.Connection{Config: map[string]any{"base_url": "https://api.mch.weixin.qq.com", "app_id": values["WECHAT_PAY_APP_ID"], "merchant_id": values["WECHAT_PAY_MERCHANT_ID"], "merchant_serial_no": values["WECHAT_PAY_MERCHANT_SERIAL_NO"], "notify_url": "https://example.invalid/domainry-wechat-pay-controlled-test", "currency": "CNY", "timeout_seconds": 30}}
	secrets := map[string]string{"merchant_private_key": values["WECHAT_PAY_MERCHANT_PRIVATE_KEY"]}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if _, err := adapter.(connector.ConnectionTester).TestConnection(ctx, connector.TestConnectionRequest{Connection: connection, Secrets: secrets}); err != nil {
		return fmt.Errorf("connection probe: %w", err)
	}
	order := fmt.Sprintf("domainry%d", time.Now().UnixNano())
	if err := call(ctx, adapter, wechatpay.CreateNativePaymentOrder.Key, wechatpay.CreateNativePaymentOrder.ContractSHA256, connector.ModeEnqueue, connection, secrets, order, wechatpay.CreateNativePaymentOrderInput{MerchantOrderNo: order, AmountMinor: 1, Subject: "Domainry release verification", Currency: "CNY"}); err != nil {
		return fmt.Errorf("create controlled-test order: %w", err)
	}
	defer func() {
		_ = call(context.Background(), adapter, wechatpay.ClosePaymentOrder.Key, wechatpay.ClosePaymentOrder.ContractSHA256, connector.ModeEnqueue, connection, secrets, order+"-cleanup", wechatpay.PaymentOrderInput{MerchantOrderNo: order})
	}()
	if err := call(ctx, adapter, wechatpay.QueryPaymentOrder.Key, wechatpay.QueryPaymentOrder.ContractSHA256, connector.ModeCall, connection, secrets, order+"-query", wechatpay.PaymentOrderInput{MerchantOrderNo: order}); err != nil {
		return fmt.Errorf("query controlled-test order: %w", err)
	}
	if err := call(ctx, adapter, wechatpay.ClosePaymentOrder.Key, wechatpay.ClosePaymentOrder.ContractSHA256, connector.ModeEnqueue, connection, secrets, order+"-close", wechatpay.PaymentOrderInput{MerchantOrderNo: order}); err != nil {
		return fmt.Errorf("close controlled-test order: %w", err)
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
