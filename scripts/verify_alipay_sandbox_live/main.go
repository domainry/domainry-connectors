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
	"github.com/domainry/domainry-connectors/providers/payment/alipay"
)

func main() {
	if err := verify(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("Alipay sandbox connector release verification passed")
}

func verify() error {
	if os.Getenv("ALIPAY_SANDBOX") != "1" {
		return errors.New("ALIPAY_SANDBOX=1 is required; production credentials are refused")
	}
	appID := strings.TrimSpace(os.Getenv("ALIPAY_APP_ID"))
	privateKey := strings.TrimSpace(os.Getenv("ALIPAY_MERCHANT_PRIVATE_KEY"))
	if appID == "" || privateKey == "" {
		return errors.New("ALIPAY_APP_ID and ALIPAY_MERCHANT_PRIVATE_KEY are required")
	}
	adapter, err := alipay.New(releaseverify.NewHTTPTransport())
	if err != nil {
		return err
	}
	connection := connector.Connection{Config: map[string]any{"base_url": "https://openapi-sandbox.dl.alipaydev.com/gateway.do", "app_id": appID, "notify_url": "https://example.invalid/domainry-alipay-sandbox", "charset": "utf-8", "sign_type": "RSA2", "timeout_seconds": 30}}
	secrets := map[string]string{"merchant_private_key": privateKey}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if _, err := adapter.(connector.ConnectionTester).TestConnection(ctx, connector.TestConnectionRequest{Connection: connection, Secrets: secrets}); err != nil {
		return fmt.Errorf("connection probe: %w", err)
	}
	order := fmt.Sprintf("domainry%d", time.Now().UnixNano())
	if err := call(adapter, alipay.CreateNativePaymentOrder.Key, alipay.CreateNativePaymentOrder.ContractSHA256, connector.ModeEnqueue, connection, secrets, order, alipay.CreateNativePaymentOrderInput{MerchantOrderNo: order, AmountMinor: 1, Subject: "Domainry release verification"}); err != nil {
		return fmt.Errorf("create sandbox order: %w", err)
	}
	defer func() {
		_ = call(adapter, alipay.ClosePaymentOrder.Key, alipay.ClosePaymentOrder.ContractSHA256, connector.ModeEnqueue, connection, secrets, order+"-cleanup", alipay.PaymentOrderInput{MerchantOrderNo: order})
	}()
	if err := call(adapter, alipay.QueryPaymentOrder.Key, alipay.QueryPaymentOrder.ContractSHA256, connector.ModeCall, connection, secrets, order+"-query", alipay.PaymentOrderInput{MerchantOrderNo: order}); err != nil {
		return fmt.Errorf("query sandbox order: %w", err)
	}
	if err := call(adapter, alipay.ClosePaymentOrder.Key, alipay.ClosePaymentOrder.ContractSHA256, connector.ModeEnqueue, connection, secrets, order+"-close", alipay.PaymentOrderInput{MerchantOrderNo: order}); err != nil {
		return fmt.Errorf("close sandbox order: %w", err)
	}
	return nil
}

func call(adapter connector.Adapter, key, hash string, mode connector.OperationMode, connection connector.Connection, secrets map[string]string, ref string, input any) error {
	payload, err := json.Marshal(input)
	if err != nil {
		return err
	}
	_, err = adapter.Call(context.Background(), connector.CallRequest{ConnectorKey: alipay.ConnectorKey, ProviderKey: alipay.ProviderKey, OperationKey: key, ContractSHA256: hash, Mode: mode, Connection: connection, Secrets: secrets, RequestRef: ref, Delivery: mode == connector.ModeEnqueue, Payload: payload})
	return err
}
