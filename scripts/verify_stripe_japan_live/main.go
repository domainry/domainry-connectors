package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/providers/payment/stripe"
)

const responseLimit int64 = 2 << 20

type releaseTransport struct{ client *http.Client }

func (transport releaseTransport) RoundTripHTTP(ctx context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	httpRequest, err := http.NewRequestWithContext(ctx, request.Method, request.URL, bytes.NewReader(request.Body))
	if err != nil {
		return connector.HTTPResponse{}, err
	}
	for key, values := range request.Headers {
		for _, value := range values {
			httpRequest.Header.Add(key, value)
		}
	}
	for key, values := range request.SecretHeaders {
		for _, value := range values {
			httpRequest.Header.Add(key, value)
		}
	}
	response, err := transport.client.Do(httpRequest)
	if err != nil {
		return connector.HTTPResponse{}, err
	}
	defer response.Body.Close()
	limit := request.MaxResponseBytes
	if limit <= 0 {
		limit = responseLimit
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return connector.HTTPResponse{}, err
	}
	if int64(len(body)) > limit {
		return connector.HTTPResponse{}, errors.New("Stripe live response exceeded limit")
	}
	return connector.HTTPResponse{StatusCode: response.StatusCode, Headers: response.Header, Body: body}, nil
}

func (releaseTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("SQL is unavailable")
}

func main() {
	if err := verify(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("Stripe Japan connector release verification passed")
}

func verify() error {
	apiKey := strings.TrimSpace(os.Getenv("STRIPE_SECRET_KEY"))
	if apiKey == "" {
		return errors.New("STRIPE_SECRET_KEY is required")
	}
	if !strings.HasPrefix(apiKey, "sk_test_") {
		return errors.New("release verification refuses non-test Stripe credentials")
	}
	adapter, err := stripe.New(releaseTransport{client: &http.Client{Timeout: 30 * time.Second}})
	if err != nil {
		return err
	}
	connection := connector.Connection{Config: map[string]any{"currency": "jpy"}}
	secrets := map[string]string{"api_key": apiKey}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if _, err := adapter.(connector.ConnectionTester).TestConnection(ctx, connector.TestConnectionRequest{Connection: connection, Secrets: secrets}); err != nil {
		return err
	}
	create, err := operation(adapter.Descriptor(), "create_payment_intent")
	if err != nil {
		return err
	}
	payload, _ := json.Marshal(map[string]any{"amount": 100, "currency": "jpy", "payment_method_types": []string{"card", "paypay", "konbini"}, "description": "Domainry Stripe Japan release verification"})
	created, err := adapter.Call(ctx, connector.CallRequest{ConnectorKey: stripe.ConnectorKey, ProviderKey: stripe.ProviderKey, OperationKey: create.Key, ContractSHA256: create.ContractSHA256, Mode: create.Mode, Connection: connection, Secrets: secrets, RequestRef: "domainry-stripe-japan-release", Payload: payload})
	if err != nil {
		return err
	}
	var result map[string]any
	if err := json.Unmarshal(created.Payload, &result); err != nil {
		return err
	}
	paymentIntent := strings.TrimSpace(fmt.Sprint(result["id"]))
	if paymentIntent == "" || paymentIntent == "<nil>" {
		return errors.New("Stripe response has no PaymentIntent id")
	}
	cancelOperation, err := operation(adapter.Descriptor(), "cancel_payment_intent")
	if err != nil {
		return err
	}
	cancelPayload, _ := json.Marshal(map[string]any{"payment_intent": paymentIntent})
	_, err = adapter.Call(ctx, connector.CallRequest{ConnectorKey: stripe.ConnectorKey, ProviderKey: stripe.ProviderKey, OperationKey: cancelOperation.Key, ContractSHA256: cancelOperation.ContractSHA256, Mode: cancelOperation.Mode, Connection: connection, Secrets: secrets, RequestRef: "domainry-stripe-japan-release-cancel", Payload: cancelPayload})
	return err
}

func operation(descriptor connector.ProviderDescriptor, key string) (connector.OperationDescriptor, error) {
	for _, candidate := range descriptor.Operations {
		if candidate.Key == key {
			return candidate, nil
		}
	}
	return connector.OperationDescriptor{}, fmt.Errorf("Stripe operation %s is missing", key)
}
