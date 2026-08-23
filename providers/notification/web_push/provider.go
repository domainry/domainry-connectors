// Package webpush implements the standards-based Web Push Provider.
package webpush

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	webpushlib "github.com/SherClockHolmes/webpush-go"
	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	ConnectorKey = "notification"
	ProviderKey  = "web_push"
)

type SendInput struct {
	SubscriptionID string `json:"subscription_id"`
	Endpoint       string `json:"endpoint"`
	P256DH         string `json:"p256dh"`
	Auth           string `json:"auth"`
	Payload        any    `json:"payload"`
	TTLSeconds     int    `json:"ttl_seconds,omitempty"`
}

var (
	Send           = connector.EnqueueOperation[SendInput]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "send", ContractSHA256: "9279b82133298849994dcb257c632bad4f5979f061fd668a8dd6c7701c4a17e9", Reliability: connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNone}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}}
	TestConnection = connector.CallOperation[struct{}, map[string]any]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection", ContractSHA256: "d66a0f8d3be976ecbc3125daa0c3aea1231c4858104fe87ebbdd5907dd9ced5f", Reliability: connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}}
)

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("Web Push transport is required")
	}
	p := &provider{transport: transport}
	send, err := connector.BindEnqueueDelivery(Send, p.send)
	if err != nil {
		return nil, err
	}
	test, err := connector.BindCall(TestConnection, p.test)
	if err != nil {
		return nil, err
	}
	adapter, err := connector.NewProvider(schema(), send, test)
	if err != nil {
		return nil, err
	}
	p.Adapter = adapter
	return p, nil
}

func schema() connector.ProviderSchema {
	minTimeout, maxTimeout := float64(1), float64(60)
	minTTL, maxTTL := float64(0), float64(2419200)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{
		{Key: "vapid_subject", Name: "VAPID subject", Type: connector.ConfigFieldText, Required: true, Validation: connector.ConfigValidation{MaxLength: 512, Pattern: "^(mailto:|https://)"}},
		{Key: "vapid_public_key", Name: "VAPID public key", Type: connector.ConfigFieldText, Required: true, Validation: connector.ConfigValidation{MaxLength: 256}},
		{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`15`), Validation: connector.ConfigValidation{Min: &minTimeout, Max: &maxTimeout}},
		{Key: "ttl_seconds", Name: "Default TTL seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`3600`), Validation: connector.ConfigValidation{Min: &minTTL, Max: &maxTTL}},
	}, SecretFields: []connector.SecretField{{Key: "vapid_private_key", Name: "VAPID private key", Required: true, CredentialKind: connector.SecretCredentialPrivateKey, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryNone, TestRequirement: connector.SecretTestWhenBound}}}
}

func (*provider) ValidateConfig(connection connector.Connection) error {
	subject := config(connection, "vapid_subject")
	publicKey := config(connection, "vapid_public_key")
	if (!strings.HasPrefix(subject, "mailto:") && !strings.HasPrefix(subject, "https://")) || publicKey == "" {
		return permanent("vapid_config_invalid", "VAPID subject and public key are required")
	}
	timeout := integer(connection.Config["timeout_seconds"], 15)
	ttl := integer(connection.Config["ttl_seconds"], 3600)
	if timeout < 1 || timeout > 60 || ttl < 0 || ttl > 2419200 {
		return permanent("config_invalid", "timeout_seconds or ttl_seconds is outside its supported range")
	}
	return nil
}

func (p *provider) test(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[map[string]any], error) {
	if err := ctx.Err(); err != nil {
		return empty(), err
	}
	if err := p.ValidateConfig(request.Connection); err != nil {
		return empty(), err
	}
	if strings.TrimSpace(request.Secrets["vapid_private_key"]) == "" {
		return empty(), permanent("vapid_secret_required", "resolved VAPID private key is required")
	}
	return connector.TypedResult[map[string]any]{Output: map[string]any{"provider_status": "configured"}, ResponseRef: "vapid:configured"}, nil
}

func (p *provider) TestConnection(ctx context.Context, request connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	result, err := p.test(ctx, connector.TypedRequest[struct{}]{Connection: request.Connection, Secrets: request.Secrets})
	if err != nil {
		return connector.TestConnectionResult{}, err
	}
	details, _ := json.Marshal(result.Output)
	return connector.TestConnectionResult{Connected: true, Details: details}, nil
}

func (p *provider) send(ctx context.Context, request connector.TypedRequest[SendInput]) (connector.DeliveryResult, error) {
	if err := p.ValidateConfig(request.Connection); err != nil {
		return connector.DeliveryResult{}, err
	}
	input := request.Input
	if !strings.HasPrefix(strings.TrimSpace(input.Endpoint), "https://") || strings.TrimSpace(input.P256DH) == "" || strings.TrimSpace(input.Auth) == "" || strings.TrimSpace(input.SubscriptionID) == "" {
		return connector.DeliveryResult{}, permanent("subscription_invalid", "a complete HTTPS push subscription is required")
	}
	privateKey := strings.TrimSpace(request.Secrets["vapid_private_key"])
	if privateKey == "" {
		return connector.DeliveryResult{}, permanent("vapid_secret_required", "resolved VAPID private key is required")
	}
	payload, err := json.Marshal(input.Payload)
	if err != nil {
		return connector.DeliveryResult{}, permanent("payload_invalid", "payload is not JSON serializable")
	}
	ttl := input.TTLSeconds
	if ttl == 0 {
		ttl = integer(request.Connection.Config["ttl_seconds"], 3600)
	}
	if ttl < 0 || ttl > 2419200 {
		return connector.DeliveryResult{}, permanent("ttl_invalid", "ttl_seconds is outside its supported range")
	}
	response, err := webpushlib.SendNotificationWithContext(ctx, payload, &webpushlib.Subscription{Endpoint: input.Endpoint, Keys: webpushlib.Keys{P256dh: input.P256DH, Auth: input.Auth}}, &webpushlib.Options{HTTPClient: runtimeHTTPClient{transport: p.transport}, Subscriber: config(request.Connection, "vapid_subject"), VAPIDPublicKey: config(request.Connection, "vapid_public_key"), VAPIDPrivateKey: privateKey, TTL: ttl})
	if err != nil {
		return connector.DeliveryResult{}, connector.UncertainError("web_push.delivery_unknown", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
	ref := "http:" + strconv.Itoa(response.StatusCode)
	switch {
	case response.StatusCode == http.StatusGone || response.StatusCode == http.StatusNotFound:
		return connector.DeliveryResult{ResponseRef: ref}, permanent("subscription_expired", "push subscription expired")
	case response.StatusCode == http.StatusTooManyRequests:
		return connector.DeliveryResult{ResponseRef: ref}, connector.RetryableError("web_push.rate_limited", errors.New("push service rate limited the request"))
	case response.StatusCode >= 500:
		return connector.DeliveryResult{ResponseRef: ref}, connector.RetryableError("web_push.unavailable", fmt.Errorf("push service returned HTTP %d", response.StatusCode))
	case response.StatusCode < 200 || response.StatusCode >= 300:
		return connector.DeliveryResult{ResponseRef: ref}, permanent("provider_rejected", fmt.Sprintf("push service returned HTTP %d", response.StatusCode))
	}
	return connector.DeliveryResult{ResponseRef: ref}, nil
}

type runtimeHTTPClient struct{ transport connector.Transport }

func (client runtimeHTTPClient) Do(request *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(io.LimitReader(request.Body, (1<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(body) > 1<<20 {
		return nil, errors.New("encrypted Web Push request exceeds one MiB")
	}
	headers := map[string][]string{}
	secretHeaders := map[string][]string{}
	for name, values := range request.Header {
		cloned := append([]string(nil), values...)
		if http.CanonicalHeaderKey(name) == "Authorization" {
			secretHeaders["Authorization"] = cloned
		} else {
			headers[http.CanonicalHeaderKey(name)] = cloned
		}
	}
	result, err := client.transport.RoundTripHTTP(request.Context(), connector.HTTPRequest{Method: request.Method, URL: request.URL.String(), Headers: headers, SecretHeaders: secretHeaders, Body: body, MaxResponseBytes: 64 << 10})
	if err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: result.StatusCode, Status: strconv.Itoa(result.StatusCode), Header: http.Header(result.Headers), Body: io.NopCloser(bytes.NewReader(result.Body)), Request: request}, nil
}

func config(connection connector.Connection, key string) string {
	return strings.TrimSpace(fmt.Sprint(connection.Config[key]))
}
func integer(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		parsed, err := typed.Int64()
		if err == nil {
			return int(parsed)
		}
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(typed))
		if err == nil {
			return parsed
		}
	}
	return fallback
}
func empty() connector.TypedResult[map[string]any] { return connector.TypedResult[map[string]any]{} }
func permanent(suffix, message string) error {
	return connector.PermanentError("web_push."+suffix, errors.New(message))
}
