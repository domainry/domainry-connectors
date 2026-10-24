// Package apns implements Apple Push Notification service delivery.
package apns

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	ConnectorKey = "notification"
	ProviderKey  = "apns"

	productionEndpoint = "https://api.push.apple.com"
	sandboxEndpoint    = "https://api.sandbox.push.apple.com"
	responseLimit      = 64 << 10
	tokenReuseDuration = 50 * time.Minute
)

type SendInput struct {
	DeviceToken string            `json:"device_token"`
	Title       string            `json:"title"`
	Body        string            `json:"body"`
	Data        map[string]string `json:"data,omitempty"`
	TTLSeconds  int               `json:"ttl_seconds,omitempty"`
	CollapseKey string            `json:"collapse_key,omitempty"`
	Priority    string            `json:"priority,omitempty"`
}

var (
	SendNative = connector.EnqueueOperation[SendInput]{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "send_native",
		ContractSHA256: "fb466c3fbaf24a445ed5784d33a0b15050141ff6885b699e581cdfe1aed855d4",
		Reliability:    connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNone}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}},
	}
	TestConnection = connector.CallOperation[struct{}, map[string]any]{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection",
		ContractSHA256: "d66a0f8d3be976ecbc3125daa0c3aea1231c4858104fe87ebbdd5907dd9ced5f",
		Reliability:    connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}},
	}
	identifierPattern = regexp.MustCompile(`^[A-Za-z0-9]{10}$`)
	bundleIDPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9.-]{2,254}$`)
	// Apple defines APNs device tokens as variable-length opaque bytes. Callers
	// serialize the bytes as an even-length hexadecimal string.
	deviceTokenPattern = regexp.MustCompile(`^[a-fA-F0-9]+$`)
)

type cachedToken struct {
	value    string
	issuedAt time.Time
}

type provider struct {
	connector.Adapter
	transport connector.Transport
	mu        sync.Mutex
	tokens    map[[32]byte]cachedToken
	now       func() time.Time
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("APNs HTTP transport is required")
	}
	p := &provider{transport: transport, tokens: map[[32]byte]cachedToken{}, now: time.Now}
	send, err := connector.BindEnqueueDelivery(SendNative, p.send)
	if err != nil {
		return nil, err
	}
	test, err := connector.BindCall(TestConnection, p.test)
	if err != nil {
		return nil, err
	}
	p.Adapter, err = connector.NewProvider(schema(), send, test)
	return p, err
}

func schema() connector.ProviderSchema {
	minTimeout, maxTimeout := float64(1), float64(60)
	minTTL, maxTTL := float64(0), float64(2419200)
	return connector.ProviderSchema{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.1",
		ConfigFields: []connector.ConfigField{
			{Key: "team_id", Name: "Apple Developer team ID", Type: connector.ConfigFieldText, Required: true, Validation: connector.ConfigValidation{MaxLength: 10}},
			{Key: "key_id", Name: "APNs signing key ID", Type: connector.ConfigFieldText, Required: true, Validation: connector.ConfigValidation{MaxLength: 10}},
			{Key: "bundle_id", Name: "Application bundle ID", Type: connector.ConfigFieldText, Required: true, Validation: connector.ConfigValidation{MaxLength: 255}},
			{Key: "environment", Name: "APNs environment", Type: connector.ConfigFieldSelect, Required: true, Default: json.RawMessage(`"production"`), Validation: connector.ConfigValidation{Options: []string{"production", "sandbox"}}},
			{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`15`), Validation: connector.ConfigValidation{Min: &minTimeout, Max: &maxTimeout}},
			{Key: "ttl_seconds", Name: "Default TTL seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`3600`), Validation: connector.ConfigValidation{Min: &minTTL, Max: &maxTTL}},
		},
		SecretFields: []connector.SecretField{{Key: "private_key", Name: "APNs private signing key", Required: true, CredentialKind: connector.SecretCredentialPrivateKey, MaterialFormat: connector.SecretMaterialPEM, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}},
	}
}

func (*provider) ValidateConfig(connection connector.Connection) error {
	teamID := setting(connection, "team_id")
	keyID := setting(connection, "key_id")
	bundleID := setting(connection, "bundle_id")
	environment := setting(connection, "environment")
	if environment == "" {
		environment = "production"
	}
	timeoutSeconds := integer(connection.Config["timeout_seconds"], 15)
	ttlSeconds := integer(connection.Config["ttl_seconds"], 3600)
	if !identifierPattern.MatchString(teamID) || !identifierPattern.MatchString(keyID) || !bundleIDPattern.MatchString(bundleID) || (environment != "production" && environment != "sandbox") || timeoutSeconds < 1 || timeoutSeconds > 60 || ttlSeconds < 0 || ttlSeconds > 2419200 {
		return permanent("config_invalid", "valid APNs team, key, bundle, environment, timeout and TTL settings are required")
	}
	return nil
}

func (p *provider) test(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[map[string]any], error) {
	if err := ctx.Err(); err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	if err := p.ValidateConfig(request.Connection); err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	key, err := parsePrivateKey(request.Secrets["private_key"])
	if err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	if _, err := p.authorizationToken(request.Connection, request.Secrets["private_key"], key); err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	return connector.TypedResult[map[string]any]{Output: map[string]any{"provider_status": "configured"}, ResponseRef: "apns:configured"}, nil
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
	defaultTTL := integer(request.Connection.Config["ttl_seconds"], 3600)
	if err := validateInput(request.Input, defaultTTL); err != nil {
		return connector.DeliveryResult{}, err
	}
	privateKeyPEM := request.Secrets["private_key"]
	key, err := parsePrivateKey(privateKeyPEM)
	if err != nil {
		return connector.DeliveryResult{}, err
	}
	token, err := p.authorizationToken(request.Connection, privateKeyPEM, key)
	if err != nil {
		return connector.DeliveryResult{}, err
	}
	payloadEnvelope := map[string]any{
		"aps": map[string]any{"alert": map[string]string{"title": request.Input.Title, "body": request.Input.Body}, "sound": "default"},
	}
	if len(request.Input.Data) > 0 {
		payloadEnvelope["data"] = request.Input.Data
	}
	payload, err := json.Marshal(payloadEnvelope)
	if err != nil || len(payload) > 4096 {
		return connector.DeliveryResult{}, permanent("payload_too_large", "APNs payload exceeds 4096 bytes")
	}
	now := p.now().UTC()
	ttl := request.Input.TTLSeconds
	if ttl == 0 {
		ttl = defaultTTL
	}
	headers := map[string][]string{
		"Accept":          {"application/json"},
		"Content-Type":    {"application/json"},
		"apns-id":         {requestID(request.RequestRef, request.Input.DeviceToken)},
		"apns-topic":      {setting(request.Connection, "bundle_id")},
		"apns-push-type":  {"alert"},
		"apns-priority":   {priorityValue(request.Input.Priority)},
		"apns-expiration": {strconv.FormatInt(now.Add(time.Duration(ttl)*time.Second).Unix(), 10)},
	}
	if request.Input.CollapseKey != "" {
		headers["apns-collapse-id"] = []string{request.Input.CollapseKey}
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{
		Method: http.MethodPost, URL: endpoint(request.Connection) + "/3/device/" + strings.ToLower(request.Input.DeviceToken),
		Headers: headers, SecretHeaders: map[string][]string{"Authorization": {"bearer " + token}}, Body: payload,
		MaxResponseBytes: responseLimit, Timeout: time.Duration(integer(request.Connection.Config["timeout_seconds"], 15)) * time.Second,
	})
	if transportErr != nil {
		return connector.DeliveryResult{}, connector.UncertainError("apns.delivery_unknown", transportErr)
	}
	responseRef := headerValue(response.Headers, "apns-id")
	if responseRef == "" {
		responseRef = "http:" + strconv.Itoa(response.StatusCode)
	}
	if response.StatusCode == http.StatusOK {
		return connector.DeliveryResult{ResponseRef: responseRef}, nil
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return connector.DeliveryResult{ResponseRef: responseRef}, connector.UncertainError("apns.receipt_invalid", fmt.Errorf("APNs returned unexpected HTTP %d", response.StatusCode))
	}
	if response.StatusCode == http.StatusTooManyRequests || response.StatusCode == http.StatusInternalServerError || response.StatusCode == http.StatusServiceUnavailable {
		return connector.DeliveryResult{ResponseRef: responseRef}, connector.RetryableError("apns.provider_unavailable", fmt.Errorf("APNs returned HTTP %d", response.StatusCode))
	}
	return connector.DeliveryResult{ResponseRef: responseRef}, permanent("request_rejected", fmt.Sprintf("APNs rejected the request with HTTP %d", response.StatusCode))
}

func (p *provider) authorizationToken(connection connector.Connection, privateKeyPEM string, key *ecdsa.PrivateKey) (string, error) {
	cacheKey := sha256.Sum256([]byte(setting(connection, "team_id") + "\x00" + setting(connection, "key_id") + "\x00" + privateKeyPEM))
	now := p.now().UTC()
	p.mu.Lock()
	defer p.mu.Unlock()
	if cached, ok := p.tokens[cacheKey]; ok && now.Sub(cached.issuedAt) >= 0 && now.Sub(cached.issuedAt) < tokenReuseDuration {
		return cached.value, nil
	}
	token, err := signJWT(setting(connection, "team_id"), setting(connection, "key_id"), key, now)
	if err != nil {
		return "", permanent("private_key_invalid", "APNs private key could not sign an authorization token")
	}
	p.tokens[cacheKey] = cachedToken{value: token, issuedAt: now}
	return token, nil
}

func parsePrivateKey(raw string) (*ecdsa.PrivateKey, error) {
	if len(raw) == 0 || len(raw) > 16<<10 {
		return nil, permanent("private_key_invalid", "valid APNs PKCS#8 private key is required")
	}
	block, _ := pem.Decode([]byte(raw))
	if block == nil {
		return nil, permanent("private_key_invalid", "valid APNs PKCS#8 private key is required")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, permanent("private_key_invalid", "valid APNs PKCS#8 private key is required")
	}
	key, ok := parsed.(*ecdsa.PrivateKey)
	if !ok || key.Curve != elliptic.P256() {
		return nil, permanent("private_key_invalid", "APNs private key must use the P-256 curve")
	}
	return key, nil
}

func signJWT(teamID, keyID string, key *ecdsa.PrivateKey, now time.Time) (string, error) {
	header, _ := json.Marshal(map[string]string{"alg": "ES256", "kid": keyID})
	claims, _ := json.Marshal(map[string]any{"iss": teamID, "iat": now.Unix()})
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(unsigned))
	r, s, err := ecdsa.Sign(rand.Reader, key, digest[:])
	if err != nil {
		return "", err
	}
	signature := make([]byte, 64)
	r.FillBytes(signature[:32])
	s.FillBytes(signature[32:])
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func validateInput(input SendInput, defaultTTL int) error {
	ttl := input.TTLSeconds
	if ttl == 0 {
		ttl = defaultTTL
	}
	priority := strings.ToLower(strings.TrimSpace(input.Priority))
	if priority == "" {
		priority = "high"
	}
	if !validDeviceToken(input.DeviceToken) || strings.TrimSpace(input.Title) == "" || len(input.Title) > 200 || strings.TrimSpace(input.Body) == "" || len(input.Body) > 2000 || len(input.CollapseKey) > 64 || len(input.Data) > 100 || ttl < 0 || ttl > 2419200 || (priority != "high" && priority != "normal") {
		return permanent("message_invalid", "valid APNs token, message, priority and TTL are required")
	}
	for key, value := range input.Data {
		if strings.TrimSpace(key) == "" || len(key) > 128 || len(value) > 2048 {
			return permanent("message_invalid", "APNs data keys and values must be bounded")
		}
	}
	return nil
}

func validDeviceToken(value string) bool {
	return len(value) > 0 && len(value) <= 4096 && len(value)%2 == 0 && deviceTokenPattern.MatchString(value)
}

func endpoint(connection connector.Connection) string {
	if setting(connection, "environment") == "sandbox" {
		return sandboxEndpoint
	}
	return productionEndpoint
}

func priorityValue(value string) string {
	if strings.EqualFold(strings.TrimSpace(value), "normal") {
		return "5"
	}
	return "10"
}

func requestID(requestRef, deviceToken string) string {
	digest := sha256.Sum256([]byte(requestRef + "\x00" + strings.ToLower(deviceToken)))
	digest[6] = (digest[6] & 0x0f) | 0x40
	digest[8] = (digest[8] & 0x3f) | 0x80
	return hex.EncodeToString(digest[0:4]) + "-" + hex.EncodeToString(digest[4:6]) + "-" + hex.EncodeToString(digest[6:8]) + "-" + hex.EncodeToString(digest[8:10]) + "-" + hex.EncodeToString(digest[10:16])
}

func headerValue(headers map[string][]string, name string) string {
	for key, values := range headers {
		if strings.EqualFold(key, name) && len(values) > 0 {
			return strings.TrimSpace(values[0])
		}
	}
	return ""
}

func setting(connection connector.Connection, key string) string {
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

func permanent(suffix, message string) error {
	return connector.PermanentError("apns."+suffix, errors.New(message))
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
