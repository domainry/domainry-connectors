// Package fcm implements Firebase Cloud Messaging HTTP v1 delivery.
package fcm

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	ConnectorKey = "notification"
	ProviderKey  = "fcm"

	googleTokenURL = "https://oauth2.googleapis.com/token"
	fcmScope       = "https://www.googleapis.com/auth/firebase.messaging"
	responseLimit  = 64 << 10
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

type serviceAccount struct {
	Type         string `json:"type"`
	ProjectID    string `json:"project_id"`
	PrivateKeyID string `json:"private_key_id"`
	PrivateKey   string `json:"private_key"`
	ClientEmail  string `json:"client_email"`
	TokenURI     string `json:"token_uri"`
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
	projectIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{4,61}[a-z0-9]$`)
)

type provider struct {
	connector.Adapter
	transport connector.Transport
}

func New(transport connector.Transport) (connector.Adapter, error) {
	if transport == nil {
		return nil, errors.New("FCM HTTP transport is required")
	}
	p := &provider{transport: transport}
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
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0",
		ConfigFields: []connector.ConfigField{
			{Key: "project_id", Name: "Firebase target project ID", Type: connector.ConfigFieldText, Required: true, Validation: connector.ConfigValidation{MaxLength: 63}},
			{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`15`), Validation: connector.ConfigValidation{Min: &minTimeout, Max: &maxTimeout}},
			{Key: "ttl_seconds", Name: "Default TTL seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`3600`), Validation: connector.ConfigValidation{Min: &minTTL, Max: &maxTTL}},
		},
		SecretFields: []connector.SecretField{{Key: "service_account_json", Name: "Firebase service account JSON", Required: true, CredentialKind: connector.SecretCredentialServiceAccount, MaterialFormat: connector.SecretMaterialJSONObject, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound}},
	}
}

func (*provider) ValidateConfig(connection connector.Connection) error {
	projectID := setting(connection, "project_id")
	timeoutSeconds := integer(connection.Config["timeout_seconds"], 15)
	ttlSeconds := integer(connection.Config["ttl_seconds"], 3600)
	if !projectIDPattern.MatchString(projectID) || timeoutSeconds < 1 || timeoutSeconds > 60 || ttlSeconds < 0 || ttlSeconds > 2419200 {
		return permanent("config_invalid", "valid Firebase project, timeout and TTL settings are required")
	}
	return nil
}

func (p *provider) test(ctx context.Context, request connector.TypedRequest[struct{}]) (connector.TypedResult[map[string]any], error) {
	if err := p.ValidateConfig(request.Connection); err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	account, key, err := parseServiceAccount(request.Secrets["service_account_json"])
	if err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	if _, err := p.accessToken(ctx, request.Connection, account, key); err != nil {
		return connector.TypedResult[map[string]any]{}, err
	}
	return connector.TypedResult[map[string]any]{Output: map[string]any{"provider_status": "authenticated"}, ResponseRef: "fcm:authenticated"}, nil
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
	if err := validateInput(request.Input, integer(request.Connection.Config["ttl_seconds"], 3600)); err != nil {
		return connector.DeliveryResult{}, err
	}
	account, key, err := parseServiceAccount(request.Secrets["service_account_json"])
	if err != nil {
		return connector.DeliveryResult{}, err
	}
	accessToken, err := p.accessToken(ctx, request.Connection, account, key)
	if err != nil {
		return connector.DeliveryResult{}, err
	}
	ttl := request.Input.TTLSeconds
	if ttl == 0 {
		ttl = integer(request.Connection.Config["ttl_seconds"], 3600)
	}
	message := map[string]any{
		"token":        request.Input.DeviceToken,
		"notification": map[string]string{"title": request.Input.Title, "body": request.Input.Body},
		"android":      map[string]any{"priority": strings.ToUpper(priority(request.Input.Priority)), "ttl": strconv.Itoa(ttl) + "s"},
	}
	if len(request.Input.Data) > 0 {
		message["data"] = request.Input.Data
	}
	if request.Input.CollapseKey != "" {
		message["android"].(map[string]any)["collapse_key"] = request.Input.CollapseKey
	}
	body, err := json.Marshal(map[string]any{"message": message})
	if err != nil || len(body) > 4096 {
		return connector.DeliveryResult{}, permanent("payload_too_large", "FCM payload exceeds 4096 bytes")
	}
	timeout := time.Duration(integer(request.Connection.Config["timeout_seconds"], 15)) * time.Second
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{
		Method:        http.MethodPost,
		URL:           "https://fcm.googleapis.com/v1/projects/" + url.PathEscape(setting(request.Connection, "project_id")) + "/messages:send",
		Headers:       map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/json"}},
		SecretHeaders: map[string][]string{"Authorization": {"Bearer " + accessToken}},
		Body:          body, MaxResponseBytes: responseLimit, Timeout: timeout,
	})
	if transportErr != nil {
		return connector.DeliveryResult{}, connector.UncertainError("fcm.delivery_unknown", transportErr)
	}
	responseRef := "http:" + strconv.Itoa(response.StatusCode)
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		var receipt struct {
			Name string `json:"name"`
		}
		if json.Unmarshal(response.Body, &receipt) != nil || strings.TrimSpace(receipt.Name) == "" {
			return connector.DeliveryResult{ResponseRef: responseRef}, connector.UncertainError("fcm.receipt_invalid", errors.New("FCM acceptance receipt is invalid"))
		}
		return connector.DeliveryResult{ResponseRef: receipt.Name}, nil
	}
	if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
		return connector.DeliveryResult{ResponseRef: responseRef}, connector.RetryableError("fcm.provider_unavailable", fmt.Errorf("FCM returned HTTP %d", response.StatusCode))
	}
	return connector.DeliveryResult{ResponseRef: responseRef}, permanent("request_rejected", fmt.Sprintf("FCM rejected the request with HTTP %d", response.StatusCode))
}

func (p *provider) accessToken(ctx context.Context, connection connector.Connection, account serviceAccount, key *rsa.PrivateKey) (string, error) {
	now := time.Now().UTC()
	assertion, err := signJWT(account, key, now)
	if err != nil {
		return "", permanent("service_account_invalid", "Firebase service account could not sign an assertion")
	}
	publicForm := url.Values{"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"}}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{
		Method: http.MethodPost, URL: googleTokenURL,
		Headers: map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/x-www-form-urlencoded"}},
		Body:    []byte(publicForm.Encode()), SecretForm: map[string]string{"assertion": assertion}, MaxResponseBytes: responseLimit,
		Timeout: time.Duration(integer(connection.Config["timeout_seconds"], 15)) * time.Second,
	})
	if transportErr != nil {
		return "", connector.RetryableError("fcm.oauth_unavailable", transportErr)
	}
	if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
		return "", connector.RetryableError("fcm.oauth_unavailable", fmt.Errorf("Google OAuth returned HTTP %d", response.StatusCode))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", permanent("service_account_rejected", "Google OAuth rejected the Firebase service account")
	}
	var token struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
	}
	if json.Unmarshal(response.Body, &token) != nil || strings.TrimSpace(token.AccessToken) == "" || !strings.EqualFold(token.TokenType, "Bearer") {
		return "", permanent("oauth_response_invalid", "Google OAuth response is invalid")
	}
	return strings.TrimSpace(token.AccessToken), nil
}

func parseServiceAccount(raw string) (serviceAccount, *rsa.PrivateKey, error) {
	var account serviceAccount
	if len(raw) == 0 || len(raw) > 64<<10 || json.Unmarshal([]byte(raw), &account) != nil || account.Type != "service_account" || strings.TrimSpace(account.ClientEmail) == "" || strings.TrimSpace(account.PrivateKeyID) == "" || account.TokenURI != googleTokenURL {
		return serviceAccount{}, nil, permanent("service_account_invalid", "valid Firebase service account JSON is required")
	}
	block, _ := pem.Decode([]byte(account.PrivateKey))
	if block == nil {
		return serviceAccount{}, nil, permanent("service_account_invalid", "Firebase private key is invalid")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return serviceAccount{}, nil, permanent("service_account_invalid", "Firebase private key is invalid")
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok || key.N.BitLen() < 2048 {
		return serviceAccount{}, nil, permanent("service_account_invalid", "Firebase private key must be RSA 2048-bit or stronger")
	}
	return account, key, nil
}

func signJWT(account serviceAccount, key *rsa.PrivateKey, now time.Time) (string, error) {
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT", "kid": account.PrivateKeyID})
	claims, _ := json.Marshal(map[string]any{"iss": account.ClientEmail, "scope": fcmScope, "aud": googleTokenURL, "iat": now.Unix(), "exp": now.Add(time.Hour).Unix()})
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		return "", err
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func validateInput(input SendInput, defaultTTL int) error {
	ttl := input.TTLSeconds
	if ttl == 0 {
		ttl = defaultTTL
	}
	if strings.TrimSpace(input.DeviceToken) == "" || len(input.DeviceToken) > 4096 || strings.ContainsAny(input.DeviceToken, "\r\n\t ") || strings.TrimSpace(input.Title) == "" || len(input.Title) > 200 || strings.TrimSpace(input.Body) == "" || len(input.Body) > 2000 || len(input.CollapseKey) > 64 || len(input.Data) > 100 || ttl < 0 || ttl > 2419200 || (priority(input.Priority) != "high" && priority(input.Priority) != "normal") {
		return permanent("message_invalid", "valid FCM token, message, priority and TTL are required")
	}
	for key, value := range input.Data {
		if strings.TrimSpace(key) == "" || len(key) > 128 || len(value) > 2048 {
			return permanent("message_invalid", "FCM data keys and values must be bounded")
		}
	}
	return nil
}

func priority(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return "high"
	}
	return value
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
	return connector.PermanentError("fcm."+suffix, errors.New(message))
}

var _ connector.ConfigValidator = (*provider)(nil)
var _ connector.ConnectionTester = (*provider)(nil)
