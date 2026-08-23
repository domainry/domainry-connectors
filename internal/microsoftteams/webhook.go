package microsoftteams

import (
	"bytes"
	"context"
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

func (p *provider) VerifyWebhook(ctx context.Context, request connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	if err := ctx.Err(); err != nil {
		return connector.VerifiedWebhook{}, err
	}
	if token := queryValue(request.Query, "validationToken"); token != "" {
		return connector.VerifiedWebhook{EventType: "url_verification", ExternalID: "challenge:" + token, Challenge: token, ChallengeFormat: "text/plain"}, nil
	}
	clientState := strings.TrimSpace(request.Secrets["client_state"])
	if clientState == "" {
		return connector.VerifiedWebhook{}, permanent("webhook_client_state_required", "resolved Graph webhook client_state is required")
	}
	payload := Response{}
	if json.Unmarshal(request.Body, &payload) != nil {
		return connector.VerifiedWebhook{}, permanent("webhook_payload_invalid", "Graph webhook payload is invalid JSON")
	}
	values, ok := payload["value"].([]any)
	if !ok || len(values) == 0 {
		return connector.VerifiedWebhook{}, permanent("webhook_notifications_missing", "Graph webhook notifications are missing")
	}
	security, err := p.validateRichNotifications(ctx, request, payload, values)
	if err != nil {
		return connector.VerifiedWebhook{}, err
	}
	identities := make([]string, 0, len(values))
	eventTypes := make([]string, 0, len(values))
	for _, value := range values {
		notification, valid := value.(map[string]any)
		if !valid || !hmac.Equal([]byte(clientState), []byte(cleanValue(notification["clientState"]))) {
			return connector.VerifiedWebhook{}, permanent("webhook_client_state_invalid", "Graph webhook client state does not match")
		}
		identity := strings.Join([]string{cleanValue(notification["subscriptionId"]), cleanValue(notification["id"]), cleanValue(notification["resource"])}, ":")
		if strings.Trim(identity, ":") == "" {
			return connector.VerifiedWebhook{}, permanent("webhook_identity_missing", "Graph webhook notification identity is missing")
		}
		identities = append(identities, identity)
		eventTypes = append(eventTypes, cleanValue(notification["changeType"]))
	}
	sum := sha256.Sum256([]byte(strings.Join(identities, "\x00")))
	eventType := "change_notification"
	if len(eventTypes) == 1 && eventTypes[0] != "" {
		eventType = eventTypes[0]
	}
	normalized, err := json.Marshal(payload)
	if err != nil {
		return connector.VerifiedWebhook{}, permanent("webhook_payload_invalid", "Graph webhook payload cannot be normalized")
	}
	return connector.VerifiedWebhook{EventType: eventType, ExternalID: "graph:" + hex.EncodeToString(sum[:]), Payload: normalized, Security: security}, nil
}

func (p *provider) validateRichNotifications(ctx context.Context, request connector.VerifyWebhookRequest, payload map[string]any, notifications []any) (*connector.WebhookSecurityEvidence, error) {
	hasEncrypted := false
	for _, item := range notifications {
		if notification, ok := item.(map[string]any); ok && notification["encryptedContent"] != nil {
			hasEncrypted = true
		}
	}
	if !hasEncrypted && payload["validationTokens"] == nil {
		return nil, nil
	}
	if err := p.verifyValidationTokens(ctx, request, payload["validationTokens"]); err != nil {
		return nil, err
	}
	for _, item := range notifications {
		notification, ok := item.(map[string]any)
		if !ok || notification["encryptedContent"] == nil {
			continue
		}
		resource, err := decryptResourceData(request, notification["encryptedContent"])
		if err != nil {
			return nil, err
		}
		notification["resourceData"] = resource
		delete(notification, "encryptedContent")
	}
	return &connector.WebhookSecurityEvidence{SignatureVerified: true}, nil
}

func (p *provider) verifyValidationTokens(ctx context.Context, request connector.VerifyWebhookRequest, raw any) error {
	tokens := stringValues(raw)
	if len(tokens) == 0 {
		return permanent("validation_token_missing", "Graph validation token is missing")
	}
	endpoint, audience, issuer := config(request.Connection, "notification_jwks_url", ""), config(request.Connection, "notification_audience", ""), config(request.Connection, "notification_issuer", "")
	if endpoint == "" || audience == "" || issuer == "" {
		return permanent("validation_config_missing", "Graph notification validation configuration is incomplete")
	}
	keys, err := p.fetchJWKS(ctx, endpoint)
	if err != nil {
		return err
	}
	for _, token := range tokens {
		if err = verifyJWT(token, keys, audience, issuer, time.Now().UTC()); err != nil {
			return err
		}
	}
	return nil
}

func (p *provider) fetchJWKS(ctx context.Context, endpoint string) (map[string]*rsa.PublicKey, error) {
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil || parsed.Host == "" || parsed.User != nil || (!(parsed.Scheme == "http" && isLoopback(parsed.Hostname())) && (parsed.Scheme != "https" || !microsoftJWKSHost(parsed.Hostname()))) {
		return nil, permanent("jwks_endpoint_invalid", "official Microsoft JWKS endpoint or loopback HTTP is required")
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodGet, URL: parsed.String(), Headers: map[string][]string{"Accept": {"application/json"}}, MaxResponseBytes: 1 << 20})
	if transportErr != nil {
		return nil, connector.RetryableError("microsoft_teams.jwks_unavailable", transportErr)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, connector.RetryableError("microsoft_teams.jwks_unavailable", fmt.Errorf("JWKS returned HTTP %d", response.StatusCode))
	}
	var document struct {
		Keys []struct{ Kid, Kty, N, E string } `json:"keys"`
	}
	if json.Unmarshal(response.Body, &document) != nil {
		return nil, permanent("jwks_invalid", "Microsoft JWKS document is invalid")
	}
	result := map[string]*rsa.PublicKey{}
	for _, key := range document.Keys {
		modulus, modulusErr := base64.RawURLEncoding.DecodeString(key.N)
		exponent, exponentErr := base64.RawURLEncoding.DecodeString(key.E)
		if key.Kty != "RSA" || key.Kid == "" || modulusErr != nil || exponentErr != nil || len(modulus) == 0 || len(exponent) == 0 || len(exponent) > 4 {
			continue
		}
		e := 0
		for _, value := range exponent {
			e = e<<8 + int(value)
		}
		if e < 3 {
			continue
		}
		result[key.Kid] = &rsa.PublicKey{N: new(big.Int).SetBytes(modulus), E: e}
	}
	if len(result) == 0 {
		return nil, permanent("jwks_invalid", "Microsoft JWKS contains no usable RSA keys")
	}
	return result, nil
}

func verifyJWT(token string, keys map[string]*rsa.PublicKey, audience, issuer string, now time.Time) error {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return permanent("validation_token_invalid", "Graph validation token is malformed")
	}
	var header, claims map[string]any
	headerRaw, headerErr := base64.RawURLEncoding.DecodeString(parts[0])
	claimsRaw, claimsErr := base64.RawURLEncoding.DecodeString(parts[1])
	signature, signatureErr := base64.RawURLEncoding.DecodeString(parts[2])
	if headerErr != nil || claimsErr != nil || signatureErr != nil || json.Unmarshal(headerRaw, &header) != nil || json.Unmarshal(claimsRaw, &claims) != nil || cleanValue(header["alg"]) != "RS256" {
		return permanent("validation_token_invalid", "Graph validation token is invalid")
	}
	key := keys[cleanValue(header["kid"])]
	if key == nil {
		return permanent("validation_key_unknown", "Graph validation signing key is unknown")
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature) != nil || !claimContains(claims["aud"], audience) || cleanValue(claims["iss"]) != issuer {
		return permanent("validation_token_invalid", "Graph validation token signature or claims are invalid")
	}
	expires, err := jwtUnixClaim(claims["exp"])
	if err != nil || !now.Before(time.Unix(expires, 0)) {
		return permanent("validation_token_expired", "Graph validation token is expired")
	}
	return nil
}

func decryptResourceData(request connector.VerifyWebhookRequest, raw any) (map[string]any, error) {
	encrypted, ok := raw.(map[string]any)
	if !ok {
		return nil, permanent("encrypted_content_invalid", "Graph encrypted content is invalid")
	}
	expectedCertificate := config(request.Connection, "encryption_certificate_id", "")
	if expectedCertificate == "" || cleanValue(encrypted["encryptionCertificateId"]) != expectedCertificate {
		return nil, permanent("encryption_certificate_invalid", "Graph encryption certificate does not match")
	}
	privateKey, err := parseRSAPrivateKey(strings.TrimSpace(request.Secrets["encryption_private_key"]))
	if err != nil {
		return nil, permanent("encryption_key_invalid", "Graph encryption private key is invalid")
	}
	dataKey, keyErr := base64.StdEncoding.DecodeString(cleanValue(encrypted["dataKey"]))
	data, dataErr := base64.StdEncoding.DecodeString(cleanValue(encrypted["data"]))
	signature, signatureErr := base64.StdEncoding.DecodeString(cleanValue(encrypted["dataSignature"]))
	if keyErr != nil || dataErr != nil || signatureErr != nil {
		return nil, permanent("encrypted_content_invalid", "Graph encrypted fields are invalid")
	}
	symmetricKey, err := rsa.DecryptOAEP(sha1.New(), rand.Reader, privateKey, dataKey, nil)
	if err != nil || (len(symmetricKey) != 16 && len(symmetricKey) != 24 && len(symmetricKey) != 32) {
		return nil, permanent("encryption_key_invalid", "Graph encrypted data key is invalid")
	}
	mac := hmac.New(sha256.New, symmetricKey)
	_, _ = mac.Write(data)
	if !hmac.Equal(mac.Sum(nil), signature) {
		return nil, permanent("encrypted_signature_invalid", "Graph encrypted data signature does not match")
	}
	block, _ := aes.NewCipher(symmetricKey)
	if len(data) == 0 || len(data)%aes.BlockSize != 0 {
		return nil, permanent("encrypted_content_invalid", "Graph ciphertext length is invalid")
	}
	plain := make([]byte, len(data))
	cipher.NewCBCDecrypter(block, symmetricKey[:aes.BlockSize]).CryptBlocks(plain, data)
	plain, err = unpadPKCS7(plain)
	if err != nil {
		return nil, permanent("encrypted_content_invalid", "Graph plaintext padding is invalid")
	}
	result := map[string]any{}
	if json.Unmarshal(plain, &result) != nil {
		return nil, permanent("encrypted_content_invalid", "Graph decrypted resource is invalid JSON")
	}
	return result, nil
}

func parseRSAPrivateKey(value string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(value))
	if block == nil {
		return nil, errors.New("missing PEM")
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("private key is not RSA")
	}
	return key, nil
}

func unpadPKCS7(value []byte) ([]byte, error) {
	if len(value) == 0 {
		return nil, errors.New("empty plaintext")
	}
	padding := int(value[len(value)-1])
	if padding < 1 || padding > aes.BlockSize || padding > len(value) || !bytes.Equal(value[len(value)-padding:], bytes.Repeat([]byte{byte(padding)}, padding)) {
		return nil, errors.New("invalid padding")
	}
	return value[:len(value)-padding], nil
}

func queryValue(query map[string][]string, key string) string {
	for candidate, values := range query {
		if strings.EqualFold(strings.TrimSpace(candidate), key) && len(values) > 0 {
			return strings.TrimSpace(values[0])
		}
	}
	return ""
}
func cleanValue(value any) string {
	result := strings.TrimSpace(fmt.Sprint(value))
	if result == "<nil>" {
		return ""
	}
	return result
}
func stringValues(value any) []string {
	items, _ := value.([]any)
	result := make([]string, 0, len(items))
	for _, item := range items {
		if text := cleanValue(item); text != "" {
			result = append(result, text)
		}
	}
	return result
}
func claimContains(value any, expected string) bool {
	if cleanValue(value) == expected {
		return true
	}
	for _, item := range stringValues(value) {
		if item == expected {
			return true
		}
	}
	return false
}
func jwtUnixClaim(value any) (int64, error) {
	switch typed := value.(type) {
	case float64:
		return int64(typed), nil
	case json.Number:
		return typed.Int64()
	default:
		return strconv.ParseInt(cleanValue(value), 10, 64)
	}
}
func microsoftJWKSHost(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "login.microsoftonline.com"
}

var _ connector.WebhookVerifier = (*provider)(nil)
