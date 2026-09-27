package google

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

const defaultGoogleJWKSURL = "https://www.googleapis.com/oauth2/v3/certs"

func (p *provider) VerifyWebhook(ctx context.Context, request connector.VerifyWebhookRequest) (connector.VerifiedWebhook, error) {
	if request.Connection.ConnectorKey != ConnectorKey || request.Connection.ProviderKey != ProviderKey {
		return connector.VerifiedWebhook{}, permanent("gmail.push_provider_mismatch", "Gmail push provider mismatch")
	}
	audience := backgroundConfig(request.Connection.Config, "gmail_pubsub_oidc_audience", "")
	serviceAccount := strings.ToLower(backgroundConfig(request.Connection.Config, "gmail_pubsub_service_account", ""))
	subscription := backgroundConfig(request.Connection.Config, "gmail_pubsub_subscription", "")
	if audience == "" || serviceAccount == "" || subscription == "" {
		return connector.VerifiedWebhook{}, permanent("gmail.push_config_missing", "Gmail Pub/Sub push verification configuration is incomplete")
	}
	token, err := bearerToken(request.Headers)
	if err != nil {
		return connector.VerifiedWebhook{}, err
	}
	jwksURL := backgroundConfig(request.Connection.Config, "gmail_pubsub_jwks_url", defaultGoogleJWKSURL)
	keys, err := p.googlePushKeys(ctx, jwksURL, request.ReceivedAt, false)
	if err != nil {
		return connector.VerifiedWebhook{}, err
	}
	claims, err := verifyGooglePushJWT(token, keys, audience, serviceAccount, request.ReceivedAt)
	if code, ok := connector.ProviderErrorCodeOf(err); ok && strings.HasSuffix(code, "gmail.push_key_unknown") {
		keys, err = p.googlePushKeys(ctx, jwksURL, request.ReceivedAt, true)
		if err == nil {
			claims, err = verifyGooglePushJWT(token, keys, audience, serviceAccount, request.ReceivedAt)
		}
	}
	if err != nil {
		return connector.VerifiedWebhook{}, err
	}
	var push struct {
		Message struct {
			Data        string            `json:"data"`
			MessageID   string            `json:"messageId"`
			PublishTime string            `json:"publishTime"`
			OrderingKey string            `json:"orderingKey,omitempty"`
			Attributes  map[string]string `json:"attributes,omitempty"`
		} `json:"message"`
		Subscription    string `json:"subscription"`
		DeliveryAttempt int    `json:"deliveryAttempt,omitempty"`
	}
	if err = strictBackgroundJSON(request.Body, &push); err != nil || strings.TrimSpace(push.Message.MessageID) == "" || len(push.Message.MessageID) > 1024 || !matchesGoogleSubscription(push.Subscription, subscription) {
		return connector.VerifiedWebhook{}, permanent("gmail.push_envelope_invalid", "Gmail Pub/Sub push envelope is invalid")
	}
	data, err := base64.StdEncoding.DecodeString(push.Message.Data)
	if err != nil {
		data, err = base64.RawStdEncoding.DecodeString(push.Message.Data)
	}
	var notification struct {
		EmailAddress string `json:"emailAddress"`
		HistoryID    string `json:"historyId"`
	}
	if err != nil || strictBackgroundJSON(data, &notification) != nil {
		return connector.VerifiedWebhook{}, permanent("gmail.push_data_invalid", "Gmail push notification is invalid")
	}
	email := strings.ToLower(strings.TrimSpace(notification.EmailAddress))
	historyID := strings.TrimSpace(notification.HistoryID)
	if !validGoogleAccountEmail(email) || historyID == "" {
		return connector.VerifiedWebhook{}, permanent("gmail.push_data_invalid", "Gmail push notification account or history is invalid")
	}
	payload, _ := json.Marshal(map[string]any{
		"routes":     []map[string]string{{"kind": "email", "value": email}},
		"wake_tasks": []string{gmailSyncTaskKey},
		"data":       map[string]string{"history_id": historyID},
	})
	eventTime, _ := time.Parse(time.RFC3339Nano, strings.TrimSpace(push.Message.PublishTime))
	return connector.VerifiedWebhook{
		EventType:  "gmail.notification",
		ExternalID: "gmail-push:" + strings.TrimSpace(push.Message.MessageID),
		Payload:    payload,
		Security: &connector.WebhookSecurityEvidence{
			SignatureVerified: true, Nonce: strings.TrimSpace(push.Message.MessageID), DeviceIdentity: strings.ToLower(strings.TrimSpace(fmt.Sprint(claims["email"]))), EventTime: eventTime,
		},
	}, nil
}

func bearerToken(headers map[string][]string) (string, error) {
	values := []string{}
	for key, current := range headers {
		if strings.EqualFold(strings.TrimSpace(key), "Authorization") {
			values = append(values, current...)
		}
	}
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
		return "", permanent("gmail.push_token_missing", "Gmail Pub/Sub push OIDC token is missing")
	}
	token := strings.TrimSpace(strings.TrimPrefix(values[0], "Bearer "))
	if token == "" || len(token) > 16384 {
		return "", permanent("gmail.push_token_invalid", "Gmail Pub/Sub push OIDC token is invalid")
	}
	return token, nil
}

func matchesGoogleSubscription(actual, expected string) bool {
	actual, expected = strings.TrimSpace(actual), strings.TrimSpace(expected)
	return actual == expected || strings.HasSuffix(actual, "/subscriptions/"+expected)
}

func (p *provider) googlePushKeys(ctx context.Context, endpoint string, now time.Time, forceRefresh bool) (map[string]*rsa.PublicKey, error) {
	p.pushMu.Lock()
	defer p.pushMu.Unlock()
	if !forceRefresh && endpoint == p.pushJWKS && len(p.pushKeys) != 0 && now.Before(p.pushExpiry) {
		return p.pushKeys, nil
	}
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" || (!(parsed.Scheme == "http" && loopback(parsed.Hostname())) && (parsed.Scheme != "https" || parsed.Hostname() != "www.googleapis.com")) {
		return nil, permanent("gmail.push_jwks_url_invalid", "Google OIDC JWKS endpoint is invalid")
	}
	response, transportErr := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{Method: http.MethodGet, URL: parsed.String(), Headers: map[string][]string{"Accept": {"application/json"}}, MaxResponseBytes: 1 << 20})
	if transportErr != nil || response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, connector.RetryableError("gmail.push_jwks_unavailable", fmt.Errorf("Google OIDC JWKS is unavailable"))
	}
	var document struct {
		Keys []struct{ Kid, Kty, Alg, N, E string } `json:"keys"`
	}
	if json.Unmarshal(response.Body, &document) != nil {
		return nil, permanent("gmail.push_jwks_invalid", "Google OIDC JWKS document is invalid")
	}
	keys := map[string]*rsa.PublicKey{}
	for _, key := range document.Keys {
		modulus, modulusErr := base64.RawURLEncoding.DecodeString(key.N)
		exponent, exponentErr := base64.RawURLEncoding.DecodeString(key.E)
		if key.Kid == "" || key.Kty != "RSA" || key.Alg != "RS256" || modulusErr != nil || exponentErr != nil || len(modulus) == 0 || len(exponent) == 0 || len(exponent) > 4 {
			continue
		}
		e := 0
		for _, value := range exponent {
			e = e<<8 + int(value)
		}
		if e >= 3 {
			keys[key.Kid] = &rsa.PublicKey{N: new(big.Int).SetBytes(modulus), E: e}
		}
	}
	if len(keys) == 0 {
		return nil, permanent("gmail.push_jwks_invalid", "Google OIDC JWKS contains no usable signing key")
	}
	ttl := 10 * time.Minute
	for key, values := range response.Headers {
		if !strings.EqualFold(key, "Cache-Control") {
			continue
		}
		for _, value := range values {
			for _, directive := range strings.Split(value, ",") {
				name, raw, found := strings.Cut(strings.TrimSpace(directive), "=")
				if found && strings.EqualFold(name, "max-age") {
					if seconds, parseErr := strconv.Atoi(strings.TrimSpace(raw)); parseErr == nil && seconds > 0 && seconds <= 86400 {
						ttl = time.Duration(seconds) * time.Second
					}
				}
			}
		}
	}
	p.pushKeys, p.pushJWKS, p.pushExpiry = keys, endpoint, now.Add(ttl)
	return keys, nil
}

func verifyGooglePushJWT(token string, keys map[string]*rsa.PublicKey, audience, serviceAccount string, now time.Time) (map[string]any, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, permanent("gmail.push_token_invalid", "Google OIDC token is malformed")
	}
	var header, claims map[string]any
	headerRaw, headerErr := base64.RawURLEncoding.DecodeString(parts[0])
	claimsRaw, claimsErr := base64.RawURLEncoding.DecodeString(parts[1])
	signature, signatureErr := base64.RawURLEncoding.DecodeString(parts[2])
	if headerErr != nil || claimsErr != nil || signatureErr != nil || json.Unmarshal(headerRaw, &header) != nil || json.Unmarshal(claimsRaw, &claims) != nil || fmt.Sprint(header["alg"]) != "RS256" {
		return nil, permanent("gmail.push_token_invalid", "Google OIDC token is invalid")
	}
	key := keys[strings.TrimSpace(fmt.Sprint(header["kid"]))]
	if key == nil {
		return nil, permanent("gmail.push_key_unknown", "Google OIDC signing key is unknown")
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	issuer := strings.TrimSpace(fmt.Sprint(claims["iss"]))
	email := strings.ToLower(strings.TrimSpace(fmt.Sprint(claims["email"])))
	verified, _ := claims["email_verified"].(bool)
	expires, expiresOK := numericJWTClaim(claims["exp"])
	issued, issuedOK := numericJWTClaim(claims["iat"])
	if rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature) != nil || !jwtAudienceContains(claims["aud"], audience) || (issuer != "https://accounts.google.com" && issuer != "accounts.google.com") || email != serviceAccount || !verified || !expiresOK || !now.Before(time.Unix(expires, 0)) || !issuedOK || time.Unix(issued, 0).After(now.Add(5*time.Minute)) {
		return nil, permanent("gmail.push_token_invalid", "Google OIDC token signature or claims are invalid")
	}
	return claims, nil
}

func numericJWTClaim(value any) (int64, bool) {
	switch typed := value.(type) {
	case float64:
		return int64(typed), typed == float64(int64(typed))
	case json.Number:
		value, err := typed.Int64()
		return value, err == nil
	default:
		return 0, false
	}
}

func jwtAudienceContains(value any, expected string) bool {
	switch typed := value.(type) {
	case string:
		return typed == expected
	case []any:
		for _, item := range typed {
			if text, ok := item.(string); ok && text == expected {
				return true
			}
		}
	}
	return false
}

var _ connector.WebhookVerifier = (*provider)(nil)
