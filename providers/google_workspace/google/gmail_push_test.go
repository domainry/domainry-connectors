package google

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"strings"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

func TestGmailPushVerifiesOIDCAndNormalizesAccountRoute(t *testing.T) {
	now := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	key := mustPushKey(t)
	transport := &recordingTransport{respond: func(request connector.HTTPRequest) (connector.HTTPResponse, error) {
		if !strings.HasSuffix(request.URL, "/jwks") {
			t.Fatalf("unexpected request URL %q", request.URL)
		}
		return connector.HTTPResponse{StatusCode: 200, Headers: map[string][]string{"Cache-Control": {"max-age=3600"}}, Body: pushJWKS(t, "current", &key.PublicKey)}, nil
	}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	request := pushWebhookRequest(t, now, pushJWT(t, key, "current", now, "https://crm.example/hooks/google", "pubsub@example.test"), "message-1")
	verified, err := adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), request)
	if err != nil || verified.ExternalID != "gmail-push:message-1" || verified.Security == nil || !verified.Security.SignatureVerified || verified.Security.DeviceIdentity != "pubsub@example.test" {
		t.Fatalf("verified=%#v err=%v", verified, err)
	}
	var payload struct {
		Routes []struct {
			Kind  string `json:"kind"`
			Value string `json:"value"`
		} `json:"routes"`
		WakeTasks []string          `json:"wake_tasks"`
		Data      map[string]string `json:"data"`
	}
	if err = json.Unmarshal(verified.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Routes) != 1 || payload.Routes[0].Kind != "email" || payload.Routes[0].Value != "person@example.test" || len(payload.WakeTasks) != 1 || payload.WakeTasks[0] != gmailSyncTaskKey || payload.Data["history_id"] != "42" {
		t.Fatalf("payload=%s", verified.Payload)
	}
	if len(transport.requests) != 1 {
		t.Fatalf("JWKS requests=%d", len(transport.requests))
	}

	badAudience := pushWebhookRequest(t, now, pushJWT(t, key, "current", now, "https://wrong.example", "pubsub@example.test"), "message-2")
	if _, err = adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), badAudience); err == nil {
		t.Fatal("OIDC token with the wrong audience was accepted")
	}
	badServiceAccount := pushWebhookRequest(t, now, pushJWT(t, key, "current", now, "https://crm.example/hooks/google", "other@example.test"), "message-3")
	if _, err = adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), badServiceAccount); err == nil {
		t.Fatal("OIDC token from the wrong service account was accepted")
	}
}

func TestGmailPushRefreshesJWKSOnceForUnknownKey(t *testing.T) {
	now := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	oldKey, nextKey := mustPushKey(t), mustPushKey(t)
	responses := 0
	transport := &recordingTransport{respond: func(connector.HTTPRequest) (connector.HTTPResponse, error) {
		responses++
		if responses == 1 {
			return connector.HTTPResponse{StatusCode: 200, Headers: map[string][]string{"Cache-Control": {"max-age=3600"}}, Body: pushJWKS(t, "old", &oldKey.PublicKey)}, nil
		}
		return connector.HTTPResponse{StatusCode: 200, Headers: map[string][]string{"Cache-Control": {"max-age=3600"}}, Body: pushJWKS(t, "next", &nextKey.PublicKey)}, nil
	}}
	adapter, _ := New(transport)
	verifier := adapter.(connector.WebhookVerifier)
	if _, err := verifier.VerifyWebhook(t.Context(), pushWebhookRequest(t, now, pushJWT(t, oldKey, "old", now, "https://crm.example/hooks/google", "pubsub@example.test"), "message-old")); err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.VerifyWebhook(t.Context(), pushWebhookRequest(t, now, pushJWT(t, nextKey, "next", now, "https://crm.example/hooks/google", "pubsub@example.test"), "message-next")); err != nil {
		t.Fatal(err)
	}
	if responses != 2 {
		t.Fatalf("JWKS requests=%d, want cached read plus one forced refresh", responses)
	}
}

func pushWebhookRequest(t *testing.T, now time.Time, token, messageID string) connector.VerifyWebhookRequest {
	t.Helper()
	notification, _ := json.Marshal(map[string]string{"emailAddress": " Person@Example.Test ", "historyId": "42"})
	body, _ := json.Marshal(map[string]any{
		"message":         map[string]any{"data": base64.StdEncoding.EncodeToString(notification), "messageId": messageID, "publishTime": now.Format(time.RFC3339Nano)},
		"subscription":    "projects/project-a/subscriptions/gmail-push-00",
		"deliveryAttempt": 2,
	})
	connection := validConnection()
	connection.ConnectorKey, connection.ProviderKey = ConnectorKey, ProviderKey
	connection.Config["gmail_pubsub_oidc_audience"] = "https://crm.example/hooks/google"
	connection.Config["gmail_pubsub_service_account"] = "pubsub@example.test"
	connection.Config["gmail_pubsub_subscription"] = "gmail-push-00"
	connection.Config["gmail_pubsub_jwks_url"] = "http://127.0.0.1/jwks"
	return connector.VerifyWebhookRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Connection: connection, Headers: map[string][]string{"Authorization": {"Bearer " + token}}, Body: body, ReceivedAt: now}
}

func mustPushKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func pushJWT(t *testing.T, key *rsa.PrivateKey, kid string, now time.Time, audience, email string) string {
	t.Helper()
	header, _ := json.Marshal(map[string]any{"alg": "RS256", "kid": kid, "typ": "JWT"})
	claims, _ := json.Marshal(map[string]any{
		"iss": "https://accounts.google.com", "aud": audience, "email": email, "email_verified": true,
		"iat": now.Add(-time.Minute).Unix(), "exp": now.Add(time.Hour).Unix(),
	})
	encodedHeader := base64.RawURLEncoding.EncodeToString(header)
	encodedClaims := base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(encodedHeader + "." + encodedClaims))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return encodedHeader + "." + encodedClaims + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func pushJWKS(t *testing.T, kid string, key *rsa.PublicKey) []byte {
	t.Helper()
	exponent := big.NewInt(int64(key.E)).Bytes()
	payload, err := json.Marshal(map[string]any{"keys": []any{map[string]string{
		"kid": kid, "kty": "RSA", "alg": "RS256",
		"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(exponent),
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}
