package microsoftteams

import (
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
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

type recordingTransport struct {
	requests  []connector.HTTPRequest
	responses []connector.HTTPResponse
	errors    []error
}

func (t *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.requests = append(t.requests, request)
	i := len(t.requests) - 1
	var response connector.HTTPResponse
	if i < len(t.responses) {
		response = t.responses[i]
	}
	var err error
	if i < len(t.errors) {
		err = t.errors[i]
	}
	return response, err
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func identity() Identity {
	return Identity{ConnectorKey: "collaboration", ProviderKey: "teams", ProviderName: "Microsoft Teams", SendContractSHA256: "c8e3420b4832306a7f1ea9beec5ae3694c0a795a6a3f8bb28a568a8fb6d501cb", TestContractSHA256: "a354f65a655c7afc141199c44f831137989741466e5c064d4a872fdb8dec4916"}
}
func connection(endpoint string) connector.Connection {
	return connector.Connection{Config: map[string]any{"target_type": "chat", "graph_base_url": endpoint, "timeout_seconds": 15}}
}

func TestGraphMessagingSecretAndEndpointBoundaries(t *testing.T) {
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: 200, Body: []byte(`{"id":"user-1","displayName":"Alice"}`)}, {StatusCode: 201, Body: []byte(`{"id":"message-1"}`)}}}
	adapter, err := New(transport, identity())
	if err != nil {
		t.Fatal(err)
	}
	validator := adapter.(connector.ConfigValidator)
	for _, endpoint := range []string{defaultGraphBase, "http://localhost:8080"} {
		if err = validator.ValidateConfig(connection(endpoint)); err != nil {
			t.Fatalf("valid=%s err=%v", endpoint, err)
		}
	}
	for _, endpoint := range []string{"https://graph.microsoft.com", "https://graph.microsoft.com/beta", "https://graph.microsoft.com/v1.0/custom", "https://graph.microsoft.com.evil.test/v1.0", "http://graph.microsoft.com/v1.0"} {
		if err = validator.ValidateConfig(connection(endpoint)); err == nil {
			t.Fatalf("invalid accepted=%s", endpoint)
		}
	}
	_, err = adapter.Call(t.Context(), call("test_connection", identity().TestContractSHA256, connector.ModeCall, false, struct{}{}, connection("http://localhost:8080")))
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := adapter.Call(t.Context(), call("send_message", identity().SendContractSHA256, connector.ModeEnqueue, true, SendMessageInput{Recipient: "chat/one", Message: "Approved"}, connection("http://localhost:8080")))
	if err != nil || delivery.ResponseRef != "teams:message-1" {
		t.Fatalf("delivery=%+v err=%v", delivery, err)
	}
	if transport.requests[1].URL != "http://localhost:8080/chats/chat%2Fone/messages" {
		t.Fatalf("url=%s", transport.requests[1].URL)
	}
	for _, request := range transport.requests {
		if request.SecretHeaders["Authorization"][0] != "Bearer delegated-token" || strings.Contains(request.URL+string(request.Body), "delegated-token") {
			t.Fatalf("secret boundary=%+v", request)
		}
	}
}

func TestGraphRichNotificationValidatesJWTAndDecryptsResource(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	kid, audience, issuer := "key-1", "app-id", "https://issuer.example/tenant"
	token := signedJWT(t, privateKey, kid, map[string]any{"aud": audience, "iss": issuer, "exp": time.Now().Add(time.Hour).Unix()})
	encrypted := encryptedResource(t, &privateKey.PublicKey, []byte(`{"id":"message-1"}`), "cert-1")
	body, _ := json.Marshal(map[string]any{"validationTokens": []any{token}, "value": []any{map[string]any{"subscriptionId": "sub-1", "id": "notification-1", "clientState": "state", "changeType": "created", "resource": "chats/1/messages/1", "encryptedContent": encrypted}}})
	jwks, _ := json.Marshal(map[string]any{"keys": []any{map[string]any{"kid": kid, "kty": "RSA", "n": base64.RawURLEncoding.EncodeToString(privateKey.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(privateKey.E)).Bytes())}}})
	transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: 200, Body: jwks}}}
	adapter, _ := New(transport, identity())
	privatePEM := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)}))
	request := connector.VerifyWebhookRequest{Connection: connector.Connection{Config: map[string]any{"target_type": "chat", "graph_base_url": "http://localhost:8080", "notification_jwks_url": "http://localhost/jwks", "notification_audience": audience, "notification_issuer": issuer, "encryption_certificate_id": "cert-1"}}, Secrets: map[string]string{"client_state": "state", "encryption_private_key": privatePEM}, Body: body}
	event, err := adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), request)
	if err != nil || event.EventType != "created" || event.Security == nil || !event.Security.SignatureVerified || strings.Contains(string(event.Payload), "encryptedContent") || !strings.Contains(string(event.Payload), "resourceData") {
		t.Fatalf("event=%+v err=%v", event, err)
	}
	if len(transport.requests) != 1 || transport.requests[0].URL != "http://localhost/jwks" {
		t.Fatalf("requests=%+v", transport.requests)
	}
	request.Body = []byte(`{"value":[{"id":"one","resource":"r","clientState":"wrong"}]}`)
	if _, err = adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), request); err == nil {
		t.Fatal("wrong client state accepted")
	}
}

func TestWebhookChallengeAndWriteFailureClassification(t *testing.T) {
	adapter, _ := New(&recordingTransport{}, identity())
	challenge, err := adapter.(connector.WebhookVerifier).VerifyWebhook(t.Context(), connector.VerifyWebhookRequest{Query: map[string][]string{"validationToken": {"token"}}})
	if err != nil || challenge.Challenge != "token" || challenge.ChallengeFormat != "text/plain" {
		t.Fatalf("challenge=%+v err=%v", challenge, err)
	}
	for _, test := range []struct {
		name         string
		response     connector.HTTPResponse
		transportErr error
		want         connector.ErrorClassification
	}{{"network", connector.HTTPResponse{}, errors.New("reset"), connector.ErrorUncertain}, {"server", connector.HTTPResponse{StatusCode: 502}, nil, connector.ErrorUncertain}, {"rate", connector.HTTPResponse{StatusCode: 429}, nil, connector.ErrorRetryable}, {"reject", connector.HTTPResponse{StatusCode: 403}, nil, connector.ErrorPermanent}, {"invalid", connector.HTTPResponse{StatusCode: 201, Body: []byte(`{`)}, nil, connector.ErrorUncertain}, {"missing id", connector.HTTPResponse{StatusCode: 201, Body: []byte(`{}`)}, nil, connector.ErrorUncertain}} {
		t.Run(test.name, func(t *testing.T) {
			current, _ := New(&recordingTransport{responses: []connector.HTTPResponse{test.response}, errors: []error{test.transportErr}}, identity())
			_, callErr := current.Call(t.Context(), call("send_message", identity().SendContractSHA256, connector.ModeEnqueue, true, SendMessageInput{Recipient: "chat", Message: "hello"}, connection("http://localhost:8080")))
			classification, ok := connector.ErrorClassificationOf(callErr)
			if !ok || classification != test.want {
				t.Fatalf("err=%v class=%q want=%q", callErr, classification, test.want)
			}
		})
	}
}

func call(key, hash string, mode connector.OperationMode, delivery bool, payload any, connection connector.Connection) connector.CallRequest {
	raw, _ := json.Marshal(payload)
	return connector.CallRequest{ConnectorKey: "collaboration", ProviderKey: "teams", OperationKey: key, ContractSHA256: hash, Mode: mode, Delivery: delivery, Connection: connection, Secrets: map[string]string{"access_token": "delegated-token"}, Payload: raw}
}
func signedJWT(t *testing.T, key *rsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	header, _ := json.Marshal(map[string]any{"alg": "RS256", "kid": kid, "typ": "JWT"})
	payload, _ := json.Marshal(claims)
	encoded := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(encoded))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return encoded + "." + base64.RawURLEncoding.EncodeToString(signature)
}
func encryptedResource(t *testing.T, key *rsa.PublicKey, plain []byte, certificateID string) map[string]any {
	t.Helper()
	symmetric := bytesRepeat(7, 32)
	padding := aes.BlockSize - len(plain)%aes.BlockSize
	plain = append(plain, bytesRepeat(byte(padding), padding)...)
	block, _ := aes.NewCipher(symmetric)
	ciphertext := make([]byte, len(plain))
	cipher.NewCBCEncrypter(block, symmetric[:aes.BlockSize]).CryptBlocks(ciphertext, plain)
	encryptedKey, err := rsa.EncryptOAEP(sha1.New(), rand.Reader, key, symmetric, nil)
	if err != nil {
		t.Fatal(err)
	}
	mac := hmac.New(sha256.New, symmetric)
	_, _ = mac.Write(ciphertext)
	return map[string]any{"data": base64.StdEncoding.EncodeToString(ciphertext), "dataKey": base64.StdEncoding.EncodeToString(encryptedKey), "dataSignature": base64.StdEncoding.EncodeToString(mac.Sum(nil)), "encryptionCertificateId": certificateID}
}
func bytesRepeat(value byte, count int) []byte {
	result := make([]byte, count)
	for i := range result {
		result[i] = value
	}
	return result
}
