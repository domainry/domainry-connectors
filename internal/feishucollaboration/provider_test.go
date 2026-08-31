package feishucollaboration

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

func TestVerifySignatureUsesFeishuProviderProtocol(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	body := []byte(`{"event":"created"}`)
	timestamp, nonce, secret := "1700000000", "nonce-1", "secret"
	sum := sha256.Sum256(append([]byte(timestamp+nonce+secret), body...))
	request := connector.VerifyWebhookRequest{
		Headers: map[string][]string{
			"x-lark-request-timestamp": {timestamp},
			"X-Lark-Request-Nonce":     {nonce},
			"X-Lark-Signature":         {strings.ToUpper(hex.EncodeToString(sum[:]))},
		},
		Body: body, ReceivedAt: now,
	}
	if err := verifySignature(request, secret); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*connector.VerifyWebhookRequest)
		code   string
	}{
		{name: "missing nonce", mutate: func(value *connector.VerifyWebhookRequest) { delete(value.Headers, "X-Lark-Request-Nonce") }, code: "feishu.webhook_signature_invalid"},
		{name: "expired", mutate: func(value *connector.VerifyWebhookRequest) { value.ReceivedAt = now.Add(301 * time.Second) }, code: "feishu.webhook_timestamp_invalid"},
		{name: "mismatch", mutate: func(value *connector.VerifyWebhookRequest) {
			value.Headers["X-Lark-Signature"] = []string{strings.Repeat("0", 64)}
		}, code: "feishu.webhook_signature_invalid"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := request
			candidate.Headers = cloneHeaders(request.Headers)
			test.mutate(&candidate)
			err := verifySignature(candidate, secret)
			if code, ok := connector.ProviderErrorCodeOf(err); !ok || code != test.code {
				t.Fatalf("error=%v code=%q, want %q", err, code, test.code)
			}
		})
	}
}

func cloneHeaders(source map[string][]string) map[string][]string {
	result := make(map[string][]string, len(source))
	for key, values := range source {
		result[key] = append([]string(nil), values...)
	}
	return result
}
