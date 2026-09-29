package feishu

import (
	"net/http"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
)

func TestRateLimitProtocolRecognizesCurrentAndLegacyResponses(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{name: "current", status: http.StatusTooManyRequests, body: `{}`, want: true},
		{name: "legacy", status: http.StatusBadRequest, body: `{"code":99991400,"msg":"request trigger frequency limit"}`, want: true},
		{name: "provider code on success", status: http.StatusOK, body: `{"code":99991400}`, want: true},
		{name: "ordinary rejection", status: http.StatusBadRequest, body: `{"code":1002002}`},
		{name: "invalid", status: http.StatusBadRequest, body: `{`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := IsRateLimitedResponse(test.status, []byte(test.body)); got != test.want {
				t.Fatalf("IsRateLimitedResponse(%d)=%v want %v", test.status, got, test.want)
			}
		})
	}
	err := RateLimitError("feishu_calendar")
	if class, ok := connector.ErrorClassificationOf(err); !ok || class != connector.ErrorRetryable {
		t.Fatalf("classification=%q ok=%v", class, ok)
	}
	if code, ok := connector.ProviderErrorCodeOf(err); !ok || code != "feishu_calendar.rate_limit_exceeded" {
		t.Fatalf("code=%q ok=%v", code, ok)
	}
}
