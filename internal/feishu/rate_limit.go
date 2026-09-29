package feishu

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
)

const RateLimitProviderCode = 99991400

// IsRateLimitedResponse recognizes both the current HTTP 429 protocol and the
// documented legacy HTTP 400 response carrying Feishu code 99991400.
func IsRateLimitedResponse(status int, body []byte) bool {
	if status == http.StatusTooManyRequests {
		return true
	}
	var payload struct {
		Code int `json:"code"`
	}
	return len(body) > 0 && json.Unmarshal(body, &payload) == nil && payload.Code == RateLimitProviderCode
}

func RateLimitError(prefix string) error {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = "feishu"
	}
	return connector.RetryableError(prefix+".rate_limit_exceeded", errors.New("Feishu rejected the request because its API frequency limit was reached"))
}
