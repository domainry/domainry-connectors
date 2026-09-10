package composio

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
)

const (
	responseLimit = 4 << 20
	requestLimit  = 1 << 20
)

func (p *provider) account(ctx context.Context, s settings, secrets map[string]string) (ConnectionOutput, error) {
	response, err := p.request(ctx, s, secrets, http.MethodGet, "/connected_accounts/"+s.AccountID, nil, false)
	if err != nil {
		return ConnectionOutput{}, err
	}
	// Never project state, params, data or status_reason: they may contain credentials.
	var account struct {
		ID       string `json:"id"`
		UserID   string `json:"user_id"`
		Status   string `json:"status"`
		Disabled bool   `json:"is_disabled"`
		Toolkit  struct {
			Slug string `json:"slug"`
		} `json:"toolkit"`
		AuthConfig struct {
			Disabled bool `json:"is_disabled"`
		} `json:"auth_config"`
	}
	if json.Unmarshal(response.Body, &account) != nil || account.ID == "" || account.UserID == "" || account.Toolkit.Slug == "" || account.Status == "" {
		return ConnectionOutput{}, failure(false, "account_response_invalid")
	}
	if account.ID != s.AccountID || account.UserID != s.UserID || account.Toolkit.Slug != s.Toolkit {
		return ConnectionOutput{}, permanent("account_mismatch", "connected account does not match the configured owner and toolkit")
	}
	if account.Status != "ACTIVE" || account.Disabled || account.AuthConfig.Disabled {
		return ConnectionOutput{}, permanent("account_inactive", "connected account or auth config is inactive; reconnect in Composio")
	}
	return ConnectionOutput{Connected: true, ConnectedAccountID: account.ID, UserID: account.UserID, Toolkit: account.Toolkit.Slug}, nil
}

func (p *provider) executeTool(ctx context.Context, s settings, tool toolMapping, secrets map[string]string, arguments map[string]json.RawMessage) (connector.TypedResult[ToolOutput], error) {
	if arguments == nil {
		arguments = map[string]json.RawMessage{}
	}
	// Structured arguments only. No natural-language execution or caller auth overrides.
	body, err := json.Marshal(struct {
		AccountID string                     `json:"connected_account_id"`
		UserID    string                     `json:"user_id"`
		Version   string                     `json:"version"`
		Arguments map[string]json.RawMessage `json:"arguments"`
	}{s.AccountID, s.UserID, tool.Version, arguments})
	if err != nil || len(body) > requestLimit {
		return connector.TypedResult[ToolOutput]{}, permanent("input_invalid", "tool arguments must be valid JSON within the 1 MiB request limit")
	}
	write := tool.Effect == "write"
	response, err := p.request(ctx, s, secrets, http.MethodPost, "/tools/execute/"+tool.Slug, body, write)
	if err != nil {
		return connector.TypedResult[ToolOutput]{}, err
	}
	var envelope struct {
		Successful *bool           `json:"successful"`
		Data       json.RawMessage `json:"data"`
		LogID      string          `json:"log_id"`
	}
	if json.Unmarshal(response.Body, &envelope) != nil || envelope.Successful == nil {
		return connector.TypedResult[ToolOutput]{}, failure(write, "response_invalid")
	}
	ref := ""
	if idPattern.MatchString(envelope.LogID) {
		ref = "composio:log:" + envelope.LogID
	}
	if !*envelope.Successful {
		// An upstream tool may have partially executed before reporting failure.
		err := permanent("tool_failed", "Composio reported unsuccessful tool execution")
		if write {
			err = failure(true, "tool_outcome_unknown")
		}
		return connector.TypedResult[ToolOutput]{ResponseRef: ref}, err
	}
	if len(envelope.Data) == 0 || ref == "" {
		return connector.TypedResult[ToolOutput]{ResponseRef: ref}, failure(write, "response_invalid")
	}
	return connector.TypedResult[ToolOutput]{Output: ToolOutput{Data: envelope.Data, LogID: envelope.LogID}, ResponseRef: ref}, nil
}

func (p *provider) request(ctx context.Context, s settings, secrets map[string]string, method, path string, body []byte, write bool) (connector.HTTPResponse, error) {
	key := secrets["api_key"]
	if strings.TrimSpace(key) == "" || strings.TrimSpace(key) != key || strings.ContainsAny(key, "\r\n\x00") {
		return connector.HTTPResponse{}, permanent("api_key_required", "a valid Runtime-resolved Composio project API key is required")
	}
	if ctx.Err() != nil {
		return connector.HTTPResponse{}, connector.RetryableError("composio.before_dispatch_cancelled", errors.New("request cancelled before dispatch"))
	}
	response, err := p.transport.RoundTripHTTP(ctx, connector.HTTPRequest{
		Method: method, URL: s.BaseURL + "/api/v3.1" + path,
		Headers:       map[string][]string{"Accept": {"application/json"}, "Content-Type": {"application/json"}},
		SecretHeaders: map[string][]string{"x-api-key": {key}}, Body: body, MaxResponseBytes: responseLimit,
	})
	if err != nil {
		// Transport errors can embed credentials, URLs or payloads. Keep only a stable code.
		return connector.HTTPResponse{}, failure(write, "transport_failed")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code := fmt.Sprintf("http_%d", response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests {
			return connector.HTTPResponse{}, connector.RetryableError("composio."+code, errors.New("Composio rate limit exceeded"))
		}
		if response.StatusCode >= 500 || response.StatusCode == http.StatusRequestTimeout {
			return connector.HTTPResponse{}, failure(write, code)
		}
		return connector.HTTPResponse{}, permanent(code, "Composio rejected the request")
	}
	if len(response.Body) > responseLimit {
		return connector.HTTPResponse{}, failure(write, "response_too_large")
	}
	return response, nil
}

func failure(write bool, code string) error {
	if write {
		return connector.UncertainError("composio."+code, errors.New("Composio tool outcome is uncertain; do not automatically replay the write"))
	}
	return connector.RetryableError("composio."+code, errors.New("Composio read did not return a valid result"))
}
