package composio

import (
	"bytes"
	"encoding/json"
	"net/url"
	"regexp"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

const defaultBaseURL = "https://backend.composio.dev"

var (
	aliasPattern   = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
	slugPattern    = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,191}$`)
	idPattern      = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.:@-]{0,255}$`)
	versionPattern = regexp.MustCompile(`^[0-9]{8}_[0-9]{2}$`)
)

type toolMapping struct {
	Slug    string `json:"slug"`
	Version string `json:"version"`
	Effect  string `json:"effect"`
}

type settings struct {
	BaseURL   string
	AccountID string
	UserID    string
	Toolkit   string
	Timeout   time.Duration
	Tools     map[string]toolMapping
}

func readSettings(c connector.Connection) (settings, error) {
	s := settings{BaseURL: defaultBaseURL, Timeout: 60 * time.Second}
	if value, exists := c.Config["base_url"]; exists {
		text, ok := value.(string)
		if !ok {
			return s, permanent("endpoint_invalid", "base_url must be a service origin")
		}
		s.BaseURL = text
	}
	u, err := url.Parse(s.BaseURL)
	if err != nil || u.Hostname() == "" || u.Opaque != "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(s.BaseURL, "#") || (u.Path != "" && u.Path != "/") || len(s.BaseURL) > 2048 {
		return s, permanent("endpoint_invalid", "base_url must contain only the Composio service origin")
	}
	loopback := u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"
	if u.Scheme != "https" && !(u.Scheme == "http" && loopback) {
		return s, permanent("endpoint_invalid", "HTTPS or loopback HTTP is required")
	}
	s.BaseURL = strings.TrimRight(s.BaseURL, "/")
	for key, target := range map[string]*string{"connected_account_id": &s.AccountID, "user_id": &s.UserID, "toolkit": &s.Toolkit} {
		value, ok := c.Config[key].(string)
		if !ok || !idPattern.MatchString(value) {
			return s, permanent("identity_invalid", "connected_account_id, user_id and toolkit must be explicit canonical identifiers")
		}
		*target = value
	}
	if !aliasPattern.MatchString(s.Toolkit) {
		return s, permanent("toolkit_invalid", "toolkit must be a canonical toolkit slug")
	}
	if value, exists := c.Config["timeout_seconds"]; exists {
		raw, err := json.Marshal(value)
		var seconds int
		if err != nil || json.Unmarshal(raw, &seconds) != nil || seconds < 1 || seconds > 300 {
			return s, permanent("timeout_invalid", "timeout_seconds must be an integer from 1 to 300")
		}
		s.Timeout = time.Duration(seconds) * time.Second
	}
	raw, err := json.Marshal(c.Config["tools"])
	if err != nil || len(raw) > 65536 {
		return s, permanent("tools_invalid", "tools must be a bounded object of explicit mappings")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&s.Tools) != nil || len(s.Tools) == 0 || len(s.Tools) > 64 {
		return s, permanent("tools_invalid", "tools must contain 1 to 64 explicit mappings")
	}
	seen := map[string]bool{}
	for alias, tool := range s.Tools {
		if !aliasPattern.MatchString(alias) || !slugPattern.MatchString(tool.Slug) || strings.HasPrefix(tool.Slug, "COMPOSIO_") || !versionPattern.MatchString(tool.Version) || (tool.Effect != "read" && tool.Effect != "write") {
			return s, permanent("mapping_invalid", "each mapping requires an alias, app tool slug, dated version and read/write effect; router meta tools are unsupported")
		}
		// A slug cannot be relabelled as both read and write through another alias.
		if seen[tool.Slug] {
			return s, permanent("mapping_duplicate", "a tool slug may have only one mapping per connection")
		}
		seen[tool.Slug] = true
	}
	return s, nil
}

func schema() connector.ProviderSchema {
	minimum, maximum := float64(1), float64(300)
	text := func(key, name, zh string, required bool, limit int) connector.ConfigField {
		return connector.ConfigField{Key: key, Name: name, Type: connector.ConfigFieldText, Required: required, Validation: connector.ConfigValidation{MaxLength: limit}, I18n: localized(name, zh)}
	}
	base := text("base_url", "Composio service origin", "Composio 服务地址", false, 2048)
	base.Default = json.RawMessage(`"https://backend.composio.dev"`)
	return connector.ProviderSchema{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0", ConfigFields: []connector.ConfigField{
		base,
		text("connected_account_id", "Connected account ID", "已连接账号 ID", true, 256),
		text("user_id", "Composio account owner ID", "Composio 账号所有者 ID", true, 256),
		text("toolkit", "Toolkit slug", "应用标识", true, 64),
		{Key: "tools", Name: "Allowed tool mappings", Type: connector.ConfigFieldJSON, Required: true, I18n: localized("Allowed tool mappings", "允许的工具映射")},
		{Key: "timeout_seconds", Name: "Total timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`60`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}, I18n: localized("Total timeout seconds", "总超时秒数")},
	}, SecretFields: []connector.SecretField{{Key: "api_key", Name: "Composio project API key", Required: true, CredentialKind: connector.SecretCredentialAPIKey, MaterialFormat: connector.SecretMaterialOpaque, RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestWhenBound, I18n: localized("Composio project API key", "Composio 项目 API Key")}}}
}

func localized(en, zh string) map[string]connector.FieldLocalization {
	return map[string]connector.FieldLocalization{"en-US": {Name: en}, "zh-CN": {Name: zh}}
}
