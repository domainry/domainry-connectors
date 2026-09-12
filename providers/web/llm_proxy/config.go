package llmproxy

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/web"
)

type configuration struct {
	origin    string
	timeout   time.Duration
	processor string
	hosts     []string
}

func schema() connector.ProviderSchema {
	minimum, maximum := float64(1), float64(60)
	return connector.ProviderSchema{
		ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderRevision: "1.0.0",
		ConfigFields: []connector.ConfigField{
			{Key: "base_url", Name: "llm-proxy origin", Description: "Exact host-governed service origin; HTTPS or explicit loopback HTTP.", Type: connector.ConfigFieldText, Required: true, Validation: connector.ConfigValidation{MaxLength: 2048}},
			{Key: "allowed_source_hosts", Name: "Allowed source hosts", Description: "JSON array of 1 to 16 exact public DNS hosts. Restricts requested and returned sources; does not prove remote DNS or redirect policy.", Type: connector.ConfigFieldJSON, Required: true},
			{Key: "processor", Name: "Search processor", Type: connector.ConfigFieldSelect, Default: json.RawMessage(`"base"`), Validation: connector.ConfigValidation{Options: []string{"base", "pro"}}},
			{Key: "timeout_seconds", Name: "Timeout seconds", Type: connector.ConfigFieldInteger, Default: json.RawMessage(`40`), Validation: connector.ConfigValidation{Min: &minimum, Max: &maximum}},
		},
		SecretFields: []connector.SecretField{{
			Key: "api_token", Name: "llm-proxy Passport token", Required: true,
			Description:    "Raw service Passport token, injected privately by the deployment host.",
			CredentialKind: connector.SecretCredentialBearerToken, MaterialFormat: connector.SecretMaterialOpaque,
			RotationPolicy: connector.SecretRotationManual, ExpiryPolicy: connector.SecretExpiryOptional, TestRequirement: connector.SecretTestOptional,
		}},
	}
}

func settings(connection connector.Connection) (configuration, error) {
	c := configuration{timeout: 40 * time.Second, processor: "base"}
	base, ok := connection.Config["base_url"].(string)
	if !ok || base == "" || len(base) > 2048 || strings.ContainsAny(base, "\\#") || strings.ContainsFunc(base, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
		return c, permanent("endpoint_invalid", "an exact web service origin is required")
	}
	u, err := url.Parse(base)
	if err != nil || u.Hostname() == "" || u.Opaque != "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || (u.Path != "" && u.Path != "/") || u.RawPath != "" || strings.HasSuffix(u.Host, ":") {
		return c, permanent("endpoint_invalid", "base_url must contain only a web service origin")
	}
	loopback := u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"
	if u.Scheme != "https" && !(u.Scheme == "http" && loopback) {
		return c, permanent("endpoint_invalid", "HTTPS or explicit loopback HTTP is required")
	}
	if u.Port() != "" {
		port, e := strconv.Atoi(u.Port())
		if e != nil || port < 1 || port > 65535 {
			return c, permanent("endpoint_invalid", "invalid service port")
		}
	}
	c.origin = strings.TrimSuffix(u.String(), "/")
	if value, exists := connection.Config["timeout_seconds"]; exists {
		seconds, e := strconv.Atoi(fmt.Sprint(value))
		if e != nil || seconds < 1 || seconds > 60 {
			return c, permanent("timeout_invalid", "timeout_seconds must be an integer between 1 and 60")
		}
		c.timeout = time.Duration(seconds) * time.Second
	}
	if value, exists := connection.Config["processor"]; exists {
		v, valid := value.(string)
		if !valid || (v != "base" && v != "pro") {
			return c, permanent("processor_invalid", "invalid configured search processor")
		}
		c.processor = v
	}
	// Use the JSON data model so decoded []any and native []string have the
	// same strict behavior. JSON strings containing an array are not arrays.
	raw, err := json.Marshal(connection.Config["allowed_source_hosts"])
	if err != nil || len(raw) > 8192 || json.Unmarshal(raw, &c.hosts) != nil || len(c.hosts) < 1 || len(c.hosts) > 16 {
		return c, permanent("sources_invalid", "1 to 16 exact public DNS source hosts are required")
	}
	seen := map[string]bool{}
	for i, host := range c.hosts {
		host = strings.ToLower(host)
		normalized, e := web.NormalizeURL("https://" + host + "/")
		if e != nil || normalized != "https://"+host+"/" || strings.ContainsAny(host, "/:@?#[]") {
			return c, permanent("sources_invalid", "source policy requires exact public DNS hosts")
		}
		if _, e := netip.ParseAddr(host); e == nil || seen[host] {
			return c, permanent("sources_invalid", "source hosts must be unique DNS names")
		}
		seen[host], c.hosts[i] = true, host
	}
	sort.Strings(c.hosts)
	return c, nil
}

// allowedSource validates syntax and source authorization, with no DNS or I/O.
func (c configuration) allowedSource(raw string) (string, bool) {
	normalized, err := web.NormalizeURL(raw)
	if err != nil {
		return "", false
	}
	u, _ := url.Parse(normalized)
	i := sort.SearchStrings(c.hosts, u.Hostname())
	return normalized, i < len(c.hosts) && c.hosts[i] == u.Hostname()
}
