package catalog

import (
	"encoding/json"
	"fmt"
	"math"
	"net/mail"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var connectorVersionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?$`)

func validateConnectorDefinition(path, directory string, connector ConnectorSchema) error {
	switch {
	case strings.TrimSpace(connector.Key) == "":
		return fmt.Errorf("Connector definition %s has no key", path)
	case !ValidConnectorIdentityKey(connector.Key):
		return fmt.Errorf("Connector definition %s has invalid key %q", path, connector.Key)
	case connector.Key != directory:
		return fmt.Errorf("Connector definition %s key %q does not match directory %q", path, connector.Key, directory)
	case strings.TrimSpace(connector.Type) == "":
		return fmt.Errorf("Connector definition %s has no type", path)
	case strings.TrimSpace(connector.Provider) == "":
		return fmt.Errorf("Connector definition %s has no provider family", path)
	case connector.Source != "connectors:"+connector.Key:
		return fmt.Errorf("Connector definition %s has invalid source %q", path, connector.Source)
	case !connectorVersionPattern.MatchString(strings.TrimSpace(connector.Version)):
		return fmt.Errorf("Connector definition %s has invalid version %q", path, connector.Version)
	case !connectorVersionPattern.MatchString(strings.TrimSpace(connector.MinimumRuntimeVersion)):
		return fmt.Errorf("Connector definition %s has invalid minimum runtime version %q", path, connector.MinimumRuntimeVersion)
	}
	classification := valueOrDefault(connector.Classification, "external_connector")
	if !stringInSet(classification, "external_connector", "mixed", "runtime_native") {
		return fmt.Errorf("Connector definition %s has invalid classification %q", path, connector.Classification)
	}
	lifecycle := valueOrDefault(connector.LifecycleStatus, "active")
	if !stringInSet(lifecycle, "active", "reclassified", "retired") {
		return fmt.Errorf("Connector definition %s has invalid lifecycle status %q", path, connector.LifecycleStatus)
	}
	if lifecycle != "active" && strings.TrimSpace(connector.ReplacementCapability) == "" {
		return fmt.Errorf("Connector definition %s lifecycle %q requires replacement_capability", path, lifecycle)
	}
	if err := validateLocalizedDisplayText("connector", connector.Key, connector.Name, connector.Description, connector.I18n); err != nil {
		return fmt.Errorf("Connector definition %s: %w", path, err)
	}
	for key := range connector.Config {
		if key == "providers" || strings.HasSuffix(strings.TrimSpace(key), "_providers") {
			return fmt.Errorf("Connector definition %s keeps legacy provider catalog in config.%s; use top-level providers[]", path, key)
		}
	}
	providerKeys := map[string]bool{}
	for _, provider := range connector.Providers {
		if !ValidConnectorIdentityKey(provider.Key) {
			return fmt.Errorf("Connector definition %s has invalid provider key %q", path, provider.Key)
		}
		if providerKeys[provider.Key] {
			return fmt.Errorf("Connector definition %s has duplicate provider key %q", path, provider.Key)
		}
		providerKeys[provider.Key] = true
		if err := validateLocalizedDisplayText("provider", provider.Key, provider.Name, provider.Description, provider.I18n); err != nil {
			return fmt.Errorf("Connector definition %s: %w", path, err)
		}
		if err := validateConnectorFields("provider "+provider.Key+" config", provider.ConfigFields, false); err != nil {
			return fmt.Errorf("Connector definition %s: %w", path, err)
		}
		if err := validateConnectorFields("provider "+provider.Key+" secret", provider.SecretFields, true); err != nil {
			return fmt.Errorf("Connector definition %s: %w", path, err)
		}
	}
	operationKeys := map[string]bool{}
	for _, operation := range connector.Operations {
		if !ValidConnectorIdentityKey(operation.Key) {
			return fmt.Errorf("Connector definition %s has invalid operation key %q", path, operation.Key)
		}
		if operationKeys[operation.Key] {
			return fmt.Errorf("Connector definition %s has duplicate operation key %q", path, operation.Key)
		}
		operationKeys[operation.Key] = true
		if err := validateLocalizedDisplayText("operation", operation.Key, operation.Name, operation.Description, operation.I18n); err != nil {
			return fmt.Errorf("Connector definition %s: %w", path, err)
		}
		if !stringInSet(operation.Method, "DELETE", "GET", "PATCH", "POST", "PUT", "SMTP") {
			return fmt.Errorf("Connector definition %s operation %q has invalid method %q", path, operation.Key, operation.Method)
		}
		if !stringInSet(operation.ExecutionMode, "sync", "async", "operation") {
			return fmt.Errorf("Connector definition %s operation %q has invalid execution mode %q", path, operation.Key, operation.ExecutionMode)
		}
		if !stringInSet(operation.SideEffect, "read", "reserve", "write") {
			return fmt.Errorf("Connector definition %s operation %q has invalid side effect %q", path, operation.Key, operation.SideEffect)
		}
		if operation.TimeoutDefaultSeconds <= 0 || operation.TimeoutMaxSeconds <= 0 || operation.TimeoutDefaultSeconds > operation.TimeoutMaxSeconds {
			return fmt.Errorf("Connector definition %s operation %q has invalid timeout contract %d/%d", path, operation.Key, operation.TimeoutDefaultSeconds, operation.TimeoutMaxSeconds)
		}
	}
	for _, operation := range connector.Operations {
		compensation := strings.TrimSpace(operation.CompensationOperation)
		if compensation != "" && !operationKeys[compensation] {
			return fmt.Errorf("Connector definition %s operation %q references unknown compensation operation %q", path, operation.Key, compensation)
		}
	}
	expectedFlags := connectorFeatureFlags(connector)
	if !reflect.DeepEqual(connector.FeatureFlags, expectedFlags) {
		return fmt.Errorf("Connector definition %s feature flags %v do not match contract %v", path, connector.FeatureFlags, expectedFlags)
	}
	return nil
}

func connectorFeatureFlags(connector ConnectorSchema) []string {
	flags := []string{"provider_catalog"}
	hasConfig, hasSecret, hasTest := false, false, false
	for _, provider := range connector.Providers {
		hasConfig = hasConfig || len(provider.ConfigFields) > 0
		hasSecret = hasSecret || len(provider.SecretFields) > 0
	}
	if hasConfig {
		flags = append(flags, "typed_config")
	}
	if hasSecret {
		flags = append(flags, "typed_secrets")
	}
	if len(connector.Operations) > 0 {
		flags = append(flags, "operations")
	}
	for _, operation := range connector.Operations {
		hasTest = hasTest || operation.Key == "test_connection"
	}
	if hasTest {
		flags = append(flags, "connection_test")
	}
	sort.Strings(flags)
	return flags
}

func validateLocalizedDisplayText(kind, key, name, description string, localized LocalizedTextMap) error {
	if strings.TrimSpace(name) == "" || strings.TrimSpace(description) == "" {
		return fmt.Errorf("%s %q requires default name and description", kind, key)
	}
	for _, locale := range []string{"en-US", "zh-CN"} {
		values := localized[locale]
		if strings.TrimSpace(values["name"]) == "" || strings.TrimSpace(values["description"]) == "" {
			return fmt.Errorf("%s %q requires localized name and description for %s", kind, key, locale)
		}
	}
	return nil
}

func validateConnectorFields(kind string, fields []FieldSchema, secret bool) error {
	seen := map[string]bool{}
	for _, field := range fields {
		if !ValidConnectorIdentityKey(field.Key) {
			return fmt.Errorf("%s has invalid field key %q", kind, field.Key)
		}
		if seen[field.Key] {
			return fmt.Errorf("%s has duplicate field key %q", kind, field.Key)
		}
		seen[field.Key] = true
		if err := validateLocalizedDisplayText(kind+" field", field.Key, field.Name, field.Description, field.I18n); err != nil {
			return err
		}
		if secret && (field.Config["sensitive"] != true || field.Config["write_only"] != true) {
			return fmt.Errorf("%s field %q must be sensitive and write-only", kind, field.Key)
		}
		if secret {
			if !stringInSet(strings.TrimSpace(fmt.Sprint(field.Config["credential_kind"])), "api_key", "basic_auth_password", "bearer_token", "certificate", "connection_string", "database_password", "generic_secret", "identifier", "oauth_client_secret", "private_key", "refresh_token", "service_account", "signing_secret") {
				return fmt.Errorf("%s field %q requires a supported credential kind", kind, field.Key)
			}
			if !stringInSet(strings.TrimSpace(fmt.Sprint(field.Config["material_format"])), "opaque", "text", "json_object", "pem_or_opaque", "pem_or_reference", "uri_or_dsn") {
				return fmt.Errorf("%s field %q requires a supported material format", kind, field.Key)
			}
			if field.Config["rotation_policy"] != "manual" || !stringInSet(strings.TrimSpace(fmt.Sprint(field.Config["expiry_policy"])), "none", "optional") || !stringInSet(strings.TrimSpace(fmt.Sprint(field.Config["test_requirement"])), "optional", "when_bound") {
				return fmt.Errorf("%s field %q requires lifecycle and test policies", kind, field.Key)
			}
			continue
		}
		if field.Key == "provider" {
			return fmt.Errorf("%s field %q duplicates top-level provider_key", kind, field.Key)
		}
		switch field.Type {
		case "text":
			if field.Validation.MaxLength <= 0 {
				return fmt.Errorf("%s field %q requires a maximum length", kind, field.Key)
			}
		case "integer":
			if field.Validation.Min == nil || field.Validation.Max == nil || *field.Validation.Min > *field.Validation.Max {
				return fmt.Errorf("%s field %q requires a numeric range", kind, field.Key)
			}
		case "boolean":
			if _, ok := field.Default.(bool); !ok {
				return fmt.Errorf("%s field %q requires a boolean default", kind, field.Key)
			}
		case "json":
			if field.Config["json_shape"] != "object" && field.Config["json_shape"] != "array" {
				return fmt.Errorf("%s field %q requires an explicit JSON shape", kind, field.Key)
			}
		}
	}
	if !secret {
		for _, field := range fields {
			for _, dependency := range providerConfigDependencyKeys(field.Config["required_with"]) {
				if dependency == field.Key || !seen[dependency] {
					return fmt.Errorf("%s field %q has unknown required_with dependency %q", kind, field.Key, dependency)
				}
			}
		}
	}
	return nil
}

// ConfigValidationError exposes a stable authoring diagnostic without making
// the Connectors catalog depend on Runtime or its application error package.
type ConfigValidationError struct {
	Code   string
	Params map[string]string
}

func (e *ConfigValidationError) Error() string     { return e.Code }
func (e *ConfigValidationError) ErrorCode() string { return strings.TrimSpace(e.Code) }
func (e *ConfigValidationError) ErrorParams() map[string]string {
	result := make(map[string]string, len(e.Params))
	for key, value := range e.Params {
		result[key] = value
	}
	return result
}

// ValidateProviderConfig validates authoring-time connection configuration
// against one Connectors-owned Provider schema. Connection state and secret
// material remain Integration-owned.
func ValidateProviderConfig(connector ConnectorSchema, providerKey string, config map[string]any) error {
	config = ApplyProviderConfigDefaults(connector, providerKey, config)
	var provider *ConnectorProviderSchema
	for index := range connector.Providers {
		if connector.Providers[index].Key == providerKey {
			provider = &connector.Providers[index]
			break
		}
	}
	if provider == nil {
		return nil
	}
	declared := make(map[string]bool, len(provider.ConfigFields))
	for _, field := range connector.ConfigFields {
		declared[strings.TrimSpace(field)] = true
	}
	switch strings.TrimSpace(connector.Type) {
	case "http", "webhook":
		declared["url"] = true
	case "mock":
		declared["responses"] = true
		declared["scenarios"] = true
	}
	for _, field := range provider.ConfigFields {
		declared[strings.TrimSpace(field.Key)] = true
		for _, dependency := range providerConfigDependencyKeys(field.Config["required_with"]) {
			declared[dependency] = true
		}
	}
	for key := range config {
		if !declared[strings.TrimSpace(key)] {
			return configValidationError("backend.integration.connection.provider_config_unknown", "field_path", "config."+key, "connector", connector.Key, "provider", providerKey, "field", key)
		}
	}
	for _, field := range provider.ConfigFields {
		value, exists := config[field.Key]
		if field.Required && (!exists || emptyConfigValue(value)) {
			return configValidationError("backend.integration.connection.provider_config_required", "field_path", "config."+field.Key, "connector", connector.Key, "provider", providerKey, "field", field.Key)
		}
		if exists && !providerConfigFieldValueMatchesType(value, field) {
			return configValidationError("backend.integration.connection.provider_config_type_invalid", "field_path", "config."+field.Key, "connector", connector.Key, "provider", providerKey, "field", field.Key, "expected", field.Type, "actual", fmt.Sprintf("%T", value))
		}
		if exists {
			if err := validateProviderConfigFieldValue(connector.Key, providerKey, field, value, config); err != nil {
				return err
			}
		}
	}
	return nil
}

func ApplyProviderConfigDefaults(connector ConnectorSchema, providerKey string, config map[string]any) map[string]any {
	result := make(map[string]any, len(config))
	for key, value := range config {
		result[key] = value
	}
	for _, provider := range connector.Providers {
		if provider.Key != providerKey {
			continue
		}
		for _, field := range provider.ConfigFields {
			if _, exists := result[field.Key]; exists || field.Default == nil {
				continue
			}
			ready := true
			for _, dependency := range providerConfigDependencyKeys(field.Config["required_with"]) {
				if emptyConfigValue(result[dependency]) {
					ready = false
					break
				}
			}
			if ready {
				result[field.Key] = cloneConfigValue(field.Default)
			}
		}
		break
	}
	return result
}

func validateProviderConfigFieldValue(connectorKey, providerKey string, field FieldSchema, value any, config map[string]any) error {
	invalid := func(rule string) error {
		return configValidationError("backend.integration.connection.provider_config_validation_failed", "field_path", "config."+field.Key, "connector", connectorKey, "provider", providerKey, "field", field.Key, "rule", rule)
	}
	if !emptyConfigValue(value) {
		for _, dependency := range providerConfigDependencyKeys(field.Config["required_with"]) {
			if emptyConfigValue(config[dependency]) {
				return invalid("required_with:" + dependency)
			}
		}
	}
	if len(field.Validation.Options) > 0 {
		matched := false
		for _, option := range field.Validation.Options {
			matched = matched || reflect.DeepEqual(value, option) || fmt.Sprint(value) == option
		}
		if !matched {
			return invalid("options")
		}
	}
	if text, ok := value.(string); ok {
		length := len([]rune(text))
		if field.Validation.MinLength > 0 && length < field.Validation.MinLength {
			return invalid("min_length")
		}
		if field.Validation.MaxLength > 0 && length > field.Validation.MaxLength {
			return invalid("max_length")
		}
		if pattern := strings.TrimSpace(field.Validation.Pattern); pattern != "" {
			expression, err := regexp.Compile(pattern)
			if err != nil || !expression.MatchString(text) {
				return invalid("pattern")
			}
		}
	}
	if field.Validation.Min != nil || field.Validation.Max != nil {
		number, ok := providerConfigNumber(value)
		if !ok {
			return invalid("number")
		}
		if field.Validation.Min != nil && number < *field.Validation.Min {
			return invalid("min")
		}
		if field.Validation.Max != nil && number > *field.Validation.Max {
			return invalid("max")
		}
	}
	return nil
}

func providerConfigFieldValueMatchesType(value any, field FieldSchema) bool {
	switch strings.ToLower(strings.TrimSpace(field.Type)) {
	case "text", "select":
		_, ok := value.(string)
		return ok
	case "email":
		text, ok := value.(string)
		if !ok || strings.TrimSpace(text) != text {
			return false
		}
		address, err := mail.ParseAddress(text)
		return err == nil && address.Address == text
	case "integer":
		number, ok := providerConfigNumber(value)
		_, stringValue := value.(string)
		return ok && !stringValue && !math.IsNaN(number) && !math.IsInf(number, 0) && math.Trunc(number) == number
	case "decimal":
		number, ok := providerConfigNumber(value)
		_, stringValue := value.(string)
		return ok && !stringValue && !math.IsNaN(number) && !math.IsInf(number, 0)
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "json":
		raw, err := json.Marshal(value)
		if err != nil || len(raw) == 0 {
			return false
		}
		switch field.Config["json_shape"] {
		case "object":
			return raw[0] == '{'
		case "array":
			return raw[0] == '['
		default:
			return false
		}
	default:
		return field.Config["contract_owner"] != "connector"
	}
}

func providerConfigNumber(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int8:
		return float64(typed), true
	case int16:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint8:
		return float64(typed), true
	case uint16:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func providerConfigDependencyKeys(value any) []string {
	result := []string{}
	switch typed := value.(type) {
	case []any:
		for _, item := range typed {
			if key := strings.TrimSpace(fmt.Sprint(item)); item != nil && key != "" && key != "<nil>" {
				result = append(result, key)
			}
		}
	case []string:
		for _, item := range typed {
			if key := strings.TrimSpace(item); key != "" {
				result = append(result, key)
			}
		}
	}
	return result
}

func emptyConfigValue(value any) bool {
	if value == nil {
		return true
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed) == ""
	case []any:
		return len(typed) == 0
	case []string:
		return len(typed) == 0
	case map[string]any:
		return len(typed) == 0
	}
	return false
}

func cloneConfigValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			result[key] = item
		}
		return result
	case []any:
		return append([]any(nil), typed...)
	case []string:
		return append([]string(nil), typed...)
	default:
		return value
	}
}

func configValidationError(code string, params ...string) error {
	values := map[string]string{}
	for index := 0; index+1 < len(params); index += 2 {
		if key := strings.TrimSpace(params[index]); key != "" {
			values[key] = params[index+1]
		}
	}
	return &ConfigValidationError{Code: strings.TrimSpace(code), Params: values}
}

func valueOrDefault(value, fallback string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return fallback
}

func stringInSet(value string, candidates ...string) bool {
	for _, candidate := range candidates {
		if value == candidate {
			return true
		}
	}
	return false
}
