package catalog

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// LocalizedTextMap is the source-owned display contract used by Connector,
// Provider, operation, and field definitions.
type LocalizedTextMap map[string]map[string]string

type FieldValidation struct {
	MinLength int      `json:"min_length,omitempty"`
	MaxLength int      `json:"max_length,omitempty"`
	Min       *float64 `json:"min,omitempty"`
	Max       *float64 `json:"max,omitempty"`
	Pattern   string   `json:"pattern,omitempty"`
	Options   []string `json:"options,omitempty"`
	Target    string   `json:"target,omitempty"`
}

type FieldSchema struct {
	Key          string           `json:"key"`
	Name         string           `json:"name"`
	Description  string           `json:"description,omitempty"`
	Type         string           `json:"type"`
	I18n         LocalizedTextMap `json:"i18n,omitempty"`
	Config       map[string]any   `json:"config,omitempty"`
	Validation   FieldValidation  `json:"validation,omitempty"`
	Options      any              `json:"options,omitempty"`
	Required     bool             `json:"required"`
	Unique       bool             `json:"unique,omitempty"`
	Default      any              `json:"default,omitempty"`
	DefaultValue any              `json:"default_value,omitempty"`
	DisabledAt   string           `json:"disabled_at,omitempty"`
}

// ConnectorSchema is the Connectors-owned product definition. Runtime may
// consume a JSON projection for manifest validation, but does not own or edit
// this contract.
type ConnectorSchema struct {
	Key                   string                     `json:"key"`
	Type                  string                     `json:"type"`
	Provider              string                     `json:"provider"`
	Version               string                     `json:"version,omitempty"`
	MinimumRuntimeVersion string                     `json:"minimum_runtime_version,omitempty"`
	FeatureFlags          []string                   `json:"feature_flags,omitempty"`
	Source                string                     `json:"source,omitempty"`
	Classification        string                     `json:"classification,omitempty"`
	LifecycleStatus       string                     `json:"lifecycle_status,omitempty"`
	ReplacementCapability string                     `json:"replacement_capability,omitempty"`
	Name                  string                     `json:"name,omitempty"`
	Description           string                     `json:"description,omitempty"`
	I18n                  LocalizedTextMap           `json:"i18n,omitempty"`
	Capabilities          []string                   `json:"capabilities,omitempty"`
	Providers             []ConnectorProviderSchema  `json:"providers,omitempty"`
	Operations            []ConnectorOperationSchema `json:"operations,omitempty"`
	Required              bool                       `json:"required,omitempty"`
	ConfigFields          []string                   `json:"config_fields,omitempty"`
	SecretRefs            []string                   `json:"secret_refs,omitempty"`
	Readiness             string                     `json:"readiness,omitempty"`
	DefinitionReady       bool                       `json:"definition_ready"`
	AdapterReady          bool                       `json:"adapter_ready"`
	ConnectionReady       bool                       `json:"connection_ready"`
	Config                map[string]any             `json:"config,omitempty"`
}

type ConnectorProviderSchema struct {
	Key               string           `json:"key"`
	ProviderRevision  string           `json:"provider_revision,omitempty"`
	Name              string           `json:"name,omitempty"`
	Description       string           `json:"description,omitempty"`
	I18n              LocalizedTextMap `json:"i18n,omitempty"`
	ConfigFields      []FieldSchema    `json:"config_fields,omitempty"`
	SecretFields      []FieldSchema    `json:"secret_fields,omitempty"`
	OperationKeys     []string         `json:"operation_keys,omitempty"`
	Readiness         string           `json:"readiness,omitempty"`
	StartupActivation string           `json:"startup_activation,omitempty"`
}

type ConnectorOperationSchema struct {
	Key                   string           `json:"key"`
	Name                  string           `json:"name,omitempty"`
	Description           string           `json:"description,omitempty"`
	I18n                  LocalizedTextMap `json:"i18n,omitempty"`
	Method                string           `json:"method,omitempty"`
	ExecutionMode         string           `json:"execution_mode,omitempty"`
	SideEffect            string           `json:"side_effect,omitempty"`
	Input                 []FieldSchema    `json:"input,omitempty"`
	Output                []FieldSchema    `json:"output,omitempty"`
	TimeoutDefaultSeconds int              `json:"timeout_default_seconds,omitempty"`
	TimeoutMaxSeconds     int              `json:"timeout_max_seconds,omitempty"`
	IdempotencySupported  bool             `json:"idempotency_supported,omitempty"`
	CompensationOperation string           `json:"compensation_operation,omitempty"`
	TestSupported         bool             `json:"test_supported,omitempty"`
	DryRunSupported       bool             `json:"dry_run_supported,omitempty"`
}

const ConnectorIdentityKeyMaximumLength = 128

var connectorIdentityKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,127}$`)

func ValidConnectorIdentityKey(key string) bool {
	return connectorIdentityKeyPattern.MatchString(key)
}

func RequiredConnectorSecretRefNames(connector ConnectorSchema) []string {
	seen := map[string]bool{}
	add := func(value any) {
		text := strings.TrimSpace(fmt.Sprint(value))
		if text != "" && text != "<nil>" {
			seen[text] = true
		}
	}
	switch values := connector.Config["required_secret_refs"].(type) {
	case []string:
		for _, value := range values {
			add(value)
		}
	case []any:
		for _, value := range values {
			add(value)
		}
	case string:
		for _, value := range strings.Split(values, ",") {
			add(value)
		}
	}
	result := make([]string, 0, len(seen))
	for value := range seen {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
