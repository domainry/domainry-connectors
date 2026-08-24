package catalog

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
)

const ContractVersion = "domainry-official-connector-catalog-v2"

//go:embed catalog.json
var raw []byte

type Document struct {
	ContractVersion string          `json:"contract_version"`
	SDK             SDKIdentity     `json:"sdk"`
	Providers       []ProviderEntry `json:"providers"`
}

type SDKIdentity struct {
	Version         string `json:"version"`
	ContractVersion string `json:"contract_version"`
	ContractSHA256  string `json:"contract_sha256"`
}

type ProviderEntry struct {
	ConnectorKey     string                `json:"connector_key"`
	ProviderKey      string                `json:"provider_key"`
	ProviderRevision string                `json:"provider_revision"`
	ImportPath       string                `json:"import_path"`
	PackageName      string                `json:"package_name"`
	Constructor      string                `json:"constructor"`
	DescriptorSHA256 string                `json:"descriptor_sha256"`
	Verification     *ProviderVerification `json:"verification,omitempty"`
	Operations       []OperationEntry      `json:"operations"`
}

// ProviderVerification binds a released Provider to the connector-owned test
// suites that prove its external protocol and webhook boundaries. Runtime may
// trust the immutable Catalog identity and only run its generic host protocol
// acceptance suite; it must not duplicate these Provider-specific scenarios.
type ProviderVerification struct {
	ContractVersion string   `json:"contract_version"`
	Profile         string   `json:"profile"`
	Mode            string   `json:"mode"`
	ManifestSHA256  string   `json:"manifest_sha256"`
	Suites          []string `json:"suites"`
	TestCommand     string   `json:"test_command"`
	IsolatedProfile string   `json:"isolated_profile,omitempty"`
	IsolatedCommand string   `json:"isolated_command,omitempty"`
	LiveProfile     string   `json:"live_profile,omitempty"`
	LiveCommand     string   `json:"live_command,omitempty"`
	RequiredSecrets []string `json:"required_secrets,omitempty"`
}

type OperationEntry struct {
	Key            string `json:"key"`
	Mode           string `json:"mode"`
	ContractSHA256 string `json:"contract_sha256"`
}

// Load returns a detached copy of the embedded official Catalog.
func Load() (Document, error) {
	var document Document
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return Document{}, fmt.Errorf("decode official Connector Catalog: %w", err)
	}
	if document.ContractVersion != ContractVersion {
		return Document{}, fmt.Errorf("official Connector Catalog version %q is unsupported", document.ContractVersion)
	}
	return document, nil
}

// Bytes returns a copy of the canonical machine-readable Catalog artifact.
func Bytes() []byte { return append([]byte(nil), raw...) }
