package model

const CatalogContractVersion = "domainry-official-connector-catalog-v2"

type Catalog struct {
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

func (provider ProviderEntry) Identity() string {
	return provider.ConnectorKey + ":" + provider.ProviderKey
}

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
