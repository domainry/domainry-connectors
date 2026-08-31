package catalog

import (
	_ "embed"

	application "github.com/domainry/domainry-connectors/internal/application/connector"
	"github.com/domainry/domainry-connectors/internal/domain/connector/model"
	infrastructure "github.com/domainry/domainry-connectors/internal/infrastructure/catalog"
)

const ContractVersion = model.CatalogContractVersion

//go:embed catalog.json
var raw []byte

type Document = model.Catalog
type SDKIdentity = model.SDKIdentity
type ProviderEntry = model.ProviderEntry

// ProviderVerification binds a released Provider to the connector-owned test
// suites that prove its external protocol and webhook boundaries. Runtime may
// trust the immutable Catalog identity and only run its generic host protocol
// acceptance suite; it must not duplicate these Provider-specific scenarios.
type ProviderVerification = model.ProviderVerification
type OperationEntry = model.OperationEntry

// Load returns a detached copy of the embedded official Catalog.
func Load() (Document, error) {
	service, err := application.NewCatalogApplicationService(infrastructure.NewEmbeddedRepository(raw))
	if err != nil {
		return Document{}, err
	}
	return service.Catalog()
}

// Bytes returns a copy of the canonical machine-readable Catalog artifact.
func Bytes() []byte { return append([]byte(nil), raw...) }
