package connector

import (
	"testing"

	"github.com/domainry/domainry-connectors/internal/domain/connector/model"
)

type catalogRepositoryFake struct{ catalog model.Catalog }

func (repository catalogRepositoryFake) Load() (model.Catalog, error) { return repository.catalog, nil }
func (catalogRepositoryFake) Bytes() []byte                           { return []byte("catalog") }

func TestCatalogApplicationServiceFindsProvider(t *testing.T) {
	repository := catalogRepositoryFake{catalog: model.Catalog{
		ContractVersion: model.CatalogContractVersion,
		Providers:       []model.ProviderEntry{{ConnectorKey: "payment", ProviderKey: "stripe", Verification: &model.ProviderVerification{}}},
	}}
	application, err := NewCatalogApplicationService(repository)
	if err != nil {
		t.Fatal(err)
	}
	provider, found, err := application.Provider(" payment ", "stripe")
	if err != nil || !found || provider.ProviderKey != "stripe" {
		t.Fatalf("Provider() = %#v, %v, %v", provider, found, err)
	}
	if _, found, err := application.Provider("payment", "missing"); err != nil || found {
		t.Fatalf("missing Provider() found=%v err=%v", found, err)
	}
}
