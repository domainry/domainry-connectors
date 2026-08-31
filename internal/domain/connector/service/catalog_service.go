package service

import (
	"fmt"
	"sort"
	"strings"

	"github.com/domainry/domainry-connectors/internal/domain/connector/model"
)

type CatalogService struct{}

func (CatalogService) Validate(catalog model.Catalog) error {
	if catalog.ContractVersion != model.CatalogContractVersion {
		return fmt.Errorf("official Connector Catalog version %q is unsupported", catalog.ContractVersion)
	}
	identities := make(map[string]struct{}, len(catalog.Providers))
	previous := ""
	for _, provider := range catalog.Providers {
		identity := provider.Identity()
		if strings.TrimSpace(provider.ConnectorKey) == "" || strings.TrimSpace(provider.ProviderKey) == "" {
			return fmt.Errorf("official Connector Catalog contains an empty Provider identity")
		}
		if _, exists := identities[identity]; exists {
			return fmt.Errorf("official Connector Catalog contains duplicate Provider %s", identity)
		}
		if previous != "" && identity < previous {
			return fmt.Errorf("official Connector Catalog Providers are not sorted: %s before %s", previous, identity)
		}
		if provider.Verification == nil {
			return fmt.Errorf("official Connector Catalog Provider %s has no release verification", identity)
		}
		operationKeys := make([]string, len(provider.Operations))
		for index, operation := range provider.Operations {
			operationKeys[index] = operation.Key
		}
		if !sort.StringsAreSorted(operationKeys) {
			return fmt.Errorf("official Connector Catalog Provider %s operations are not sorted", identity)
		}
		identities[identity] = struct{}{}
		previous = identity
	}
	return nil
}
