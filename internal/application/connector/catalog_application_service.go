package connector

import (
	"fmt"
	"strings"

	"github.com/domainry/domainry-connectors/internal/domain/connector/model"
	"github.com/domainry/domainry-connectors/internal/domain/connector/repository"
	"github.com/domainry/domainry-connectors/internal/domain/connector/service"
)

type CatalogApplicationService struct {
	repository repository.CatalogRepository
	domain     service.CatalogService
}

func NewCatalogApplicationService(repository repository.CatalogRepository) (*CatalogApplicationService, error) {
	if repository == nil {
		return nil, fmt.Errorf("Connector Catalog repository is required")
	}
	application := &CatalogApplicationService{repository: repository}
	if _, err := application.Catalog(); err != nil {
		return nil, err
	}
	return application, nil
}

func (application *CatalogApplicationService) Catalog() (model.Catalog, error) {
	catalog, err := application.repository.Load()
	if err != nil {
		return model.Catalog{}, err
	}
	if err := application.domain.Validate(catalog); err != nil {
		return model.Catalog{}, err
	}
	return catalog, nil
}

func (application *CatalogApplicationService) Provider(connectorKey, providerKey string) (model.ProviderEntry, bool, error) {
	catalog, err := application.Catalog()
	if err != nil {
		return model.ProviderEntry{}, false, err
	}
	identity := strings.TrimSpace(connectorKey) + ":" + strings.TrimSpace(providerKey)
	for _, provider := range catalog.Providers {
		if provider.Identity() == identity {
			return provider, true, nil
		}
	}
	return model.ProviderEntry{}, false, nil
}

func (application *CatalogApplicationService) Bytes() []byte { return application.repository.Bytes() }
