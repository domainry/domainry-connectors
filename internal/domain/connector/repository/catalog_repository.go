package repository

import "github.com/domainry/domainry-connectors/internal/domain/connector/model"

// CatalogRepository is the source port for the immutable official Provider catalog.
type CatalogRepository interface {
	Load() (model.Catalog, error)
	Bytes() []byte
}
