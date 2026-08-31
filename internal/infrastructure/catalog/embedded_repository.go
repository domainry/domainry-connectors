package catalog

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/domainry/domainry-connectors/internal/domain/connector/model"
)

type EmbeddedRepository struct{ raw []byte }

func NewEmbeddedRepository(raw []byte) EmbeddedRepository {
	return EmbeddedRepository{raw: append([]byte(nil), raw...)}
}

func (repository EmbeddedRepository) Load() (model.Catalog, error) {
	var catalog model.Catalog
	decoder := json.NewDecoder(bytes.NewReader(repository.raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&catalog); err != nil {
		return model.Catalog{}, fmt.Errorf("decode official Connector Catalog: %w", err)
	}
	return catalog, nil
}

func (repository EmbeddedRepository) Bytes() []byte { return append([]byte(nil), repository.raw...) }
