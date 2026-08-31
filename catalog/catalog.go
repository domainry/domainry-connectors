package catalog

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	application "github.com/domainry/domainry-connectors/internal/application/connector"
	"github.com/domainry/domainry-connectors/internal/domain/connector/model"
	infrastructure "github.com/domainry/domainry-connectors/internal/infrastructure/catalog"
)

const ContractVersion = model.CatalogContractVersion

//go:embed catalog.json
var raw []byte

//go:embed definitions/*/connector.json
var connectorDefinitions embed.FS

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

// ConnectorDefinition is the source-owned product schema for one Connector.
// ProviderEntry remains the executable-provider release index; this document
// carries display, configuration, secret and operation metadata.
type ConnectorDefinition struct {
	Key     string
	Name    string
	Payload json.RawMessage
}

// Definitions returns detached, key-ordered Connector product definitions.
func Definitions() ([]ConnectorDefinition, error) {
	documents, payloads, err := loadDefinitionDocuments()
	if err != nil {
		return nil, err
	}
	result := make([]ConnectorDefinition, 0, len(documents))
	for index, document := range documents {
		name := strings.TrimSpace(document.Name)
		if name == "" {
			name = document.Key
		}
		result = append(result, ConnectorDefinition{Key: document.Key, Name: name, Payload: append(json.RawMessage(nil), payloads[index]...)})
	}
	return result, nil
}

// DefinitionDocuments returns detached, typed, key-ordered Connector source
// definitions. It is the authoring/catalog API for consumers such as Plane.
func DefinitionDocuments() ([]ConnectorSchema, error) {
	documents, _, err := loadDefinitionDocuments()
	return documents, err
}

func loadDefinitionDocuments() ([]ConnectorSchema, []json.RawMessage, error) {
	paths, err := fs.Glob(connectorDefinitions, "definitions/*/connector.json")
	if err != nil {
		return nil, nil, fmt.Errorf("list Connector definitions: %w", err)
	}
	documents := make([]ConnectorSchema, 0, len(paths))
	payloads := make([]json.RawMessage, 0, len(paths))
	for _, path := range paths {
		raw, err := fs.ReadFile(connectorDefinitions, path)
		if err != nil {
			return nil, nil, fmt.Errorf("read Connector definition %s: %w", path, err)
		}
		var document ConnectorSchema
		if err := json.Unmarshal(raw, &document); err != nil {
			return nil, nil, fmt.Errorf("decode Connector definition %s: %w", path, err)
		}
		parts := strings.Split(strings.ReplaceAll(path, "\\", "/"), "/")
		directory := parts[len(parts)-2]
		if err := validateConnectorDefinition(path, directory, document); err != nil {
			return nil, nil, err
		}
		documents = append(documents, document)
		payloads = append(payloads, append(json.RawMessage(nil), raw...))
	}
	indexes := make([]int, len(documents))
	for index := range indexes {
		indexes[index] = index
	}
	sort.Slice(indexes, func(i, j int) bool { return documents[indexes[i]].Key < documents[indexes[j]].Key })
	sortedDocuments := make([]ConnectorSchema, 0, len(documents))
	sortedPayloads := make([]json.RawMessage, 0, len(payloads))
	for _, index := range indexes {
		sortedDocuments = append(sortedDocuments, documents[index])
		sortedPayloads = append(sortedPayloads, payloads[index])
	}
	return sortedDocuments, sortedPayloads, nil
}
