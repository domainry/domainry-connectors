// Package module exposes stable in-process composition for selected Connector Providers.
package module

import (
	connector "github.com/domainry/domainry-connector-sdk"
	moduleassembly "github.com/domainry/domainry-connectors/internal/assembly/module"
)

type Options = moduleassembly.Options
type Factory = moduleassembly.Factory

func NewFactory(options Options) *Factory { return moduleassembly.NewFactory(options) }

func WorkAccountProviders(transport connector.Transport) (connector.ProviderSet, error) {
	return moduleassembly.WorkAccountProviders(transport)
}

func PublicWebProviders(transport connector.Transport) (connector.ProviderSet, error) {
	return moduleassembly.PublicWebProviders(transport)
}
