// Package module exposes stable in-process composition for selected Connector Providers.
package module

import moduleassembly "github.com/domainry/domainry-connectors/internal/assembly/module"

type Options = moduleassembly.Options
type Factory = moduleassembly.Factory

func NewFactory(options Options) *Factory { return moduleassembly.NewFactory(options) }
