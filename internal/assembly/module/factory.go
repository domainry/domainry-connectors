package module

import (
	"fmt"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/internal/adapter/connectorsdk"
)

type Options struct {
	Providers connector.ProviderSet
}

type Factory struct{ providers connector.ProviderSet }

func NewFactory(options Options) *Factory {
	return &Factory{providers: connector.ProviderSet{Providers: append([]connector.Adapter(nil), options.Providers.Providers...)}}
}

// Registry creates one validated, frozen SDK registry without performing outbound I/O.
func (factory *Factory) Registry() (*connector.Registry, error) {
	if factory == nil {
		return nil, fmt.Errorf("Connector module Factory is required")
	}
	return connectorsdk.BuildRegistry(factory.providers)
}
