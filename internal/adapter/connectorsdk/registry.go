package connectorsdk

import (
	"fmt"

	connector "github.com/domainry/domainry-connector-sdk"
)

// BuildRegistry adapts a source-owned Provider set to the SDK registry boundary.
func BuildRegistry(set connector.ProviderSet) (*connector.Registry, error) {
	registry := connector.NewRegistry()
	if err := registry.RegisterProviderSet(set); err != nil {
		return nil, fmt.Errorf("register Connector Provider set: %w", err)
	}
	registry.Freeze()
	return registry, nil
}
