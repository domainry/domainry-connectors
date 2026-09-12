package module

import (
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/providers/google_workspace/google"
	"github.com/domainry/domainry-connectors/providers/microsoft_365/microsoft"
)

// WorkAccountProviders composes the official work-account protocols with the
// deployment host's transport. The host need not import Provider implementations.
func WorkAccountProviders(transport connector.Transport) (connector.ProviderSet, error) {
	googleProvider, err := google.New(transport)
	if err != nil {
		return connector.ProviderSet{}, err
	}
	microsoftProvider, err := microsoft.New(transport)
	if err != nil {
		return connector.ProviderSet{}, err
	}
	return connector.ProviderSet{Providers: []connector.Adapter{googleProvider, microsoftProvider}}, nil
}
