package module

import (
	connector "github.com/domainry/domainry-connector-sdk"
	llmproxy "github.com/domainry/domainry-connectors/providers/web/llm_proxy"
)

// PublicWebProviders is opt-in composition. The host chooses the transport and
// service origin; it does not add web capabilities to work-account Providers.
func PublicWebProviders(transport connector.Transport) (connector.ProviderSet, error) {
	provider, err := llmproxy.New(transport)
	if err != nil {
		return connector.ProviderSet{}, err
	}
	return connector.ProviderSet{Providers: []connector.Adapter{provider}}, nil
}
