package module

import (
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/providers/google_workspace/google"
	"github.com/domainry/domainry-connectors/providers/mcp_tool/mcp"
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

// MCPToolProviders exposes the MCP protocol adapter as an explicit project
// composition choice. It does not add MCP to unrelated account deployments.
func MCPToolProviders(transport connector.Transport) (connector.ProviderSet, error) {
	provider, err := mcp.New(transport)
	if err != nil {
		return connector.ProviderSet{}, err
	}
	return connector.ProviderSet{Providers: []connector.Adapter{provider}}, nil
}
