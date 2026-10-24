package module

import (
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/providers/notification/apns"
	"github.com/domainry/domainry-connectors/providers/notification/fcm"
)

// NotificationProviders composes the official native mobile push protocols
// with Runtime-owned outbound transport. Selecting this set does not install
// unrelated Connector providers into the product process.
func NotificationProviders(transport connector.Transport) (connector.ProviderSet, error) {
	fcmProvider, err := fcm.New(transport)
	if err != nil {
		return connector.ProviderSet{}, err
	}
	apnsProvider, err := apns.New(transport)
	if err != nil {
		return connector.ProviderSet{}, err
	}
	return connector.ProviderSet{Providers: []connector.Adapter{fcmProvider, apnsProvider}}, nil
}
