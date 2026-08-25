// Package microsoftteams contains private Microsoft Graph collaboration
// protocol code shared by Domainry's official Teams provider identities.
//
// This package is internal deliberately: third-party Connector authors depend
// only on domainry-connector-sdk and must not couple to official implementation
// details.
package microsoftteams

type Identity struct {
	ConnectorKey       string
	ProviderKey        string
	ProviderName       string
	SendContractSHA256 string
	TestContractSHA256 string
}

type SendMessageInput struct {
	Recipient           string         `json:"recipient"`
	Message             string         `json:"message"`
	Text                string         `json:"text,omitempty"`
	ProviderPayload     map[string]any `json:"provider_payload,omitempty"`
	NotificationContent map[string]any `json:"notification_content,omitempty"`
}

type Response map[string]any
