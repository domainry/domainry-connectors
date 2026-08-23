// Package mqtt implements the official generic MQTT telemetry Provider.
package mqtt

import (
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connectors/internal/mqtttelemetry"
)

const (
	ConnectorKey = mqtttelemetry.ConnectorKey
	ProviderKey  = "mqtt"
)

type PublishInput = mqtttelemetry.PublishInput
type PublishOutput = mqtttelemetry.PublishOutput
type TestOutput = mqtttelemetry.TestOutput

var (
	PublishTelemetry = mqtttelemetry.PublishOperation(ProviderKey)
	TestConnection   = mqtttelemetry.TestOperation(ProviderKey)
)

func New(transport connector.Transport) (connector.Adapter, error) {
	return mqtttelemetry.New(transport, mqtttelemetry.Options{ProviderKey: ProviderKey, ProviderName: "MQTT"}, PublishTelemetry, TestConnection)
}
