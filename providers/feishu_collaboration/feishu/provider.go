// Package feishu exposes the official Feishu Provider for the dedicated
// Feishu collaboration Connector contract.
package feishu

import (
	connector "github.com/domainry/domainry-connector-sdk"
	shared "github.com/domainry/domainry-connectors/internal/feishucollaboration"
)

const (
	ConnectorKey = "feishu_collaboration"
	ProviderKey  = "feishu"
)

type SendMessageInput = shared.SendMessageInput
type Response = shared.Response

var (
	SendMessage    = connector.EnqueueOperation[SendMessageInput]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "send_message", ContractSHA256: "f4e5855b952b0c58704e7a30f51af50efb2f4c3cc878a329a56aa08a7755e279", Reliability: writeReliability()}
	TestConnection = connector.CallOperation[struct{}, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection", ContractSHA256: "0d6f0301c181a15f6994dc2a94f7dc0fbe56ee80ced619b4e1c1e1b83ddee318", Reliability: readReliability()}
)

func New(transport connector.Transport) (connector.Adapter, error) {
	return shared.New(transport, shared.Identity{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderName: "Feishu Collaboration", SendContractSHA256: SendMessage.ContractSHA256, TestContractSHA256: TestConnection.ContractSHA256})
}

func readReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}
func writeReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNone}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}
