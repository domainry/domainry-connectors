// Package microsoft365 exposes Microsoft Teams through the stable Microsoft
// 365 collaboration Provider identity.
package microsoft365

import (
	connector "github.com/domainry/domainry-connector-sdk"
	shared "github.com/domainry/domainry-connectors/internal/microsoftteams"
)

const (
	ConnectorKey = "collaboration"
	ProviderKey  = "microsoft_365"
)

type SendMessageInput = shared.SendMessageInput
type Response = shared.Response

var (
	SendMessage    = connector.EnqueueOperation[SendMessageInput]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "send_message", ContractSHA256: "c8e3420b4832306a7f1ea9beec5ae3694c0a795a6a3f8bb28a568a8fb6d501cb", Reliability: writeReliability()}
	TestConnection = connector.CallOperation[struct{}, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection", ContractSHA256: "a354f65a655c7afc141199c44f831137989741466e5c064d4a872fdb8dec4916", Reliability: readReliability()}
)

func New(transport connector.Transport) (connector.Adapter, error) {
	return shared.New(transport, shared.Identity{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ProviderName: "Microsoft 365 (Teams)", SendContractSHA256: SendMessage.ContractSHA256, TestContractSHA256: TestConnection.ContractSHA256})
}
func readReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}
func writeReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectWrite, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNone}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}
