// Package snowflake implements the official Snowflake analytics-warehouse Provider.
package snowflake

import (
	connector "github.com/domainry/domainry-connector-sdk"
	internalsnowflake "github.com/domainry/domainry-connectors/internal/snowflakesql"
)

const (
	ConnectorKey = "analytics_warehouse"
	ProviderKey  = "snowflake"
)

type ListTablesInput struct {
	MaxRows int `json:"max_rows,omitempty"`
}
type SampleQueryInput struct {
	Query   string `json:"query"`
	MaxRows int    `json:"max_rows,omitempty"`
}
type Response map[string]any

var (
	ListTables     = connector.CallOperation[ListTablesInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_tables", ContractSHA256: "05a9e4c852f9eb6717263fee63ae62f4110945dfb09de90ef903ad87859b4690", Reliability: readReliability()}
	SampleQuery    = connector.CallOperation[SampleQueryInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "sample_query", ContractSHA256: "cfe4d686d9d9f685d4fcd364ab5b70ba086869076684864b66c8fc965ca64594", Reliability: readReliability()}
	TestConnection = connector.CallOperation[struct{}, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection", ContractSHA256: "30db39d8f12566e5a218413c5ba468aed3a1825527b3f4fca5b54fe307f635b5", Reliability: readReliability()}
)

func New(transport connector.Transport) (connector.Adapter, error) {
	return internalsnowflake.New(internalsnowflake.Identity{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ListTablesHash: ListTables.ContractSHA256, SampleQueryHash: SampleQuery.ContractSHA256, TestConnectionHash: TestConnection.ContractSHA256}, transport)
}
func readReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}
