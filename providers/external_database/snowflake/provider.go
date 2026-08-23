// Package snowflake implements the official Snowflake external-database Provider.
package snowflake

import (
	connector "github.com/domainry/domainry-connector-sdk"
	internalsnowflake "github.com/domainry/domainry-connectors/internal/snowflakesql"
)

const (
	ConnectorKey = "external_database"
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
	ListTables     = connector.CallOperation[ListTablesInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "list_tables", ContractSHA256: "d03d37a2abbe4b3ee52533969701dfe6cab3521c59307580d31363ef702a14fe", Reliability: readReliability()}
	SampleQuery    = connector.CallOperation[SampleQueryInput, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "sample_query", ContractSHA256: "8cdaa65782d29c2bff21b1c26d6f895440443c2c280f9107180f9c1ffc9b2ba2", Reliability: readReliability()}
	TestConnection = connector.CallOperation[struct{}, Response]{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, Key: "test_connection", ContractSHA256: "52de68ed6a73aefa755d209a067f2d0e9697d6840e1fda86cdf9be4ed764b853", Reliability: readReliability()}
)

func New(transport connector.Transport) (connector.Adapter, error) {
	return internalsnowflake.New(internalsnowflake.Identity{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, ListTablesHash: ListTables.ContractSHA256, SampleQueryHash: SampleQuery.ContractSHA256, TestConnectionHash: TestConnection.ContractSHA256}, transport)
}
func readReliability() connector.ReliabilityContract {
	return connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}}
}
