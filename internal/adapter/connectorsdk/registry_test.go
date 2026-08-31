package connectorsdk

import (
	"context"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
)

func TestBuildRegistryValidatesAndFreezesProviderSet(t *testing.T) {
	operation := connector.CallOperation[struct{}, struct{}]{
		ConnectorKey: "test", ProviderKey: "fake", Key: "probe",
		ContractSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Reliability:    connector.ReliabilityContract{Effect: connector.EffectRead, Idempotency: connector.IdempotencyContract{Strategy: connector.IdempotencyNatural}, Reconciliation: connector.ReconciliationNone, Compensation: connector.CompensationContract{Mode: connector.CompensationNone}},
	}
	bound, err := connector.BindCall(operation, func(context.Context, connector.TypedRequest[struct{}]) (connector.TypedResult[struct{}], error) {
		return connector.TypedResult[struct{}]{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	provider, err := connector.NewProvider(connector.ProviderSchema{ConnectorKey: "test", ProviderKey: "fake", ProviderRevision: "1.0.0"}, bound)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := BuildRegistry(connector.ProviderSet{Providers: []connector.Adapter{provider}})
	if err != nil {
		t.Fatal(err)
	}
	if !registry.Frozen() {
		t.Fatal("Registry is not frozen")
	}
	if _, found := registry.Provider("test", "fake"); !found {
		t.Fatal("registered Provider is missing")
	}
}
