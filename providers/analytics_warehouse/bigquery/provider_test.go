package bigquery

import (
	"context"
	"errors"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
)

type inertTransport struct{}

func (inertTransport) RoundTripHTTP(context.Context, connector.HTTPRequest) (connector.HTTPResponse, error) {
	return connector.HTTPResponse{}, errors.New("unused")
}
func (inertTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unused")
}

func TestAnalyticsWarehouseIdentityIsIndependent(t *testing.T) {
	adapter, err := New(inertTransport{})
	if err != nil {
		t.Fatal(err)
	}
	if err = contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	descriptor := adapter.Descriptor()
	if descriptor.ConnectorKey != ConnectorKey || descriptor.ProviderKey != ProviderKey || len(descriptor.Operations) != 3 {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	hashes := map[string]string{ListTables.Key: ListTables.ContractSHA256, SampleQuery.Key: SampleQuery.ContractSHA256, TestConnection.Key: TestConnection.ContractSHA256}
	for _, operation := range descriptor.Operations {
		if hashes[operation.Key] != operation.ContractSHA256 {
			t.Fatalf("operation=%+v", operation)
		}
	}
}
