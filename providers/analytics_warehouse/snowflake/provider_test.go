package snowflake

import (
	"context"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
	"testing"
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
	d := adapter.Descriptor()
	if d.ConnectorKey != ConnectorKey || d.ProviderKey != ProviderKey || len(d.Operations) != 3 {
		t.Fatalf("descriptor=%+v", d)
	}
	hashes := map[string]string{ListTables.Key: ListTables.ContractSHA256, SampleQuery.Key: SampleQuery.ContractSHA256, TestConnection.Key: TestConnection.ContractSHA256}
	for _, operation := range d.Operations {
		if hashes[operation.Key] != operation.ContractSHA256 {
			t.Fatalf("operation=%+v", operation)
		}
	}
}
