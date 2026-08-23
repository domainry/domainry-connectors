package feishu

import (
	"context"
	"errors"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
)

type inertTransport struct{}

func (*inertTransport) RoundTripHTTP(context.Context, connector.HTTPRequest) (connector.HTTPResponse, error) {
	return connector.HTTPResponse{}, errors.New("not called")
}
func (*inertTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("not called")
}

func TestDedicatedIdentityUsesSharedKernelWithoutChangingContract(t *testing.T) {
	adapter, err := New(&inertTransport{})
	if err != nil {
		t.Fatal(err)
	}
	if err = contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	descriptor := adapter.Descriptor()
	if descriptor.ConnectorKey != ConnectorKey || descriptor.ProviderKey != ProviderKey || len(descriptor.Operations) != 2 {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	for _, operation := range descriptor.Operations {
		switch operation.Key {
		case SendMessage.Key:
			if operation.ContractSHA256 != SendMessage.ContractSHA256 || operation.Mode != connector.ModeEnqueue {
				t.Fatalf("send operation=%+v", operation)
			}
		case TestConnection.Key:
			if operation.ContractSHA256 != TestConnection.ContractSHA256 || operation.Mode != connector.ModeCall {
				t.Fatalf("test operation=%+v", operation)
			}
		default:
			t.Fatalf("unexpected operation=%+v", operation)
		}
	}
}
