package teams

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

func TestPublicTeamsIdentityAndCapabilities(t *testing.T) {
	adapter, err := New(&inertTransport{})
	if err != nil {
		t.Fatal(err)
	}
	if err = contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	descriptor := adapter.Descriptor()
	if descriptor.ConnectorKey != ConnectorKey || descriptor.ProviderKey != ProviderKey || len(descriptor.Operations) != 2 || len(descriptor.SecretFields) != 3 {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	if _, ok := adapter.(connector.WebhookVerifier); !ok {
		t.Fatal("Teams webhook verifier missing")
	}
}
