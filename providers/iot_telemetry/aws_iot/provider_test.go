package awsiot

import (
	"context"
	"errors"
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
	"testing"
)

type transport struct{}

func (transport) RoundTripHTTP(context.Context, connector.HTTPRequest) (connector.HTTPResponse, error) {
	return connector.HTTPResponse{}, errors.New("unused")
}
func (transport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unused")
}
func (transport) ExecuteMQTT(context.Context, connector.MQTTRequest) (connector.MQTTResult, error) {
	return connector.MQTTResult{Connected: true}, nil
}
func TestProvider(t *testing.T) {
	adapter, err := New(transport{})
	if err != nil || contracttest.ValidateAdapter(adapter) != nil || !adapter.Descriptor().SecretFields[2].Required || adapter.Descriptor().ProviderKey != ProviderKey {
		t.Fatalf("adapter=%v err=%v", adapter, err)
	}
}
