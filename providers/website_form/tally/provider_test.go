package tally

import (
	"context"
	"errors"
	"net/http"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
)

type recordingTransport struct{ response connector.HTTPResponse }

func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func (t *recordingTransport) RoundTripHTTP(context.Context, connector.HTTPRequest) (connector.HTTPResponse, error) {
	return t.response, nil
}

func TestWebsiteFormIdentityAndSchema(t *testing.T) {
	adapter, err := New(&recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"id":"form"}`)}})
	if err != nil {
		t.Fatal(err)
	}
	if err = contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	descriptor := adapter.Descriptor()
	if descriptor.ConnectorKey != "website_form" || descriptor.ProviderKey != "tally" || len(descriptor.Operations) != 1 || len(descriptor.ConfigFields) != 11 || descriptor.ConfigFields[0].Key != "site_id" || len(descriptor.SecretFields) != 2 {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	request := connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: TestConnection.Key, ContractSHA256: TestConnection.ContractSHA256, Mode: connector.ModeCall, Connection: connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080", "form_id": "form"}}, Secrets: map[string]string{"api_token": "token"}, Payload: []byte(`{}`)}
	if _, err = adapter.Call(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if _, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: TestConnection.Key, ContractSHA256: TestConnection.ContractSHA256, Mode: connector.ModeCall, Connection: request.Connection, Secrets: request.Secrets, Payload: []byte(`{"unknown":true}`)}); err == nil {
		t.Fatal("unknown test input accepted")
	}
}
