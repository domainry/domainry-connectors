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

func TestLeadSourceIdentityAndSchema(t *testing.T) {
	adapter, err := New(&recordingTransport{response: connector.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"id":"form"}`)}})
	if err != nil {
		t.Fatal(err)
	}
	if err = contracttest.ValidateAdapter(adapter); err != nil {
		t.Fatal(err)
	}
	descriptor := adapter.Descriptor()
	if descriptor.ConnectorKey != "lead_source" || descriptor.ProviderKey != "tally" || len(descriptor.Operations) != 1 || len(descriptor.ConfigFields) != 11 || descriptor.ConfigFields[0].Key != "account_id" || len(descriptor.SecretFields) != 2 {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	request := connector.TestConnectionRequest{Connection: connector.Connection{Config: map[string]any{"base_url": "http://localhost:8080", "form_id": "form"}}, Secrets: map[string]string{"api_token": "token"}}
	probe, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), request)
	if err != nil || !probe.Connected {
		t.Fatalf("probe=%+v error=%v", probe, err)
	}
}
