package smtp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
)

type recordingTransport struct {
	request connector.SMTPRequest
	result  connector.SMTPResult
	err     error
}

func (*recordingTransport) RoundTripHTTP(context.Context, connector.HTTPRequest) (connector.HTTPResponse, error) {
	return connector.HTTPResponse{}, errors.New("unexpected HTTP")
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}
func (t *recordingTransport) SendSMTP(_ context.Context, request connector.SMTPRequest) (connector.SMTPResult, error) {
	t.request = request
	return t.result, t.err
}

func TestDescriptorAndSMTPDelivery(t *testing.T) {
	transport := &recordingTransport{result: connector.SMTPResult{Connected: true, Accepted: true}}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	payload, _ := json.Marshal(SendEmailInput{To: []string{"recipient@example.test"}, Subject: "hello\r\nBcc: injected@example.test", Text: "body"})
	request := connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SendEmail.Key, ContractSHA256: SendEmail.ContractSHA256, Mode: connector.ModeEnqueue, Connection: validConnection(), Secrets: map[string]string{"password": "top-secret"}, Payload: payload, RequestRef: "delivery-1", Delivery: true}
	result, err := adapter.Call(t.Context(), request)
	if err != nil || !strings.HasPrefix(result.ResponseRef, "smtp:") {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if transport.request.SecretPassword != "top-secret" || !acceptedFieldsForTest(transport.request) {
		t.Fatalf("SMTP request was not populated: %+v", transport.request)
	}
	if strings.Contains(string(transport.request.Message), "\r\nBcc:") {
		t.Fatal("subject header injection was not removed")
	}
	encoded, _ := json.Marshal(transport.request)
	if strings.Contains(string(encoded), "top-secret") {
		t.Fatal("SMTP password leaked through JSON")
	}
}

func TestProbeAndValidation(t *testing.T) {
	transport := &recordingTransport{result: connector.SMTPResult{Connected: true}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: validConnection()})
	if err != nil || !result.Connected || !transport.request.ProbeOnly {
		t.Fatalf("result=%+v request=%+v err=%v", result, transport.request, err)
	}
	if err := adapter.(connector.ConfigValidator).ValidateConfig(connector.Connection{Config: map[string]any{"host": "smtp.example.test", "port": 25, "from_email": "sender@example.test", "tls": true, "start_tls": true}}); err == nil {
		t.Fatal("expected conflicting TLS modes to fail")
	}
}

func TestUsernameRequiresResolvedPassword(t *testing.T) {
	transport := &recordingTransport{result: connector.SMTPResult{Connected: true, Accepted: true}}
	adapter, _ := New(transport)
	payload, _ := json.Marshal(SendEmailInput{To: []string{"recipient@example.test"}, Text: "body"})
	_, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SendEmail.Key, ContractSHA256: SendEmail.ContractSHA256, Mode: connector.ModeEnqueue, Connection: validConnection(), Payload: payload, Delivery: true})
	if err == nil {
		t.Fatal("expected missing password to fail")
	}
}

func validConnection() connector.Connection {
	return connector.Connection{Config: map[string]any{"host": "smtp.example.test", "port": 587, "from_email": "sender@example.test", "username": "sender@example.test", "start_tls": true, "timeout_seconds": 30}}
}

// AcceptedFieldsForTest keeps assertions readable without exposing provider internals.
func acceptedFieldsForTest(r connector.SMTPRequest) bool {
	return r.Host == "smtp.example.test" && r.Port == 587 && r.StartTLS && r.EnvelopeFrom == "sender@example.test" && len(r.Recipients) == 1 && len(r.Message) > 0
}
