package webpush

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	webpushlib "github.com/SherClockHolmes/webpush-go"
	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-connector-sdk/contracttest"
)

type recordingTransport struct {
	request connector.HTTPRequest
	status  int
	err     error
}

func (transport *recordingTransport) RoundTripHTTP(_ context.Context, request connector.HTTPRequest) (connector.HTTPResponse, error) {
	transport.request = request
	if transport.err != nil {
		return connector.HTTPResponse{}, transport.err
	}
	return connector.HTTPResponse{StatusCode: transport.status}, nil
}
func (*recordingTransport) ExecuteSQL(context.Context, connector.SQLRequest) (connector.SQLResult, error) {
	return connector.SQLResult{}, errors.New("unexpected SQL")
}

func TestWebPushUsesRuntimeHTTPAndKeepsVAPIDAuthorizationSecret(t *testing.T) {
	transport := &recordingTransport{status: http.StatusCreated}
	adapter, err := New(transport)
	if err != nil || contracttest.ValidateAdapter(adapter) != nil {
		t.Fatalf("new=%v validation=%v", err, contracttest.ValidateAdapter(adapter))
	}
	private, public, err := webpushKeys()
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(SendInput{SubscriptionID: "sub-1", Endpoint: "https://push.example.test/send", P256DH: public, Auth: base64.RawURLEncoding.EncodeToString([]byte("0123456789abcdef")), Payload: map[string]any{"title": "hello"}})
	result, err := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: Send.Key, ContractSHA256: Send.ContractSHA256, Mode: connector.ModeEnqueue, Connection: validConnection(public), Secrets: map[string]string{"vapid_private_key": private}, Payload: payload})
	if err != nil || result.ResponseRef != "http:201" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if transport.request.URL != "https://push.example.test/send" || len(transport.request.Body) == 0 || len(transport.request.SecretHeaders["Authorization"]) != 1 || transport.request.Headers["Authorization"] != nil {
		t.Fatalf("request=%+v", transport.request)
	}
	raw, _ := json.Marshal(transport.request)
	if strings.Contains(string(raw), private) || strings.Contains(string(raw), "Authorization") {
		t.Fatalf("serialized transport request leaked VAPID material: %s", raw)
	}
}

func TestConfigSubscriptionAndDeliveryFailuresAreClassified(t *testing.T) {
	adapter, _ := New(&recordingTransport{status: http.StatusCreated})
	if adapter.(connector.ConfigValidator).ValidateConfig(connector.Connection{}) == nil {
		t.Fatal("accepted empty configuration")
	}
	private, public, _ := webpushKeys()
	connection := validConnection(public)
	if _, err := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: connection}); err == nil {
		t.Fatal("accepted missing private key")
	}
	for status, expected := range map[int]connector.ErrorClassification{http.StatusGone: connector.ErrorPermanent, http.StatusTooManyRequests: connector.ErrorRetryable, http.StatusServiceUnavailable: connector.ErrorRetryable, http.StatusBadRequest: connector.ErrorPermanent} {
		transport := &recordingTransport{status: status}
		current, _ := New(transport)
		payload, _ := json.Marshal(SendInput{SubscriptionID: "sub-1", Endpoint: "https://push.example.test/send", P256DH: public, Auth: base64.RawURLEncoding.EncodeToString([]byte("0123456789abcdef")), Payload: map[string]any{"title": "hello"}})
		_, err := current.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: Send.Key, ContractSHA256: Send.ContractSHA256, Mode: connector.ModeEnqueue, Connection: connection, Secrets: map[string]string{"vapid_private_key": private}, Payload: payload})
		if classification, ok := connector.ErrorClassificationOf(err); !ok || classification != expected {
			t.Fatalf("status=%d classification=%q err=%v", status, classification, err)
		}
	}
}

func validConnection(public string) connector.Connection {
	return connector.Connection{Config: map[string]any{"vapid_subject": "mailto:ops@example.test", "vapid_public_key": public, "timeout_seconds": 15, "ttl_seconds": 3600}, SecretRefs: map[string]string{"vapid_private_key": "secret:vapid"}}
}

func webpushKeys() (string, string, error) {
	return webpushlib.GenerateVAPIDKeys()
}
