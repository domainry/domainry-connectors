package googlecalendar

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
)

func TestRotatedCredentialsSurviveFollowupFailure(t *testing.T) {
	for _, entry := range []string{"call", "connection-test"} {
		for _, failure := range []string{"http", "network"} {
			t.Run(entry+"/"+failure, func(t *testing.T) {
				transport := &recordingTransport{responses: []connector.HTTPResponse{{StatusCode: 401, Body: []byte(`{}`)}, {StatusCode: 200, Body: []byte(`{"access_token":"rotated-access","refresh_token":"rotated-refresh"}`)}, {StatusCode: 503, Body: []byte(`{}`)}}}
				if failure == "network" {
					transport.errors = []error{nil, nil, errors.New("synthetic connection reset")}
				}
				adapter, err := New(transport)
				if err != nil {
					t.Fatal(err)
				}
				connection := testConnection("http://localhost:8080", "http://localhost:8080/token")
				var public json.RawMessage
				var updates map[string]string
				if entry == "call" {
					var result connector.CallResult
					result, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: TestConnection.Key, ContractSHA256: TestConnection.ContractSHA256, Mode: connector.ModeCall, Connection: connection, Secrets: secrets(), Payload: []byte(`{}`)})
					public, updates = result.Payload, result.SecretUpdates
				} else {
					var result connector.TestConnectionResult
					result, err = adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: connection, Secrets: secrets()})
					if result.Connected {
						t.Fatal("failed followup reported connected")
					}
					public, updates = result.Details, result.SecretUpdates
				}
				classification, ok := connector.ErrorClassificationOf(err)
				if !ok || classification != connector.ErrorRetryable || len(transport.requests) != 3 {
					t.Fatal("followup failure was lost or retried", err, len(transport.requests))
				}
				if updates["access_token"] != "rotated-access" || updates["refresh_token"] != "rotated-refresh" || transport.requests[2].SecretHeaders["Authorization"][0] != "Bearer rotated-access" {
					t.Fatal("successfully rotated credentials were discarded or unused")
				}
				raw, err := json.Marshal(public)
				if err != nil || strings.Contains(string(raw), "rotated-access") || strings.Contains(string(raw), "rotated-refresh") {
					t.Fatal("credential material entered public JSON")
				}
			})
		}
	}
}
