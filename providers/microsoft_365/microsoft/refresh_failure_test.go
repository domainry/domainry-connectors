package microsoft

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
				transport := &recordingTransport{}
				transport.respond = func(request connector.HTTPRequest) (connector.HTTPResponse, error) {
					switch len(transport.requests) {
					case 1:
						return connector.HTTPResponse{StatusCode: 401, Body: []byte(`{}`)}, nil
					case 2:
						if !strings.Contains(request.URL, "/oauth2/v2.0/token") || request.SecretForm["refresh_token"] != "stale-refresh" {
							t.Fatal("expected refresh through private transport fields")
						}
						return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"access_token":"rotated-access","refresh_token":"rotated-refresh"}`)}, nil
					case 3:
						if request.SecretHeaders["Authorization"][0] != "Bearer rotated-access" {
							t.Fatal("followup did not use the new credential")
						}
						if failure == "network" {
							return connector.HTTPResponse{}, errors.New("synthetic connection reset")
						}
						return connector.HTTPResponse{StatusCode: 503, Body: []byte(`{}`)}, nil
					default:
						t.Fatal("unexpected retry")
						return connector.HTTPResponse{}, nil
					}
				}
				adapter, err := New(transport)
				if err != nil {
					t.Fatal(err)
				}
				secrets := map[string]string{"access_token": "stale-access", "refresh_token": "stale-refresh", "client_id": "synthetic-client"}
				var public json.RawMessage
				var updates map[string]string
				if entry == "call" {
					var result connector.CallResult
					result, err = adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: SyncCalendar.Key, ContractSHA256: SyncCalendar.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Secrets: secrets, Payload: []byte(`{}`)})
					public, updates = result.Payload, result.SecretUpdates
				} else {
					var result connector.TestConnectionResult
					result, err = adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: validConnection(), Secrets: secrets})
					if result.Connected {
						t.Fatal("failed followup reported connected")
					}
					public, updates = result.Details, result.SecretUpdates
				}
				classification, ok := connector.ErrorClassificationOf(err)
				if !ok || classification != connector.ErrorRetryable || len(transport.requests) != 3 {
					t.Fatal("followup failure was lost or retried", err, len(transport.requests))
				}
				if updates["access_token"] != "rotated-access" || updates["refresh_token"] != "rotated-refresh" {
					t.Fatal("successfully rotated credentials were discarded")
				}
				raw, err := json.Marshal(public)
				if err != nil || strings.Contains(string(raw), "rotated-access") || strings.Contains(string(raw), "rotated-refresh") {
					t.Fatal("credential material entered public JSON")
				}
			})
		}
	}
}
