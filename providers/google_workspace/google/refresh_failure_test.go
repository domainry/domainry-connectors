package google

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

func TestRotatedCredentialsSurviveFollowupFailure(t *testing.T) {
	for _, entry := range []string{"call", "connection-test"} {
		t.Run(entry, func(t *testing.T) {
			transport := &recordingTransport{respond: refreshThenFailResponse(t)}
			adapter, err := New(transport)
			if err != nil {
				t.Fatal(err)
			}
			secrets := map[string]string{"access_token": "stale-access", "refresh_token": "stale-refresh", "client_id": "synthetic-client"}
			var public json.RawMessage
			var updates map[string]string
			if entry == "call" {
				result, callErr := adapter.Call(t.Context(), connector.CallRequest{ConnectorKey: ConnectorKey, ProviderKey: ProviderKey, OperationKey: TestConnection.Key, ContractSHA256: TestConnection.ContractSHA256, Mode: connector.ModeCall, Connection: validConnection(), Secrets: secrets, Payload: []byte(`{}`)})
				err, public, updates = callErr, result.Payload, result.SecretUpdates
			} else {
				result, testErr := adapter.(connector.ConnectionTester).TestConnection(t.Context(), connector.TestConnectionRequest{Connection: validConnection(), Secrets: secrets})
				err, public, updates = testErr, result.Details, result.SecretUpdates
				if result.Connected {
					t.Fatal("failed followup reported connected")
				}
			}
			classification, ok := connector.ErrorClassificationOf(err)
			if !ok || classification != connector.ErrorRetryable || len(transport.requests) != 3 {
				t.Fatal("followup failure was lost or retried", err, len(transport.requests))
			}
			if updates["access_token"] != "rotated-access" || updates["refresh_token"] != "rotated-refresh" || transport.requests[2].SecretHeaders["Authorization"][0] != "Bearer rotated-access" {
				t.Fatal("successfully rotated credentials were discarded or unused")
			}
			if strings.Contains(string(public), "rotated-access") || strings.Contains(string(public), "rotated-refresh") {
				t.Fatal("credential material entered public payload/details")
			}
		})
	}
}

func TestBackgroundRetainsRotationWhenLaterRequestFails(t *testing.T) {
	transport := &recordingTransport{respond: func(request connector.HTTPRequest) (connector.HTTPResponse, error) {
		if strings.HasSuffix(request.URL, "/token") {
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"access_token":"rotated-access","refresh_token":"rotated-refresh"}`)}, nil
		}
		if strings.Contains(request.URL, "/profile") {
			if request.SecretHeaders["Authorization"][0] == "Bearer stale-access" {
				return connector.HTTPResponse{StatusCode: 401, Body: []byte(`{}`)}, nil
			}
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"emailAddress":"person@example.test","historyId":"12"}`)}, nil
		}
		if strings.Contains(request.URL, "/history") {
			return connector.HTTPResponse{StatusCode: 503, Body: []byte(`{}`)}, nil
		}
		return connector.HTTPResponse{}, errors.New("unexpected Google background request")
	}}
	adapter, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	connection := validConnection()
	connection.Key, connection.WorkspaceID, connection.ConnectorKey, connection.ProviderKey, connection.Status = "gmail", "workspace", ConnectorKey, ProviderKey, "active"
	connection.Config["gmail_ingest_enabled"] = true
	result, err := adapter.(connector.BackgroundProcessor).ProcessBackground(t.Context(), connector.BackgroundRequest{TaskKey: gmailSyncTaskKey, StateVersion: 1, Connection: connection, State: json.RawMessage(`{"account_email":"person@example.test","history_id":"10"}`), Secrets: map[string]string{"access_token": "stale-access", "refresh_token": "stale-refresh", "client_id": "synthetic-client"}, Now: time.Date(2026, 9, 11, 0, 0, 0, 0, time.UTC), Principal: connector.Principal{IsAuthenticated: true, WorkspaceID: "workspace"}})
	classification, ok := connector.ErrorClassificationOf(err)
	if !ok || classification != connector.ErrorRetryable || len(transport.requests) != 4 {
		t.Fatal("background failure was lost or retried", err, len(transport.requests))
	}
	if result.SecretUpdates["access_token"] != "rotated-access" || result.SecretUpdates["refresh_token"] != "rotated-refresh" || transport.requests[3].SecretHeaders["Authorization"][0] != "Bearer rotated-access" {
		t.Fatal("background discarded or failed to use rotated credentials")
	}
}

func refreshThenFailResponse(t *testing.T) func(connector.HTTPRequest) (connector.HTTPResponse, error) {
	t.Helper()
	return func(request connector.HTTPRequest) (connector.HTTPResponse, error) {
		if strings.HasSuffix(request.URL, "/token") {
			if request.SecretForm["refresh_token"] != "stale-refresh" {
				t.Fatal("refresh token did not use private transport field")
			}
			return connector.HTTPResponse{StatusCode: 200, Body: []byte(`{"access_token":"rotated-access","refresh_token":"rotated-refresh"}`)}, nil
		}
		if request.SecretHeaders["Authorization"][0] == "Bearer stale-access" {
			return connector.HTTPResponse{StatusCode: 401, Body: []byte(`{}`)}, nil
		}
		return connector.HTTPResponse{StatusCode: 503, Body: []byte(`{}`)}, nil
	}
}
