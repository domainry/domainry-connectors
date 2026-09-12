package google

import (
	connector "github.com/domainry/domainry-connector-sdk"
	calendar "github.com/domainry/domainry-connector-sdk/calendar"
	"testing"
)

func TestCalendarOperationGrantsRemainDistinctFromProbeAndBusy(t *testing.T) {
	a, _ := New(&recordingTransport{})
	r := connector.NewRegistry()
	if err := r.Register(a); err != nil {
		t.Fatal(err)
	}
	r.Freeze()
	a, _ = r.Provider(ConnectorKey, ProviderKey)
	for _, key := range []string{calendar.ListOperationKey, calendar.EventsOperationKey, calendar.EventOperationKey, calendar.AvailabilityOperationKey} {
		alternatives, declared := connector.ResolveOAuthOperationScopes(a, key)
		if !declared || len(alternatives) == 0 {
			t.Fatal(key)
		}
		busy := false
		for _, v := range alternatives {
			if len(v) != 1 {
				t.Fatal(v)
			}
			busy = busy || v[0] == "https://www.googleapis.com/auth/calendar.events.freebusy"
			if v[0] == "openid" {
				t.Fatal("profile grant enabled calendar")
			}
		}
		if busy != (key == calendar.AvailabilityOperationKey) {
			t.Fatal("busy scope enabled event content", key)
		}
		alternatives[0][0] = "changed"
		fresh, _ := connector.ResolveOAuthOperationScopes(a, key)
		if fresh[0][0] == "changed" {
			t.Fatal("mutable scope registry")
		}
	}
	for _, key := range []string{"test_connection", "sync_calendar", "send_mail", "missing"} {
		if _, declared := connector.ResolveOAuthOperationScopes(a, key); declared {
			t.Fatal("unexpected account operation", key)
		}
	}
}
