package microsoft

import (
	connector "github.com/domainry/domainry-connector-sdk"
	calendar "github.com/domainry/domainry-connector-sdk/calendar"
	"testing"
)

func TestCalendarOperationGrantsKeepBasicReadOutOfDetail(t *testing.T) {
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
		scopes := map[string]bool{}
		for _, v := range alternatives {
			if len(v) != 1 {
				t.Fatal(v)
			}
			scopes[v[0]] = true
		}
		if !scopes["Calendars.Read"] || !scopes["https://graph.microsoft.com/Calendars.Read"] || scopes["User.Read"] || scopes["Calendars.ReadBasic.All"] {
			t.Fatal(scopes)
		}
		if scopes["Calendars.ReadBasic"] != (key != calendar.EventOperationKey) {
			t.Fatal("basic permission enabled detail", key)
		}
	}
	for _, key := range []string{"test_connection", "sync_calendar", "send_mail", "missing"} {
		if _, declared := connector.ResolveOAuthOperationScopes(a, key); declared {
			t.Fatal("unexpected account operation", key)
		}
	}
}
