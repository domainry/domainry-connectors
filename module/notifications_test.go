package module_test

import (
	"testing"

	"github.com/domainry/domainry-connectors/module"
)

func TestNotificationProviderCompositionIsExplicitAndComplete(t *testing.T) {
	if _, err := module.NotificationProviders(nil); err == nil {
		t.Fatal("nil Runtime transport accepted")
	}
	transport := &noIO{}
	set, err := module.NotificationProviders(transport)
	if err != nil || len(set.Providers) != 2 {
		t.Fatalf("providers=%d err=%v", len(set.Providers), err)
	}
	registry, err := module.NewFactory(module.Options{Providers: set}).Registry()
	if err != nil {
		t.Fatal(err)
	}
	for _, provider := range []string{"fcm", "apns"} {
		if _, found := registry.Provider("notification", provider); !found {
			t.Fatalf("notification/%s Provider missing", provider)
		}
	}
	if transport.calls != 0 {
		t.Fatal("Provider composition performed outbound I/O")
	}
}
