package catalog

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"
	"testing"
)

func TestCatalogIsCanonicalAndHasUniqueIdentities(t *testing.T) {
	document, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(document.Providers) == 0 {
		t.Fatal("official Connector Catalog is empty")
	}
	identities := map[string]bool{}
	previous := ""
	for _, provider := range document.Providers {
		identity := provider.ConnectorKey + ":" + provider.ProviderKey
		if identities[identity] {
			t.Fatalf("duplicate Provider identity %s", identity)
		}
		if previous != "" && identity < previous {
			t.Fatalf("Provider identities are not sorted: %s before %s", previous, identity)
		}
		identities[identity] = true
		previous = identity
		if !strings.HasPrefix(provider.ImportPath, "github.com/domainry/domainry-connectors/providers/"+provider.ConnectorKey+"/"+provider.ProviderKey) {
			t.Fatalf("Provider %s import path does not match stable keys: %s", identity, provider.ImportPath)
		}
		operations := make([]string, len(provider.Operations))
		for index, operation := range provider.Operations {
			operations[index] = operation.Key
		}
		if !sort.StringsAreSorted(operations) {
			t.Fatalf("Provider %s operations are not sorted", identity)
		}
	}
	var canonical bytes.Buffer
	if err := json.Indent(&canonical, bytes.TrimSpace(Bytes()), "", "  "); err != nil {
		t.Fatal(err)
	}
	canonical.WriteByte('\n')
	if !bytes.Equal(canonical.Bytes(), Bytes()) {
		t.Fatal("catalog.json is not canonical two-space-indented JSON")
	}
}
