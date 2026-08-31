package catalog

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
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
		expectedImportPath := "github.com/domainry/domainry-connectors/providers/" + provider.ConnectorKey + "/" + provider.ProviderKey
		if provider.ImportPath != expectedImportPath {
			t.Fatalf("Provider %s import path does not match stable keys: %s", identity, provider.ImportPath)
		}
		if provider.Verification == nil {
			t.Fatalf("Provider %s has no release verification", identity)
		} else {
			manifestPath := filepath.Join("..", "providers", provider.ConnectorKey, provider.ProviderKey, "verification.json")
			manifest, err := os.ReadFile(manifestPath)
			if err != nil {
				t.Fatalf("read Provider verification %s: %v", identity, err)
			}
			if got := fmt.Sprintf("%x", sha256.Sum256(manifest)); got != provider.Verification.ManifestSHA256 {
				t.Fatalf("Provider %s verification digest=%s want=%s", identity, provider.Verification.ManifestSHA256, got)
			}
			if !sort.StringsAreSorted(provider.Verification.Suites) {
				t.Fatalf("Provider %s verification suites are not sorted", identity)
			}
			if provider.Verification.Mode == "" || provider.Verification.TestCommand == "" {
				t.Fatalf("Provider %s verification is incomplete", identity)
			}
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

func TestConnectorDefinitionsAreEmbeddedAndDetached(t *testing.T) {
	definitions, err := Definitions()
	if err != nil {
		t.Fatal(err)
	}
	if len(definitions) < 50 || definitions[0].Key != "accounting" {
		t.Fatalf("definitions count=%d first=%q", len(definitions), definitions[0].Key)
	}
	definitions[0].Payload[0] = 'x'
	again, err := Definitions()
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(again[0].Payload) {
		t.Fatal("Definitions returned shared mutable payload")
	}
}

func TestConnectorDefinitionDocumentsAreSourceOwnedAndTyped(t *testing.T) {
	documents, err := DefinitionDocuments()
	if err != nil {
		t.Fatal(err)
	}
	if len(documents) < 50 {
		t.Fatalf("definitions count=%d", len(documents))
	}
	for _, document := range documents {
		if document.Source != "connectors:"+document.Key {
			t.Fatalf("Connector %s source=%q", document.Key, document.Source)
		}
	}
	email := documents[0]
	for _, document := range documents {
		if document.Key == "email" {
			email = document
			break
		}
	}
	if len(email.Operations) == 0 || len(email.Providers) == 0 {
		t.Fatal("Email Connector did not decode typed operations and Providers")
	}
	left, err := OperationContractSHA256(email.Key, email.Providers[0].Key, email.Operations[0])
	if err != nil {
		t.Fatal(err)
	}
	right, err := OperationContractSHA256(email.Key, "another-provider", email.Operations[0])
	if err != nil || left == "" || left != right {
		t.Fatalf("operation contract is not stable across Providers: %q %q (%v)", left, right, err)
	}
}

func TestCatalogCoversEveryProviderPackage(t *testing.T) {
	document, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	catalogPaths := make(map[string]bool, len(document.Providers))
	for _, provider := range document.Providers {
		catalogPaths[provider.ImportPath] = true
	}

	providerRoot := filepath.Join("..", "providers")
	err = filepath.WalkDir(providerRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() || path == providerRoot {
			return nil
		}
		relative, err := filepath.Rel(providerRoot, path)
		if err != nil {
			return err
		}
		segments := strings.Split(filepath.ToSlash(relative), "/")
		if len(segments) != 2 {
			return nil
		}
		if _, err := os.Stat(filepath.Join(path, "provider.go")); err != nil {
			return nil
		}
		importPath := "github.com/domainry/domainry-connectors/providers/" + strings.Join(segments, "/")
		if !catalogPaths[importPath] {
			t.Errorf("Provider package %s is missing from the official Catalog generator", importPath)
		}
		return filepath.SkipDir
	})
	if err != nil {
		t.Fatal(err)
	}
}
