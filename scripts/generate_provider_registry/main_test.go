package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiscoverUsesPackageContractInsteadOfProviderFilename(t *testing.T) {
	root := t.TempDir()
	writeProviderFixture(t, root, "calendar/holidays_jp", "holidaysjp", "implementation.go")
	writeProviderFixture(t, root, "weather/open_meteo", "openmeteo", "open_meteo.go")

	providers, err := discover(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(providers) != 2 {
		t.Fatalf("providers=%+v", providers)
	}
	if providers[0].ImportPath != modulePath+"/calendar/holidays_jp" || providers[0].PackageName != "holidaysjp" || providers[0].Alias != "calendar_holidays_jp" {
		t.Fatalf("first Provider=%+v", providers[0])
	}
	if providers[1].ImportPath != modulePath+"/weather/open_meteo" || providers[1].PackageName != "openmeteo" || providers[1].Alias != "weather_open_meteo" {
		t.Fatalf("second Provider=%+v", providers[1])
	}
}

func TestDiscoverRequiresOneExplicitFactory(t *testing.T) {
	for _, test := range []struct {
		name   string
		source string
	}{
		{name: "missing", source: "package acme\n"},
		{name: "duplicate", source: "package acme\nfunc New(a any) (any, error) { return nil, nil }\nfunc helper() {}\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			directory := filepath.Join(root, "sample", "acme")
			if err := os.MkdirAll(directory, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(directory, "implementation.go"), []byte(test.source), 0o600); err != nil {
				t.Fatal(err)
			}
			if test.name == "duplicate" {
				if err := os.WriteFile(filepath.Join(directory, "second.go"), []byte("package acme\nfunc New(a any) (any, error) { return nil, nil }\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := discover(root); err == nil {
				t.Fatal("invalid Provider factory inventory accepted")
			}
		})
	}
}

func writeProviderFixture(t *testing.T, root, relative, packageName, filename string) {
	t.Helper()
	directory := filepath.Join(root, filepath.FromSlash(relative))
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	source := "package " + packageName + "\nfunc New(transport any) (any, error) { return nil, nil }\n"
	if err := os.WriteFile(filepath.Join(directory, filename), []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
}
