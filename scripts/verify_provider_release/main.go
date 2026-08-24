// Command verify_provider_release executes a Provider-owned certification gate.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

type manifest struct {
	ContractVersion string   `json:"contract_version"`
	Profile         string   `json:"profile"`
	Mode            string   `json:"mode"`
	ManifestSHA256  string   `json:"manifest_sha256"`
	Suites          []string `json:"suites"`
	TestCommand     string   `json:"test_command"`
	IsolatedProfile string   `json:"isolated_profile,omitempty"`
	IsolatedCommand string   `json:"isolated_command,omitempty"`
	LiveProfile     string   `json:"live_profile,omitempty"`
	LiveCommand     string   `json:"live_command,omitempty"`
	RequiredSecrets []string `json:"required_secrets,omitempty"`
}

func main() {
	provider := flag.String("provider", "", "Provider identity, for example payment/stripe; omit with --all")
	mode := flag.String("mode", "deterministic", "deterministic, isolated, or live")
	all := flag.Bool("all", false, "verify every Provider")
	flag.Parse()
	if (*provider == "") == !*all {
		fatal(errors.New("set exactly one of --provider or --all"))
	}
	if *mode != "deterministic" && *mode != "isolated" && *mode != "live" {
		fatal(fmt.Errorf("unsupported mode %q", *mode))
	}
	identities := []string{*provider}
	if *all {
		matches, err := filepath.Glob("providers/*/*/verification.json")
		if err != nil {
			fatal(err)
		}
		identities = identities[:0]
		for _, path := range matches {
			identities = append(identities, filepath.ToSlash(strings.TrimSuffix(strings.TrimPrefix(path, "providers/"), "/verification.json")))
		}
		sort.Strings(identities)
	}
	if len(identities) == 0 {
		fatal(errors.New("no Provider verification manifests found"))
	}
	for _, identity := range identities {
		if err := verify(identity, *mode); err != nil {
			fatal(err)
		}
	}
	fmt.Printf("Provider release verification passed (%s): %d Provider(s)\n", *mode, len(identities))
}

func verify(identity, mode string) error {
	if identity == "" || filepath.Clean(identity) != identity || strings.Count(identity, "/") != 1 || strings.Contains(identity, "..") {
		return fmt.Errorf("invalid Provider identity %q", identity)
	}
	path := filepath.Join("providers", identity, "verification.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	var m manifest
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&m); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	expectedCommand := "go test ./providers/" + identity
	if m.ContractVersion != "domainry-provider-verification-v1" || m.Profile == "" || m.Mode != "deterministic" || m.TestCommand != expectedCommand {
		return fmt.Errorf("%s has invalid deterministic certification metadata", path)
	}
	required := []string{"adapter_contract", "error_classification", "network_failure_semantics", "request_translation", "response_normalization", "secret_non_disclosure"}
	for _, suite := range required {
		if !contains(m.Suites, suite) {
			return fmt.Errorf("%s does not certify %s", path, suite)
		}
	}
	if !sort.StringsAreSorted(m.Suites) {
		return fmt.Errorf("%s suites are not sorted", path)
	}
	command := exec.Command("go", "test", "./providers/"+identity)
	if mode == "isolated" {
		if m.IsolatedProfile == "" || m.IsolatedCommand != expectedCommand {
			return fmt.Errorf("Provider %s has no isolated certification profile", identity)
		}
	}
	if mode == "live" {
		if m.LiveCommand == "" || m.LiveProfile == "" {
			return fmt.Errorf("Provider %s has no live certification profile; deterministic certification remains available", identity)
		}
		for _, secret := range m.RequiredSecrets {
			if os.Getenv(secret) == "" {
				return fmt.Errorf("Provider %s live certification requires %s", identity, secret)
			}
		}
		fields := strings.Fields(m.LiveCommand)
		if len(fields) < 2 || fields[0] != "go" || fields[1] != "run" {
			return fmt.Errorf("Provider %s live command is not an approved go run command", identity)
		}
		command = exec.Command(fields[0], fields[1:]...)
	}
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("Provider %s %s certification failed: %w", identity, mode, err)
	}
	return nil
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
