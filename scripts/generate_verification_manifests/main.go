// Command generate_verification_manifests creates the mandatory deterministic
// release-certification manifest for every Provider package. Existing live
// commands and secret requirements are preserved.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const contractVersion = "domainry-provider-verification-v1"

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
	check := flag.Bool("check", false, "fail if any generated manifest is missing or stale")
	flag.Parse()
	directories, err := filepath.Glob("providers/*/*")
	if err != nil {
		fatal(err)
	}
	seen := 0
	for _, directory := range directories {
		tests, globErr := filepath.Glob(filepath.Join(directory, "*_test.go"))
		if globErr != nil {
			fatal(globErr)
		}
		if len(tests) == 0 {
			continue
		}
		relative := filepath.ToSlash(strings.TrimPrefix(directory, "providers/"))
		path := filepath.Join(directory, "verification.json")
		current := manifest{}
		if raw, readErr := os.ReadFile(path); readErr == nil {
			if err := json.Unmarshal(raw, &current); err != nil {
				fatal(fmt.Errorf("decode %s: %w", path, err))
			}
		}
		current.ContractVersion = contractVersion
		current.Profile = strings.ReplaceAll(relative, "/", "_") + "_deterministic_v1"
		current.Mode = "deterministic"
		current.ManifestSHA256 = ""
		current.TestCommand = "go test ./providers/" + relative
		current.IsolatedProfile = strings.ReplaceAll(relative, "/", "_") + "_isolated_v1"
		current.IsolatedCommand = current.TestCommand
		current.Suites = merge(current.Suites, []string{"adapter_contract", "error_classification", "network_failure_semantics", "request_translation", "response_normalization", "secret_non_disclosure"})
		if _, webhookErr := os.Stat(filepath.Join(directory, "webhook.go")); webhookErr == nil {
			current.Suites = merge(current.Suites, []string{"webhook_signature_and_normalization"})
		}
		if relative == "payment/stripe" && current.LiveCommand != "" {
			current.LiveProfile = "stripe_japan_online_v1"
		}
		sort.Strings(current.Suites)
		raw, err := json.MarshalIndent(current, "", "  ")
		if err != nil {
			fatal(err)
		}
		raw = append(raw, '\n')
		if *check {
			committed, readErr := os.ReadFile(path)
			if readErr != nil || !bytes.Equal(committed, raw) {
				fatal(fmt.Errorf("%s is missing or stale; run go run ./scripts/generate_verification_manifests", path))
			}
		} else if err := os.WriteFile(path, raw, 0o644); err != nil {
			fatal(err)
		}
		seen++
	}
	if seen == 0 {
		fatal(fmt.Errorf("no Provider packages found"))
	}
}

func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }

var isolatedProviders = map[string]bool{
	"document_generation/pdf": true, "email/smtp": true,
	"external_database/mysql": true, "external_database/postgres": true,
	"external_database/sqlserver": true, "external_database/bigquery": true,
	"external_database/snowflake": true, "file_storage/local": true,
	"iot_telemetry/mqtt": true, "webhook/http": true,
	"file_storage/s3": true, "iot_telemetry/aws_iot": true,
	"iot_telemetry/datadog_metrics": true, "mcp_tool/mcp": true,
	"notification/web_push": true, "observability_compliance/prometheus": true,
	"whatsapp/meta_cloud_api": true, "social_message/facebook_messenger": true,
	"support/zendesk": true, "lead_source/tally": true,
	"website_form/tally": true, "product_event/posthog": true,
	"weather/open_meteo": true, "map_address/nominatim": true,
	"calendar/holidays_jp": true, "openai/openai": true,
	"logistics/dhl": true, "logistics/fedex": true, "logistics/ups": true,
	"logistics/sf_express": true, "logistics/freightos": true,
	"logistics/shopify_fulfillment": true, "logistics/authorized_carrier_gateway": true,
	"help_center/confluence": true, "help_center/gitbook": true,
	"help_center/help_scout_docs": true, "help_center/intercom_articles": true,
	"help_center/notion_docs": true, "help_center/readme": true,
	"help_center/zendesk_guide": true, "esign/adobe_sign": true,
	"esign/docusign": true, "esign/fadada": true, "esign/signnow": true,
	"customer_feedback/delighted": true, "customer_feedback/surveymonkey": true,
	"customer_feedback/trustpilot": true, "customer_feedback/typeform": true,
}

func merge(groups ...[]string) []string {
	unique := map[string]bool{}
	for _, group := range groups {
		for _, value := range group {
			if value != "" {
				unique[value] = true
			}
		}
	}
	values := make([]string, 0, len(unique))
	for value := range unique {
		values = append(values, value)
	}
	return values
}
