.PHONY: test verify-providers live-stripe fmt-check vet boundary catalog-check license-check dependency-license-check vulnerability-check release-check

test:
	go test ./...

live-stripe:
	go run ./scripts/verify_provider_release --provider payment/stripe --mode live

verify-providers:
	go run ./scripts/generate_verification_manifests --check
	go run ./scripts/verify_provider_release --all --mode deterministic

fmt-check:
	@files="$$(find . -name '*.go' -not -path './.git/*' -print | xargs gofmt -l)"; \
	if [ -n "$$files" ]; then \
		printf 'gofmt needed:\n%s\n' "$$files" >&2; \
		exit 1; \
	fi

vet:
	go vet ./...

boundary:
	@deps="$$(go list -deps ./...)"; \
	if printf '%s\n' "$$deps" | grep -Eq '^github\.com/domainry/domainry-plane(/|$$)'; then \
		printf 'forbidden Plane dependency detected\n' >&2; \
		exit 1; \
	fi
	@if grep -R -n -E 'github\.com/domainry/domainry-connectors/providers/.+/.' providers --include='*.go'; then \
		printf 'Provider-to-Provider import detected\n' >&2; \
		exit 1; \
	fi
	@if grep -R -n -E 'http\.DefaultClient|http\.Client[[:space:]]*\{|sql\.Open\(|os\.Getenv\(|exec\.Command\(|plugin\.Open\(|"unsafe"' providers --include='*.go'; then \
		printf 'uncontrolled Provider capability detected\n' >&2; \
		exit 1; \
	fi

catalog-check:
	go run ./scripts/generate_verification_manifests --check
	go run ./scripts/generate_catalog --check

license-check:
	@grep -qx 'Domainry Connectors Proprietary License' LICENSE
	@grep -qx 'Copyright (c) 2026 Domainry. All rights reserved.' LICENSE

dependency-license-check:
	@report="$$(mktemp)"; errors="$$(mktemp)"; \
	trap 'rm -f "$$report" "$$errors"' EXIT; \
	if ! GOTOOLCHAIN=go1.26.6 go run github.com/google/go-licenses@v1.6.0 report ./... >"$$report" 2>"$$errors"; then \
		cat "$$errors" >&2; \
		exit 1; \
	fi; \
	unknown="$$(awk -F, '$$1 !~ /^github\.com\/domainry\// && $$3 == "Unknown" { print }' "$$report")"; \
	if [ -n "$$unknown" ]; then \
		printf 'unknown third-party dependency licenses:\n%s\n' "$$unknown" >&2; \
		exit 1; \
	fi

vulnerability-check:
	GOTOOLCHAIN=go1.26.6 go run golang.org/x/vuln/cmd/govulncheck@v1.1.4 ./...

release-check: fmt-check test vet boundary catalog-check license-check dependency-license-check vulnerability-check
