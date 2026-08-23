.PHONY: test fmt-check vet boundary

test:
	go test ./...

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
