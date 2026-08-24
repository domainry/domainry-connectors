# Repository architecture

`domainry-connectors` is the source owner for official external Provider
implementations. It is a library collection, not a Runtime and not a service.

## Package layout

```text
catalog/                              machine-readable discovery metadata
providers/<connector-key>/<provider-key>/
                                      one external Provider implementation
internal/<protocol-or-helper>/         repository-private reuse with two or
                                      more proven consumers
```

The two identity path segments under `providers/` match the stable
`ConnectorKey` and `ProviderKey` exactly. A Go package may use the idiomatic
identifier without underscores; for example, path `weather/open_meteo` uses
package name `openmeteo`. Catalog entries always carry both stable keys and
the full import path, so package spelling is never used as runtime identity.

Do not add `pkg/`, `shared`, `common`, category-level implementation packages,
or Provider-to-Provider imports. Shared code moves to `internal/` only after a
second real Provider proves the abstraction.

## Provider entrypoint

Every Provider package publishes:

```go
func New(connector.Transport) (connector.Adapter, error)
```

`New` is intentionally short because the import path already names the exact
Provider. Constructors receive all outbound authority explicitly and perform
no I/O. Provider packages may additionally export typed operation contracts
for generated callers and tests.

No package registration occurs in `init`. Runtime composition imports exact
selected packages and calls `New` explicitly.

## Release verification boundary

Every Provider commits a `verification.json` beside its implementation. Catalog
generation refuses to publish a Provider without one and hashes the manifest
into the immutable Provider entry. Every manifest names a mandatory,
credential-free deterministic contract gate. Providers with a safe sandbox or
controlled test tenant may additionally name an opt-in live command and its
required test-only credentials. Absence of a live command is explicit and must
never be presented as live certification.

Run one gate with `go run ./scripts/verify_provider_release --provider
payment/stripe --mode deterministic`, or all deterministic gates with
`go run ./scripts/verify_provider_release --all --mode deterministic`. Live
mode is deliberately opt-in and fails closed when no live profile or credential
is declared.

Protocol and local Providers may additionally declare an `isolated_profile`.
This means their complete external boundary is exercised with an isolated test
service or deterministic Runtime-transport fake; it is not a claim of external
live-account verification.

Provider-specific API translation, regional payment methods, refunds,
reconciliation, webhook signatures, and remote error classification are tested
here. Runtime consumes the immutable Catalog entry and runs only generic host
acceptance: construction, governed transport, secret injection, durable retry,
ingress, replay protection, audit, and writeback. Runtime must not maintain a
second provider-specific live matrix.

## Ownership boundary

Provider code owns protocol translation, Provider-specific validation,
signature verification, response normalization, and stable Provider errors.
Runtime owns connection persistence, authorization, secret resolution,
outbound policy, HTTP and SQL clients, retries, durable delivery, ingress,
replay protection, audit, and lifecycle.

Committed Provider code may depend on the tagged Connector SDK and
Provider-specific protocol libraries. It must never import Plane or another
Provider package.
