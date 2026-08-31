# Domainry Connectors

This repository owns official Domainry Connector product definitions and
Provider implementations. Each Provider is an independently importable Go
package and is linked into a project Runtime only when generated project
composition imports its factory.

The repository does not own Runtime governance or the public Connector SPI.
Providers depend on `github.com/domainry/domainry-connector-sdk` and receive
bounded outbound transport capabilities from deployment composition; Runtime
does not own their product definitions or implementations.

## Package rules

- One stable Connector/Provider identity per Provider package.
- No Provider-to-Provider imports.
- No imports from `domainry-plane/internal/**`.
- No implicit global registration or blank-import side effects.
- Descriptor and operation identities must pass SDK contract tests.

## Source layout

- `internal/domain/connector`: catalog models, repository ports, and domain validation.
- `internal/application/connector`: catalog discovery use cases.
- `internal/adapter/connectorsdk`: adapter to the public Connector SDK Registry.
- `internal/assembly/module`: in-process composition for a selected Provider set.
- `internal/infrastructure/catalog`: embedded catalog repository implementation.
- `module`: stable public in-process factory facade.
- `providers/<connector-key>/<provider-key>`: stable, independently importable Provider entrypoints.
- `catalog`: public Connector definition and Provider release catalog contracts.

## Development

```sh
make fmt-check
make test
make vet
make boundary
make catalog-check
make license-check
make dependency-license-check
make vulnerability-check
```

`make release-check` runs every required release gate. Releases depend on an
immutable tagged SDK version and publish a deterministic Catalog identity.

## License

This repository is proprietary and closed source. No public license is
granted; see [LICENSE](LICENSE) for the governing terms. Third-party dependency
licenses are audited separately by `make dependency-license-check`.
