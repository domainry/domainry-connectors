# Domainry Connectors

This repository owns official Domainry Connector Provider implementations.
Each Provider is an independently importable Go package and is linked into a
project Runtime only when generated project composition imports its factory.

The repository does not own Runtime governance or the public Connector SPI.
Providers depend on `github.com/domainry/domainry-connector-sdk` and use
Runtime-owned transports for outbound effects.

## Package rules

- One stable Connector/Provider identity per Provider package.
- No Provider-to-Provider imports.
- No imports from `domainry-plane/internal/**`.
- No implicit global registration or blank-import side effects.
- Descriptor and operation identities must pass SDK contract tests.

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
