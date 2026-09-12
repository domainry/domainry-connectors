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

The [llm-proxy Expense OCR integration](docs/expense-ocr-llm-proxy.md) describes
receipt recognition, connection configuration, and the required Runtime release.

## Development

The [Composio SaaS tools integration](docs/composio.md) covers pinned tool
mappings, connected accounts, synchronous calls and Runtime durable delivery.

The [Knowledge HTTP API integration](docs/knowledge-base.md) provides
document search, source fetch, binary document push, indexing status and deletion,
reusable by Agent and Runtime hosts. Hosts own document authorization and durable lifecycle work.

The Google Workspace provider exposes `mail_list`, `mail_search` and `mail_read`
through Connector SDK's independent `mail-read-v1` contract. Metadata-only grants
enable listing; searching and body reads require full mail read grants. Search
uses the explicitly declared `gmail` syntax. Lists fetch only headers, with
account/query-bound pagination; body reads extract bounded text from MIME,
exclude attachments and remote resources, and mark incomplete/truncated content.
All requests use host-owned Transport, including at most four separately stored
text body parts of at most 256 KiB each. Existing Gmail sync/send contracts retain
their identities and behavior; these new read operations do not send or save drafts.

Microsoft 365 exposes the same three read contracts with `graph-kql` search.
Metadata listing accepts delegated Mail.ReadBasic (including Shared); search and
body reads require delegated Mail.Read/ReadWrite or their Shared variants. Every
request opts into ImmutableId. Graph continuations preserve the original query
and can only target the same `/me/messages` endpoint; reaching the 1,000-result
search cap is explicitly incomplete. Mail.Read requests plain text, with bounded
HTML text conversion if Graph returns HTML. No attachment or external body link
is fetched. Application-only `.All`, Mail.Send and User.Read do not enable these
current-user content reads.

Public web access uses the independent `web/llm_proxy` Provider, selected with
`module.PublicWebProviders(hostTransport)`. Its `web_search` and `web_fetch`
operations implement Connector SDK `web`'s `public-web-read-v1` identities; the
generated Provider Catalog carries those exact hashes. The source definition is
the management display/configuration projection, not a replacement wire identity.
The Provider uses only host Transport and the fixed `/tool/web_search` and
`/tool/web_fetch_jina` routes, and does not depend on the Expense OCR Provider.
Configure `base_url`, `allowed_source_hosts` (1–16 exact public DNS names), and
the private `api_token` Passport credential. Optional processor and timeout are
administrator configuration, never operation input. The caller and host must
also authorize the service connection and proxy origin. Sources are checked
before fetch and on returned pages; search filters unauthorized sources. These
checks do not attest remote DNS or redirect policy. Search is ranked excerpts,
page completeness stays unknown, and UTF-8 truncation is explicit. Read effects
do not guarantee upstream billing deduplication; there are no automatic retries
or fabricated connection probes.

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
