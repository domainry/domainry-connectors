# Composio SaaS tools

`saas_tool/composio` implements Composio v3.1 through the Runtime-owned HTTP
transport. It binds existing Composio connected accounts and explicitly mapped
tool aliases. It does not import a Python/TypeScript SDK or start a sidecar.

## Connection setup

1. In your Composio project, configure the application's auth config and connect
   the intended account. OAuth consent and token refresh stay with Composio.
2. Record the account's `connected_account_id`, exact owning `user_id`, and
   toolkit slug. Use a stable owner ID that includes your tenant/workspace
   namespace. A shared Domainry connection still identifies its Composio owner;
   Domainry connection permissions control who can use it.
3. Create a Domainry secret containing the Composio **project API key**, and bind
   that secret reference as `api_key`. Never put the key or OAuth credentials in
   Blueprint metadata or connection config.
4. Select `connector_key: saas_tool`, `provider_key: composio`, revision `1.0.0`.
   Set the connection config using the [Slack example](../examples/composio/slack-connection.json) below. Replace the example
   identity values and secret reference with your actual records.
5. Check the selected tools' schemas in Composio at the pinned toolkit version.
   The connection owner reviews and declares each mapping's `read` or `write`
   effect. Changing mappings changes the capabilities of that connection.

```json
{
  "key": "slack_tools",
  "workspace_id": "workspace_1",
  "connector_key": "saas_tool",
  "provider_key": "composio",
  "config": {
    "base_url": "https://backend.composio.dev",
    "connected_account_id": "ca_REPLACE_WITH_ACCOUNT_ID",
    "user_id": "workspace_1:owner_1",
    "toolkit": "slack",
    "timeout_seconds": 60,
    "tools": {
      "search_messages": {
        "slug": "SLACK_SEARCH_MESSAGES",
        "version": "20260826_00",
        "effect": "read"
      },
      "send_message": {
        "slug": "SLACK_SEND_MESSAGE",
        "version": "20260826_00",
        "effect": "write"
      }
    }
  },
  "secret_refs": {
    "api_key": "REPLACE_WITH_DOMAINRY_SECRET_REFERENCE"
  }
}
```

The Slack documentation listed version `20260826_00` on 2026-09-09. Verify the
desired tools against that version before using the example. The provider
requires explicit dated versions and rejects `latest`, duplicate tool slugs,
unknown mapping fields, and `COMPOSIO_*` router meta tools. One connection holds
one account and toolkit, with 1–64 tool aliases. Another app uses another
connection and its own mappings.

## Operations

| Operation | Runtime mode | Allowed mapping | Result |
| --- | --- | --- | --- |
| `test_connection` | `call` | No tool execution | Active account, owner and toolkit identity |
| `query_tool` | `call` | `read` | `data` and `log_id` |
| `start_tool` | `start_operation` | `write` | Runtime operation ID; delivery stores the Composio log reference |
| `enqueue_tool` | `enqueue` | `write` | Runtime receipt; delivery stores the Composio log reference |

Tool operations accept `tool_key` and `arguments`. For example, an operation
invocation can select `tool_key: search_messages` with the structured arguments
required by the pinned Slack search schema. The caller cannot override the
endpoint, connected account, user, tool slug, version or effect. Arguments are
passed as JSON without converting large integers through floating point.

Use generated operation clients inside business Actions. `enqueue_tool` stages
the existing Runtime durable intent and only executes through durable delivery;
it does not return upstream business data. Use `start_tool` when the Action needs an operation ID for tracking.
Current Plane permits only read effects in synchronous Action calls; writes
therefore always use durable delivery. For reads, the generic adapter
keeps the upstream `data` value; business Actions must validate any fields they
depend on. App-specific typed projections can be layered over these mappings.

Before every execution the provider reads the configured connected account and
checks its ID, owning user, toolkit, active state, account disable flag and auth
config disable flag. Raw account credentials and upstream error bodies are not
returned. The account read and tool execution share one total deadline.

## Failure and delivery semantics

- Read transport failures, HTTP 408/5xx and invalid responses are retryable.
- Account mismatch, inactive accounts, HTTP 401/403 and rejected arguments are
  permanent failures that require configuration or authorization changes.
- HTTP 429 is retryable. Provider code performs no internal retry loop.
- Once a write is dispatched, transport failures, HTTP 408/5xx, malformed
  success responses, and tool-level failures are **uncertain**. A tool-level
  failure can follow partial execution. Runtime must not blindly replay it.
- Writes declare no general idempotency, compensation or reconciliation
  guarantee. A Domainry request reference or Composio log ID is not an upstream
  idempotency key. Review the log and external record before manual recovery.
- The connection test checks account state only; it does not prove every tool's
  scopes, pinned version, input schema or downstream result semantics.

The default origin is Composio Cloud. A configured HTTPS origin can point to an
enterprise deployment; only loopback origins may use HTTP. Request bodies are
bounded to 1 MiB, responses to 4 MiB, and the total timeout to 1–300 seconds.

## Read-only live probe

Save an actual connection document in a private local file and set
`COMPOSIO_API_KEY` using your existing secret environment. Run:

```sh
go run ./scripts/verify_composio_connection --connection /absolute/path/connection.json
```

The command reads existing account state only. It does not send a Slack message,
execute a tool, create an account or expose account credentials.

## Delivery and scope

The implementation, typed product definition, operation hashes, verification
manifest and generated Catalog are source-owned by `domainry-connectors`.
Plane discovers this provider through `catalog.DefinitionDocuments()` after
consuming a released Connector module that contains it. Generated project
composition imports only the selected `providers/saas_tool/composio` package.
Release that source as an immutable module version before updating Plane and
project dependency locks; the existing `v0.1.0` release does not contain it.

This integration reuses already authorized Composio accounts. Embedded OAuth
onboarding, Trigger subscription/webhook ingestion, and replacement of existing
native Providers are separate work; they are not enabled by this provider.

## Cross-repository verification

From the Plane repository, verify the actual definition, operation hashes,
Action execution modes, generated clients and selected Provider composition:

```sh
DOMAINRY_EXTERNAL_CONNECTOR_DEFINITION="$PWD/../domainry-connectors/catalog/definitions/saas_tool/connector.json" \
DOMAINRY_OFFICIAL_CONNECTOR_CATALOG="$PWD/../domainry-connectors/catalog/catalog.json" \
go test ./internal/controlplane/domaincodegen -run '^TestExternalConnectorCatalogGeneratesSelectedClients$' -count=1
```

## References

- [Execute tool v3.1](https://docs.composio.dev/reference/api-reference/tools/postToolsExecuteByToolSlug)
- [Connected account details v3.1](https://docs.composio.dev/reference/api-reference/connected-accounts/getConnectedAccountsByNanoid)
- [Toolkit versioning](https://docs.composio.dev/docs/tools-direct/toolkit-versioning)
- [Slack toolkit](https://docs.composio.dev/toolkits/slack)
