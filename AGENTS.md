# Repository guide

- Do not use `domainry-builder-v1` to develop this repository.
- Provider packages may depend on `domainry-connector-sdk` but must never import `domainry-plane/internal/**`.
- Provider packages must not import one another.
- Production outbound I/O must use Runtime-owned SDK transports.
- Do not start multiple local service instances. Connector tests should use isolated test servers or deterministic fakes.
