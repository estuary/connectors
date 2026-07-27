# Context Map

This repo holds two domains that share a build system and little else. Read the `CONTEXT.md` for the context you're working in — not both.

Consumer rules (when to read what, how ADR scoping works) live in [`docs/agents/domain.md`](docs/agents/domain.md).

## Captures

**Glossary:** [`docs/contexts/captures/CONTEXT.md`](docs/contexts/captures/CONTEXT.md)
**Decisions:** `docs/contexts/captures/adr/`

Connectors that read from a source system and emit documents into Flow collections — they implement [`capture.proto`](https://github.com/estuary/flow/blob/master/go/protocols/capture/capture.proto).

Code in this context:

| Path                | Role                                                    |
| ------------------- | ------------------------------------------------------- |
| `source-*/`         | Individual capture connectors (Python, Go, Rust)        |
| `estuary-cdk/`      | Python Connector Development Kit                        |
| `sqlcapture/`       | SQL CDC abstractions (source-postgres, source-mysql, …)  |
| `filesource/`       | File-based source abstractions (S3, GCS, HTTP)           |
| `source-boilerplate/` | Shared capture lifecycle and state handling            |

## Materializations

**Glossary:** [`docs/contexts/materializations/CONTEXT.md`](docs/contexts/materializations/CONTEXT.md)
**Decisions:** `docs/contexts/materializations/adr/`

Connectors that write Flow collections into a destination system — they implement [`materialize.proto`](https://github.com/estuary/flow/blob/master/go/protocols/materialize/materialize.proto). Each destination has its own DDL limitations; see [`docs/materialize/README.md`](docs/materialize/README.md) for the shared patterns.

Code in this context:

| Path                       | Role                                                  |
| -------------------------- | ----------------------------------------------------- |
| `materialize-*/`           | Individual materialization connectors                 |
| `materialize-sql/`         | SQL destination base library                           |
| `materialize-boilerplate/` | Shared transaction lifecycle, Apply, and state handling |
| `filesink/`                | File-based destination abstractions                    |

## Shared / system-wide

**Decisions:** `docs/adr/`

`go/` (auth, network-tunnel, schema-gen), `base-image/`, `tests/`, `scripts/`, and the top-level build tooling serve both contexts. A decision that has to hold on both sides of the protocol belongs here, not in a context.
