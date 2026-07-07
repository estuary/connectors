# Runtime v2 scale-out tests

End-to-end tests for connectors that support multi-shard ("scale-out")
operation under the v2 runtime. The harness drives the connector with **two
shards across two sessions** using `flowctl raw preview-next`, sourcing from a
real Flow collection.

Two sessions are the essential part: runtime v2 halts a session after its
final commit *without* running the post-commit Acknowledge — applying that
committed transaction is left to the next session's recovery. So session two
exercises the full scale-out recovery path: every shard receives the
consolidated connector state, the non-primary shards discard it, and the
primary shard replays every shard's staged work into the destination.

Each connector's test asserts, at minimum:

- the run completes without connector errors;
- session two performed a recovery commit;
- the destination contains the recovered data with no duplicate keys.

## Running locally

```bash
CONNECTOR=materialize-databricks tests/scale-out/run.sh
```

Requirements:

- `flowctl` with the `raw preview-next` subcommand on `$PATH` (any recent
  `dev-next` release, or a local build of estuary/flow).
- `sops` with GCP credentials able to decrypt the connector's
  `testdata/config.local.yaml`.
- A logged-in `flowctl` profile (or `FLOW_AUTH_TOKEN`) with read access to the
  `demo/` collections, used by preview-next to read source journals.

## CI

The `Runtime v2 scale-out tests` step in `.github/workflows/ci.yaml` runs this
harness for each connector listed in its `if:` gate. It authenticates journal
reads with the `FLOW_SCALE_OUT_AUTH_TOKEN` repository secret, which must hold
a Flow refresh token (dashboard → Admin → CLI-API) for an account with read
capability on `demo/`.

## Adding a connector

Create `tests/scale-out/<connector>/` with:

- `setup.sh` — sourced by `run.sh`. Must write `${WORKDIR}/local.flow.yaml`
  (a materialization using the built connector binary at
  `${WORKDIR}/connector` as a `local:` endpoint, with its scale-out feature
  flag enabled and a unique destination resource name) and export
  `TASK_NAME`. May define a `test_cleanup` function, which runs on exit.
- `verify.sh` — sourced after a successful run. Asserts on destination
  contents; use `bail` to fail.

The connector itself must implement the scale-out contract: range-scoped
connector-state checkpoints emitted as merge patches, and shard-zero-executes
semantics via `m.AcknowledgeStatePatcher` (see materialize-databricks).
