# Connector integration tests

These tests run a connector's built Docker image under `flowctl raw preview-next` and compare the
documents it captures against a checked-in snapshot.

## Test tiers

Connectors here fall into one of two tiers:

- **Full capture** — `tests/run.sh` runs the connector end-to-end and snapshots its output. This is
  the tier for connectors whose only end-to-end coverage lives here.
- **Image smoke test** — `tests/smoke.sh` fetches the spec from the built image and checks that it
  is valid JSON. This is the tier for connectors which already have a thorough test suite of their
  own, driving the connector as a local command or an in-process driver. For those, the one thing
  their own suite can't tell you is whether the *image* is broken, and that is all this checks.

Every connector in the full-capture tier also runs the smoke test first, so a broken image fails in
about a second rather than after a capture.

## Adding a full-capture test

Each subdirectory here is a test of the source connector with the same name, and must contain:

- `setup.sh`: Executed to set up the test. Expected to export the env variables `CONNECTOR_CONFIG`
  and `RESOURCE`, which are used to generate the Flow catalog yaml. Can also export other env
  variables, for example so it can track things that need to be cleaned up.
- `cleanup.sh`: Cleans up any test resources. Can read any variables exported by `setup.sh` so it
  knows what to clean up.
- `snapshot.jsonl`: The documents the capture is expected to produce, one `["collection", {...}]`
  line each, sorted.

Run a test with:

```bash
CONNECTOR=source-s3 VERSION=local ./tests/run.sh
```

Intermediate data is written to `.build/tests/<connector>/`, including the generated Flow catalog
and the raw (pre-normalization) capture output.

## Snapshots

The capture runs as a single session of one transaction with `--delay 30s`, so that transaction
batches everything the connector produces within a 30 second window and then stops. All of it lands
in one transaction, so document order isn't guaranteed; `run.sh` sorts the output before comparing.

To regenerate a snapshot after an intentional change:

```bash
UPDATE_SNAPSHOTS=true CONNECTOR=source-s3 VERSION=local ./tests/run.sh
```

Review the resulting diff carefully. Because the snapshot is compared in full, a capture which was
cut short — by a connector too slow to drain inside the delay window, say — shows up as missing
lines rather than as an error, and regenerating would silently bake that truncation in.

Test resource names are pinned to constants so snapshots are stable. `source-gcs` is the exception:
its bucket lives in a real, shared GCP project where names are globally unique, so it keeps a random
suffix and `run.sh` rewrites the bucket component of `/_meta/file` before comparing. Giving it a
stable fixture bucket would remove that special case.

### Generating a snapshot from CI

Three of these fixtures are local docker-compose stacks, but `source-gcs` needs a real GCS bucket,
and all of them need the connector's image built. If you can't run the test locally, let CI run it:
a failing integration test uploads the capture output as a `capture-output-<connector>` artifact.
Download it, review `actual.jsonl`, and commit it as `tests/<connector>/snapshot.jsonl`.

Review it rather than committing it blind — see the truncation caveat above.
