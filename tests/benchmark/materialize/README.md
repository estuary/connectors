# Materialization benchmarks

Drive any `materialize-*` connector with a configured-size workload and
record throughput. Built on top of `flowctl preview --fixture`, mirroring
the integration-test framework under `tests/materialize/` but designed
for many-GB workloads with fine-grained control over transaction sizes,
document sizes, and overlap (updates / deletes against earlier
transactions).

## Quick start

```bash
# One-time:
pip3 install pyyaml

# Run the smoke scenario against materialize-clickhouse:
./tests/benchmark/materialize/run.sh \
    --connector materialize-clickhouse \
    --scenario  tests/benchmark/materialize/scenarios/small-smoke.yaml
```

This brings up the connector's existing
`tests/materialize/<connector>/docker-compose.yaml`, generates a
materialization spec from the connector's existing
`<connector>/testdata/config.local.yaml`, streams a synthetic fixture
through a FIFO into `flowctl preview`, and writes per-run artifacts
(spec, generator state, preview log, `results.json`) to
`tests/benchmark/materialize/runs/<timestamp>-<connector>/`.

For cloud-only endpoints (BigQuery, Snowflake, …) pass a `--config`
pointing at a real credentials file. If no `docker-compose.yaml` is
found for the connector, the script assumes the endpoint is already
reachable.

## Scenario file

```yaml
collections:
  - name: bench/wide
    schema: { type: object, properties: { id: {type: integer},
                                          val: {type: integer},
                                          payload: {type: string},
                                          _meta: {type: object,
                                                  properties: {op: {type: string}}} },
              required: [id] }
    key: [/id]
    # Optional. Defaults: id, payload.
    # key_field:     id
    # payload_field: payload
    # Optional. Key type: "integer" (default) or "uuid".
    # key_type: uuid
    # Optional. Connector-specific resource fields; defaults to {table: <basename>}.
    # resource: { table: my_table, schema: public }

transactions:
  - doc_count: 10_000_000      # Specify any TWO of doc_count / doc_size / total_size.
    doc_size:  1KB             # The third is derived.
    op: c                      # Operation for the "fresh" remainder. Default: c.

  - total_size: 30GB
    doc_size:   1KB
    overlaps:                  # Each entry samples without replacement from the
      - with: 0                # referenced earlier transaction's key range.
        fraction: 0.20         # 20% of this tx updates keys from tx 0.
        op: u
      - with: 0
        fraction: 0.10
        op: d                  # 10% of this tx deletes keys from tx 0.
      # Remaining 70% are fresh creates with new sequential keys.
```

Sizing rules:

* **`doc_size` is exact.** The generator pads `payload_field` until the
  JSON-encoded document is exactly `doc_size` bytes (using a non-escaping
  ASCII alphabet so JSON length equals UTF-8 byte length).
* `doc_size` must accommodate the rest of the schema. If it's too small
  for a particular key (large keys take more digits), the generator
  raises a clear error.
* Sizes accept `B` / `KB` / `MB` / `GB` / `TB` suffixes (binary, base 1024).

Overlap rules:

* `overlap.with` must reference an earlier transaction.
* All `overlap.fraction`s within one transaction must sum to ≤ 1.0;
  the remainder are fresh creates with new keys.
* Within a single transaction, overlap key sets are pairwise disjoint
  (a key is never both updated and deleted in the same tx).

## CLI flags

```
--connector NAME       e.g. materialize-postgres (required)
--scenario  PATH       scenario yaml (required)
--config    PATH       endpoint config; default: <connector>/testdata/config.local.yaml
                       falling back to <connector>/testdata/config.yaml
--docker               run connector as Docker image (default: local binary)
--seed      N          fixture RNG seed; default: 0 (runs are byte-reproducible)
--keep                 don't tear docker-compose down on exit
--out-dir   DIR        output directory; default: runs/<ts>-<connector>
```

## Output

Each run produces:

| File | Contents |
|---|---|
| `flow.yaml`        | The materialization spec rendered from the scenario. |
| `state.json`       | Per-tx fresh-key range and overlap counts (from the generator). |
| `generator.stderr` | Generator stderr / errors. |
| `preview.stdout`   | flowctl preview stdout (apply actions, connector state). |
| `preview.log`      | flowctl preview stderr (per-tx commit boundaries live here). |
| `results.json`     | Aggregate `{ wall_seconds, total_docs, total_bytes, mb_per_sec, transactions: [...] }`. |

## Verifying overlap correctness

The fixture is fully deterministic given `--seed`. To sanity-check that a
connector applied an overlap correctly, look at `state.json`:

```json
{
  "index": 1,
  "fresh":    {"start": 10000000, "count": 7000000, "op": "c"},
  "overlaps": [{"op": "u", "with": 0, "count": 2000000},
               {"op": "d", "with": 0, "count": 1000000}]
}
```

Then query the destination: tx 0's key range is `[0, 10_000_000)`; after
tx 1, exactly 1M of those keys should be absent and 2M should have
updated `val`/`payload` columns.

## Reproducibility

```bash
diff \
  <(python3 generate.py --scenario s.yaml --seed 7) \
  <(python3 generate.py --scenario s.yaml --seed 7)
# (empty)
```

Different seeds produce different fixtures.

## Unit tests

```bash
python3 -m unittest tests.benchmark.materialize.test_generate -v
```
