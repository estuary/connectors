# Materialization benchmarks

Drive any `materialize-*` connector with a configured-size workload and
record throughput. Built on `flowctl raw preview-next --fixture`,
mirroring the integration-test framework under `tests/materialize/` but
designed for many-GB workloads.

The default workload is the TPC-DS dataset, streamed from the
`source-tpc-ds` capture: 24 related tables with realistic column types and
document sizes, at a scale factor that fixes the volume. Use it unless the
question is about updates and deletes, which the synthetic scenarios model
with fine-grained control over transaction sizes, document sizes, and overlap
against earlier transactions.

## Quick start

```bash
# One-time:
pip3 install pyyaml

# Load TPC-DS at scale 0.01 into materialize-clickhouse:
./tests/benchmark/materialize/run.sh \
    --connector materialize-clickhouse \
    --scenario  tests/benchmark/materialize/scenarios/tpcds-smoke.yaml
```

This brings up the connector's existing
`tests/materialize/<connector>/docker-compose.yaml`, generates a
materialization spec from the connector's existing
`<connector>/testdata/config.local.yaml`, streams the fixture
through a FIFO into `flowctl raw preview-next`, and writes per-run artifacts
(spec, generator state, preview log, `results.json`) to
`tests/benchmark/materialize/runs/<timestamp>-<connector>/`.

For cloud-only endpoints (BigQuery, Snowflake, …) pass a `--config`
pointing at a real credentials file. If no `docker-compose.yaml` is
found for the connector, the script assumes the endpoint is already
reachable.

Check the config's feature flags before benchmarking. Integration-test
configs often set `allow_existing_tables_for_new_bindings` (or
`retain_existing_data_on_backfill`), and with either flag the connector
tells the runtime to disable its load optimization, so the runtime issues a
`Load` for every key in every transaction — even against a table the run
just created. That load phase then dominates small-document runs and
measures the runtime rather than the connector. Make a copy with the flag
removed and pass it with `--config`; the copy stays encrypted:

```bash
cp materialize-redshift/testdata/config.local.yaml /tmp/config.bench.yaml
sops set /tmp/config.bench.yaml '["advanced"]["feature_flags"]' '""'
```

## TPC-DS scenarios

```yaml
tpcds:
  scale: 1                 # source-tpc-ds scale factor, 0.01 and up
  transaction_size: 1GB    # commit after this many bytes of documents
  tables: [store_sales, store_returns]   # optional; default all 24
  resource: { delta_updates: true }      # optional; merged into every binding
```

| Scenario | Scale | Documents |
|---|---|---|
| `tpcds-smoke.yaml` | 0.01 | 278K (112 MB) |
| `tpcds-sf1.yaml`   | 1    | 19.6M (5.9 GB) |
| `tpcds-sf10.yaml`  | 10   | 191M |
| `tpcds-sf100.yaml` | 100  | 959M |

The collections are `bench/tpcds/<table>`, with the schemas and keys that
`source-tpc-ds` discovers, and each binding's table is the TPC-DS table name.
During the run `tpcds.py relay` drives the capture with
`flowctl raw preview-next` and copies its documents into the fixture,
closing a transaction once it holds `transaction_size` bytes of documents.
All tables generate concurrently, so a transaction holds a mix of them, and
the run ends when the capture has emitted every table in full. Documents are
inserts only and every key is unique.

The capture runs `$TPCDS_IMAGE` (default `ghcr.io/estuary/source-tpc-ds:dev`).
To use local changes to the connector, build it and point the variable at it:

```bash
docker build --platform linux/amd64 -t ghcr.io/estuary/source-tpc-ds:local -f source-tpc-ds/Dockerfile .
TPCDS_IMAGE=ghcr.io/estuary/source-tpc-ds:local ./tests/benchmark/materialize/run.sh ...
```

A live capture produces about 45 MB/s, bounded by the capture preview's
single busy flowctl thread on an M-series Mac and a c3-standard-22 VM alike,
and shares the machine with the materialization, so a fast connector can end
up measuring the capture. Pass `--tpcds-cache DIR` to
generate the fixture into `DIR` before the preview starts and replay it from
disk; later runs of the same workload and image reuse the file, so every
connector sees the same transactions. It needs the dataset's size in free disk.
The cache is keyed on the image reference, so delete it after rebuilding a
`:local` image.

`state.json` and `results.json` record per-table document and byte totals and
whether the capture finished (`tpcds.complete`). A run whose capture fails
before finishing exits non-zero.

The flowctl 0.6.13 release panics in `preview-next --fixture` once a
transaction holds more than 65,536 documents
(`assertion failed: entries.len() <= 1 << 16`) and then hangs. TPC-DS
transactions always do, so use a flowctl built from Flow master.

## Synthetic scenarios

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
        range: [0.6, 1.0]      # (optional) restrict sampling to the last 40%
                               # of tx 0's keys. Models recency-biased updates
                               # (newer records get updated more than old).
                               # Defaults to [0.0, 1.0] (full range).
        distribution: zipfian  # (optional) sampling distribution within the
                               # (sub-)range. "uniform" (default) or "zipfian".
                               # Zipfian concentrates picks on a few keys near
                               # zipf_bias's end with a long tail decaying
                               # across the rest -- a closer match to many
                               # real workloads (a hot recent set + occasional
                               # touches of older rows) than pure uniform.
        zipf_s: 1.0            # (optional) Zipfian skew exponent; default 1.0.
                               # Larger s = more concentrated. s ~ 0 ≈ uniform.
        zipf_bias: tail        # (optional) "tail" (default) or "head" --
                               # which end of the (sub-)range gets rank 1
                               # (the highest-weight key).
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
* `overlap.range` (optional) is a `[lo, hi]` pair of fractions in `[0, 1]`
  with `lo < hi`. Restricts sampling to a sub-range of the source tx's
  keys: e.g. `[0.6, 1.0]` for the last 40% (recency-biased updates),
  `[0.0, 0.1]` for the first 10% (cold-data updates). Each transaction
  emits `fresh` keys sequentially, so a sub-range maps to a contiguous
  slice of the id space — useful for exercising partition pruning on
  destinations that cluster on the key column.
* `overlap.distribution` (optional) is `uniform` (default) or `zipfian`.
  `zipfian` weights each key in the (sub-)range by `1/rank^s` so a small
  set of keys near `zipf_bias` (default `tail`) gets the bulk of the
  picks, with a long tail trailing across the rest. This is closer to
  many production workloads than pure uniform: most updates land on the
  newest rows, but a few reach back into older history. Composes with
  `range` — e.g. `range: [0.0, 1.0]` + `distribution: zipfian` says
  "pull from anywhere in the prior tx, but mostly from the tail",
  whereas `range: [0.6, 1.0]` + `distribution: zipfian` further
  restricts the candidate window. Tunables: `zipf_s` (skew exponent,
  default `1.0`; larger = more concentrated; `~0` ≈ uniform) and
  `zipf_bias` (`tail` or `head`).

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
--tpcds-cache DIR      TPC-DS only: generate the fixture into DIR once and replay it
```

## Output

Each run produces:

| File | Contents |
|---|---|
| `flow.yaml`        | The materialization spec rendered from the scenario. |
| `state.json`       | Per-tx fresh-key range and overlap counts (from the generator), or per-tx and per-table counts for TPC-DS. |
| `generator.stderr` | Generator stderr / errors. |
| `capture.flow.yaml`, `capture.log` | TPC-DS only: the `source-tpc-ds` capture spec and its preview log. |
| `preview.stdout`   | Preview stdout. |
| `preview.log`      | Preview stderr (the per-tx timings are derived from here). |
| `results.json`     | Aggregate `{ wall_seconds, total_docs, total_bytes, mb_per_sec, transactions: [...] }`. |

## Cleanup

A run names its task `bench/<connector>_flow_test_<timestamp>` and removes what
it created.

If a run is killed outright, its table survives until the next run of that
scenario drops it, and its task metadata until swept:

```bash
go run ./tests/materialize/testctl \
    -connector materialize-redshift \
    -config materialize-redshift/testdata/config.local.yaml \
    -resource <a resource config from a run directory> \
    -mode sweep
```

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
cd tests/benchmark/materialize && python3 -m unittest test_generate test_results test_tpcds -v
```

## Comparing two connector configurations

To A/B a connector-side change that is gated on a feature flag, run the same
scenario twice against separate tables, once per flag value. The flag goes into
a copy of the encrypted config with `sops set`, which needs no decryption:

```bash
cp materialize-databricks/testdata/config.local.yaml /tmp/config.flag.yaml
sops set /tmp/config.flag.yaml '["advanced"]["feature_flags"]' '"some_flag"'

for arm in base:materialize-databricks/testdata/config.local.yaml flag:/tmp/config.flag.yaml; do
  ./tests/benchmark/materialize/run.sh \
      --connector materialize-databricks \
      --scenario  tests/benchmark/materialize/scenarios/uuid-uniform-3tx.yaml \
      --config    "${arm#*:}" \
      --table-suffix "_${arm%%:*}" \
      --shard-log-level info \
      --out-dir   "tests/benchmark/materialize/runs/$(date +%Y%m%d-%H%M%S)-databricks-${arm%%:*}"
done
```

Run the arms serially on one machine so they share the feeder and network.
`results.py` measures whole transactions, which include the fixture feeder and
the connector's uploads; the warehouse-side work a staging or query change
moves is read from the connector's own phase logs instead, which is why the
runs above raise the shard log level:

```bash
python3 tests/benchmark/materialize/phases.py runs/*-databricks-base/preview.log runs/*-databricks-flag/preview.log
```

This prints, per round, the load-query time and the commit (COPY INTO / MERGE)
time. For the Databricks arms, also pull the query profiles of the MERGE and
load queries from the SQL warehouse's query history (filter on the
`materialization_name` query tag) to compare Photon coverage and bytes read.
