# TPC-DS load benchmarks

Records how long a production materialization took to load the full TPC-DS
dataset from a `source-tpc-ds` capture, and checks that every table received
exactly the number of documents dsdgen produces at that scale factor. It uses
only Flow's specs and per-transaction stats via `flowctl`, so it works for any
materialization connector and never touches the destination.

```bash
flowctl auth login   # once
go run ./tests/benchmark/tpcds -materialization estuary/bench-tpc-100/materialize-databricks
```

The tool pulls the materialization's spec, follows `source.capture` to the
capture, reads the capture's `scale`, maps collections to TPC-DS tables from the
capture bindings, and replays `flowctl raw stats` for both tasks. A table is
complete at the first transaction whose cumulative stored documents reach the
oracle count; the run is complete when every table is. The materialization is
timed from the first transaction that stored anything to the last table's
completion. An incomplete run prints its progress and records nothing. A table
with more documents than dsdgen produces fails the run.

Stats are only retained for a limited time, so record a run soon after it
finishes; `-since` (default 14d) must cover the whole run.

## Results

Complete runs are written to
`results/<connector>/sf<scale>/<completion time>-<image tag>.json` and are
meant to be committed, so the same connector and scale can be compared across
connector versions. Each file holds the images, the scale, per-table expected
and observed counts with completion times, and for both tasks the start and
end timestamps, document and byte totals, transaction count and throughput.

## Oracles

`oracle/sf<scale>.json` holds dsdgen's exact row count per table at that scale
(from the estuary/tpcds-kit fork the connector ships). Scales 0.01, 1, 10 and
100 are included. To add one, stream every parent table through dsdgen at that
scale and count lines per table, splitting the returns tables from their sales
parent by field count; at scale 1000 that is a few hours on one machine.

## Offline use

`-from-dir DIR` reads `flow.yaml` (as written by `flowctl catalog pull-specs`
for both tasks), `mat-stats.jsonl` and `cap-stats.jsonl` from `DIR` instead of
calling `flowctl`.
