# source-tpc-ds

A capture that emits the [TPC-DS](https://www.tpc.org/tpcds/) benchmark
dataset: 24 related fact and dimension tables, one binding per table, at a
configurable scale factor. It exists to verify materializations end to end.
Load the dataset into a destination, run the 99 TPC-DS reference queries
there, and compare the results to the published answers.

The rows are exactly what TPC's `dsdgen` produces at the same scale factor.
They come from a patched build of the generator, run as a subprocess and read
row by row from its stdout, so the connector uses no disk and a few megabytes
of memory per table regardless of scale.

## Configuration

| Field   | Description |
|---------|-------------|
| `scale` | Scale factor. Values of 1 and above should be one of the benchmark's official factors (1, 3, 10, 30, 100, 300, 1000, 3000, 10000, 30000, 100000) and give roughly that many GB of raw data. Values from 0.01 up to 1 produce a proportionally smaller dataset for smoke tests and demos; fixed-size tables (`date_dim`, `time_dim`, `catalog_page`, `customer_demographics`, `household_demographics`, `income_band`, `ship_mode`) keep their full size. Below 0.01 dsdgen's day-by-day search for each sale's date becomes pathologically slow, so smaller values are rejected. Default 1. |

Each binding's resource config is `{"table": "<name>"}`. Discovery lists all
24 tables, enabled, keyed on the specification's primary keys and named after
the table. Disable the bindings you do not need; a returns table works without
its sales parent enabled.

Changing `scale` on a running task fails at startup with an error asking you
to backfill the bindings, so a destination never holds rows from two datasets.

## Typing

Column names are the specification's, unchanged (`ss_sold_date_sk`, ...).

| DDL type            | Document value                 | Schema                                |
|---------------------|--------------------------------|---------------------------------------|
| `integer`           | JSON integer                   | `type: integer`                       |
| `decimal(p,s)`      | exact decimal string, `"11.41"` | `type: string, format: number`        |
| `date`              | `"1998-12-31"`                 | `type: string, format: date`          |
| `char(n)`, `varchar(n)` | JSON string               | `type: string`                        |
| NULL                | field absent                   | column not in `required`              |

SQL materializations therefore create INTEGER, DECIMAL/NUMERIC, DATE and
text columns, and decimal aggregates in the reference queries stay exact.
Documents carry no `_meta`: the dataset is a fixed load, not a change stream.

## Execution and resumption

Every enabled table generates concurrently, one `dsdgen` process at a time
per table, so wall-clock time is bounded by the largest table. The three
returns tables are produced by their sales parent's process, interleaved on
its stdout, and routed to their own binding by field count.

Tables over about one million dsdgen rows (for the sales tables a dsdgen row is
a ticket or order of several line items) are split into chunks with dsdgen's
`-PARALLEL`/`-CHILD` mode, which the fork makes byte-identical to a serial
run. Progress is checkpointed every 10,000 rows and at every chunk boundary.
On restart, completed chunks are skipped and the in-flight chunk is regenerated
with the rows already emitted discarded, so the destination ends up identical
to an uninterrupted run and restart cost is bounded to one chunk. Once every
binding has emitted its dataset the connector logs completion and idles; a
restart in that state emits nothing.

## Generator provenance

The generator is built from [estuary/tpcds-kit](https://github.com/estuary/tpcds-kit),
a fork of [gregrahn/tpcds-kit](https://github.com/gregrahn/tpcds-kit) (TPC-DS
tools v2.10.0). Its README lists every patch, one commit each: working stdout
mode, chunking for tables under 1M rows and exact chunk boundaries, a hidden
row-count flag used to plan chunks, fractional scale factors, and build fixes
for current compilers. The fork publishes `ghcr.io/estuary/dsdgen:<commit>`
containing the static binary and its `tpcds.idx` distributions file; the
Dockerfile here pins tag `5ea1641` and copies both into the build stage (so the
tests run against the real generator) and the runtime image. The dsdgen
source is distributed under TPC's legal notice, which the fork keeps intact.

`tables_gen.go` is generated from the fork's `tpcds.sql` DDL (copied into this
directory) by `go generate`; CI fails if it is stale.

Fractional scale factors follow the row-count semantics of DuckDB's `tpcds`
extension. Row content, however, follows upstream dsdgen: DuckDB's embedded
copy of the generator diverges from upstream in text and null generation, so
DuckDB's shipped answer sets do not describe this connector's output. Use TPC's
published qualification answers at scale 1, or compute reference answers by
loading the captured data into any engine of your choice.

## Running the reference queries

1. Create the capture with the desired `scale` and materialize all 24
   bindings into the destination under test. Wait until the connector logs
   that every binding has emitted its full dataset.
2. Generate the 99 queries with the kit's `dsqgen` from
   `query_templates/` in the fork, using the same scale factor and the
   dialect template for your destination, and run them there.
3. For scale 1, compare against `answer_sets/` in the fork. For other
   scales, load the same capture into a second engine (DuckDB reads the
   captured JSON directly) and compare the two result sets.

## Tests

`go test ./source-tpc-ds/` runs the decoder unit tests without the generator
and skips the harness tests unless `dsdgen` is on `$PATH` or named by
`$DSDGEN` (with `tpcds.idx` beside it or named by `$DSDGEN_IDX`). The harness
tests run a full load of all 24 tables at scale 0.01 and snapshot, per
binding, the document count, a digest of every document in emission order,
and the first three documents. They also check that chunked generation and an
interrupted-then-resumed run reproduce the same digests. Refresh snapshots
with `UPDATE_SNAPSHOTS=true`.
