from __future__ import annotations

import re
import sys

from common import (
    NestedField,
    common_args,
    get_spark_session,
    read_csv_opts,
    run_with_status,
)


def _log_staging_null_counts(spark, binding_idx: int, columns: list[NestedField]) -> None:
    """Log per-column null counts in the staging CSV view before the merge.

    Helps distinguish between "source data has real nulls" vs "statistics
    metadata is wrong" when diagnosing non-nullable column errors.
    """
    view = f"merge_view_{binding_idx}"
    try:
        exprs = [
            f"SUM(CASE WHEN `{col.name.replace('`', '``')}` IS NULL THEN 1 ELSE 0 END)"
            for col in columns
        ]
        row = spark.sql(f"SELECT {', '.join(exprs)} FROM {view}").collect()[0]
        nulls = {
            columns[i].name: row[i]
            for i in range(len(columns))
            if row[i] is not None and row[i] > 0
        }
        print(
            f"[iceberg-diag] binding={binding_idx} staging null counts "
            f"({len(nulls)} columns with nulls out of {len(columns)} total): {nulls}",
            file=sys.stderr, flush=True,
        )
    except Exception as e:
        print(
            f"[iceberg-diag] binding={binding_idx} could not check staging null counts: {e}",
            file=sys.stderr, flush=True,
        )


def _log_iceberg_file_stats(spark, binding_idx: int, query: str) -> None:
    """Log null_value_counts from Iceberg file metadata after the merge.

    Directly shows whether statistics are present and what null counts were
    recorded per column ID — the numbers that appear in errors like
    "non-nullable column (columnId N) without default has null values according
    to file statistics."
    """
    m = re.search(r"MERGE\s+INTO\s+((?:`[^`]+`\.){2}`[^`]+`)", query, re.IGNORECASE)
    if not m:
        print(
            f"[iceberg-diag] binding={binding_idx} could not parse table FQN from merge query",
            file=sys.stderr, flush=True,
        )
        return

    table_fqn = m.group(1)
    try:
        rows = spark.sql(
            f"SELECT file_path, null_value_counts FROM {table_fqn}.files"
        ).collect()
        print(
            f"[iceberg-diag] binding={binding_idx} table={table_fqn}: "
            f"{len(rows)} data file(s) in current snapshot",
            file=sys.stderr, flush=True,
        )
        for row in rows:
            print(
                f"[iceberg-diag]   file={row.file_path} null_value_counts={row.null_value_counts}",
                file=sys.stderr, flush=True,
            )
    except Exception as e:
        print(
            f"[iceberg-diag] binding={binding_idx} could not read file stats for {table_fqn}: {e}",
            file=sys.stderr, flush=True,
        )


def run(spark, input):
    for binding in input["bindings"]:
        bindingIdx: int = binding["binding"]
        query: str = binding["query"]
        columns: list[NestedField] = [NestedField(**col) for col in binding["columns"]]
        files: list[str] = binding["files"]

        spark.read.csv(**read_csv_opts(files, columns)).createTempView(
            f"merge_view_{bindingIdx}"
        )

        _log_staging_null_counts(spark, bindingIdx, columns)

        try:
            spark.sql(query)
        except Exception as e:
            raise RuntimeError(
                f"Running merge query failed:\n{query}\nOriginal Error:\n{str(e)}"
            ) from e
        finally:
            spark.catalog.dropTempView(f"merge_view_{bindingIdx}")

        _log_iceberg_file_stats(spark, bindingIdx, query)


if __name__ == "__main__":
    args = common_args()
    spark = get_spark_session(args)
    run_with_status(args, lambda inp: run(spark, inp))
