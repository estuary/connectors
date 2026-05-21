from __future__ import annotations

import re
import sys

from common import (
    NestedField,
    LogEntry,
    common_args,
    get_spark_session,
    read_csv_opts,
    run_with_status,
)


def _staging_null_counts(spark, binding_idx: int, columns: list[NestedField]) -> list[LogEntry]:
    """Return per-column null counts in the staging CSV view before the merge.

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
        entry = {"msg": "staging null counts", "binding": binding_idx, "nullCounts": nulls}
    except Exception as e:
        entry = {"msg": "could not check staging null counts", "binding": binding_idx, "error": str(e)}

    print(f"[iceberg-diag] {entry}", file=sys.stderr, flush=True)
    return [entry]


def _iceberg_file_stats(spark, binding_idx: int, query: str) -> list[LogEntry]:
    """Return null_value_counts from Iceberg file metadata after the merge.

    Streams file rows one at a time to avoid loading the full result set into
    memory. Prints every file to stderr immediately. Only files with non-zero
    null counts are included in the returned list (passed to the Go connector);
    this keeps the status payload small regardless of table size.
    """
    m = re.search(r"MERGE\s+INTO\s+((?:`[^`]+`\.){2}`[^`]+`)", query, re.IGNORECASE)
    if not m:
        entry = {"msg": "could not parse table FQN from merge query", "binding": binding_idx}
        print(f"[iceberg-diag] {entry}", file=sys.stderr, flush=True)
        return [entry]

    table_fqn = m.group(1)
    try:
        total = 0
        files_with_nulls: list[LogEntry] = []
        for row in spark.sql(
            f"SELECT file_path, null_value_counts FROM {table_fqn}.files"
        ).toLocalIterator():
            total += 1
            entry = {
                "msg": "iceberg file null_value_counts",
                "binding": binding_idx,
                "table": table_fqn,
                "filePath": row.file_path,
                "nullValueCounts": row.null_value_counts,
            }
            print(f"[iceberg-diag] {entry}", file=sys.stderr, flush=True)
            if any(v > 0 for v in (row.null_value_counts or {}).values()):
                files_with_nulls.append(entry)

        summary = {
            "msg": "iceberg file stats summary",
            "binding": binding_idx,
            "table": table_fqn,
            "totalFiles": total,
            "filesWithNulls": len(files_with_nulls),
        }
        print(f"[iceberg-diag] {summary}", file=sys.stderr, flush=True)
        # Return summary + only the files that have non-zero null counts so the
        # status payload stays small regardless of how many files the table has.
        return [summary] + files_with_nulls
    except Exception as e:
        entry = {"msg": "could not read iceberg file stats", "binding": binding_idx, "table": table_fqn, "error": str(e)}
        print(f"[iceberg-diag] {entry}", file=sys.stderr, flush=True)
        return [entry]


def run(spark, input) -> list[LogEntry]:
    logs: list[LogEntry] = []
    for binding in input["bindings"]:
        bindingIdx: int = binding["binding"]
        query: str = binding["query"]
        columns: list[NestedField] = [NestedField(**col) for col in binding["columns"]]
        files: list[str] = binding["files"]

        spark.read.csv(**read_csv_opts(files, columns)).createTempView(
            f"merge_view_{bindingIdx}"
        )

        logs += _staging_null_counts(spark, bindingIdx, columns)

        try:
            spark.sql(query)
        except Exception as e:
            raise RuntimeError(
                f"Running merge query failed:\n{query}\nOriginal Error:\n{str(e)}"
            ) from e
        finally:
            spark.catalog.dropTempView(f"merge_view_{bindingIdx}")

        logs += _iceberg_file_stats(spark, bindingIdx, query)

    return logs


if __name__ == "__main__":
    args = common_args()
    spark = get_spark_session(args)
    run_with_status(args, lambda inp: run(spark, inp))
