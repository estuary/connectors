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
    """Return null_value_counts and bounds presence from Iceberg file metadata after the merge.

    Streams file rows one at a time to avoid loading the full result set into
    memory. Prints every file to stderr immediately. Only files with non-zero
    null counts are included in the returned list (passed to the Go connector);
    this keeps the status payload small regardless of table size.

    Also captures lower_bounds from the first file to indicate the effective
    metrics mode: truncate(N) populates lower_bounds for every column; counts
    mode leaves lower_bounds null. Absent lower_bounds means downstream engines
    that require bounds to trust null_value_counts (e.g. some Snowflake paths)
    may still reject NOT NULL columns even when null_value_counts is 0.
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
        sample_lower_bound_col_ids: list[int] | None = None
        for row in spark.sql(
            f"SELECT file_path, null_value_counts, lower_bounds FROM {table_fqn}.files"
        ).toLocalIterator():
            total += 1
            if sample_lower_bound_col_ids is None:
                sample_lower_bound_col_ids = (
                    sorted(row.lower_bounds.keys()) if row.lower_bounds else []
                )
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
            # Column IDs from the first file's lower_bounds. Empty means the
            # table uses counts metrics mode (no bounds written); non-empty
            # means truncate(N) mode is active and all listed columns have bounds.
            "sampleLowerBoundColIds": sample_lower_bound_col_ids or [],
        }
        print(f"[iceberg-diag] {summary}", file=sys.stderr, flush=True)
        # Return summary + only the files that have non-zero null counts so the
        # status payload stays small regardless of how many files the table has.
        return [summary] + files_with_nulls
    except Exception as e:
        entry = {"msg": "could not read iceberg file stats", "binding": binding_idx, "table": table_fqn, "error": str(e)}
        print(f"[iceberg-diag] {entry}", file=sys.stderr, flush=True)
        return [entry]


def _parquet_row_group_stats(
    spark, binding_idx: int, table_fqn: str, columns: list[NestedField]
) -> list[LogEntry]:
    """Read Parquet file footer metadata via the JVM Parquet library and log
    row-group statistics for string/binary columns.

    The Iceberg manifest records null_value_counts at the file level. Parquet
    files separately store null_count (and optional min/max) per column chunk
    inside each row group. Some downstream engines (Snowflake, Bauplan) read
    these Parquet-level statistics directly rather than relying solely on the
    Iceberg manifest.

    For large string columns (e.g. flow_document, body, html_body), Parquet
    writers may omit column statistics when values exceed a size threshold,
    leaving null_count absent inside the file. This causes errors like
    "non-nullable column without default missing data for columnId N" even
    when the Iceberg manifest correctly shows null_value_counts = 0.

    Uses parquet-hadoop via Py4J — no extra Python dependencies required.

    Logs per-file, per-column row-group statistics aggregates for all string
    and binary columns: how many row groups have statistics, null_count, and
    min/max. rgWithNullCount < rowGroups is the smoking gun.
    """
    try:
        file_rows = spark.sql(
            f"SELECT file_path FROM {table_fqn}.files LIMIT 5"
        ).collect()
    except Exception as e:
        entry = {
            "msg": "parquet row group stats: could not list files",
            "binding": binding_idx,
            "table": table_fqn,
            "error": str(e),
        }
        print(f"[iceberg-diag] {entry}", file=sys.stderr, flush=True)
        return [entry]

    string_col_names = {col.name for col in columns if col.type in ("string", "binary")}

    jvm = spark._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    JPath = jvm.org.apache.hadoop.fs.Path
    HadoopInputFile = jvm.org.apache.parquet.hadoop.util.HadoopInputFile
    ParquetFileReader = jvm.org.apache.parquet.hadoop.ParquetFileReader

    logs = []
    for file_row in file_rows:
        file_path = file_row.file_path
        try:
            j_input = HadoopInputFile.fromPath(JPath(file_path), hadoop_conf)
            reader = ParquetFileReader.open(j_input)
        except Exception as e:
            entry = {
                "msg": "parquet row group stats: could not open file",
                "binding": binding_idx,
                "filePath": file_path,
                "error": str(e),
            }
            print(f"[iceberg-diag] {entry}", file=sys.stderr, flush=True)
            logs.append(entry)
            continue

        try:
            footer = reader.getFooter()
            row_groups = footer.getBlocks()
            num_rg = row_groups.size()

            # Build column-name → index mapping from the first row group so we
            # can look up columns by position in O(1) for subsequent row groups.
            name_to_idx: dict[str, int] = {}
            if num_rg > 0:
                first_rg_cols = row_groups.get(0).getColumns()
                for i in range(first_rg_cols.size()):
                    col_path = first_rg_cols.get(i).getPath().toDotString()
                    name_to_idx[col_path] = i

            col_stats: dict = {}
            for col_name in sorted(string_col_names):
                col_idx = name_to_idx.get(col_name)
                if col_idx is None:
                    continue

                rg_with_stats = 0
                rg_with_null_count = 0
                rg_with_min_max = 0
                total_null_count = 0

                for rg_idx in range(num_rg):
                    stats = row_groups.get(rg_idx).getColumns().get(col_idx).getStatistics()
                    if stats is None:
                        continue
                    rg_with_stats += 1
                    if stats.isNumNullsSet():
                        rg_with_null_count += 1
                        total_null_count += int(stats.getNumNulls())
                    try:
                        if stats.hasNonNullValue():
                            rg_with_min_max += 1
                    except Exception:
                        pass

                col_stats[col_name] = {
                    "rowGroups": num_rg,
                    "rgWithStats": rg_with_stats,
                    "rgWithNullCount": rg_with_null_count,
                    "rgWithMinMax": rg_with_min_max,
                    "totalNullCount": total_null_count,
                }
        finally:
            reader.close()

        entry = {
            "msg": "parquet row group stats",
            "binding": binding_idx,
            "filePath": file_path,
            "stringColStats": col_stats,
        }
        print(f"[iceberg-diag] {entry}", file=sys.stderr, flush=True)
        logs.append(entry)

    return logs


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

        m = re.search(r"MERGE\s+INTO\s+((?:`[^`]+`\.){2}`[^`]+`)", query, re.IGNORECASE)
        if m:
            logs += _parquet_row_group_stats(spark, bindingIdx, m.group(1), columns)

    return logs


if __name__ == "__main__":
    args = common_args()
    spark = get_spark_session(args)
    run_with_status(args, lambda inp: run(spark, inp))
