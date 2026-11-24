from __future__ import annotations

import sys
import time
from typing import Any

from common import (
    NestedField,
    common_args,
    get_spark_session,
    read_csv_opts,
    run_with_status,
    with_variant_columns,
)
from pyspark.sql import SparkSession

RETRY_ATTEMPTS = 3
RETRY_BASE_DELAY_SECONDS = 2
RETRYABLE_EXC_NAME = "org.apache.iceberg.exceptions.ValidationException"
RETRYABLE_MESSAGE_FRAGMENTS = (
    # A concurrent task removed files we planned to delete.
    "Missing required files to delete",
    # A concurrent writer modified the same key as we are.
    "Found conflicting files",
)


def _is_retryable(exc: Exception) -> bool:
    text = str(exc)
    return RETRYABLE_EXC_NAME in text and any(
        frag in text for frag in RETRYABLE_MESSAGE_FRAGMENTS
    )


def _run_with_retries(spark: SparkSession, bindingIdx: int, query: str, label: str) -> int:
    """Run the query, returning how many times it was retried."""
    for attempt in range(RETRY_ATTEMPTS + 1):
        try:
            spark.sql(query)
            return attempt
        except Exception as e:
            if attempt == RETRY_ATTEMPTS or not _is_retryable(e):
                raise RuntimeError(
                    f"Running {label} query failed:\n{query}\nOriginal Error:\n{str(e)}"
                ) from e
            print(
                f"merge: retrying binding {bindingIdx} {label} after retryable error "
                f"(attempt {attempt + 1}/{RETRY_ATTEMPTS}): {e}",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(RETRY_BASE_DELAY_SECONDS * 2**attempt)


def _commit_summary(
    spark: SparkSession, table: str, since_ms: int
) -> dict[str, str] | None:
    """Return the operation and summary of the snapshot this application
    committed to the table at or after since_ms, or None if there is none or
    it can't be read. The merge has already committed, so a failure to read
    it back must not fail the job."""
    try:
        rows = spark.sql(
            f"SELECT operation, summary FROM {table}.snapshots "
            f"WHERE committed_at >= timestamp_millis({since_ms}) "
            f"AND summary['spark.app.id'] = '{spark.sparkContext.applicationId}' "
            "ORDER BY committed_at DESC LIMIT 1"
        ).collect()
    except Exception as e:
        print(
            f"merge: reading the commit summary of {table} failed: {e}",
            file=sys.stderr,
            flush=True,
        )
        return None
    if not rows:
        return None
    return {"operation": rows[0]["operation"], **rows[0]["summary"]}


def run(spark: SparkSession, input: dict[str, Any]) -> dict[str, Any]:
    stats: list[dict[str, Any]] = []
    for binding in input["bindings"]:
        bindingIdx: int = binding["binding"]
        query: str = binding.get("query", "")
        truncate_query: str = binding.get("truncate_query", "")
        table: str = binding.get("table", "")
        binding_stats: dict[str, Any] = {"binding": bindingIdx}
        stats.append(binding_stats)

        # A binding whose backfill just completed, but which has nothing
        # staged this transaction, has no merge to run: only the truncation
        # below applies.
        if query:
            columns: list[NestedField] = [
                NestedField(**col) for col in binding["columns"]
            ]
            files: list[str] = binding["files"]

            df = spark.read.csv(**read_csv_opts(files, columns))
            with_variant_columns(df, columns).createTempView(f"merge_view_{bindingIdx}")

            started = time.time()
            try:
                binding_stats["retries"] = _run_with_retries(
                    spark, bindingIdx, query, "merge"
                )
            finally:
                spark.catalog.dropTempView(f"merge_view_{bindingIdx}")
            binding_stats["merge_ms"] = int((time.time() - started) * 1000)
            if table:
                binding_stats["commit"] = _commit_summary(
                    spark, table, int(started * 1000)
                )

        # Truncation runs after the merge, in the same job, because a store
        # of an existing row is a MERGE that would match nothing if its old
        # row were already deleted.
        if truncate_query:
            started = time.time()
            _run_with_retries(spark, bindingIdx, truncate_query, "truncate")
            binding_stats["truncate_ms"] = int((time.time() - started) * 1000)

    return {"stats": {"bindings": stats}}


if __name__ == "__main__":
    args = common_args()
    spark = get_spark_session(args)
    run_with_status(args, lambda inp: run(spark, inp))
