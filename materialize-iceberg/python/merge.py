from __future__ import annotations

from common import (
    NestedField,
    common_args,
    get_spark_session,
    read_csv_opts,
    run_with_status,
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

        try:
            spark.sql(query)
        except Exception as e:
            raise RuntimeError(
                f"Running merge query failed:\n{query}\nOriginal Error:\n{str(e)}"
            ) from e
        finally:
            spark.catalog.dropTempView(f"merge_view_{bindingIdx}")


if __name__ == "__main__":
    args = common_args()
    spark = get_spark_session(args)
    run_with_status(args, lambda inp: run(spark, inp))
