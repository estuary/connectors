from __future__ import annotations

from common import (
    NestedField,
    common_args,
    get_spark_session,
    read_csv_opts,
    run_with_status,
)


def run(spark, input):
    query = input["query"]
    bindings = input["bindings"]
    output_location = input["output_location"]

    for binding in bindings:
        bindingIdx: int = binding["binding"]
        keys: list[NestedField] = [NestedField(**key) for key in binding["keys"]]
        files: list[str] = binding["files"]

        spark.read.csv(**read_csv_opts(files, keys)).createTempView(
            f"load_view_{bindingIdx}"
        )

    spark.sql(query).write.csv(
        output_location,
        mode="error",
        header=False,
        compression="gzip",
        sep=chr(0),
        quote=chr(0),
    )

    for binding in bindings:
        bindingIdx: int = binding["binding"]
        spark.catalog.dropTempView(f"load_view_{bindingIdx}")


if __name__ == "__main__":
    args = common_args()
    spark = get_spark_session(args)
    run_with_status(args, lambda inp: run(spark, inp))
