import json

from common import (
    NestedField,
    common_args,
    fields_to_struct,
    get_spark_session,
    run_with_status,
)

args = common_args()
spark = get_spark_session(args)

input = json.loads(args.input)
bindings = input["bindings"]


def run():
    for binding in bindings:
        bindingIdx: int = binding["binding"]
        query: str = binding["query"]
        columns: list[NestedField] = [NestedField(**col) for col in binding["columns"]]
        files: list[str] = binding["files"]

        spark.read.csv(
            path=files,
            schema=fields_to_struct(columns),
            quote="`",
            header=False,
            inferSchema=False,
        ).createTempView(f"merge_view_{bindingIdx}")

        spark.sql(query)

        spark.catalog.dropTempView(f"merge_view_{bindingIdx}")


run_with_status(args, run)
