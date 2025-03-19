from common import (
    common_args,
    get_spark_session,
    run_with_status,
)

args = common_args()
spark = get_spark_session(args)


def run(input):
    query = input["query"]

    try:
        spark.sql(query)
    except Exception as e:
        raise RuntimeError(
            f"Running exec query failed:\n{query}\nOriginal Error:\n{str(e)}"
        ) from e


run_with_status(args, run)
