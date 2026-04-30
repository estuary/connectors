from __future__ import annotations

from common import common_args, get_spark_session, run_with_status


def run(spark, input):
    query = input["query"]

    try:
        spark.sql(query)
    except Exception as e:
        raise RuntimeError(
            f"Running exec query failed:\n{query}\nOriginal Error:\n{str(e)}"
        ) from e


if __name__ == "__main__":
    args = common_args()
    spark = get_spark_session(args)
    run_with_status(args, lambda inp: run(spark, inp))
