import argparse
import json
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

import boto3
import botocore
import botocore.session
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@dataclass
class NestedField:
    name: str
    type: str
    element: Optional[str] = None


def data_type_for_field(field: NestedField) -> DataType:
    if field.type == "string":
        return StringType()
    elif field.type == "binary":
        # Binary data is base64 encoded as strings in CSV files. Queries must
        # handle the unbase64'ing.
        return StringType()
    elif field.type == "boolean":
        return BooleanType()
    elif field.type == "long":
        return LongType()
    elif field.type == "double":
        return DoubleType()
    elif field.type == "date":
        return DateType()
    elif field.type == "timestamptz":
        return TimestampType()
    elif field.type == "decimal(38, 0)":
        return DecimalType(38, 0)
    elif field.type == "list":
        assert field.element
        return ArrayType(
            elementType=data_type_for_field(
                NestedField(name=field.name, type=field.element, element=None)
            )
        )
    else:
        raise ValueError(f"Unsupported type: {field.type}")


def fields_to_struct(fields: list[NestedField]) -> StructType:
    return StructType([StructField(f.name, data_type_for_field(f)) for f in fields])


def common_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input", required=True, help="Input for the program, as serialized JSON."
    )
    parser.add_argument(
        "--status-output",
        required=True,
        help="Location where the final status object will be written.",
    )
    parser.add_argument("--catalog-url", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument(
        "--credential-secret-name",
        required=False,
        help="The secret name in Secrets Manager containing the catalog credential value. Will use SigV4 authentication if not provided.",
    )
    parser.add_argument(
        "--scope",
        required=False,
        help="Scope when authenticating with client credentials.",
    )
    return parser.parse_args()


def get_spark_session(args: argparse.Namespace) -> SparkSession:
    builder = (
        SparkSession.Builder()
        .appName("Iceberg Test")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.1,software.amazon.awssdk:bundle:2.20.160,software.amazon.awssdk:url-connection-client:2.20.160",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.defaultCatalog", "estuary")
        .config("spark.sql.catalog.estuary", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.estuary.type", "rest")
        .config(
            "spark.sql.catalog.estuary.header.X-Iceberg-Access-Delegation",
            "vended-credentials",
        )
        .config("spark.sql.catalog.estuary.uri", args.catalog_url)
        .config("spark.sql.catalog.estuary.warehouse", args.warehouse)
    )

    if args.credential_secret_name:
        credential = (
            botocore.session.get_session()
            .create_client("ssm", region_name=args.region)
            .get_parameter(Name=args.credential_secret_name, WithDecryption=True)[
                "Parameter"
            ]["Value"]
        )

        builder = builder.config("spark.sql.catalog.estuary.credential", credential)
    else:
        builder = (
            builder.config("spark.sql.catalog.estuary.rest.sigv4-enabled", "true")
            .config("spark.sql.catalog.estuary.rest.signing-name", "glue")
            .config("spark.sql.catalog.estuary.rest.signing-region", args.region)
        )

    if args.scope:
        builder = builder.config("spark.sql.catalog.estuary.scope", args.scope)

    return builder.getOrCreate()


def run_with_status(
    parsed_args: argparse.Namespace,
    fn,
    *args,
    **kwargs,
):
    parsed_url = urlparse(parsed_args.status_output)
    bucket_name = parsed_url.netloc
    file_path = parsed_url.path.lstrip("/")
    s3 = boto3.client("s3")

    try:
        fn(*args, **kwargs)
        s3.put_object(
            Bucket=bucket_name, Key=file_path, Body=json.dumps({"success": True})
        )
    except Exception as e:
        s3.put_object(
            Bucket=bucket_name,
            Key=file_path,
            Body=json.dumps({"success": False, "error": str(e)}),
        )
        raise
