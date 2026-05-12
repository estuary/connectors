from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

import boto3
import botocore
import botocore.config
import botocore.session
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
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


def read_csv_opts(files: list[str], cols: list[NestedField]):
    return {
        "path": files,
        "schema": fields_to_struct(cols),
        "quote": "`",
        "escape": "`",
        "emptyValue": '""',
        "header": False,
        "inferSchema": False,
        "enforceSchema": False,
        "multiLine": True,
        "unescapedQuoteHandling": "RAISE_ERROR",
    }


def common_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-uri",
        required=True,
        help="Location of the program input, as serialized JSON.",
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
        "--credential",
        required=False,
        help="Catalog credential value, used directly. Alternative to --credential-secret-name for environments where Systems Manager is not available (e.g. local docker-compose tests).",
    )
    parser.add_argument(
        "--scope",
        required=False,
        help="Scope when authenticating with client credentials.",
    )
    parser.add_argument(
        "--signing-name",
        required=False,
        help="Signing name to use when authenticating with AWS SigV4. Either 'glue' or 's3tables'.",
    )
    parser.add_argument(
        "--oauth2-server-uri",
        required=False,
        help="OAuth2 token endpoint URI.",
    )
    parser.add_argument(
        "--s3-endpoint",
        required=False,
        help="S3-compatible endpoint URL. Setting this routes Spark and the Iceberg catalog at a non-AWS S3 (e.g. RustFS or MinIO) for tests.",
    )
    parser.add_argument(
        "--s3-access-key",
        required=False,
        help="Static access key for the S3-compatible store. Used together with --s3-endpoint.",
    )
    parser.add_argument(
        "--s3-secret-key",
        required=False,
        help="Static secret key for the S3-compatible store. Used together with --s3-endpoint.",
    )
    parser.add_argument(
        "--s3-path-style",
        required=False,
        default="false",
        help="Set to 'true' to use path-style addressing for the S3-compatible store. Required by RustFS and MinIO.",
    )
    return parser.parse_args()


def _build_session(c: dict) -> SparkSession:
    """Construct a SparkSession from a config dict.

    Used by both get_spark_session() (CLI args, EMR scripts) and
    get_spark_session_from_env() (env vars, local test daemon).
    """
    use_local_s3 = bool(c.get("s3_endpoint"))

    builder = (
        SparkSession.Builder()
        .appName(c.get("app_name") or "Iceberg Test")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # The defaultCatalog is no longer used, we always execute fully
        # qualified queries to avoid abigiuity if the namespace/database is
        # also named estuary.  It is still specified to ensure tasks migrating
        # from an older version, which may have a partially qualified query,
        # are still able to run during the post-commit apply.
        .config("spark.sql.defaultCatalog", "estuary")
        .config("spark.sql.catalog.estuary", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.estuary.type", "rest")
        .config("spark.sql.catalog.estuary.uri", c["catalog_url"])
        .config("spark.sql.catalog.estuary.warehouse", c["warehouse"])
    )

    if c.get("spark_master_url"):
        builder = builder.master(c["spark_master_url"])

    if c.get("disable_catalog_cache"):
        # The long-lived daemon shares a SparkSession across submissions.
        # Without this, Iceberg's REST catalog caches table metadata for
        # 30s, masking schema changes the connector makes between jobs
        # (notably the column migration's add-temp-columns step). EMR
        # entrypoints get a fresh SparkSession per job, so they leave the
        # cache enabled to avoid extra catalog round trips.
        builder = builder.config("spark.sql.catalog.estuary.cache-enabled", "false")

    if not use_local_s3:
        # Production path: Polaris/Glue vend short-lived credentials per request.
        # In local test mode the connector passes static credentials directly,
        # so vended credentials would point Spark at the wrong endpoint.
        builder = builder.config(
            "spark.sql.catalog.estuary.header.X-Iceberg-Access-Delegation",
            "vended-credentials",
        )

    if c.get("oauth2_server_uri"):
        builder = builder.config(
            "spark.sql.catalog.estuary.oauth2-server-uri", c["oauth2_server_uri"]
        )

    if c.get("credential"):
        builder = builder.config(
            "spark.sql.catalog.estuary.credential", c["credential"]
        )
    elif c.get("signing_name"):
        builder = (
            builder.config("spark.sql.catalog.estuary.rest.sigv4-enabled", "true")
            .config("spark.sql.catalog.estuary.rest.signing-name", c["signing_name"])
            .config("spark.sql.catalog.estuary.rest.signing-region", c["region"])
        )

    if c.get("scope"):
        builder = builder.config("spark.sql.catalog.estuary.scope", c["scope"])

    if use_local_s3:
        # The Iceberg Spark catalog uses the AWS S3 client (S3FileIO); both
        # that client and Hadoop's s3a (used for spark.read.csv on staged
        # files) need the endpoint, path-style flag, and static credentials.
        path_style = (c.get("s3_path_style") or "false").lower() == "true"
        builder = (
            builder.config("spark.sql.catalog.estuary.s3.endpoint", c["s3_endpoint"])
            .config(
                "spark.sql.catalog.estuary.s3.path-style-access",
                "true" if path_style else "false",
            )
            .config(
                "spark.sql.catalog.estuary.s3.access-key-id", c["s3_access_key"]
            )
            .config(
                "spark.sql.catalog.estuary.s3.secret-access-key",
                c["s3_secret_key"],
            )
            .config("spark.hadoop.fs.s3a.endpoint", c["s3_endpoint"])
            .config(
                "spark.hadoop.fs.s3a.path.style.access",
                "true" if path_style else "false",
            )
            .config("spark.hadoop.fs.s3a.access.key", c["s3_access_key"])
            .config("spark.hadoop.fs.s3a.secret.key", c["s3_secret_key"])
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            # The connector emits s3:// URIs (matching EMR conventions) but
            # upstream Hadoop only ships the S3A filesystem implementation.
            # Map s3:// to S3A so the existing scripts work unchanged.
            .config(
                "spark.hadoop.fs.s3.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem",
            )
            .config(
                "spark.hadoop.fs.AbstractFileSystem.s3.impl",
                "org.apache.hadoop.fs.s3a.S3A",
            )
        )

    return builder.getOrCreate()


def get_spark_session(args: argparse.Namespace) -> SparkSession:
    """Build a SparkSession from CLI args. Used by EMR script entrypoints
    in load.py / merge.py / exec.py."""
    credential = args.credential
    if not credential and args.credential_secret_name:
        credential = (
            botocore.session.get_session()
            .create_client("ssm", region_name=args.region)
            .get_parameter(Name=args.credential_secret_name, WithDecryption=True)[
                "Parameter"
            ]["Value"]
        )

    return _build_session(
        {
            "catalog_url": args.catalog_url,
            "warehouse": args.warehouse,
            "region": args.region,
            "credential": credential,
            "scope": args.scope,
            "oauth2_server_uri": args.oauth2_server_uri,
            "signing_name": args.signing_name,
            "s3_endpoint": args.s3_endpoint,
            "s3_path_style": args.s3_path_style,
            "s3_access_key": args.s3_access_key,
            "s3_secret_key": args.s3_secret_key,
        }
    )


def get_spark_session_from_env(shared_across_submissions: bool = False) -> SparkSession:
    """Build a SparkSession for the long-lived test-only daemon (daemon.py).

    Reads its config from environment variables prefixed `ICEBERG_`, with the
    OAuth client credential pulled from the JSON file at
    ICEBERG_CREDENTIALS_PATH (the file is written by the polaris-bootstrap
    container during compose startup).

    Set `shared_across_submissions=True` if the resulting session will service
    multiple back-to-back jobs that may include schema changes; this disables
    the Iceberg REST catalog metadata cache so later jobs see schema changes
    made by earlier ones.
    """
    creds_path = os.environ.get("ICEBERG_CREDENTIALS_PATH")
    credential = os.environ.get("ICEBERG_CREDENTIAL")
    if not credential and creds_path:
        with open(creds_path) as f:
            creds = json.load(f)
            credential = f"{creds['client_id']}:{creds['client_secret']}"

    return _build_session(
        {
            "app_name": "Iceberg Test Daemon",
            "catalog_url": os.environ["ICEBERG_CATALOG_URL"],
            "warehouse": os.environ["ICEBERG_WAREHOUSE"],
            "region": os.environ.get("ICEBERG_REGION", "us-east-1"),
            "credential": credential,
            "scope": os.environ.get("ICEBERG_SCOPE"),
            "oauth2_server_uri": os.environ.get("ICEBERG_OAUTH2_SERVER_URI"),
            "signing_name": os.environ.get("ICEBERG_SIGNING_NAME"),
            "s3_endpoint": os.environ.get("ICEBERG_S3_ENDPOINT"),
            "s3_path_style": os.environ.get("ICEBERG_S3_PATH_STYLE", "false"),
            "s3_access_key": os.environ.get("ICEBERG_S3_ACCESS_KEY"),
            "s3_secret_key": os.environ.get("ICEBERG_S3_SECRET_KEY"),
            "spark_master_url": os.environ.get("SPARK_MASTER_URL"),
            "disable_catalog_cache": shared_across_submissions,
        }
    )


def run_with_status(
    parsed_args: argparse.Namespace,
    fn,
):
    input_uri = urlparse(parsed_args.input_uri)
    input_bucket_name = input_uri.netloc
    input_file_path = input_uri.path.lstrip("/")

    output_uri = urlparse(parsed_args.status_output)
    output_bucket_name = output_uri.netloc
    output_file_path = output_uri.path.lstrip("/")

    if parsed_args.s3_endpoint:
        s3 = boto3.client(
            "s3",
            endpoint_url=parsed_args.s3_endpoint,
            aws_access_key_id=parsed_args.s3_access_key,
            aws_secret_access_key=parsed_args.s3_secret_key,
            region_name=parsed_args.region,
            config=botocore.config.Config(s3={"addressing_style": "path"}),
        )
    else:
        s3 = boto3.client("s3")

    try:
        input = s3.get_object(Bucket=input_bucket_name, Key=input_file_path)
        with input["Body"] as body:
            input = json.loads(body.read().decode("utf-8"))
        s3.delete_object(Bucket=input_bucket_name, Key=input_file_path)

        fn(input)
        s3.put_object(
            Bucket=output_bucket_name,
            Key=output_file_path,
            Body=json.dumps({"success": True}),
        )
    except Exception as e:
        s3.put_object(
            Bucket=output_bucket_name,
            Key=output_file_path,
            Body=json.dumps({"success": False, "error": str(e)}),
        )
        raise
