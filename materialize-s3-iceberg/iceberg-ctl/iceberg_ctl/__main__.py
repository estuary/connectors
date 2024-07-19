import json
from typing import Any, Literal

import click
from click import Context
from iceberg_ctl.models import EndpointConfig
from pydantic import BaseModel, TypeAdapter
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.io import PY_IO_IMPL
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    IcebergType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
    TimeType,
    UUIDType,
)


def _field_to_type(typ_str: str) -> IcebergType:
    match typ_str:
        case "boolean":
            return BooleanType()
        case "string":
            return StringType()
        case "long":
            return LongType()
        case "double":
            return DoubleType()
        case "timestamptz":
            return TimestamptzType()
        case "date":
            return DateType()
        case "time":
            return TimeType()
        case "uuid":
            return UUIDType()
        case "binary":
            return BinaryType()
        case _:
            raise Exception(f"unhandled type: {typ_str}")


@click.group()
@click.option("--endpoint-config-json")
@click.pass_context
def run(
    ctx: Context,
    endpoint_config_json: str | None,
):
    ctx.ensure_object(dict)

    if endpoint_config_json is not None:
        cfg = EndpointConfig.model_validate_json(endpoint_config_json)

        ctx.obj["catalog"] = GlueCatalog(
            "default",
            **{
                "region_name": cfg.region,
                "aws_access_key_id": cfg.aws_access_key_id,
                "aws_secret_access_key": cfg.aws_secret_access_key,
                PY_IO_IMPL: "pyiceberg.io.fsspec.FsspecFileIO",  # use S3 file IO instead of Arrow
                "s3.region": cfg.region,
                "s3.access-key-id": cfg.aws_access_key_id,
                "s3.secret-access-key": cfg.aws_secret_access_key,
            },
        )


@run.command()
def print_config_schema():
    print(
        TypeAdapter(dict[str, Any])
        .dump_json(EndpointConfig.model_json_schema())
        .decode()
    )


class IcebergColumn(BaseModel):
    name: str
    nullable: bool
    type: Literal[
        "boolean",
        "string",
        "long",  # 64 bit integer
        "double",  # 64 bit float
        "timestamptz",
        "date",
        "time",
        "uuid",
        "binary",
    ]


@run.command()
@click.pass_context
@click.argument("resource-paths", type=str)
def info_schema(
    ctx: Context,
    resource_paths: str,
):
    '''
    Get a list of all pre-existing tables and columns in destination.

    The resource-paths argument is a JSON array of tuples of the form
    (namespace, table).
    
    For efficiency, we only list tables that are included in the list of
    resources for the materialization. For the purposes of computing apply
    actions and validation constraints, we don't care about tables other than
    these, and attempting to list their information can really slow things down
    if there are a lot of non-related pre-existing namespaces and tables.
    '''

    catalog = ctx.obj["catalog"]
    assert isinstance(catalog, Catalog)

    tables: dict[str, list[IcebergColumn]] = {}
    find_paths = TypeAdapter(list[tuple[str, str]]).validate_json(resource_paths)
    find_namespaces = set(path[0] for path in find_paths)
    find_tables: dict[str, list[str]] = {}
    for path in find_paths:
        find_tables.setdefault(path[0], []).append(path[1])

    for ns in catalog.list_namespaces():
        if ns[0] not in find_namespaces:
            # Namespace is not relevant for the list of tables and namespaces
            # are care about.
            continue

        for tbl in catalog.list_tables(ns):
            if tbl[1] not in find_tables[tbl[0]]:
                # Table is not relevant for the list of tables and namespaces
                # are care about.
                continue

            loaded = catalog.load_table(tbl)

            tables[f"{tbl[0]}.{tbl[1]}"] = [
                IcebergColumn(
                    name=f.name,
                    nullable=not f.required,
                    type=f.field_type.__str__(),  # type: ignore
                )
                for f in loaded.schema().fields
            ]

    print(TypeAdapter(dict[str, list[IcebergColumn]]).dump_json(tables).decode())


@run.command()
@click.pass_context
def list_namespaces(ctx: Context):
    catalog = ctx.obj["catalog"]
    assert isinstance(catalog, Catalog)

    print(json.dumps(list(ns[0] for ns in catalog.list_namespaces())))


class TableCreate(BaseModel, extra="forbid"):
    location: str
    fields: list[IcebergColumn]


@run.command()
@click.argument("table", type=str)
@click.argument("table-create-json", type=str)
@click.pass_context
def create_table(
    ctx: Context,
    table: str,
    table_create_json: str,
):
    catalog = ctx.obj["catalog"]
    assert isinstance(catalog, Catalog)

    table_create = TableCreate.model_validate_json(table_create_json)

    columns = [
        NestedField(
            field_id=idx,
            name=f.name,
            field_type=_field_to_type(f.type),
            required=not f.nullable,
        )
        for idx, f in enumerate(table_create.fields)
    ]

    catalog.create_table(table, Schema(*columns), table_create.location)


@run.command()
@click.argument("table", type=str)
@click.pass_context
def drop_table(
    ctx: Context,
    table: str,
):
    catalog = ctx.obj["catalog"]
    assert isinstance(catalog, Catalog)

    catalog.drop_table(table)


@run.command()
@click.argument("namespace", type=str)
@click.pass_context
def create_namespace(
    ctx: Context,
    namespace: str,
):
    catalog = ctx.obj["catalog"]
    assert isinstance(catalog, Catalog)

    catalog.create_namespace(namespace)


class TableAlter(BaseModel, extra="forbid"):
    new_columns: list[IcebergColumn] | None
    newly_nullable_columns: list[str] | None


@run.command()
@click.argument("table", type=str)
@click.argument("table-alter-json", type=str)
@click.pass_context
def alter_table(
    ctx: Context,
    table: str,
    table_alter_json: str,
):
    catalog = ctx.obj["catalog"]
    assert isinstance(catalog, Catalog)

    table_alter = TableAlter.model_validate_json(table_alter_json)

    tbl = catalog.load_table(table)

    with tbl.update_schema() as update:
        if table_alter.new_columns is not None:
            for c in table_alter.new_columns:
                update.add_column(
                    path=c.name,
                    field_type=_field_to_type(c.type),
                    required=not c.nullable,
                )

        if table_alter.newly_nullable_columns is not None:
            for c in table_alter.newly_nullable_columns:
                update.update_column(path=c, required=False)


@run.command()
@click.argument("table", type=str)
@click.argument("prev-checkpoint", type=str)
@click.argument("next-checkpoint", type=str)
@click.argument("file-paths", type=str)
@click.pass_context
def append_files(
    ctx: Context,
    table: str,
    prev_checkpoint: str,
    next_checkpoint: str,
    file_paths: str,
):
    '''
    Appends files at "file-paths" to the table.

    The "prev-checkpoint" and "next-checkpoint" arguments are used to provide a best-effort
    avoidance of duplicating data from appending the same files that have previously been appended.
    A possible scenario is this: Files are successfully appended to Table1 and Table2 but not Table3
    in response to the connector receiving a StartCommit message, but the connector is restarted
    before the transaction is fully completed and acknowledged. Upon restart, a re-application of
    the persisted driver checkpoint is attempted (ref: "Recovery Log with Idempotent Apply"
    pattern). Table1 and Table2 should not have the same files appended again, but Table3 does still
    need to have the files appended.

    When a table is updated to append files, its "checkpoint" property is updated to
    "next-checkpoint", and only tables with "checkpoint" equal to "prev-checkpoint" are appended to.
    The previously described scenario would then play out like this:

    1) The materialization connector persists values for "prev-checkpoint" and "next-checkpoint" of
    "0001" and "0002", respectively, in its driver checkpoint via StartedCommit.

    2) During the partial completion of the transaction, Table1 and Table2 are updated to have a
    "checkpoint" property of "0002" atomically with appending files to them.

    3) The re-application of the checkpoint on connector restart sees that Table1 and Table2 already
    have "checkpoint" of "0002" and so the files are not appended to them again. Table3 is still
    at "0001" and so files are appended.

    It is important to note that this is not a 100% guarantee against duplicated data, since there
    is a possibility of zombie processes racing to append files _between_ the check to see if the
    table is already at "next-checkpoint" and the actual append operation.
    '''

    catalog = ctx.obj["catalog"]
    assert isinstance(catalog, Catalog)

    tbl = catalog.load_table(table)
    snap = tbl.current_snapshot()

    cp = None
    if snap is not None and snap.summary is not None:
        cp = snap.summary["checkpoint"]
        if cp is None:
            print("table snapshot existed but checkpoint was not set", snap.summary)

    if cp is not None:
        assert isinstance(cp, str)

        if cp == next_checkpoint:
            print(f"checkpoint is already '{next_checkpoint}'")
            return  # already appended these files
        elif cp != prev_checkpoint:
            raise Exception(
                f"checkpoint from snapshot ({cp}) did not match either previous ({prev_checkpoint}) or next ({next_checkpoint}) checkpoint"
            )
        
    # Files are only added if the table checkpoint property has the prior checkpoint. The checkpoint
    # property is updated to the current checkpoint in an atomic operation with appending the files.
    # Note that this is not 100% correct exactly-once semantics, since there is a potential race
    # between retrieving the table snapshot and appending the files, where a zombie process could
    # append the same files concurrently. In principal Iceberg catalogs support the atomic
    # operations necessary for true exactly-once semantics, but we'd need to work with the catalog
    # at a lower level than PyIceberg currently makes available.

    tbl.add_files(
        file_paths.split(","), snapshot_properties={"checkpoint": next_checkpoint}
    )


if __name__ == "__main__":
    run(auto_envvar_prefix="ICEBERG")
