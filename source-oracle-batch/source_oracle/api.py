from datetime import datetime, UTC
from estuary_cdk.http import HTTPSession
from logging import Logger
from pydantic import TypeAdapter
import json
import pytz
from typing import Iterable, Any, Callable, Awaitable, AsyncGenerator, Dict
import asyncio
import itertools
from copy import deepcopy
import oracledb
import inspect
from jinja2 import Template


from .models import (
    OracleTable,
    OracleColumn,
    EndpointConfig,
    Table,
    Document,
)


def connect(config: EndpointConfig) -> oracledb.Connection:
    return oracledb.connect(user=config.credentials.username,
                            password=config.credentials.password,
                            dsn=config.address)


async def fetch_tables(
    log: Logger, conn: oracledb.Connection,
) -> list[OracleTable]:
    cursor = conn.cursor()

    sql_columns = ','.join([f.alias for (k, f) in OracleTable.model_fields.items()])

    query = f"SELECT {sql_columns} FROM user_tables"

    tables = []
    for values in cursor.execute(query):
        cols = [col[0] for col in cursor.description]
        row = dict(zip(cols, values))
        tables.append(
            OracleTable(**row)
        )

    return tables


async def fetch_columns(
    log: Logger, conn: oracledb.Connection,
) -> list[OracleColumn]:
    cursor = conn.cursor()

    sql_columns = ','.join(["t." + f.alias for (k, f) in OracleColumn.model_fields.items() if f.alias != 'COL_IS_PK'])

    query = f"""
    SELECT {sql_columns}, NVL2(c.constraint_type, 1, 0) as COL_IS_PK FROM user_tab_columns t
        LEFT JOIN (
                SELECT c.table_name, c.constraint_type, ac.column_name FROM all_constraints c
                    INNER JOIN all_cons_columns ac ON (
                        c.constraint_name = ac.constraint_name
                        AND c.table_name = ac.table_name
                        AND c.constraint_type = 'P'
                    )
                ) c
        ON (t.table_name = c.table_name AND t.column_name = c.column_name)
    """

    columns = []
    for values in cursor.execute(query):
        cols = [col[0] for col in cursor.description]
        row = dict(zip(cols, values))
        row = {k: v for (k, v) in row.items() if v is not None}
        columns.append(OracleColumn(**row))

    return columns


async def fetch_rows(
    # Closed over via functools.partial:
    table: Table,
    conn: oracledb.Connection,
    cursor: list[str],
    # Remainder is common.FetchPageFn:
    log: Logger,
    page: str | None,
    cutoff: datetime,
) -> AsyncGenerator[Document | str, None]:
    is_first_query = page is None

    # page is in fact a json encoded object where keys are cursor fields and values
    # are the last value for that cursor
    cursor_values = json.loads(page) if page is not None else {}
    query = query_template.render(table_name=table.table_name,
                                  cursor_fields=cursor,
                                  is_first_query=is_first_query)

    last_row = None
    bind_params = {}
    if page is not None:
        bind_params = cursor_values

    log.info(query, page, cursor)

    with conn.cursor() as c:
        for values in c.execute(query, bind_params):
            cols = [col[0] for col in c.description]
            row = dict(zip(cols, values))
            row = {k.lower(): v for (k, v) in row.items() if v is not None}
            last_row = row
            log.info("setting last_row to", last_row)

            doc = Document()
            doc.meta_ = Document.Meta(op='c')
            for (k, v) in row.items():
                setattr(doc, k, v)

            yield doc

    if len(cursor) > 0:
        if last_row is not None:
            yield json.dumps({k: last_row[k] for k in cursor})
        else:
            yield page

query_template = Template("""
{#***********************************************************
  * This is a generic query template which is provided so that *
  * discovered bindings can have a vaguely reasonable default  *
  * behavior.                                                  *
  *                                                            *
  * You are entirely free to delete this template and replace  *
  * it with whatever query you want to execute. Just be aware  *
  * that this query will be executed over and over every poll  *
  * interval, and if you intend to lower the polling interval  *
  * from its default of 24h you should probably try and use a  *
  * cursor to capture only new rows each time.                 *
  *                                                            *
  * By default this template generates a 'SELECT * FROM table' *
  * query which will read the whole table on each poll.        *
  *                                                            *
  * If the table has a suitable "cursor" column (or columns)   *
  * which can be used to identify only changed rows, add the   *
  * name(s) to the "Cursor" property of this binding. If you   *
  * do that, the generated query will have the form:           *
  *                                                            *
  *     SELECT * FROM table                                    *
  *       WHERE (ka > :0) OR (ka = :0 AND kb > :1)             *
  *       ORDER BY ka, kb;                                     *
  *                                                            *
  * This can be used to incrementally capture new rows if the  *
  * table has a serial ID column or a 'created_at' timestamp,  *
  * or it could be used to capture updated rows if the table   *
  * has an 'updated_at' timestamp.                             *
  ***********************************************************/ #}
{%- if cursor_fields|length -%}
  {%- if is_first_query -%}
    SELECT * FROM {{ table_name }}
  {%- else -%}
    SELECT * FROM {{ table_name }}
	{% for k in cursor_fields -%}
      {%- set outer_loop = loop -%}
	  {%- if loop.first %} WHERE ({%- else -%}) OR ({% endif -%}
      {%- for n in cursor_fields -%}
		{% if loop.index < outer_loop.index %}
          {{ n }} = :{{ n }} AND {% endif -%}
	  {% endfor %}
	  {{ k }} > :{{ k }}
	{%- endfor %}
	)
  {% endif %} ORDER BY {% for k in cursor_fields -%}{%- if not loop.first -%}, {% endif -%}{{k}}{%- endfor -%}
{%- else -%}
  SELECT * FROM {{ table_name }}
{%- endif -%}""")
