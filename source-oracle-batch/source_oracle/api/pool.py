import asyncio

import pyodbc
from asyncio_connection_pool import ConnectionPool, ConnectionStrategy

from source_netsuite.api.odbc_auth import (
    generate_connection_config,
    generate_connection_string,
)
from source_netsuite.models import NETSUITE_ODBC_CONSTANTS, EndpointConfig

NetsuiteConnectionPool = ConnectionPool[pyodbc.Connection]


def make_pool(cfg: EndpointConfig, limit: int) -> NetsuiteConnectionPool:
    class NetsuiteConnectionStrategy(ConnectionStrategy[pyodbc.Connection]):
        async def make_connection(self) -> pyodbc.Connection:
            # NOTE these options do not seem to impact the NetSuite driver behavior at all
            # pyodbc.odbcversion = "3.8"
            # pyodbc.pooling = False

            database_attributes: dict[int, int | str] = {
                # NOTE the timeout configuration here is very carefully configured, yet does still not enforce a timeout!
                #      read the ODBC section of the readme for more information.
                #
                #      The connection timeout should generally be *very* fast, so we don't use the same timeout value as the query
                NETSUITE_ODBC_CONSTANTS.SQL_ATTR_CONNECTION_TIMEOUT: 60,
            }

            # if config("ENABLE_ODBC_TRACING", default=False, cast=bool):
            # database_attributes[NETSUITE_ODBC_CONSTANTS.SQL_OPT_TRACE] = 1
            # database_attributes[NETSUITE_ODBC_CONSTANTS.SQL_OPT_TRACEFILE] = "/tmp/odbc.log"

            conn_fut = asyncio.to_thread(
                pyodbc.connect,
                generate_connection_string(cfg),
                # controls the SQL_LOGIN_TIMEOUT, which could be set via `attrs_before` instead
                # https://github.com/iloveitaly/pyodbc/blob/00f1f3d90e389366b98e2336ed61b142ba67022a/src/connection.cpp#L68-L76
                timeout=15,
                # sets `SQL_ACCESS_MODE`, it's unclear if this impacts the NetSuite driver which is always read-only
                readonly=True,
                # https://github.com/mkleehammer/pyodbc/issues/106
                # https://github.com/iloveitaly/pyodbc/blob/00f1f3d90e389366b98e2336ed61b142ba67022a/src/connection.cpp#L102
                attrs_before=database_attributes,
            )
            conn: pyodbc.Connection = await asyncio.wait_for(conn_fut, timeout=30)

            # NOTE this does *not* work in the NetSuite driver, which is why the query timeout must be set manually on the cursor
            # connection.timeout = attempted_timeout

            return conn

        def connection_is_closed(self, conn):
            return conn.closed

        async def close_connection(self, conn):
            conn.close()

    return ConnectionPool(strategy=NetsuiteConnectionStrategy(), max_size=limit)
