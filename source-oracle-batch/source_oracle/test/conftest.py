import asyncio

from source_netsuite.test.utils import feasible_tables


def pytest_generate_tests(metafunc):
    # Parameterize with the fetched data
    if "feasible_table_name" in metafunc.fixturenames:
        data = asyncio.run(feasible_tables(5_000_000))
        metafunc.parametrize("feasible_table_name", data)
    elif "table_name" in metafunc.fixturenames:
        data = asyncio.run(feasible_tables(None))
        metafunc.parametrize("table_name", data)
