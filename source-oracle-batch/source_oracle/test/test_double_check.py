# We want to test that the number of unique primary keys that were captured
# match the number of unique primary keys in the table itself.
from datetime import timedelta

import pytest
from estuary_cdk.logger import init_logger

from source_netsuite.api.pool import make_pool
from source_netsuite.test.utils import count_rows, load_config


# Fixture to define the folder in which table counts will get written
@pytest.fixture
def table_counts_folder() -> str:
    return "table_counts"


@pytest.mark.asyncio
async def test_export_table_size(table_name: str, table_counts_folder: str):
    # debugpy.listen(5678)
    log = init_logger()
    config = load_config()

    pool = make_pool(config, config.advanced.connection_limit)

    count = await count_rows(log, pool, table_name, timeout=timedelta(seconds=20), timeout_val=-1)

    with open(f"{table_counts_folder}/{table_name}.json", "w") as f:
        f.write(f"{count}")
