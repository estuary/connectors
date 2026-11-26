import asyncio

import source_iceberg

asyncio.run(source_iceberg.Connector().serve())
