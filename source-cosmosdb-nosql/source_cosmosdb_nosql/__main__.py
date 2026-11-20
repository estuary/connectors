import asyncio

import source_cosmosdb_nosql

asyncio.run(source_cosmosdb_nosql.Connector().serve())
