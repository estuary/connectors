import asyncio
import source_influxdb

asyncio.run(source_influxdb.Connector().serve())
