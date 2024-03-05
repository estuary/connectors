import asyncio
import source_hubspot_native

asyncio.run(source_hubspot_native.Connector().serve())
