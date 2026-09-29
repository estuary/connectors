import asyncio
import source_customer_io

asyncio.run(source_customer_io.Connector().serve())
