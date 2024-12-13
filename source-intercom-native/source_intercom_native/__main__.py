import asyncio
import source_intercom_native

asyncio.run(source_intercom_native.Connector().serve())
