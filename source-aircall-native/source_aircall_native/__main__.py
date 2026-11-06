import asyncio

import source_aircall_native

asyncio.run(source_aircall_native.Connector().serve())
