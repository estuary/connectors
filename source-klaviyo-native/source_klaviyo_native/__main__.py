import asyncio

import source_klaviyo_native

asyncio.run(source_klaviyo_native.Connector().serve())
