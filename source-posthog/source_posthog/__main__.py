import asyncio

import source_posthog

asyncio.run(source_posthog.Connector().serve())
