import asyncio

import source_claude_api

asyncio.run(source_claude_api.Connector().serve())
