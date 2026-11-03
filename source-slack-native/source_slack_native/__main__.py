import asyncio

import source_slack_native

asyncio.run(source_slack_native.Connector().serve())
