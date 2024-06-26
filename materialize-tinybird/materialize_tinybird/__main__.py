import asyncio

import materialize_tinybird # type: ignore

asyncio.run(materialize_tinybird.Connector().serve())