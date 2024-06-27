import asyncio

import materialize_test


# uvloop.run(materialize_test.Connector().serve())
asyncio.run(materialize_test.Connector().serve())
