import asyncio

import source_aws_cost_explorer

asyncio.run(source_aws_cost_explorer.Connector().serve())
