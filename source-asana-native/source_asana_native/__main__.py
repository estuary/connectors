import asyncio
from source_asana_native import Connector

if __name__ == "__main__":
    asyncio.run(Connector().serve())
