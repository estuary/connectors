import asyncio
import source_jira_native

asyncio.run(source_jira_native.Connector().serve())
