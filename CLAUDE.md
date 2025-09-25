# Estuary Connectors Development Guide

This document provides essential context for AI-assisted development of Estuary connectors, focusing on the Python CDK framework and development patterns specific to this project.

## Project Overview

**Repository**: Estuary Flow Connectors  
**Primary Framework**: Estuary CDK (Python-based)  
**Architecture**: Multi-language project supporting both source (capture) and materialize connectors  
**Deployment**: Docker containers via GitHub Container Registry

### Tech Stack
- **Python 3.11+** with Poetry dependency management
- **Estuary CDK**: Core framework for connector development
- **Pydantic v2**: Data validation and serialization
- **AsyncIO**: Asynchronous operations and HTTP requests
- **Docker**: Containerization and deployment
- **Flow Protocol**: Estuary's data streaming protocol

## Project Structure

```
/connectors/
├── estuary-cdk/              # Core Python CDK framework
├── source-*/                 # Source connectors (data ingestion)
├── materialize-*/            # Materialize connectors (data output)
├── base-image/               # Docker base image
├── build-local.sh           # Local build script
├── connector-variant.Dockerfile  # Variant builds
└── poetry.toml              # Poetry configuration
```

### Connector Directory Structure
Standard layout for Python connectors:
```
source-<name>/
├── pyproject.toml           # Poetry dependencies
├── config.yaml             # Test configuration (encrypted)
├── test.flow.yaml           # Flow testing configuration
├── source_<name>/
│   ├── __init__.py         # Main connector class
│   ├── models.py           # Pydantic data models
│   ├── api.py              # API client implementation
│   └── resources.py        # Resource definitions
└── tests/                  # Test files and snapshots
```

## Core CDK Components

### BaseCaptureConnector
Main base class for source connectors:
```python
from estuary_cdk.capture import BaseCaptureConnector, Request, Task, common

class Connector(BaseCaptureConnector[EndpointConfig, ResourceConfig, ConnectorState]):
    def request_class(self):
        return Request[EndpointConfig, ResourceConfig, ConnectorState]
    
    async def spec(self, log: Logger, _: request.Spec) -> ConnectorSpec:
        return ConnectorSpec(
            documentationUrl="https://go.estuary.dev/connector-docs",
            configSchema=EndpointConfig.model_json_schema(),
            oauth2=OAUTH2_SPEC,  # Optional OAuth2 config
            resourceConfigSchema=ResourceConfig.model_json_schema(),
            resourcePathPointers=ResourceConfig.PATH_POINTERS,
        )
```

### Resource Abstractions

#### ResourceConfig
Defines connector-specific configuration:
```python
from estuary_cdk.capture.common import ResourceConfig as BaseResourceConfig

class ResourceConfig(BaseResourceConfig):
    name: str
    interval: timedelta = timedelta(minutes=5)  # Default interval
    PATH_POINTERS: ClassVar[list[str]] = ["/name"]  # JSON pointers for resource identification
```

#### Resource Definition
Standard pattern for defining resources:
```python
from estuary_cdk.capture.common import Resource, ResourceState, open_binding

def create_resource(name: str, model_class: type, http: HTTPSession) -> Resource:
    def open(binding, binding_index, state, task, all_bindings):
        open_binding(
            binding, binding_index, state, task,
            fetch_changes=fetch_changes_function,    # Incremental updates
            fetch_page=fetch_page_function,          # Backfill pagination
            fetch_snapshot=fetch_snapshot_function,  # Full snapshot (optional)
        )
    
    return Resource(
        name=name,
        key=["/id"],  # Primary key path
        model=model_class,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=datetime.now(tz=UTC)),
            backfill=ResourceState.Backfill(next_page=None, cutoff=datetime.now(tz=UTC)),
        ),
        initial_config=ResourceConfig(name=name),
        schema_inference=True,
    )
```

### Function Types

#### FetchPageFn
For backfill operations:
```python
async def fetch_page(
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[Document | PageCursor, None]:
    # Yield documents older than cutoff
    # Yield next page cursor if more data available
```

#### FetchChangesFn  
For incremental updates:
```python
async def fetch_changes(
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Document | LogCursor, None]:
    # Yield new/updated documents since log_cursor
    # Yield updated cursor to checkpoint progress
```

#### FetchSnapshotFn
For periodic full snapshots:
```python
async def fetch_snapshot(
    log: Logger,
) -> AsyncGenerator[Document, None]:
    # Yield all current documents
    # Used for relatively static data
```

### Authentication Patterns

#### OAuth2 Configuration
```python
from estuary_cdk.capture.common import BaseOAuth2Credentials, OAuth2Spec

OAUTH2_SPEC = OAuth2Spec(
    provider="service_name",
    authUrlTemplate="https://service.com/oauth/authorize?client_id={{{ client_id }}}&...",
    accessTokenUrlTemplate="https://service.com/oauth/token",
    accessTokenHeaders={"content-type": "application/x-www-form-urlencoded"},
    accessTokenBody="grant_type=authorization_code&client_id={{{ client_id }}}&...",
    accessTokenResponseMap={"refresh_token": "/refresh_token"},
)

# Auto-generate OAuth2Credentials class
OAuth2Credentials = BaseOAuth2Credentials.for_provider(OAUTH2_SPEC.provider)
```

#### HTTP Session with Token Management
```python
from estuary_cdk.http import HTTPSession, TokenSource

# In resources setup:
http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)

# HTTP requests automatically handle token refresh
result = await http.request(log, url, method="GET", params=params)
```

## Development Commands

### Setup
```bash
cd source-<connector-name>
poetry install                    # Install dependencies
poetry shell                     # Activate virtual environment
```

### Local Testing with flowctl
For local development and testing of Python connectors, ensure your [`pyproject.toml`](source-hackernews/pyproject.toml:1) includes the proper estuary-cdk dependency and connector script entry point:

```toml
[tool.poetry.dependencies]
python = "^3.11"
estuary-cdk = {path = "../estuary-cdk", develop = true}

[tool.poetry.scripts]
connector = "source_<connector_name>:main"
```

This configuration allows you to:
1. **Install with local CDK development**: The `develop = true` flag enables live editing of the CDK
2. **Run connector commands**: Use `poetry run connector <command>` for testing
3. **Local flowctl integration**: Test with `flowctl preview --source test.flow.yaml`

### Testing
```bash
flowctl preview --source test.flow.yaml  # Preview capture with Flow
flowctl raw spec --source test.flow.yaml --capture <capture-name> | jq  # Inspect spec output
```

### Building
```bash
./build-local.sh source-<connector-name>  python/Dockerfile # Build Docker image with base image
```

## Best Practices

### Error Handling
- Use proper HTTP error handling with retries
- Log meaningful error messages with context
- Implement graceful degradation for API rate limits

### Performance
- Use async/await patterns consistently
- Implement proper pagination for large datasets
- Consider memory usage for large response processing

### Data Modeling
- Extend `BaseDocument` for all data models
- Use Pydantic validators for data transformation
- Include `_meta` field for operational metadata

### State Management
- Always emit cursors to checkpoint progress
- Handle connector restarts gracefully
- Use appropriate cursor types (datetime, string, int)

## Common Patterns

### CRM Object with Associations
Many connectors fetch objects with related entity associations:
```python
async def fetch_with_associations(
    cls: type[CRMObject],
    object_name: str, 
    ids: list[str],
    http: HTTPSession,
) -> list[CRMObject]:
    # Fetch main objects and associations in parallel
    batch, associations = await asyncio.gather(
        fetch_batch(ids),
        fetch_associations(ids)
    )
    # Merge associations into objects
    return merge_associations(batch, associations)
```

### Rate Limiting and Retry Logic
```python
import asyncio
from estuary_cdk.http import HTTPError

async def api_call_with_retry(log: Logger, http: HTTPSession, url: str):
    attempt = 1
    while True:
        try:
            return await http.request(log, url)
        except HTTPError as e:
            if attempt == 5 or e.code not in [429, 500, 502, 503]:
                raise
            log.warning(f"Retrying API call (attempt {attempt})", {"error": str(e)})
            await asyncio.sleep(attempt * 2)
            attempt += 1
```

## Advanced CDK Patterns

### Emitted Changes Cache
Used for deduplication in dual-stream patterns:
```python
import estuary_cdk.emitted_changes_cache as cache

# Add recent change to cache
cache.add_recent(object_name, key, timestamp)

# Check if more recent change exists
if cache.has_as_recent_as(object_name, key, timestamp):
    continue  # Skip duplicate

# Cleanup old entries
evicted = cache.cleanup(object_name, cutoff_timestamp)
```

### Buffer Ordered Processing
For async batch processing with concurrency control:
```python
from estuary_cdk.buffer_ordered import buffer_ordered

async def _batches_gen() -> AsyncGenerator[Awaitable[Iterable[Document]], None]:
    for batch in itertools.batched(items, batch_size):
        yield process_batch(batch)

# Process batches with limited concurrency
async for result in buffer_ordered(_batches_gen(), max_concurrent=3):
    for doc in result:
        yield doc
```

### Backfill Triggers
For requesting automatic backfill:
```python
from estuary_cdk.capture.common import Triggers

async def fetch_changes(log: Logger, log_cursor: LogCursor):
    try:
        # Attempt incremental fetch
        async for item in fetch_incremental(log_cursor):
            yield item
    except APILimitExceeded:
        # Trigger automatic backfill when incremental fails
        log.info("triggering automatic backfill due to API limits")
        yield Triggers.BACKFILL
```

### Tombstone Records
For handling deletions in snapshot-based resources:
```python
def create_snapshot_resource(model_class: type) -> Resource:
    tombstone = model_class(_meta=model_class.Meta(op="d"))
    
    def open(binding, binding_index, state, task, all_bindings):
        open_binding(
            binding, binding_index, state, task,
            fetch_snapshot=fetch_snapshot_function,
            tombstone=tombstone,  # Used to mark deleted records
        )
```

### Metadata and Operations
Document metadata for change tracking:
```python
class MyDocument(BaseDocument):
    id: int
    name: str
    # _meta field is automatically added by BaseDocument
    
    def with_meta(self, op: str = "u", row_id: int | None = None):
        if isinstance(self, dict):
            self["_meta"] = {"op": op, "row_id": row_id}
        else:
            self._meta = BaseDocument.Meta(op=op, row_id=row_id)
        return self

# Operation types:
# "c" = create/insert
# "u" = update/upsert
# "d" = delete
```

### Scheduled Resources
For cron-based execution patterns:
```python
from estuary_cdk.capture.common import ResourceConfigWithSchedule

class ScheduledResourceConfig(ResourceConfigWithSchedule):
    name: str
    schedule: str = "0 */6 * * *"  # Every 6 hours
    
# Resource will automatically trigger backfill on schedule
```

## Connector Development Guidelines

### Project Setup
1. **Use Poetry for dependency management**: All Python connectors use [`pyproject.toml`](source-hubspot-native/pyproject.toml:1) with Poetry
2. **Follow naming conventions**: `source-<name>` for captures, `materialize-<name>` for materializations
3. **Include required files**: `config.yaml`, `test.flow.yaml`, and proper package structure
4. **Configure pyproject.toml for local testing**: Include estuary-cdk path dependency and connector script entry:
   ```toml
   [tool.poetry.dependencies]
   estuary-cdk = {path = "../estuary-cdk", develop = true}
   
   [tool.poetry.scripts]
   connector = "source_<connector_name>:main"
   ```
   The `develop = true` flag enables live editing of the CDK during development, and the script entry point allows testing with `poetry run connector <command>`.

### Testing Strategy
- **Local testing**: Use `flowctl preview --source test.flow.yaml` for quick validation
- **Integration tests**: Place test files in `tests/` directory with snapshot expectations
- **Configuration**: Keep sensitive test config in encrypted `config.yaml` files

### Docker Integration
- **Base image**: Extend from project's `base-image/Dockerfile`
- **Build locally**: Use `build-local.sh` for development builds
- **Deployment**: Automated via GitHub Container Registry on merge to main

### API Client Best Practices
- **Use `HTTPSession` with automatic token management**
- **Implement proper pagination**: Handle large datasets with appropriate cursor strategies
- **Rate limiting**: Build in retry logic and respect API limits
- **Error handling**: Graceful degradation for temporary failures

### Resource Design Patterns
- **Incremental + Backfill**: Most resources should support both `fetch_changes` and `fetch_page`
- **State management**: Always emit cursors to checkpoint progress
- **Schema inference**: Enable automatic schema detection where possible
- **Key selection**: Choose appropriate primary keys for deduplication

### Performance Considerations
- **Concurrency**: Use `buffer_ordered` for controlled parallel processing
- **Memory efficiency**: Process large datasets in batches, avoid loading everything into memory
- **Cursor strategy**: Use appropriate cursor types (timestamp, offset, token) based on API capabilities
- **Deduplication**: Implement `emitted_changes_cache` for dual-stream patterns

### Authentication Implementation
- **OAuth2**: Use `OAuth2Spec` and `BaseOAuth2Credentials` patterns
- **API Keys**: Store in endpoint configuration with proper validation
- **Token refresh**: Leverage `TokenSource` for automatic token management

### Debugging and Monitoring
- **Structured logging**: Use provided Logger with contextual information
- **Error context**: Include relevant state information in error messages
- **Progress tracking**: Emit regular cursor updates for long-running operations
- **Metrics**: Consider performance implications of connector design

### Documentation Standards
- **Resource documentation**: Document each resource's purpose and key fields
- **Configuration examples**: Provide clear examples in connector documentation
- **API limitations**: Document known limitations and workarounds
- **Changelog**: Maintain version history for connector updates
