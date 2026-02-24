# Python Connector Development (estuary-cdk)

Async Python framework for building Flow connectors. Uses Pydantic v2 for models and JSON schema generation.

See [README.md](README.md) for architecture overview and design rationale.

## Connector Layout

```
source-example/
├── pyproject.toml              # Poetry deps: estuary-cdk, pydantic
├── poetry.lock
├── VERSION
├── test.flow.yaml              # Flow catalog for testing
├── acmeCo/                     # Test fixtures
└── source_example/
    ├── __init__.py             # Connector class (extends BaseCaptureConnector)
    ├── __main__.py             # Entry: asyncio.run(Connector().serve())
    ├── models.py               # Pydantic models for config, resources, documents
    ├── api.py                  # Pure functions for API interactions
    └── resources.py            # Binds models + API into Resource objects
```

## Getting Started

```bash
# Create virtual environment and install dependencies
cd source-example
python -m venv .venv && source .venv/bin/activate
poetry install

# Build Docker image (from repo root)
./build-local.sh source-example
```

## Test

Commands assume you're in the connector directory with the venv activated.

```bash
# Run tests
poetry run pytest

# Update test snapshots
poetry run pytest --insta=update
```

## Reference Connectors

Study these for common patterns:

| Connector                      | Pattern                                  |
| ------------------------------ | ---------------------------------------- |
| `source-front/`                | Simple REST API with incremental capture |
| `source-salesforce-native/`    | OAuth authentication, complex resources  |
| `source-google-sheets-native/` | Google OAuth, service accounts           |
| `source-airtable-native/`      | Pagination, nested resources             |

Note: The `-native` suffix indicates a first-party CDK connector that replaces an existing third-party connector with the same base name. Connectors without the suffix may also be first-party CDK connectors if there was no naming conflict.

## Development Guidelines

### Clarity over cleverness

Write clear, self-explanatory code. Avoid clever solutions that require mental gymnastics to understand. If code needs a comment to explain what it does, consider rewriting it to be more obvious.

### Explicit over implicit

Prefer explicit parameter passing over implicit state. Make data flow visible and traceable.

### Use type annotations

Annotate all function signatures and class attributes. The CDK relies heavily on types for schema generation and validation.

### Pydantic modelling

- Always model output objects as `BaseDocument` subclasses, use `extra="allow"`
- Keep defined fields to just primary keys, cursors and anything referenced in the codebase
- Don't over-specify schemas—Estuary's schema inference handles additional fields automatically
- Prefer type parameter syntax (`class Foo[T]`) over `TypeVar` + `Generic[T]`

### Error handling

- Raise `ValidationError` for configuration problems
- Let `HTTPError` propagate for API failures (CDK handles retries)
- Include actionable context in error messages

### API response processing

- Always prefer using `IncrementalJsonProcessor` and `IncrementalCsvProcessor` over manually parsing responses.

#### Basic usage

```python
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

headers, body = await http.request_stream(log, url, params=params)
processor = IncrementalJsonProcessor(
    body(),                    # Async generator of bytes
    "results.item",            # JSON path to streamed item
    MyModel,                   # Output Pydantic model
    remainder_cls=ResponseMeta # Optional: model for metadata outside the array
)

async for item in processor:
    yield item

# Access remainder metadata after iteration
meta = processor.get_remainder()
next_url = meta.next  # e.g., pagination URL
```

#### When to use remainder classes

- **Pagination metadata**: Extract `next`/`previous` URLs from response envelope
- **Query metadata**: Extract column names, totals, cursor tokens
- **Any structure outside the array path**: Remainder captures everything not at the prefix

### Incremental cursor behavior

- When implementing an incremental stream, always try to implement a backfill function (`fetch_page`) alongside `fetch_changes`. The backfill collects all historical data up to the log cursor.
- Cursors are most commonly based on `updated_at` timestamps. A `created_at` timestamp is useless for incremental sync — you'd have to traverse the entire dataset to ensure no updates to older records are missed. In those cases, use a snapshot stream instead, which also captures deletions.
- `fetch_changes` may return without yielding a LogCursor if no documents were emitted.
- A LogCursor MUST be yielded after documents are emitted.

### Pagination

- Always research and document the maximum page size per endpoint when building a new connector. Different endpoints within the same API may have different limits.

### Testing

- Use snapshot tests for discovery and validation outputs
- Test against real APIs when possible (with fixtures in `acmeCo/`)
