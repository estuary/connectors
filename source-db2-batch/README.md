# IBM Db2 Batch Query Connector

This connector captures data from IBM Db2 databases into Flow collections by
periodically executing queries and translating the results into JSON documents.

## Supported Platforms

- Db2 for Linux, UNIX, and Windows (LUW)
- Potentially Db2 for i (AS/400) and Db2 for z/OS (requires additional testing)

## Development

### Starting and stopping the test database

```bash
docker compose -f source-db2-batch/docker-compose.yaml up -d
docker compose -f source-db2-batch/docker-compose.yaml down --volumes
```

### Running Connector Tests

Note that unlike many connectors, the Db2 connector build requires various
environment variables to be set. For simplicity, we have a `run_tests.sh`
script which sets these variables before running the test suite.

```bash
# Run all tests
source-db2-batch/run_tests.sh

# Update test snapshots
env UPDATE_SNAPSHOTS=1 source-db2-batch/run_tests.sh

# Run a specific test
source-db2-batch/run_tests.sh -run=TestFoobar
```
