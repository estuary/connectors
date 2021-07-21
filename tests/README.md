# Connector integration tests

These tests run each connector as part of a basic Flow catalog that captures from the source and
materializes into a sqlite table. The contents of the sqlite table are then checked against the
expected contents.

Each subdirectory here is a test of the source connector with the same name. Each test directory
must contain the following files:

- `setup.sh`: Executed to setup the test. This script is expected to export the env variables
  `CONNECTOR_CONFIG` and `STREAM`, which are used to generate the Flow catalog yaml.
- `cleanup.sh`: Cleans up any test resources. Can read the `STREAM` variable so it knows what to
  cleanup.
- `expected.txt`: The expected contents of the materialized table. The tests will execute the query:
  `select id, canary from test_results;` and expect that the results of that match `expected.txt`
  exactly.

Each test will have intermediate data written to `.build/tests/<connector>/`. This will contain the
Flow catalog that was run and the actual results from querying the materialization.

