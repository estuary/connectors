# Materialize connector integration tests

The test framework runs a Flow catalog, which contains a few predefined test data collections, and
allows customized materialize destinations for testing purposes. During a test execution, the framework
starts the Flow catalog, ingests data to the collections, collects the materialization results, and
checks the results against the expected ones.

A few pieces of logic in the test execution is defined by each testing connector, which are contained in the
scripts located in the subdirectory with the same name as the connector. These scripts include:

- `setup.sh`: Executed to setup the test. This script is expected to export the env variables
  `CONNECTOR_CONFIG` and `RESOURCES_CONFIG`, which are used to generate the Flow catalog yaml.
  Can also export other env variables, for example so it can track things that need to be cleaned
  up.
- `ingest-data.sh`: Executed to perform data ingestion to the catalogs. The script controls the content, 
  sequence and speed of data ingestion. Predefined testing datasets are available for the script to use. It is
  encouraged to enhance the testing datasets/collections for higher testing coverages.
- `fetch-materialize-results.sh`: Executed to fetch the materialization results from the destinations, and store
   them into a result directory. Multi-files are allowed to be generated for different materialization destinations.
   The results are recommended to be JSONL files.
- `cleanup.sh`: Cleans up any test resources. Can read any variables exported by `setup.sh` so it
  knows what to cleanup.
- `expected`: The directory of expected files generated from `fetch-materialize-results.sh`. It is used by the
   framework to verify the results from `fetch-materialize-results.sh`.

Each test will have intermediate data written to `.build/tests/<connector>/`. This will contain the
Flow catalog that was run and the actual results from querying the materialization.

