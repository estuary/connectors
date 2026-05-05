# Test fixtures

By default, integration tests run against the local Bigtable emulator (brought
up by the connector's `docker-compose.yaml`) using `config.local.yaml`. CI runs
this path so the test suite is fast and free.

A second materialization task (`acmeCo/tests/materialize-bigtable-gcp`) is
defined in `materialize.flow.yaml` and `apply.flow.yaml` for testing against a
real Cloud Bigtable instance using the encrypted `config.gcp.yaml`. It is
**commented out by default**. Run it manually when you need to verify behavior
the emulator doesn't cover (auth, PingAndWarm, real network errors, etc.).

## Running against real Cloud Bigtable

### 1. Uncomment the GCP task

In both `materialize.flow.yaml` and `apply.flow.yaml`, uncomment the
`acmeCo/tests/materialize-bigtable-gcp` materialization block. Be sure to
re-comment it before pushing.

### 2. Create the Bigtable instance

`config.gcp.yaml` is configured to talk to an instance named `estuary-test` in
the `estuary-theatre` project, single SSD node in `us-east1-d`. Create it:

```bash
gcloud bigtable instances create estuary-test \
  --display-name=estuary-test \
  --cluster-config=id=estuary-test-c1,zone=us-east1-d,nodes=1 \
  --project=estuary-theatre
```

### 3. Add a placeholder table

The Bigtable Go client primes its connection pool by calling `PingAndWarm` at
startup. The server returns `NotFound: No tables found for instance` when the
instance is empty, which the client treats as a fatal startup error. Creating
any single table makes priming succeed:

```bash
cbt -project=estuary-theatre -instance=estuary-test createtable __keepalive
```

(`cbt` is part of the gcloud SDK: `gcloud components install cbt`.)

### 4. Grant the test SA Bigtable roles on the instance

The service account whose key is embedded in `config.gcp.yaml` needs Bigtable
permissions on the new instance. The connector creates tables and column
families during Apply, so it needs admin in addition to user:

```bash
SA="<service-account-email>"  # decrypt config.gcp.yaml to find it

gcloud bigtable instances add-iam-policy-binding estuary-test \
  --member="serviceAccount:${SA}" \
  --role='roles/bigtable.user' \
  --project=estuary-theatre

gcloud bigtable instances add-iam-policy-binding estuary-test \
  --member="serviceAccount:${SA}" \
  --role='roles/bigtable.admin' \
  --project=estuary-theatre
```

IAM bindings can take 30-120 seconds to propagate. If the first test attempt
fails with `PermissionDenied: bigtable.instances.ping`, wait a minute and retry.

### 5. Run the tests

From the repo root:

```bash
go test -v -run TestIntegration -timeout 20m ./materialize-bigtable/...
```

Both the emulator-backed and GCP-backed materialization tasks will run.

### 6. Tear down the instance

To stop being billed, delete the instance when you're done. Tables,
column families, and IAM bindings on the instance go with it.

```bash
gcloud bigtable instances delete estuary-test --project=estuary-theatre
```
