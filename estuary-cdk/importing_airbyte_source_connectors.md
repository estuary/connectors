# Importing Airbyte Source Connectors

The Estuary CDK supports adapting Airbyte connectors to run on Estuary Flow. These are the
requirements for importing Airbyte connectors into this repository to run using the Estuary CDK. It
is intended as a reference for developers working on importing these connectors, and reviewers of
the connector import pull requests.

### A Note on Pre-existing Connectors

For the time being, all imported connectors will have already been running as Estuary Flow
connectors using a separate mechanism which is referred to as airbyte-to-flow, also known as ATF.
These connectors reside in the [estuary/airbyte repository](https://github.com/estuary/airbyte), and
you can see the list of pre-existing connectors there. ATF is a binary which wraps a specific Docker
image of an Airbyte connector and adapts its inputs and outputs to be compatible with Estuary Flow.

Since these imported connectors already exist in a different form, it is necessary to preserve
compatibility with them when the connectors are imported. Ideally, both versions of the connector
will have exactly the same outputs for their specification, discovered bindings, and captured
documents, but this is often not the case, especially when importing an Airbyte connector version
that is more recent than the version used by ATF. The requirements listed here assume that there is
a pre-existing connector, and the process for importing a connector from Airbyte that did not
previously exist as a connector in Estuary Flow will be different and are TBD.

## Requirements

Each connector is imported in a separate pull request, and each connector import pull request must
include the following it its commit history. It is not strictly necessary for each of these items to
be a separate commit, and in most cases the commits do not have to be in this order (with the
exception of snapshot outputs), but this sequence of commits is recommended for ease of review.

1. Import commit of the source code from the [airbyte
   repository](https://github.com/airbytehq/airbyte).
2. Snapshot test outputs for the pre-existing connector, at a minimum covering the connector `Spec`
   and `Discovery` outputs.
3. Snapshot test outputs for the imported connector. This must be in a separate, subsequent commit
   from [2], as the pre-existing connector snapshots must be carefully compared to the imported
   connector snapshots and all differences fully understood and noted to ensure backward
   compatibility.
4. Removal of unnecessary files from the imported source code. The final state of the connector
   folder should include only what is needed for the connector to run under the Estuary CDK.
5. Addition of the imported connector to the [python CI
   workflow](https://github.com/estuary/connectors/blob/main/.github/workflows/python.yaml) and
   verification that the tests pass when run in CI.

Each pull request should include the following if at all possible. If these items are not included,
it must be noted as to why they are not:
- Capture snapshot tests that verify the output of the connector for both the pre-existing version
  and imported version.
- Adapted unit tests from the Airbyte source, moved into the top-level `tests` directory for the
  connector and modified as needed to pass.

For the pull request description itself:
- Comment on the differences in snapshots for the following connector outputs. If they are
identical, note that they are identical. If they are different, describe how they are different and
why that is acceptable:
    - Spec
    - Discovery
    - Capture (if applicable)
- If there is documentation to be updated, describe that and include a link to the docs PR in the
  [estuary/flow repository](https://github.com/estuary/flow). The originator of the PR is
  responsible for any documentation updates that result from it, and the PR should not be merged
  until the corresponding documentation update PR is well on its way to being merged as well.
- Unless there are no previously existing capture tasks running on production that use this
  connector, a manual migration must be tested and document in the pull request description.

## Importing Airbyte Source Code

See instructions
[here](https://github.com/estuary/connectors/blob/main/python/README.md#pulling-an-open-source-connector-in-tree)
for how to do the import commit. Import the latest commit from the Airbyte repository that has an
MIT license.

## Comparing Snapshots

Due to the nature of JSON documents it may be difficult to compare changes to snapshots based on the
git diff alone. If the salient changes are not obvious from the diff, consider providing a link to
the
[jsondiff](https://github.com/zgrossbart/jdd?tab=readme-ov-file#load-my-json-data-from-the-internet)
for the snapshots in the PR description.

Generally there should be very few changes for the connector **Spec** output. If there are
non-trivial changes, this should almost always be fully backward compatible, such as a field that
used to be required now being optimal, or the addition of a new optional field. Other kinds of
changes may break compatibility with existing tasks. If there are any significant changes in the
spec output, the connector config should also be verified to work properly in the Estuary UI.

The connector **Discovery** output includes all of the bindings available for the connector and
their schemas. There will likely be differences in the snapshots for the Discovery output, but again
these should be backward compatible in the majority of cases. The discovered bindings should be the
same or perhaps with additional bindings for the new snapshots, but not fewer than before. New
fields may be added to the collection schemas, or their types changed in compatible ways such as
added nullability. Other kinds of changes to the Discovery output may break existing tasks and must
be thoroughly understood and explained.

Similarly, the imported connector **Capture** output should be backward compatible with the
pre-existing connector. There will always be additional metadata fields added by the Estuary CDK,
but other than that any differences in the data emitted by the connector should be scrutinized.

## Access to Test Systems

The originator of the connector import PR is responsible for setting up a suitable testing account
for the system to be captured from. The connector cannot be developed without suitable access for
verifying that the integration is functional. If a test account cannot readily be set up, start a
thread in Slack to discuss a plan for proceeding. See
[here](https://github.com/estuary/connectors/blob/main/python/README.md#encrypting-test-credentials)
for a reference on how to encrypt test system credentials so that they can be committed.

## Migration Testing

In the majority of cases, there are existing user capture tasks running on ATF, and they must be
able to transition seamlessly to the imported connector running on the Estuary CDK. To verify this
is indeed the case, a manual test should be performed and documented in the pull request description
that follows these steps:

1. Using the current latest version of the connector, set up a capture task.
2. Let it run until it outputs a checkpoint, and ideally captures some data.
3. Build the "new" connector Docker image using a `:local` tag.
4. Edit the spec of the capture task set up in (1) to use the newly build `:local` tag.
5. Verify that the task picks up where it left off after restarting on the new image.

The description of the migration test can be very simple if the migration is not noteworthy, but be
sure to call out any special scenarios that may be worth considering before transitioning user tasks
over.

The preference is to run these steps on a [local
stack](https://github.com/estuary/flow/blob/master/Tiltfile).

## File Structure

Imported connectors must have a `source-<connector>` folder, and that folder must contain a file
structure like [this](https://github.com/estuary/connectors/tree/main/source-notion).

## Connector Versioning

The `VERSION` file defines the version of the connector. Except in rare circumstances, this version
should be the same as the pre-existing connector version. You can find the pre-existing connector
version in the `estuary/airbyte` repository, in the `Dockerfile` for the connector, for example
[here](https://github.com/estuary/airbyte/blob/54a7b5838fa6ae045b93f9eef1295386a87f2690/airbyte-integrations/connectors/source-aircall/Dockerfile#L14).

## Deployment

Pre-existing connectors will already have a package that is published from the `estuary/airbyte`
repository, and that package will need to be removed prior to the package from the
`estuary/connectors` repository going live. If you don't have access to update packages, ask for
assistance in Slack. Don't remove the `estuary/airbyte` package until you are 100% sure that it
won't be needed anymore, as it will make the connector images unavailable until the
`estuary/connectors` image is pushed.

## Reference Pull Request

The requirements described here are illustrated in this pull request, which imports `source-notion`:
https://github.com/estuary/connectors/pull/1407