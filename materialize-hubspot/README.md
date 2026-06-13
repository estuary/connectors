# HubSpot

The HubSpot connector materializes CRM Object into the HubSpot CRM.  This
document describes some background and tips for development.

## Transaction Pattern

This connector implements the "Recovery Log with Non-Transactional Destination"
transactional pattern.  Transactions are at-least-once and as it operates in
delta-update mode it is always last write wins.

## Philosophy

Typical connectors assume full control over the destination system and disallow
modifications.  When building this connector, we anticipated that users would
need to do a hybrid of materialization and manual editing.

It is also expected that updates to objects need to be partial.  To this end the
connector always operates in delta-update mode.

To help maintain data quality in HubSpot, the connector does not automatically
define new properties.  The user should create a new property group and
properties for any desired fields.  It may be useful to use derivations for
more advanced collection shaping.

## Deletions

Backfills do not remove records.

Hard deletions are not supported.  In order to have the `/_meta/op` field create
the corresponding property `meta_op`.

## Enumerations

Many types in HubSpot are enumerations which require their value to be in a
pre-defined set.  A regular string field in a schema may have strings outside
of this set.

For ease of use and implementation, it is not checked if string fields are
subsets of an enumeration.  If a string is not allowed for an enumeration you
may see an error like:
```
transactor.Store: unexpected response: 400 Bad Request: HOWDY was not one of the allowed options: [IN_PROGRESS, NEW, UNQUALIFIED, ...]
```

## Match Properties

Record IDs cannot be controlled by the connector, they are created
automatically when the record is created and are not editable.  Because of this
a separate property for the record must be used to match the record to the
documents in your collection.

Properties in HubSpot can be unique or non-unique.  It is highly recommended to
use a unique property as the match property.  If you use a non-unique property
there are race conditions that can create *duplicate* records or missed updates.

When using a binding with a non-unique match property, a search must occur to
determine if the object exists or not, so that the new record can be either
inserted or updated.

However, the search API has a few known limitations:
- Immediately after creating a new record it may not be contained in the search
  results.
- If the search results require pagination, and a record is archived during
  pagination it may result in other records being skipped.

These issues can cause it to appear as if a record does not exist when in
actuality it does, and trigger an insert instead of an update creating a
duplicate record.

## HubSpot Project

To run this connector in a development environment you will need a HubSpot
project.  You can use the [HubSpot CLI][hs-cli] (`hs`) to create and update
this project.

To install the hubspot/cli and initialize it, which will save its configuration
in `~/.hscli/config.yml`:
```
$ npm install @hubspot/cli@latest
$ hs init
```

If you are also working with the HubSpot capture, this should be a separate
app.

```
$ hs project create
✔ [--name] Enter your project name: myproject
✔ [--dest] Choose where to create the project: /home/alf/myproject
✔ [--project-base] Choose what to include in your project: App
✔ [--distribution] Choose how to distribute your app: Privately
✔ [--auth] Choose your authentication type: OAuth

$ cd myproject
$ vi src/app/app-hsmeta.json
```

Add the scopes from `materialize-hubspot/auth.go`, the connector and the
project must agree on the scopes.

Add the OAuth Client ID and OAuth Client Secret to the `connectors` table of the
Estuary database.

[hs-cli]: https://developers.hubspot.com/docs/apps/developer-platform/build-apps/create-an-app

## Test Account

You will want to create a test account to have an environment separate from
your main account.  This can be done in the webapp, but for automation you can
also do this programmatically.

From an already initialized `hs` tool.
```
$ hs test-account create-config
✔ [--name] Enter the name of the test account: test-0002
✔ [--description] Enter the description of the test account: test-0002
✔ Would you like to create a default test account, or customize your own? Default (All Hubs, ENTERPRISE)
✔ [--path] Enter the name of the test account config file:  test-0002.json
✔ SUCCESS Test account config successfully created at test-0002.json

$ cat test-0002.json
{
  "accountName": "test-0002",
  "description": "test-0002",
  "marketingLevel": "ENTERPRISE",
  "opsLevel": "ENTERPRISE",
  "serviceLevel": "ENTERPRISE",
  "salesLevel": "ENTERPRISE",
  "contentLevel": "ENTERPRISE",
  "commerceLevel": "ENTERPRISE"
}
$ hs test-account create --config-path test-0002.json
```

When you are ready to remove the project you can do this by name:
```
$ hs test-account delete test-0002
```

## `hubspot-cli`

In the `materialize-hubspot/cmd/hubspot-cli` is a tool for performing some
operations on the HubSpot API using the connector code.

It uses the materialization spec to configure the client and you can pass the
decrypted config in over stdin.

Pull the spec:
```sh
flowctl catalog pull-specs --name acmeCo/test/materialize-hubspot
```

Get an access token:
```sh
sops -d --output-type json acmeCo/test/materialize-hubspot | \
    sed 's/_sops//g' | \
    go run ./materialize-hubspot/cmd/hubspot-cli refresh-tokens
```
