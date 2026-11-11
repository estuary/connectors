---
description: Materialize collections to Twenty CRM objects such as Companies, People and custom objects.
---

# Twenty

The Twenty connector writes collections to objects in [Twenty](https://twenty.com), an open
source CRM, on Twenty Cloud or self-hosted. It writes standard objects like Companies and People
and custom objects.

Each binding writes one collection to one Twenty object. The connector creates a record for each
new collection key and updates it when the key's document changes. It deletes records only if
the binding's **Deletes** setting says to.

## Prerequisites

- A Twenty workspace on Twenty Cloud, or self-hosted Twenty v2.30 or later (v2.35 or later for
  the `trash` and `delete` modes).
- An API key from **Settings > APIs & Webhooks**. Its role needs read and write access to the
  objects you materialize and read access to their metadata. The `trash` and `delete` modes need
  permission to delete records, **Create Fields** needs permission to change the data model, and
  workspace-member relations need read access to workspace members.
- On each object, a **unique Text field** to hold the collection key, such as `externalId`.
  **Create Fields** can add it.

A key of several fields, like `[/region, /id]`, is written as its parts joined by `/`, each
percent-encoded, so `eu/west` and `1` give `eu%2Fwest/1`. Keys longer than about 3,000 bytes
once URL-encoded, empty keys, and keys with a double quote, backslash or control character are
rejected (see [Rejected records](#rejected-records)). A published binding can't change its key's
fields, their order, or its ID Field, since its records would no longer be found.

## How fields map to Twenty

A collection field is written to the Twenty field with the same API name. To write a value from
another location, add a [projection](/concepts/advanced/projections) named after the Twenty field.
A composite field (Links, Emails, Phones, Full Name, Address, Currency) can be written whole, or
one subfield at a time with a projection named `field/subfield`:

```yaml
collections:
  acmeCo/companies:
    projections:
      domainName: /website        # a string goes to the link's primary URL
      address/addressCity: /city  # one subfield of the address
```

Fields are checked against the object's field types when you publish. A field the connector
can't write, like a system field or a type mismatch, is marked forbidden with the reason.

| Twenty type | Collection values |
| --- | --- |
| Text | strings; numbers and booleans are written as text |
| Number | numbers, or strings with `format: number` or `format: integer` |
| Boolean | booleans |
| Date and Time | strings with `format: date-time`, written in UTC |
| Date | strings with `format: date` |
| Select | strings that are one of the field's option values |
| Rating | strings from `RATING_1` to `RATING_5` |
| UUID | strings |
| Multi-select, Array | arrays of strings |
| JSON | objects or arrays |
| Links, Emails | a string for the primary value, or the whole object |
| Rich Text | a string of Markdown, or the whole object; each write replaces the whole field |
| Phones, Full Name, Address, Currency | the whole object, or single subfields |
| Relation to one record | a string or integer key, set in the binding's [relations](#relations) |

The connector doesn't write file, position or actor fields, or relations that can point at more
than one object, other than the targets of notes and tasks.

Twenty stores numbers as doubles, so a whole number above 2^53 is rejected rather than written
inexactly. Map it to a Text field instead.

**Currency amounts.** Twenty stores amounts in micros (millionths of a unit), so 19.99 is
19990000. A projection named `field/amount`, like `annualRevenue/amount`, takes the amount in
units and writes micros, rounded half away from zero. Above about 9 billion units Twenty rounds
the micros, by less than 0.002 units. Amounts above about 9.2 trillion units are rejected. A
binding with `field/amount` can't also select the whole field or `field/amountMicros`.

**Phone numbers.** A number starting with `+` (or `00`) carries its country. For others, map
`phones/primaryPhoneCountryCode` or set **Default Phone Country** (`US`, `GB` and so on).

**Select values.** Twenty rejects a Select value that isn't one of the field's options, an empty
string included. Convert source codes, odd date formats and similar in a
[derivation](/concepts/derivations) first.

### Missing and null values

A field missing from a document is left out of the write, so Twenty keeps its value. A `null`
field clears the value, and a `null` composite clears all its subfields.

With **Null Values** set to `keep`, a `null` field or subfield is left out like a missing one.
Empty strings still clear, and an empty primary link URL clears the whole link, label and
secondary links too. Values inside a RAW_JSON field are written as they are. With `keep`, a `null` no longer frees a unique value, so two records
swapping that value meet as a conflict (see [Unique fields](#unique-fields)).

### Creating fields

The connector doesn't create objects. It creates a field only when the binding lists it in
**Create Fields** and the object lacks it. The list can name collection fields, other than the
collection key and the whole document, and the binding's own ID Field (created as a unique Text field), Deleted Field (Boolean) and Deleted At Field (Date
and Time). The connector never changes or deletes a field. Don't rename a created field in
Twenty: the connector finds fields by name and would create it again.

| Collection field type | Twenty field it creates |
|---|---|
| string | Text |
| string with format `date-time` / `date` | Date and Time / Date |
| integer | Number, shown with 0 decimals |
| number | Number |
| boolean | Boolean |
| array of strings | Array |
| other arrays, objects | JSON |

A field that can hold several types, other than null, must be created in Twenty. Names must start with a lowercase
letter, use letters and digits, have at most 63 characters, and not be one Twenty reserves, like
`type`. If Twenty refuses a field, publishing fails; fields created before it stay.

## Several collections writing one object

Several bindings can write the same object if each uses a different ID Field, for example
`externalId` for HubSpot companies and `salesforceId` for Salesforce accounts. Two bindings
writing one object by the same ID Field are refused. If both sources describe the same company,
they create two records, or the second meets the first's unique values (see
[Unique fields](#unique-fields)). To merge sources, union them in a derivation first.

## Relations

A relation that points at one record, like a person's `company`, is set from a collection field
that holds the linked record's key:

```yaml
bindings:
  - resource:
      object: people
      id_field: externalId
      relations:
        - field: company            # the relation on people
          id_field: externalId      # the unique field on companies that holds the keys
          source_field: companyKey  # optional, defaults to the relation's name
    source: acmeCo/people
```

The collection field must hold strings or integers and can't be the collection key; to link by
the key's location, add another projection of it. The relation's `id_field` can be:

- a unique Text field of the linked object, usually its own binding's ID Field. Keys match
  exactly. For a linked collection with a key of several fields, hold the whole encoded key.
- `id`, when the collection holds Twenty ids. An id not in Twenty is skipped rather than
  waiting, so write records with ids another tool chose before the records that link to them.
- a unique Links or Emails field, like `domainName` or `emails`. Values match the primary value
  the way Twenty compares it: emails in any case, domains by bare host, so
  `https://www.Acme.com/about` finds `acme.com`.
- `userEmail` or `name`, for relations to workspace members such as `accountOwner` or
  `assignee`. Only active members match. A key that matches two members is skipped.

For each document:

- A `null` or empty key clears the link. A missing field leaves the link as it is.
- A key not yet in Twenty leaves the link as it is and waits. The connector tries again after
  the transaction's other writes, then every 5 minutes, up to 600 waiting links per binding each
  time. Waiting links survive restarts. A link waits at most 7 days, and a binding keeps at most
  10,000 waiting links. A newer document of the record replaces its waiting link.
- Keys that can't be looked up, ids that aren't Twenty UUIDs, and links that expire or are
  dropped are counted as skipped and logged.

Putting the bindings of linked objects first, like companies before people, saves the second
try. Each relation adds about one lookup per 60 documents. If Twenty rejects a link,
**Rejected Records** decides.

A relation that lists records, like a company's `people`, is set from the other object's binding.

### Linking notes and tasks to records

Twenty attaches notes and tasks to records through `noteTargets` and `taskTargets`, with one
field per target object, like `targetCompany` or `targetPerson`. Bind a collection of link rows
to `noteTargets` and list the note and one target field in **Relations**:

```yaml
bindings:
  - resource:
      object: noteTargets
      id_field: externalId
      create_fields: [externalId]
      relations:
        - field: note
          id_field: externalId
          source_field: noteKey
        - field: targetCompany
          id_field: externalId
          source_field: companyKey
    source: acmeCo/noteLinks
```

- Each binding links through one target field. For companies and people, use two bindings with
  different ID Fields.
- These are system objects that Twenty's settings don't show, so list their ID Field in
  **Create Fields**.
- From v2.38, Twenty allows one target per note and record. A second row for the same pair is
  rejected, and **Rejected Records** decides.

## Unique fields

Twenty refuses a value for a unique field, like a company's domain, that another record holds.
The connector looks up unique values before writing. When another record holds one,
**Unique Value Conflicts** decides:

- `skip_field` (default) writes the record without that value and logs it. For a record with no
  link URL yet, it leaves out the whole link, since Twenty makes a link's first secondary link the
  primary one when its URL is empty.
- `fail` stops the task.

A value can move between records in one transaction, as when records swap values or a chain of
records each take the next one's value. The records are written without the moving values, then
the records giving values up hold a stand-in, then all take their new values. A stand-in is
`<old value>.<record id>.invalid`, which Twenty accepts as a domain, an email or text, and which
keeps a link's label and secondary links. If the task stops between these writes, a record can
show its stand-in until the task restarts: at its first transaction, and after one that fails,
the connector finds stand-ins and writes their old values back.

A move goes through only if every record holding the value gives it up in the same transaction,
so a swap or chain goes whole or not at all, and no record gives up its value without taking its
new one. If Twenty refuses the new values, the records take their old ones back, and a record
whose old value someone else took meanwhile is left without one. Records are written 60 at a
time, and a value held by a record outside those 60 moves once the binding's documents are all
written. Links found by a value that moved in the transaction are looked up again after the
moves, so they point at the record that holds the value now. A value that still can't move is a
conflict, so with `fail` the task stops after writing the moves that can go through. A value held
by a record with an empty ID Field, or in Twenty's trash while **Restore Records Deleted in
Twenty** is off, can never move, so with `fail` the task stops before writing the record that
wants it. A record Twenty rejects takes none of its document's moved values, and the records it
would have taken from keep theirs.

A deleted record keeps its unique values when **Deletes** is `flag` or `trash`, or when someone
moved it to Twenty's trash. Only `delete` frees them for another record.

### Matching existing records

By default the connector never takes over records that people or other tools made. Set
**Match Existing Records By** (`adopt_by`) to a unique field the binding writes, like `domainName`
or `emails`. A document with no record then takes the live record with the same value whose ID
Field is empty, and updates it like any other. The log counts these as `adopted`.

This needs **Deletes** set to `ignore` or `flag`, so a deleted document can't remove a record a
person made. The binding must be the only one writing its object.

## Rejected records

When Twenty rejects a record, for example for a malformed email or phone number,
**Rejected Records** decides:

- `fail` (default) stops the task with Twenty's reason.
- `skip` first leaves out the email, phone or link values Twenty calls invalid and tries again.
  If Twenty still rejects the record, it is logged and skipped. The task stops if a binding
  skips more than 10 records and more than 10% of its documents in one transaction.

Error messages mask email addresses and phone numbers.

## Deletes

A document counts as deleted when its `/_meta/op` is `"d"`. **Deletes** says what that does:

- `ignore` (default) leaves the record as it is.
- `flag` sets the **Deleted Field** to `true`, and the optional **Deleted At Field** to the time.
  If the document comes back, the flag goes back to `false`.
- `trash` moves the record to Twenty's trash. If the document comes back, it is skipped unless
  **Restore Records Deleted in Twenty** is on.
- `delete` deletes the record for good. If the document comes back, a new record is created.
  Twenty clears other records' links to the deleted one, and the connector doesn't set them
  again: only later documents of the linking records do.

`trash` and `delete` leave records already in Twenty's trash alone, so switching from `trash`
to `delete` doesn't purge them. A record in the trash keeps its unique values until it is deleted for good. A record someone
deleted in Twenty stays deleted, and changes to it are skipped, unless **Restore Records Deleted
in Twenty** is on. That setting restores records deleted by the connector too.

## Twenty versions

The connector is tested with Twenty Cloud and self-hosted v2.35, v2.51 and v2.52. Before v2.30
it can't start. From v2.30 to v2.34, Twenty skips some deletes without an error, so bindings with
`trash` or `delete` are refused there.

## Rate limits

Twenty Cloud allows about 100 API requests per minute per workspace. The connector sends at most
**Requests Per Minute** (40 by default) across all bindings, writes up to 60 records per request,
and waits and retries when Twenty says the limit was reached.

## Known limits

- Twenty Cloud workspaces come with an active workflow, **Create company when adding a new
  person**. When a person is created or updated, it sets their company from their email domain,
  replacing the company the connector wrote. To keep the connector's links, open **Workflows** in
  Twenty and deactivate it.
- The connector looks up records before writing. A change someone makes in Twenty between the
  lookup and the write may be overwritten. The next change to the document usually corrects it.
- A unique value stays with a record in Twenty's trash or flagged as deleted, so a document
  recreated under a new key can't take it. See [Unique fields](#unique-fields).
- A record giving up a unique value to another holds a stand-in, such as `<record id>.invalid`
  for a domain with a label, for the moment between two writes, or until the transaction is
  replayed if the task stops in between.

## Configuration

You configure connectors either in the Estuary web app or by directly editing the Data Flow
specification file. See [connectors](/concepts/connectors#using-connectors) to learn more.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| `/base_url` | Twenty URL | The address of the Twenty API. Use the default for Twenty Cloud or your self-hosted Twenty's address. | string | `https://api.twenty.com` |
| **`/api_key`** | API Key | An API key from Settings > APIs & Webhooks in Twenty. | string | Required |
| `/default_phone_country` | Default Phone Country | The two-letter country code for phone numbers that don't start with + and whose document gives no country. | string | |
| `/advanced/requests_per_minute` | Requests Per Minute | The most requests per minute the connector sends to Twenty across all bindings. | integer | `40` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| **`/object`** | Object | The Twenty object to write to, by its plural API name, like `companies` or `people`. | string | Required |
| **`/id_field`** | ID Field | A unique Text field on the object that holds the collection key. **Create Fields** can create it. | string | Required |
| `/delete_mode` | Deletes | `ignore`, `flag`, `trash` or `delete`. | string | `ignore` |
| `/deleted_field` | Deleted Field | A Boolean field set to true when the document is deleted. Required when Deletes is `flag`. | string | |
| `/deleted_at_field` | Deleted At Field | A Date and Time field set to the time the record is flagged. | string | |
| `/restore_deleted` | Restore Records Deleted in Twenty | If true, a change to a record deleted in Twenty restores it. | boolean | `false` |
| `/unique_conflict` | Unique Value Conflicts | `skip_field` or `fail`. | string | `skip_field` |
| `/rejected_records` | Rejected Records | `fail` or `skip`. | string | `fail` |
| `/null_values` | Null Values | `clear` writes a `null` field as an empty value. `keep` leaves Twenty's value. | string | `clear` |
| `/relations` | Relations | Relation fields to link, like a person's company. See [Relations](#relations). | array | |
| **`/relations/-/field`** | Relation Field | A relation field of the object that points at one record. | string | Required |
| **`/relations/-/id_field`** | Linked ID Field | The unique Text, Links or Emails field that holds the keys of the records the relation points at, `id` for Twenty ids, or `userEmail` or `name` for workspace members. | string | Required |
| `/relations/-/source_field` | Collection Field | The collection field that holds the key of the record to link. | string | The relation's name |
| `/create_fields` | Create Fields | Collection fields, and the binding's ID and delete fields, to add to the object as new Twenty fields if it lacks them. See [Creating fields](#creating-fields). | string[] | |
| `/adopt_by` | Match Existing Records By | A unique field the binding writes. A document with no record takes the live record with the same value and an empty ID Field. See [Matching existing records](#matching-existing-records). | string | |

### Sample

```yaml
materializations:
  ${PREFIX}/${MATERIALIZATION_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-twenty:v1
        config:
          base_url: https://api.twenty.com
          api_key: <secret>
    bindings:
      - resource:
          object: companies
          id_field: externalId
          delete_mode: flag
          deleted_field: externalDeleted
          deleted_at_field: externalDeletedAt
        source: ${PREFIX}/${COLLECTION_NAME}
      - resource:
          object: people
          id_field: externalId
          relations:
            - field: company
              id_field: externalId
        source: ${PREFIX}/${PEOPLE_COLLECTION_NAME}
```
