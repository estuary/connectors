# 2025-12-10: SQL Server Change Tracking Capture

This is a project to build a Microsoft SQL Server capture connector which
uses Change Tracking instead of CDC.

## Background

We have a CDC (Change Data Capture) connector for Microsoft SQL Server named
`source-sqlserver` which uses Microsoft's CDC mechanism. This mechanism works
by having a "worker job" on the master DB which scans the WAL periodically and
copies change events into "change tables" within the database in the `cdc`
schema.

There is another similar replication mechanism called "Change Tracking" in SQL
Server. This is very similar at a high level, in that it also involves change
tables which hold a record of change events. Just comparing the two:

- CDC
  - Enable on the database with `EXEC sys.sp_cdc_enable_db`
  - Enable for each table with `EXEC sys.sp_cdc_enable_table`
  - Creates a change table internally which stores complete row updates, plus
    some other metadata tracking tables about DML operations and LSN time
    mappings
  - Change table retention is based on a configurable time period
  - An asynchronous worker process follows the WAL and writes change events into the change tables.
- Change Tracking
  - Enable on the database with `ALTER DATABASE foo SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)`
  - Enable for each table with `ALTER TABLE bar.baz ENABLE CHANGE_TRACKING`
  - Creates a change table internally which stores primary keys for each change
    to a row in the source table, plus a per-database table tracking each
    committed transaction.
  - Change tables are updated as part of DML operations on the source tables

The main difference with Change Tracking versus CDC is that the change tables
only track the _primary key_ of a change. This means that a full capture needs
to join the change table contents with the _current_ state of the source table
at readout time, which in turn means that we cannot guarantee a complete audit
history if a specific row of a table is modified multiple times within a short
time period.

The tradeoff is that in exchange for that limitation, the source database only
needs to store the history of primary key changes instead of duplicating full
row contents.

Also Change Tracking only works on tables with primary keys, as far as I know.

## Basic Plan

We need to build a `source-sqlserver-change-tracking` (name subject to change
if I think of a more concise one) connector. Most of the things which the
current `source-sqlserver` does can more or less be duplicated unchanged:
discovery, backfill logic, and a lot of support code.

The core thing which needs to change is just the implementation of the whole
"replication stream" abstraction.

In fact, the two are so similar it's worth considering whether we might simply
build both into a single connector versus the overhead of making it an entirely
distinct connector.

But I think that for various reasons we will _probably_ want it to be a distinct
connector. Which means that the real challenge here is going to be figuring out
how we can factor out all the shared functionality into helper package(s) and
keep the main connector codebases fairly minimal and dealing just with the parts
that can't be shared.

This line of attack is also motivated in part because of upcoming work on CDC
captures using a "CDC Agent" which runs on the database server and decodes the
raw WAL data on disk and streams changes to Estuary. We will need a similar
model of "distinct connector but with a lot of shared behaviors" there, so it
seems worthwhile to start tackling that problem here and now.

So, initial steps:
1. Copy the entire `source-sqlserver` over to `source-sqlserver-ct`
2. Stub out the replication stream implementation
3. Write a new version which uses Change Tracking instead
4. Get the existing and fairly comprehensive test suite passing
5. Figure out how to start factoring out large chunks of shared logic into
   a helper package instead.

That mention of the test suite actually feels kind of important. If we take the
naive route of just copying it, the two test suites will eventually diverge.
But most tests basically just consist of the _test setup_ in terms of database
operations which exercise some interesting scenario, and then we snapshot the
connector outputs to verify that it's doing what we want in that scenario.

And that means that ideally we'd have a single shared test suite which each of
the SQL Server connectors uses.

This is not a new idea, in fact we already have a suite of "generic" tests which
are shared across all our SQL CDC connectors. It wouldn't be a terrible idea to
just also have a suite of "SQL Server" tests which are shared across all our SQL
Server connectors.

We'll come back to this.
