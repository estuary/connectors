# `source-airtable-native`

`source-airtable-native` is a capture connector built with the `estuary-cdk` for capturing data from Airtable bases and tables. This README is intended to document non-standard / non-obvious API behavior and connector design decisions.

## Notable API Features and Behaviors

### Formula Field Errors

Airtable formula fields can produce errors such as circular references, NaN values, or divide-by-zero errors. When this happens, the API returns an object with a `specialValue` or `error` property instead of the expected result, like:

```json
{
  "myFormulaField": {
    "specialValue": "NaN"
  }
}
```
or
```json
{
    "myFormulaField": {
        "error": "#ERROR!"
    }
}

```

Reference: https://support.airtable.com/docs/common-formula-errors-and-how-to-fix-them

### Pagination Behavior

The Airtable API uses offset-based pagination with a page size of 100 records. Pagination uses snapshot-based ordering: the sort order is frozen at the time of the first request, but field values reflect the most recent modifications including those made after the first request was made. Records modified after pagination starts keep their original position in the returned result set, but the record's fields will contain the most recent values. This means that when requesting records in sorted order of a `lastModifiedTime` field, records may not be strictly sorted; any records modified after the first request will have a `lastModifiedTime` field value that's after the first request was submitted.

For example, if the current time is 11:30 and we're slowly paginating records modified between 11:00 and 11:30, a record originally at position 50 that gets modified mid-pagination will still be at position 50, but its `lastModifiedTime` may be 11:31.

---

## Connector Design Decisions

### Full Refresh vs. Incremental Resources

The connector uses the presence of a specific type of field to determine if a table can be captured incrementally.

Valid incremental cursor fields meet the following criteria:
- The field type is `lastModifiedTime`.
- The result type is `dateTime`.
- The referenced field IDs array is empty, which means that all field changes are tracked.

Airtable automatically updates these types of `lastModifiedTime` fields whenever a change is made to any non-formula fields. Airtable does not create these fields by default, so users must explicitly create an appropriate `lastModifiedTime` field in order for the connector to incrementally capture from a given table.

If no valid incremental cursor field exists, the connector uses full refreshes to re-capture all records in a table.

### Incremental Strategy

For incremental resources, the connector uses the [`filterByFormula` query parameter](https://airtable.com/developers/web/api/list-records#query-filterbyformula) along with the `IS_AFTER` and `IS_BEFORE` functions to query for records updated in a specific time window. Due to the [API's pagination behavior](#pagination-behavior), it's possible for records to be returned that have been modified after the `IS_BEFORE` upper bound. These records are ignored in the current incremental sweep since they'll be captured in a later incremental sweep.

### Formula Fields

#### Scheduled Refreshes

Formula fields present a challenge for incremental replication: when underlying field dependencies change, the formula result updates but the record's `lastModifiedTime` does not change.

The connector supports scheduled formula field refreshes via a configurable cron expression. When the schedule triggers, the binding performs a backfill that only captures the formula fields and cursor field for the table's current contents. `merge` reduction strategies are then used to ensure materializations read complete documents from the captured collections instead of the partial documents from formula field refreshes.

#### Omitting Errors

If a formula results in an error, formula fields can contain an object with a `specialValue` field describing the error instead of the usual scalar formula result. Allowing these `specialValue` errors into captured collections would widen the inferred schema, making formula fields appear as either their usual scalar type or an object. Widening the inferred schema for these errors likely isn't what users would want, as resetting the inferred schema would require a collection reset and discarding all previously captured documents. The connector filters out fields containing an object with a `specialValue` field before emitting any documents to prevent formula field errors from widening the inferred schema.

### Resource Naming

Resources are named using the format: `{base_name}/{table_name}/{table_id}`. The `base_name` and `table_name` are not stable since users can rename either, but including them in the resource name makes it clearer to users which resources are capturing from which bases and tables.

This means that if users rename a base or a table, the connector will detect the renamed bases and tables as brand new resources, stop populating collections using the old base and table names, and start populating collections with the new names.
