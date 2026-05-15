**Title:** `Transaction.AddFiles` panics / rejects Parquet files containing `timestamptz_ns` (Iceberg v3) columns

**Body:**

---

### Summary

`Transaction.AddFiles` fails when the target table contains `timestamptz_ns` (nanosecond-precision timestamp, Iceberg format v3) columns. The internal Arrow conversion path panics or returns an error when it encounters a nanosecond-unit Arrow timestamp schema, even though the Parquet file is valid and the column statistics are representable as raw `int64`.

### Steps to reproduce

1. Create an Iceberg table with a `timestamptz_ns` column (format v3).
2. Write a Parquet file with a `TIMESTAMP(isAdjustedToUTC=true, unit=NANOS)` column.
3. Call `tx.AddFiles(ctx, []string{path}, nil, false)`.

**Result:** panic or error from the Arrow→Iceberg schema converter inside `AddFiles`.

**Expected:** the file is appended to the table, same as `timestamptz` (microsecond) columns.

### Root cause

`AddFiles` reads each Parquet file via Arrow to extract column statistics and build manifest entries. The Arrow→Iceberg type converter does not handle nanosecond-unit Arrow timestamps (`arrow.Timestamp` with `arrow.Nanosecond` unit), causing it to panic or return an unsupported-type error.

The column statistics for a `timestamptz_ns` column are plain `int64` nanoseconds-since-epoch values — there is no fundamental reason the converter needs to understand the timestamp unit to extract min/max bounds.

### Workaround

We currently bypass `AddFiles` entirely for nanosecond-timestamp tables and build `DataFile` entries manually from raw Parquet footer metadata:

```go
pqr, _ := parquetfile.NewParquetReader(f)
rowCount := pqr.NumRows()
pqr.Close()
fi, _ := f.Stat()
df, _ := iceberg.NewDataFileBuilder(..., rowCount, fi.Size())
tx.AddDataFiles(ctx, []iceberg.DataFile{df.Build()}, nil)
```

This loses column-level statistics (min/max bounds per column), which degrades query planning for engines that use Iceberg manifest stats for predicate pushdown.

### Suggested fix

In the Arrow→Iceberg type converter used by `AddFiles`, map `arrow.Timestamp{Unit: arrow.Nanosecond, TimeZone: "UTC"}` to `iceberg.TimestampTzNsType` rather than failing. The statistics extraction path should then work identically to the microsecond case.

### Environment

- `github.com/apache/iceberg-go` v0.5.0
- Iceberg spec format version 3 (`timestamptz_ns` type)
