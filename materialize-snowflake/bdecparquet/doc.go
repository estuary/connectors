// Package bdecparquet is a frozen copy of the parquet writer from go/writer,
// used only by the Snowflake bdec write path. Changes to the shared writer
// have repeatedly broken bdec, so this copy is intentionally not updated.
// Bug fixes and features go to go/writer; delete this package with bdec.
package bdecparquet
