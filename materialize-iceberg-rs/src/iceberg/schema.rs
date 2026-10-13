use std::sync::OnceLock;

use apache_avro::Schema;

// Field names and field-ids follow the Iceberg v2 spec:
// https://iceberg.apache.org/spec/#appendix-a-avro-schemas
//
// Only the fields we currently populate are included. Column-level stats
// (column_sizes, value_counts, null_value_counts, lower_bounds,
// upper_bounds), key_metadata, equality_ids, and sort_order_id are all
// optional per the spec and are omitted for now.
const MANIFEST_ENTRY_SCHEMA: &str = r#"
{
  "type": "record",
  "name": "manifest_entry",
  "fields": [
    {"name": "status", "type": "int", "field-id": 0},
    {"name": "snapshot_id", "type": ["null", "long"], "default": null, "field-id": 1},
    {"name": "sequence_number", "type": ["null", "long"], "default": null, "field-id": 3},
    {"name": "file_sequence_number", "type": ["null", "long"], "default": null, "field-id": 4},
    {"name": "data_file", "type": {
      "type": "record",
      "name": "r2",
      "fields": [
        {"name": "content", "type": "int", "field-id": 134},
        {"name": "file_path", "type": "string", "field-id": 100},
        {"name": "file_format", "type": "string", "field-id": 101},
        {"name": "partition", "type": {"type": "record", "name": "r102", "fields": []}, "field-id": 102},
        {"name": "record_count", "type": "long", "field-id": 103},
        {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
        {"name": "split_offsets", "type": ["null", {"type": "array", "items": "long", "element-id": 133}], "default": null, "field-id": 132}
      ]
    }, "field-id": 2}
  ]
}
"#;

// The manifest-list row (`manifest_file`), per the same spec section.
const MANIFEST_LIST_SCHEMA: &str = r#"
{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    {"name": "manifest_path", "type": "string", "field-id": 500},
    {"name": "manifest_length", "type": "long", "field-id": 501},
    {"name": "partition_spec_id", "type": "int", "field-id": 502},
    {"name": "content", "type": "int", "field-id": 517, "default": 0},
    {"name": "sequence_number", "type": "long", "field-id": 515, "default": 0},
    {"name": "min_sequence_number", "type": "long", "field-id": 516, "default": 0},
    {"name": "added_snapshot_id", "type": "long", "field-id": 503},
    {"name": "added_files_count", "type": "int", "field-id": 504},
    {"name": "existing_files_count", "type": "int", "field-id": 505},
    {"name": "deleted_files_count", "type": "int", "field-id": 506},
    {"name": "added_rows_count", "type": "long", "field-id": 512},
    {"name": "existing_rows_count", "type": "long", "field-id": 513},
    {"name": "deleted_rows_count", "type": "long", "field-id": 514}
  ]
}
"#;

pub fn manifest_entry_schema() -> &'static Schema {
    static SCHEMA: OnceLock<Schema> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        Schema::parse_str(MANIFEST_ENTRY_SCHEMA).expect("manifest_entry schema is valid Avro")
    })
}

pub fn manifest_list_schema() -> &'static Schema {
    static SCHEMA: OnceLock<Schema> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        Schema::parse_str(MANIFEST_LIST_SCHEMA).expect("manifest_file schema is valid Avro")
    })
}
