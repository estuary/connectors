// `ParquetObjectReader` is deprecated in favor of implementing
// `AsyncFileReader` directly with a `SpawnedReader`, which would require a
// dedicated runtime for the blocking decode work; deferred as a follow-up,
// not needed for correctness.
#![allow(deprecated)]

use std::collections::BTreeSet;
use std::collections::HashMap;

use anyhow::Context;
use anyhow::Result;
use arrow::array::Int64Array;
use arrow::array::StringArray;
use arrow::datatypes::DataType;
use arrow::datatypes::Schema;
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use tuple::Element;

use super::catalog::Catalog;
use super::manifest;
use super::manifest::CONTENT_DATA;
use super::manifest::CONTENT_DELETES;
use super::manifest_list;
use super::single_value::Bound;

/// One matched document from [`load`]: the index into the `requested_keys`
/// slice it answers, and the augmented `flow_document` JSON bytes (with
/// `_meta.file_path`/`_meta.positional_delete` injected — `file_path` is an
/// index into the accompanying [`LoadResult::file_paths`] table, not the
/// path itself; see that field's doc comment for why).
#[derive(Debug)]
pub struct LoadedDoc {
    pub request_index: usize,
    pub doc_json: Vec<u8>,
}

/// The result of one [`load`] call.
pub struct LoadResult {
    pub docs: Vec<LoadedDoc>,
    /// Distinct data file paths referenced by `docs`' injected
    /// `_meta.file_path` indices, in assignment order. Deduplicated so the
    /// table's size is bounded by the number of distinct files touched by
    /// this call, not the number of matched rows — row count for an
    /// arbitrarily long transaction isn't bounded, but the files backing a
    /// table in practice are far fewer than its rows.
    pub file_paths: Vec<String>,
}

/// Assigns small integer indices to file paths, deduplicating repeats.
#[derive(Default)]
struct FilePathTable {
    paths: Vec<String>,
    index: HashMap<String, usize>,
}

impl FilePathTable {
    fn intern(&mut self, path: &str) -> usize {
        if let Some(&i) = self.index.get(path) {
            return i;
        }
        let i = self.paths.len();
        self.paths.push(path.to_string());
        self.index.insert(path.to_string(), i);
        i
    }
}

/// Scans `table`'s current snapshot for rows matching any of
/// `requested_keys` (each a decoded key tuple, in the same column order as
/// `schema`'s first `num_keys` fields), skipping files and row groups that
/// provably can't match via manifest bounds and Parquet statistics. Always
/// re-reads the manifest-list fresh from object storage rather than relying
/// on any cached checkpoint state. At most one undeleted row exists per key
/// at a time (merge-on-read via positional deletes maintains that
/// invariant), so a key is removed from consideration as soon as it's
/// matched.
pub async fn load(
    catalog: &Catalog,
    table: &str,
    schema: &Schema,
    num_keys: usize,
    requested_keys: &[Vec<Element<'_>>],
) -> Result<LoadResult> {
    tracing::warn!(table, ?schema, num_keys, ?requested_keys, "scan::load");

    let Some(current) = catalog.current_snapshot(table).await? else {
        return Ok(LoadResult {
            docs: Vec::new(),
            file_paths: Vec::new(),
        });
    };

    let manifests =
        manifest_list::read_manifest_list(&catalog.object_store, &current.manifest_list).await?;

    tracing::warn!(?manifests);

    let key_field_ids: Vec<i32> = (1..=num_keys as i32).collect();
    let key_types: Vec<DataType> = schema.fields()[..num_keys]
        .iter()
        .map(|f| f.data_type().clone())
        .collect();
    let doc_index = schema.fields().len() - 1;

    // Positions already masked by a prior commit's positional deletes,
    // keyed by the data file_path they apply to.
    let mut deleted: HashMap<String, BTreeSet<i64>> = HashMap::new();
    for manifest_file in manifests.iter().filter(|m| m.content == CONTENT_DELETES) {
        let manifest_path = strip_bucket_prefix(&catalog.bucket, &manifest_file.manifest_path)?;
        for entry in manifest::read_manifest(&catalog.object_store, manifest_path).await? {
            let Some(referenced) = entry.referenced_data_file else {
                continue;
            };
            let positions = read_delete_positions(catalog, &entry.file_path).await?;
            deleted.entry(referenced).or_default().extend(positions);
        }
    }

    let mut remaining: Vec<usize> = (0..requested_keys.len()).collect();
    let mut found = Vec::new();
    let mut file_paths = FilePathTable::default();

    for manifest_file in manifests.iter().filter(|m| m.content == CONTENT_DATA) {
        if remaining.is_empty() {
            break;
        }
        let manifest_path = strip_bucket_prefix(&catalog.bucket, &manifest_file.manifest_path)?;
        for entry in manifest::read_manifest(&catalog.object_store, manifest_path).await? {
            if remaining.is_empty() {
                break;
            }

            let lower = decode_bounds(&entry.lower_bounds, &key_field_ids, &key_types);
            let upper = decode_bounds(&entry.upper_bounds, &key_field_ids, &key_types);

            let candidates: Vec<usize> = remaining
                .iter()
                .copied()
                .filter(|&i| {
                    file_may_contain(
                        &requested_keys[i],
                        &key_field_ids,
                        &key_types,
                        &lower,
                        &upper,
                    )
                })
                .collect();
            if candidates.is_empty() {
                continue;
            }

            let deleted_positions = deleted.get(&entry.file_path).cloned().unwrap_or_default();
            let file_index = file_paths.intern(&entry.file_path);

            let matches = scan_file(
                catalog,
                &entry.file_path,
                file_index,
                num_keys,
                &key_types,
                doc_index,
                &candidates,
                requested_keys,
                &deleted_positions,
            )
            .await?;

            for (request_index, doc_json) in matches {
                found.push(LoadedDoc {
                    request_index,
                    doc_json,
                });
                remaining.retain(|&i| i != request_index);
            }
        }
    }

    Ok(LoadResult {
        docs: found,
        file_paths: file_paths.paths,
    })
}

fn decode_bounds(
    bounds: &[(i32, Vec<u8>)],
    key_field_ids: &[i32],
    key_types: &[DataType],
) -> HashMap<i32, Bound> {
    bounds
        .iter()
        .filter_map(|(id, bytes)| {
            let index = key_field_ids.iter().position(|k| k == id)?;
            let decoded = Bound::decode(&key_types[index], bytes).ok()?;
            Some((*id, decoded))
        })
        .collect()
}

fn file_may_contain(
    key: &[Element<'_>],
    key_field_ids: &[i32],
    key_types: &[DataType],
    lower: &HashMap<i32, Bound>,
    upper: &HashMap<i32, Bound>,
) -> bool {
    for ((element, field_id), data_type) in key.iter().zip(key_field_ids).zip(key_types) {
        let Some(target) = Bound::from_element(data_type, element) else {
            continue;
        };
        if let Some(lower) = lower.get(field_id) {
            if target < *lower {
                return false;
            }
        }
        if let Some(upper) = upper.get(field_id) {
            if target > *upper {
                return false;
            }
        }
    }
    true
}

#[allow(clippy::too_many_arguments)]
async fn scan_file(
    catalog: &Catalog,
    file_path: &str,
    file_index: usize,
    num_keys: usize,
    key_types: &[DataType],
    doc_index: usize,
    candidates: &[usize],
    requested_keys: &[Vec<Element<'_>>],
    deleted_positions: &BTreeSet<i64>,
) -> Result<Vec<(usize, Vec<u8>)>> {
    let relative_path = strip_bucket_prefix(&catalog.bucket, file_path)?;
    let reader = ParquetObjectReader::new(
        catalog.object_store.clone(),
        ObjectPath::from(relative_path.to_string()),
    );
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .context("opening parquet file for load scan")?;

    let row_group_starts: Vec<i64> = {
        let mut offset = 0i64;
        builder
            .metadata()
            .row_groups()
            .iter()
            .map(|rg| {
                let start = offset;
                offset += rg.num_rows();
                start
            })
            .collect()
    };

    let mut remaining_candidates = candidates.to_vec();
    let mut surviving_groups = Vec::new();
    for (i, row_group) in builder.metadata().row_groups().iter().enumerate() {
        if remaining_candidates.is_empty() {
            break;
        }
        let may_contain = remaining_candidates.iter().any(|&request_index| {
            key_row_group_may_contain(&requested_keys[request_index], key_types, row_group)
        });
        if may_contain {
            surviving_groups.push(i);
        }
    }
    if surviving_groups.is_empty() {
        return Ok(Vec::new());
    }

    let mut matches = Vec::new();
    let mut stream = builder.with_row_groups(surviving_groups.clone()).build()?;

    let mut group_cursor = 0;
    while let Some(batch) = stream
        .try_next()
        .await
        .context("reading load-scan row group")?
    {
        let row_group_index = surviving_groups[group_cursor];
        let row_start = row_group_starts[row_group_index];
        group_cursor += 1;

        let key_columns: Vec<_> = (0..num_keys).map(|i| batch.column(i).clone()).collect();
        let doc_array = batch
            .column(doc_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("flow_document column is not a string array")?
            .clone();

        for row in 0..batch.num_rows() {
            if remaining_candidates.is_empty() {
                break;
            }
            let position = row_start + row as i64;
            if deleted_positions.contains(&position) {
                continue;
            }

            let row_values: Vec<Option<Bound>> = key_columns
                .iter()
                .zip(key_types)
                .map(|(col, dt)| Bound::from_arrow_scalar(dt, col.as_ref(), row))
                .collect();

            let Some(pos) = remaining_candidates.iter().position(|&request_index| {
                row_matches(&row_values, &requested_keys[request_index], key_types)
            }) else {
                continue;
            };
            let request_index = remaining_candidates.remove(pos);

            let doc_text = doc_array.value(row);
            let doc_json = augment_doc(doc_text, file_index, position)?;
            matches.push((request_index, doc_json));
        }
    }

    Ok(matches)
}

fn key_row_group_may_contain(
    key: &[Element<'_>],
    key_types: &[DataType],
    row_group: &RowGroupMetaData,
) -> bool {
    for (i, (element, data_type)) in key.iter().zip(key_types).enumerate() {
        let Some(target) = Bound::from_element(data_type, element) else {
            continue;
        };
        let Some(stats) = row_group.column(i).statistics() else {
            continue;
        };
        let (lower, upper) = match (data_type, stats) {
            (DataType::Int64, Statistics::Int64(v)) => (
                v.min_opt().map(|v| Bound::Int64(*v)),
                v.max_opt().map(|v| Bound::Int64(*v)),
            ),
            (DataType::Float64, Statistics::Double(v)) => (
                v.min_opt().map(|v| Bound::Float64(*v)),
                v.max_opt().map(|v| Bound::Float64(*v)),
            ),
            (DataType::Boolean, Statistics::Boolean(v)) => (
                v.min_opt().map(|v| Bound::Boolean(*v)),
                v.max_opt().map(|v| Bound::Boolean(*v)),
            ),
            (DataType::Utf8, Statistics::ByteArray(v)) => (
                v.min_bytes_opt()
                    .map(|b| Bound::Utf8(String::from_utf8_lossy(b).to_string())),
                v.max_bytes_opt()
                    .map(|b| Bound::Utf8(String::from_utf8_lossy(b).to_string())),
            ),
            _ => (None, None),
        };
        if let Some(lower) = lower {
            if target < lower {
                return false;
            }
        }
        if let Some(upper) = upper {
            if target > upper {
                return false;
            }
        }
    }
    true
}

fn row_matches(
    row_values: &[Option<Bound>],
    candidate: &[Element<'_>],
    key_types: &[DataType],
) -> bool {
    row_values
        .iter()
        .zip(candidate)
        .zip(key_types)
        .all(
            |((row_val, elem), dt)| match (row_val, Bound::from_element(dt, elem)) {
                (Some(a), Some(b)) => *a == b,
                _ => false,
            },
        )
}

async fn read_delete_positions(catalog: &Catalog, file_path: &str) -> Result<Vec<i64>> {
    let relative_path = strip_bucket_prefix(&catalog.bucket, file_path)?;
    let reader = ParquetObjectReader::new(
        catalog.object_store.clone(),
        ObjectPath::from(relative_path.to_string()),
    );
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .context("opening positional-delete file")?;
    let mut stream = builder.build()?;

    let mut positions = Vec::new();
    while let Some(batch) = stream
        .try_next()
        .await
        .context("reading positional-delete file")?
    {
        let pos_column = batch
            .column_by_name("pos")
            .context("delete file is missing pos column")?
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("pos column is not int64")?;
        positions.extend(pos_column.iter().flatten());
    }

    Ok(positions)
}

/// `file_index` is an index into the `load()` call's [`LoadResult::file_paths`]
/// table, not the file path itself — `Store` resolves it back via that table.
fn augment_doc(doc_text: &str, file_index: usize, position: i64) -> Result<Vec<u8>> {
    let mut doc: serde_json::Value =
        serde_json::from_str(doc_text).context("flow_document is not valid JSON")?;
    let object = doc
        .as_object_mut()
        .context("flow_document must be a JSON object")?;
    let meta = object
        .entry("_meta")
        .or_insert_with(|| serde_json::json!({}));
    let meta_object = meta.as_object_mut().context("_meta must be an object")?;
    meta_object.insert("file_path".to_string(), serde_json::json!(file_index));
    meta_object.insert("positional_delete".to_string(), serde_json::json!(position));

    serde_json::to_vec(&doc).context("re-serializing augmented flow_document")
}

fn strip_bucket_prefix<'a>(bucket: &str, path: &'a str) -> Result<&'a str> {
    let prefix = format!("s3://{bucket}/");
    path.strip_prefix(prefix.as_str())
        .with_context(|| format!("path {path} does not start with {prefix}"))
}

#[cfg(test)]
#[path = "tests/test_scan.rs"]
mod tests;
