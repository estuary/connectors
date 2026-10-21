// `ParquetObjectReader` is deprecated in favor of implementing
// `AsyncFileReader` directly with a `SpawnedReader`, which would require a
// dedicated runtime for the blocking decode work; deferred as a follow-up,
// not needed for correctness.
#![allow(deprecated)]

use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BTreeSet;
use std::collections::BinaryHeap;
use std::collections::HashMap;

use anyhow::Context;
use anyhow::Result;
use arrow::array::Int64Array;
use arrow::array::StringArray;
use arrow::datatypes::DataType;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use tuple::Element;

use super::catalog::Catalog;
use super::manifest;
use super::manifest::CONTENT_DATA;
use super::manifest::CONTENT_DELETES;
use super::manifest_list;
use super::manifest_list::ManifestFile;
use super::single_value::Bound;
use super::single_value::compare_keys;

/// Requested keys are processed this many at a time, bounding memory to one
/// batch's worth of open files/row-group buffers rather than an entire
/// (potentially arbitrarily long) transaction's worth of `Load` calls.
const DEFAULT_LOAD_BATCH_SIZE: usize = 1000;

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
/// `schema`'s first `num_keys` fields). Always re-reads the manifest-list
/// fresh from object storage rather than relying on any cached checkpoint
/// state.
///
/// Requested keys are processed in batches of [`DEFAULT_LOAD_BATCH_SIZE`].
/// Each batch is sorted and reduced to a per-column `[lower, upper]` range,
/// which prunes data files (via manifest bounds) and row groups (via
/// Parquet statistics) that can't overlap it. The surviving data files —
/// each paired inline with its own positional-delete file, if any — are
/// merged by key using a heap, so the whole batch is answered with a single
/// ascending pass over the table rather than one pass per key. When the
/// same key is live in more than one data file at once, the file with the
/// highest sequence number wins.
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

    let manifest_paths: Vec<String> = manifests.iter().map(|m| m.manifest_path.clone()).collect();
    tracing::warn!(manifest_paths = %serde_json::to_string(&manifest_paths)?, "found manifest paths");

    let key_field_ids: Vec<i32> = (1..=num_keys as i32).collect();
    let key_types: Vec<DataType> = schema.fields()[..num_keys]
        .iter()
        .map(|f| f.data_type().clone())
        .collect();
    let doc_index = schema.fields().len() - 1;

    let mut file_paths = FilePathTable::default();
    let mut found = Vec::new();

    let indices: Vec<usize> = (0..requested_keys.len()).collect();
    for batch_indices in indices.chunks(DEFAULT_LOAD_BATCH_SIZE) {
        let mut batch: Vec<(usize, Vec<Bound>)> = batch_indices
            .iter()
            .map(|&i| {
                let key = decode_key(&requested_keys[i], &key_types)?;
                Ok((i, key))
            })
            .collect::<Result<_>>()?;
        batch.sort_by(|(_, a), (_, b)| compare_keys(a, b));

        let matches = scan_batch(
            catalog,
            &manifests,
            &key_field_ids,
            &key_types,
            doc_index,
            &batch,
            &mut file_paths,
        )
        .await?;
        found.extend(matches);
    }

    Ok(LoadResult {
        docs: found,
        file_paths: file_paths.paths,
    })
}

fn decode_key(key: &[Element<'_>], key_types: &[DataType]) -> Result<Vec<Bound>> {
    key.iter()
        .zip(key_types)
        .map(|(element, data_type)| {
            Bound::from_element(data_type, element)
                .context("requested key element does not match its column's type")
        })
        .collect()
}

/// Answers one sorted batch by merging the table's surviving data files in
/// key order.
async fn scan_batch(
    catalog: &Catalog,
    manifests: &[ManifestFile],
    key_field_ids: &[i32],
    key_types: &[DataType],
    doc_index: usize,
    batch: &[(usize, Vec<Bound>)],
    file_paths: &mut FilePathTable,
) -> Result<Vec<LoadedDoc>> {
    let Some((_, first_key)) = batch.first() else {
        return Ok(Vec::new());
    };

    // Per-column [lower, upper] across the whole batch — independent per
    // column, not the endpoints of the sorted composite key, since a
    // multi-column key's lexicographic order doesn't track any single
    // column's min/max.
    let mut batch_lower = first_key.clone();
    let mut batch_upper = first_key.clone();
    for (_, key) in &batch[1..] {
        for (i, bound) in key.iter().enumerate() {
            if bound.compare(&batch_lower[i]) == Ordering::Less {
                batch_lower[i] = bound.clone();
            }
            if bound.compare(&batch_upper[i]) == Ordering::Greater {
                batch_upper[i] = bound.clone();
            }
        }
    }

    // Data-file manifest entries whose bounds overlap the batch's range,
    // paired with their manifest's sequence number (manifest entries always
    // have status ADDED with no sequence number of their own in this
    // connector, i.e. they inherit the manifest-list row's).
    let mut data_files = Vec::new();
    for manifest_file in manifests.iter().filter(|m| m.content == CONTENT_DATA) {
        let manifest_path = catalog.strip_uri_prefix(&manifest_file.manifest_path)?;
        for entry in manifest::read_manifest(&catalog.object_store, manifest_path).await? {
            let lower = decode_bounds(&entry.lower_bounds, key_field_ids, key_types);
            let upper = decode_bounds(&entry.upper_bounds, key_field_ids, key_types);
            if !ranges_overlap(key_field_ids, &batch_lower, &batch_upper, &lower, &upper) {
                continue;
            }
            data_files.push((entry, manifest_file.sequence_number));
        }
    }
    if data_files.is_empty() {
        return Ok(Vec::new());
    }

    tracing::warn!(?data_files, "reading data files");

    // Positions masked in each surviving data file, read from whichever
    // delete files reference one of them.
    let mut deleted: HashMap<String, Vec<i64>> = HashMap::new();
    for manifest_file in manifests.iter().filter(|m| m.content == CONTENT_DELETES) {
        let manifest_path = catalog.strip_uri_prefix(&manifest_file.manifest_path)?;
        for entry in manifest::read_manifest(&catalog.object_store, manifest_path).await? {
            let Some(referenced) = &entry.referenced_data_file else {
                continue;
            };
            if !data_files.iter().any(|(d, _)| &d.file_path == referenced) {
                continue;
            }
            let positions = read_delete_positions(catalog, &entry.file_path).await?;
            deleted
                .entry(referenced.clone())
                .or_default()
                .extend(positions);
        }
    }

    tracing::warn!(?deleted);

    let mut file_iters = Vec::with_capacity(data_files.len());
    for (entry, sequence_number) in &data_files {
        let file_deleted = deleted.remove(&entry.file_path).unwrap_or_default();
        let iter = DataFileIter::open(
            catalog,
            &entry.file_path,
            key_types,
            doc_index,
            &batch_lower,
            &batch_upper,
            file_deleted,
            *sequence_number,
        )
        .await?;
        file_iters.push(iter);
    }

    heap_merge(file_iters, batch, file_paths).await
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

/// Whether a file/manifest-entry's per-column bounds could overlap the
/// batch's per-column `[lower, upper]` range. A missing bound for a column
/// (on either side) means "unknown" and never excludes the file.
fn ranges_overlap(
    key_field_ids: &[i32],
    batch_lower: &[Bound],
    batch_upper: &[Bound],
    file_lower: &HashMap<i32, Bound>,
    file_upper: &HashMap<i32, Bound>,
) -> bool {
    for (i, field_id) in key_field_ids.iter().enumerate() {
        if let Some(upper) = file_upper.get(field_id) {
            if batch_lower[i].compare(upper) == Ordering::Greater {
                return false;
            }
        }
        if let Some(lower) = file_lower.get(field_id) {
            if batch_upper[i].compare(lower) == Ordering::Less {
                return false;
            }
        }
    }
    true
}

/// Whether a row group's per-column statistics could overlap the batch's
/// per-column `[lower, upper]` range.
fn row_group_may_overlap(
    key_types: &[DataType],
    row_group: &RowGroupMetaData,
    batch_lower: &[Bound],
    batch_upper: &[Bound],
) -> bool {
    for (i, data_type) in key_types.iter().enumerate() {
        let Some(stats) = row_group.column(i).statistics() else {
            continue;
        };
        let (lower, upper) = stats_bounds(data_type, stats);
        if let Some(upper) = upper {
            if batch_lower[i].compare(&upper) == Ordering::Greater {
                return false;
            }
        }
        if let Some(lower) = lower {
            if batch_upper[i].compare(&lower) == Ordering::Less {
                return false;
            }
        }
    }
    true
}

fn stats_bounds(data_type: &DataType, stats: &Statistics) -> (Option<Bound>, Option<Bound>) {
    match (data_type, stats) {
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
    }
}

/// Streams one data file's rows in ascending key order (guaranteed by
/// `Store`'s own arrival-order assertion, which this scan relies on),
/// skipping row groups that can't overlap the batch range and positions
/// already masked by this file's positional-delete file, if any.
struct DataFileIter {
    stream: ParquetRecordBatchStream<ParquetObjectReader>,
    current: Option<RecordBatch>,
    current_row: usize,
    row_group_sizes: Vec<i64>,
    row_group_starts: Vec<i64>,
    surviving_groups: Vec<usize>,
    group_cursor: usize,
    consumed_in_group: i64,
    key_types: Vec<DataType>,
    doc_index: usize,
    deleted: BTreeSet<i64>,
    file_path: String,
    sequence_number: i64,
    exhausted: bool,
}

impl DataFileIter {
    #[allow(clippy::too_many_arguments)]
    async fn open(
        catalog: &Catalog,
        file_path: &str,
        key_types: &[DataType],
        doc_index: usize,
        batch_lower: &[Bound],
        batch_upper: &[Bound],
        deleted: Vec<i64>,
        sequence_number: i64,
    ) -> Result<DataFileIter> {
        let relative_path = catalog.strip_uri_prefix(file_path)?;
        let reader = ParquetObjectReader::new(
            catalog.object_store.clone(),
            ObjectPath::from(relative_path.to_string()),
        );
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .context("opening parquet file for load scan")?;

        let row_group_sizes: Vec<i64> = builder
            .metadata()
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows())
            .collect();
        let mut row_group_starts = Vec::with_capacity(row_group_sizes.len());
        let mut offset = 0i64;
        for &size in &row_group_sizes {
            row_group_starts.push(offset);
            offset += size;
        }

        let surviving_groups: Vec<usize> = builder
            .metadata()
            .row_groups()
            .iter()
            .enumerate()
            .filter(|(_, rg)| row_group_may_overlap(key_types, rg, batch_lower, batch_upper))
            .map(|(i, _)| i)
            .collect();

        let exhausted = surviving_groups.is_empty();
        let stream = builder.with_row_groups(surviving_groups.clone()).build()?;

        Ok(DataFileIter {
            stream,
            current: None,
            current_row: 0,
            row_group_sizes,
            row_group_starts,
            surviving_groups,
            group_cursor: 0,
            consumed_in_group: 0,
            key_types: key_types.to_vec(),
            doc_index,
            deleted: deleted.into_iter().collect(),
            file_path: file_path.to_string(),
            sequence_number,
            exhausted,
        })
    }

    /// Returns the next undeleted `(key, position, flow_document text)`
    /// tuple in ascending key order, or `None` once the file is exhausted.
    /// A `RecordBatch` the stream yields never spans a row-group boundary,
    /// so position tracking only needs to roll over to the next surviving
    /// group once `consumed_in_group` reaches that group's own row count —
    /// this holds regardless of how many `RecordBatch`es a single row group
    /// is split across.
    async fn next(&mut self) -> Result<Option<(Vec<Bound>, i64, String)>> {
        loop {
            if self.exhausted {
                return Ok(None);
            }
            let Some(batch) = &self.current else {
                match self
                    .stream
                    .try_next()
                    .await
                    .context("reading load-scan row group")?
                {
                    Some(batch) => {
                        self.current = Some(batch);
                        self.current_row = 0;
                    }
                    None => {
                        self.exhausted = true;
                        return Ok(None);
                    }
                }
                continue;
            };

            if self.current_row >= batch.num_rows() {
                self.current = None;
                continue;
            }

            let row = self.current_row;
            self.current_row += 1;

            let group_index = self.surviving_groups[self.group_cursor];
            let position = self.row_group_starts[group_index] + self.consumed_in_group;
            self.consumed_in_group += 1;
            if self.consumed_in_group == self.row_group_sizes[group_index] {
                self.group_cursor += 1;
                self.consumed_in_group = 0;
            }

            if self.deleted.contains(&position) {
                continue;
            }

            let key: Vec<Bound> = (0..self.key_types.len())
                .map(|i| {
                    Bound::from_arrow_scalar(&self.key_types[i], batch.column(i).as_ref(), row)
                })
                .collect::<Option<Vec<_>>>()
                .context("key column contains a null value")?;
            let doc_text = batch
                .column(self.doc_index)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("flow_document column is not a string array")?
                .value(row)
                .to_string();

            return Ok(Some((key, position, doc_text)));
        }
    }
}

/// One file's current candidate row, ordered by key for the merge heap.
#[derive(Debug)]
struct HeapItem {
    key: Vec<Bound>,
    sequence_number: i64,
    file_index: usize,
    position: i64,
    doc_text: String,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        compare_keys(&self.key, &other.key) == Ordering::Equal
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_keys(&self.key, &other.key)
    }
}

/// Merges `file_iters` by ascending key (a heap of their current candidate
/// rows), resolving a key live in more than one file by keeping the highest
/// sequence number, then merge-joins that ascending stream against the
/// sorted `batch` to pick out exactly the requested keys — stopping as soon
/// as every batch key has been resolved.
async fn heap_merge(
    mut file_iters: Vec<DataFileIter>,
    batch: &[(usize, Vec<Bound>)],
    file_paths: &mut FilePathTable,
) -> Result<Vec<LoadedDoc>> {
    let mut heap: BinaryHeap<Reverse<HeapItem>> = BinaryHeap::new();
    for (file_index, iter) in file_iters.iter_mut().enumerate() {
        if let Some((key, position, doc_text)) = iter.next().await? {
            heap.push(Reverse(HeapItem {
                key,
                sequence_number: iter.sequence_number,
                file_index,
                position,
                doc_text,
            }));
        }
    }

    let mut found = Vec::new();
    let mut batch_ptr = 0;

    while let Some(Reverse(first)) = heap.pop() {
        tracing::warn!(?first, "heap pop");
        let mut group = vec![first];
        while let Some(Reverse(top)) = heap.peek() {
            if compare_keys(&top.key, &group[0].key) == Ordering::Equal {
                group.push(heap.pop().unwrap().0);
            } else {
                break;
            }
        }

        for item in &group {
            let iter = &mut file_iters[item.file_index];
            if let Some((key, position, doc_text)) = iter.next().await? {
                heap.push(Reverse(HeapItem {
                    key,
                    sequence_number: iter.sequence_number,
                    file_index: item.file_index,
                    position,
                    doc_text,
                }));
            }
        }

        let winner_key = group[0].key.clone();
        while batch_ptr < batch.len()
            && compare_keys(&batch[batch_ptr].1, &winner_key) == Ordering::Less
        {
            batch_ptr += 1;
        }
        if batch_ptr >= batch.len() {
            break;
        }
        if compare_keys(&batch[batch_ptr].1, &winner_key) == Ordering::Equal {
            let winner = group
                .into_iter()
                .max_by_key(|item| item.sequence_number)
                .expect("group is never empty");
            let file_index = file_paths.intern(&file_iters[winner.file_index].file_path);
            while batch_ptr < batch.len()
                && compare_keys(&batch[batch_ptr].1, &winner_key) == Ordering::Equal
            {
                let doc_json = augment_doc(&winner.doc_text, file_index, winner.position)?;
                found.push(LoadedDoc {
                    request_index: batch[batch_ptr].0,
                    doc_json,
                });
                batch_ptr += 1;
            }
        }
    }

    Ok(found)
}

async fn read_delete_positions(catalog: &Catalog, file_path: &str) -> Result<Vec<i64>> {
    let relative_path = catalog.strip_uri_prefix(file_path)?;
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

#[cfg(test)]
#[path = "tests/test_scan.rs"]
mod tests;
