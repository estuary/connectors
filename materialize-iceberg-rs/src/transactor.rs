use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use anyhow::Result;
use arrow::array::ArrayRef;
use arrow::array::BooleanBuilder;
use arrow::array::Float64Builder;
use arrow::array::Int64Builder;
use arrow::array::StringBuilder;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::ClientOptions;
use object_store::GetOptions;
use object_store::GetRange;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::PutPayload;
use object_store::WriteMultipart;
use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as ObjectPath;
use parquet::arrow::ArrowWriter;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;
use proto_flow::flow::Projection;
use proto_flow::flow::materialization_spec;
use proto_flow::flow::{self};
use proto_flow::materialize::Response;
use proto_flow::materialize::request;
use proto_flow::materialize::request::Open;
use proto_flow::materialize::response;
use proto_flow::materialize::response::Acknowledged;
use proto_flow::materialize::response::Flushed;
use proto_flow::materialize::response::Opened;
use proto_flow::materialize::response::StartedCommit;
use serde_json::json;
use tuple::Element;
use uuid::Uuid;

use crate::Input;
use crate::Output;
use crate::configuration::EndpointConfig;
use crate::configuration::Store as StoreConfig;
use crate::iceberg::catalog::CATALOG_PREFIX;
use crate::iceberg::catalog::Catalog;
use crate::iceberg::catalog::CommitTransactionRequest;
use crate::iceberg::catalog::TableChange;
use crate::iceberg::catalog::TableIdentifier;
use crate::iceberg::catalog::TableUpdate;
use crate::iceberg::counting_store;
use crate::iceberg::manifest::CONTENT_DATA;
use crate::iceberg::manifest::CONTENT_DELETES;
use crate::iceberg::manifest::DataFileInfo;
use crate::iceberg::manifest::DeleteFileInfo;
use crate::iceberg::manifest::{self};
use crate::iceberg::manifest_list;
use crate::iceberg::scan;
use crate::iceberg::single_value::Bound;
use crate::iceberg::single_value::BoundTracker;
use crate::iceberg::single_value::compare_keys;
use crate::iceberg::snapshot::Snapshot;
use crate::iceberg::snapshot::build_snapshot;
use crate::iceberg::state::BindingCheckpoint;
use crate::iceberg::state::ConnectorState;
use crate::server;

pub async fn run_transactions(input: &mut Input, output: Output, open: Open) -> Result<()> {
    // Shared so the `Acknowledge` task spawned below (see
    // `commit_pending_acknowledge`) can send `Response::Acknowledged`
    // itself, as soon as its commit finishes, without waiting for this loop
    // to get back around to it — the runtime won't send `Flush` until it's
    // seen `Acknowledged`, so deferring the send would deadlock whenever a
    // transaction has no `Load` calls to otherwise give this loop a chance
    // to notice.
    let output = Arc::new(Mutex::new(output));

    let spec = open
        .materialization
        .expect("must have a materialization spec");

    let config: EndpointConfig = serde_json::from_slice(&spec.config_json)?;
    let (object_store, prefix, bucket, scheme) = build_object_store(&config.store)?;

    let request_counts = Arc::new(counting_store::RequestCounts::default());
    let object_store: Arc<dyn ObjectStore> = Arc::new(counting_store::CountingObjectStore::new(
        object_store,
        request_counts.clone(),
    ));

    let http_client = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .context("building embedded-catalog HTTP client")?;

    let catalog = Arc::new(Catalog {
        object_store,
        request_counts,
        http_client,
        prefix,
        bucket,
        scheme,
    });
    // Torn down (aborted) when this function returns, since the server is
    // only meaningful for the lifetime of this materialization's commits.
    let _server = server::spawn(catalog.clone());

    let mut connector_state = ConnectorState::from_json(open.state_json.as_ref())?;

    let mut bindings = spec
        .bindings
        .iter()
        .map(|binding| {
            let checkpoint = connector_state
                .bindings
                .remove(&binding.state_key)
                .unwrap_or_default();
            binding_state(binding, checkpoint, &catalog.prefix)
        })
        .collect::<Result<Vec<_>>>()?;

    for binding in &mut bindings {
        if binding.checkpoint.snapshot_id.is_none()
            && let Some(current) = catalog.current_snapshot(&binding.table).await?
        {
            binding.checkpoint.snapshot_id = Some(current.snapshot_id);
            binding.checkpoint.sequence_number = current.sequence_number;
            binding.checkpoint.manifest_list = Some(current.manifest_list);
        }
    }

    output.lock().unwrap().send(Response {
        kind: Some(response::Kind::Opened(Opened {
            runtime_checkpoint: None,
            ..Default::default()
        })),
        ..Default::default()
    })?;

    // The binding a `Store` call most recently targeted. Flow sends `Store`
    // requests grouped by (binding, key) ascending, so once this changes,
    // the previous binding has seen every row it's going to see this
    // transaction — flushing it immediately (rather than leaving its
    // sub-threshold buffer sitting in memory until `StartCommit`) keeps at
    // most one binding's buffer "hot" at a time, instead of every binding
    // touched this transaction independently approaching
    // `DATA_FILE_FLUSH_BYTES` and all still being unflushed when
    // `StartCommit` finally iterates them.
    let mut last_store_binding: Option<u32> = None;

    // Set right after `Acknowledge`, cleared on the next transaction's
    // first `Load` — logging that moment tells us how long the runtime
    // took to send the next transaction's first message, which a gap in
    // the logs alone can't distinguish from the connector itself being
    // stuck on something.
    let mut awaiting_first_load = true;

    // Total `Load` requests received so far this transaction, across all
    // bindings — logged every `LOAD_COUNT_LOG_INTERVAL` so a stall's exact
    // position is visible even when it happens well short of any binding's
    // `DEFAULT_LOAD_BATCH_SIZE` threshold (which logs nothing on its own).
    let mut loads_this_transaction: u64 = 0;
    const LOAD_COUNT_LOG_INTERVAL: u64 = 100_000;

    // The prior transaction's `Acknowledge` commit, running in the
    // background so `Load` keeps being read and staged while it's still in
    // flight (see `commit_pending_acknowledge`'s doc comment). Always
    // `None` by the time an `Acknowledge` arm runs again, since `Flush`
    // unconditionally resolves it first and `Store`/`StartCommit` only
    // happen after `Flush`.
    let mut pending_ack: Option<tokio::task::JoinHandle<Result<Vec<BindingCheckpoint>>>> = None;

    loop {
        let request = match input.read()? {
            Some(req) => req,
            None => return Ok(()),
        };

        match request.kind {
            Some(request::Kind::Load(load)) => {
                if awaiting_first_load {
                    tracing::info!("first Load received for transaction");
                    awaiting_first_load = false;
                }
                loads_this_transaction += 1;
                if loads_this_transaction.is_multiple_of(LOAD_COUNT_LOG_INTERVAL) {
                    tracing::info!(loads_this_transaction, "Load requests received so far");
                }

                let binding = &mut bindings[load.binding as usize];
                binding.stage_pending_load(&catalog, &load.key_packed).await?;
            }
            Some(request::Kind::Flush(_)) => {
                tracing::warn!("Flush");

                // `Load` never scans — it only stages keys in object storage
                // — so any in-flight `Acknowledge` must be resolved first,
                // so the scan below (via `catalog.current_snapshot`)
                // reflects the commit it just made rather than stale
                // pre-commit state. `Response::Acknowledged` itself was
                // already sent by the spawned task (see
                // `commit_pending_acknowledge`'s doc comment) — this is just
                // applying its result to `bindings`.
                if let Some(handle) = pending_ack.take() {
                    let checkpoints = handle
                        .await
                        .context("acknowledge task panicked")??;
                    for (binding, checkpoint) in bindings.iter_mut().zip(checkpoints) {
                        binding.checkpoint = checkpoint;
                        // The only point a binding's snapshot can have
                        // advanced — always invalidate so the load-scan
                        // below re-fetches instead of reusing a cache from
                        // before this commit.
                        binding.scan_cache = None;
                    }
                }

                for (index, binding) in bindings.iter_mut().enumerate() {
                    flush_pending_loads(&catalog, &output, index as u32, binding).await?;
                }

                output.lock().unwrap().send(Response {
                    kind: Some(response::Kind::Flushed(Flushed { state: None })),
                    ..Default::default()
                })?;
                log_object_store_requests("load", &catalog.request_counts);
            }
            Some(request::Kind::Store(store)) => {
                if let Some(prev_binding) = last_store_binding
                    && prev_binding != store.binding
                {
                    bindings[prev_binding as usize]
                        .flush_data_file(&catalog)
                        .await?;
                }
                last_store_binding = Some(store.binding);

                let binding = &mut bindings[store.binding as usize];
                binding.total_keys_handled += 1;

                let raw_doc_text =
                    std::str::from_utf8(&store.doc_json).context("doc_json must be valid UTF-8")?;

                let doc_text: String = if store.exists {
                    let mut doc: serde_json::Value =
                        serde_json::from_str(raw_doc_text).context("doc_json is not valid JSON")?;
                    let meta = doc
                        .get_mut("_meta")
                        .and_then(|m| m.as_object_mut())
                        .context(
                            "existing document is missing _meta: Load must have populated \
                             _meta.file_path/_meta.positional_delete",
                        )?;
                    let file_index = meta
                        .remove("file_path")
                        .and_then(|v| v.as_u64())
                        .context("existing document is missing _meta.file_path")?
                        as usize;
                    let file_path = binding
                        .load_file_paths
                        .get(file_index)
                        .map(str::to_string)
                        .with_context(|| {
                            format!("_meta.file_path index {file_index} is out of range")
                        })?;
                    let position = meta
                        .remove("positional_delete")
                        .and_then(|v| v.as_i64())
                        .context("existing document is missing _meta.positional_delete")?;
                    binding
                        .pending_deletes
                        .entry(file_path)
                        .or_default()
                        .push(position);

                    if store.delete {
                        binding.transaction_counts.deletes += 1;
                        continue;
                    }

                    binding.transaction_counts.updates += 1;
                    serde_json::to_string(&doc)
                        .context("re-serializing document after stripping _meta")?
                } else {
                    binding.transaction_counts.inserts += 1;
                    raw_doc_text.to_string()
                };

                let key_elements: Vec<Element> = tuple::unpack(&store.key_packed)?;

                // scan::load's merge-scan streams a data file's rows in
                // ascending key order on the assumption that they were
                // written in that order — Flow guarantees Store calls
                // arrive sorted by (binding, key), so this should never
                // trip.
                let key: Vec<Bound> = key_elements
                    .iter()
                    .zip(&binding.key_types)
                    .map(|(element, data_type)| {
                        Bound::from_element(data_type, element)
                            .context("key element does not match its column's type")
                    })
                    .collect::<Result<_>>()?;
                if let Some(last_key) = &binding.last_key {
                    anyhow::ensure!(
                        compare_keys(last_key, &key) != std::cmp::Ordering::Greater,
                        "Store keys must arrive in ascending order per binding; \
                         got {key:?} after {last_key:?}",
                    );
                }
                binding.last_key = Some(key);

                for ((builder, tracker), element) in binding
                    .key_builders
                    .iter_mut()
                    .zip(binding.key_bound_trackers.iter_mut())
                    .zip(key_elements.iter())
                {
                    builder.append(element)?;
                    tracker.update(element);
                }

                let elements: Vec<Element> = tuple::unpack(&store.values_packed)?;
                for (builder, element) in binding.value_builders.iter_mut().zip(elements.iter()) {
                    builder.append(element)?;
                }

                binding.doc_builder.append_value(&doc_text);
                binding.rows += 1;

                binding.estimated_bytes +=
                    store.key_packed.len() + store.values_packed.len() + doc_text.len();
                if binding.estimated_bytes >= DATA_FILE_FLUSH_BYTES {
                    binding.flush_data_file(&catalog).await?;
                }
            }
            Some(request::Kind::StartCommit(_)) => {
                for binding in bindings.iter_mut() {
                    binding.write_and_upload(&catalog).await?;

                    tracing::warn!(
                        table = %binding.table,
                        collection = %binding.collection,
                        inserts = binding.transaction_counts.inserts,
                        updates = binding.transaction_counts.updates,
                        deletes = binding.transaction_counts.deletes,
                        total_keys_handled = binding.total_keys_handled,
                        total_bytes_written = binding.total_bytes_written,
                        "transaction summary"
                    );
                    binding.transaction_counts = TransactionCounts::default();
                    // `_meta.file_path` indices only need to resolve for
                    // `Store` calls within the transaction that produced
                    // them; starting the next one fresh keeps this table
                    // from growing for the connector's whole lifetime.
                    binding.load_file_paths = scan::FilePathTable::default();
                }

                let state = checkpoint_from_bindings(&bindings)?;
                output.lock().unwrap().send(Response {
                    kind: Some(response::Kind::StartedCommit(StartedCommit {
                        state: Some(state),
                    })),
                    ..Default::default()
                })?;
                tracing::warn!("StartedCommit");
                log_object_store_requests("store", &catalog.request_counts);
            }
            Some(request::Kind::Acknowledge(_)) => {
                tracing::warn!("Acknowledge");
                // Measured from here (request receipt), not from when the
                // spawned commit below actually finishes — the runtime may
                // send the next transaction's first `Load` well before
                // that, now that it's no longer blocked on it.
                awaiting_first_load = true;
                loads_this_transaction = 0;

                let jobs: Vec<AckJob> = bindings
                    .iter_mut()
                    .map(|binding| AckJob {
                        state_key: binding.state_key.clone(),
                        table: binding.table.clone(),
                        table_schema: binding.table_schema.clone(),
                        last_column_id: binding.last_column_id,
                        checkpoint: std::mem::take(&mut binding.checkpoint),
                    })
                    .collect();

                let catalog = catalog.clone();
                let output = output.clone();
                pending_ack = Some(tokio::spawn(commit_pending_acknowledge(
                    catalog, output, jobs,
                )));
            }
            _ => {}
        }
    }
}

/// Reads back whatever `Load` keys `stage_pending_load` staged in object
/// storage for `binding` this transaction, scans for their matches, and
/// sends them — a no-op if nothing was staged. Only called from the `Flush`
/// arm, once any in-flight `Acknowledge` has resolved, so the scan sees
/// this table's post-commit state (see the `Flush` arm's comment).
async fn flush_pending_loads(
    catalog: &Catalog,
    output: &Mutex<Output>,
    index: u32,
    binding: &mut BindingState,
) -> Result<()> {
    let Some(upload) = binding.pending_loads_upload.take() else {
        return Ok(());
    };
    upload
        .finish()
        .await
        .context("completing Load staging upload")?;

    let total_len = catalog
        .object_store
        .head(&binding.pending_loads_path)
        .await
        .context("reading staged Load keys")?
        .size;

    // Records (see `write_pending_load_record`) can straddle the boundary
    // between successive chunks a stream hands back, so whatever's left over
    // after parsing as many whole records as `pending` currently holds
    // carries forward to be completed by the next chunk.
    let mut offset: u64 = 0;
    let mut pending: Vec<u8> = Vec::new();
    let mut batch: Vec<Bytes> = Vec::new();
    while offset < total_len {
        // A fresh ranged read every time we resume here (rather than one
        // stream held open across the whole file) so the object store
        // connection is never left idle while `scan_and_emit_loaded` runs —
        // that scan can take tens of seconds, comfortably past
        // `REQUEST_TIMEOUT`, and an idle GCS/S3 stream left open across it
        // fails with a timeout reading the stream's next chunk.
        let mut stream = catalog
            .object_store
            .get_opts(
                &binding.pending_loads_path,
                GetOptions {
                    range: Some(GetRange::Offset(offset)),
                    ..Default::default()
                },
            )
            .await
            .context("reading staged Load keys")?
            .into_stream();

        while let Some(chunk) = stream
            .try_next()
            .await
            .context("reading staged Load keys")?
        {
            offset += chunk.len() as u64;
            pending.extend_from_slice(&chunk);

            let mut consumed = 0;
            while let Some((key_packed, record_len)) =
                take_pending_load_record(&pending[consumed..])
            {
                consumed += record_len;
                batch.push(key_packed);
                if batch.len() >= scan::DEFAULT_LOAD_BATCH_SIZE {
                    break;
                }
            }
            pending.drain(..consumed);

            if batch.len() >= scan::DEFAULT_LOAD_BATCH_SIZE {
                break;
            }
        }
        // `stream` drops here, closing its connection before the scan below
        // runs — the whole point of re-opening per batch instead of reusing
        // one stream across the file.

        if batch.len() >= scan::DEFAULT_LOAD_BATCH_SIZE {
            scan_and_emit_loaded(catalog, output, index, binding, &batch).await?;
            batch.clear();
        }
    }
    anyhow::ensure!(pending.is_empty(), "staged Load keys ended mid-record");

    if !batch.is_empty() {
        scan_and_emit_loaded(catalog, output, index, binding, &batch).await?;
    }

    catalog
        .object_store
        .delete(&binding.pending_loads_path)
        .await
        .ok();
    Ok(())
}

async fn scan_and_emit_loaded(
    catalog: &Catalog,
    output: &Mutex<Output>,
    index: u32,
    binding: &mut BindingState,
    key_bytes: &[Bytes],
) -> Result<()> {
    scan::load(
        catalog,
        &binding.table,
        &binding.collection,
        &binding.schema,
        binding.key_builders.len(),
        key_bytes,
        &mut binding.load_file_paths,
        &mut binding.scan_cache,
        |loaded| {
            output.lock().unwrap().send(Response {
                kind: Some(response::Kind::Loaded(response::Loaded {
                    binding: index,
                    doc_json: loaded.doc_json.into(),
                })),
                ..Default::default()
            })
        },
    )
    .await
}

/// Stages one `Load` key, length-prefixed the same way `Input`/`Output`
/// frame protobuf messages (`src/lib.rs`): a 4-byte little-endian length
/// followed by that many bytes. `WriteMultipart::write` is synchronous —
/// actual uploads only happen once a chunk's worth has buffered (or
/// `finish` is called), so this can't fail on its own.
fn write_pending_load_record(upload: &mut WriteMultipart, key_packed: &[u8]) {
    upload.write(&(key_packed.len() as u32).to_le_bytes());
    upload.write(key_packed);
}

/// Parses one record written by `write_pending_load_record` off the front
/// of `buf`, if a complete one is present, returning its bytes and how much
/// of `buf` it occupied — `None` if `buf` doesn't yet hold a whole record
/// (the caller should accumulate more from the stream and retry).
fn take_pending_load_record(buf: &[u8]) -> Option<(Bytes, usize)> {
    let length_bytes: [u8; 4] = buf.get(..4)?.try_into().unwrap();
    let length = u32::from_le_bytes(length_bytes) as usize;

    let key_packed = buf.get(4..4 + length)?;
    Some((Bytes::copy_from_slice(key_packed), 4 + length))
}

/// Logs how many of each object-store request kind were made since the last
/// call (or since connector start, the first time) — `request_counts` is
/// reset on every call, so each log line reports only the named `phase`'s
/// own requests, not a running total.
fn log_object_store_requests(phase: &str, request_counts: &counting_store::RequestCounts) {
    let counts = request_counts.take();
    tracing::info!(
        phase,
        get = counts.get,
        get_range = counts.get_range,
        put = counts.put,
        put_multipart = counts.put_multipart,
        list = counts.list,
        list_with_delimiter = counts.list_with_delimiter,
        delete = counts.delete,
        copy = counts.copy,
        total = counts.total(),
        "object store requests"
    );
}

fn checkpoint_from_bindings(bindings: &[BindingState]) -> Result<flow::ConnectorState> {
    connector_state_json(
        bindings
            .iter()
            .map(|binding| (binding.state_key.clone(), binding.checkpoint.clone())),
    )
}

/// Builds the `flow::ConnectorState` payload a `Response`'s `state` field
/// carries, from `(state_key, checkpoint)` pairs — shared by
/// `checkpoint_from_bindings` (reading straight from `bindings`) and
/// `commit_pending_acknowledge` (which has no `BindingState`s to read, only
/// the checkpoints its spawned task just computed).
fn connector_state_json(
    bindings: impl IntoIterator<Item = (String, BindingCheckpoint)>,
) -> Result<flow::ConnectorState> {
    let mut state = ConnectorState::default();
    for (state_key, checkpoint) in bindings {
        state.bindings.insert(state_key, checkpoint);
    }

    Ok(flow::ConnectorState {
        updated_json: state.to_json()?.into(),
        merge_patch: false,
    })
}

/// Applied to every network call this connector makes — both object-store
/// requests and the embedded catalog's self-HTTP commit call — so a stuck
/// connection fails loudly with an error (and the connector exits) instead
/// of hanging indefinitely. No retry yet; a timeout here is a hard failure.
pub(crate) const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

pub(crate) fn build_object_store(
    store: &StoreConfig,
) -> Result<(Arc<dyn ObjectStore>, String, String, &'static str)> {
    let client_options = ClientOptions::new().with_timeout(REQUEST_TIMEOUT);

    match store {
        StoreConfig::S3(s3) => {
            let mut builder = AmazonS3Builder::new()
                .with_bucket_name(&s3.bucket)
                .with_region(&s3.region)
                .with_access_key_id(&s3.credentials.aws_access_key_id)
                .with_secret_access_key(&s3.credentials.aws_secret_access_key)
                // rustfs and MinIO require path-style addressing.
                .with_virtual_hosted_style_request(false)
                .with_client_options(client_options);

            if let Some(endpoint) = &s3.endpoint {
                builder = builder.with_endpoint(endpoint).with_allow_http(true);
            }

            Ok((
                Arc::new(builder.build()?),
                s3.prefix.clone(),
                s3.bucket.clone(),
                "s3",
            ))
        }
        StoreConfig::Gcs(gcs) => {
            let builder = GoogleCloudStorageBuilder::new()
                .with_bucket_name(&gcs.bucket)
                .with_service_account_key(&gcs.credentials.service_account_json)
                .with_client_options(client_options);

            Ok((
                Arc::new(builder.build()?),
                gcs.prefix.clone(),
                gcs.bucket.clone(),
                "gs",
            ))
        }
    }
}

/// Flush a binding's buffered rows into a new data file once this many
/// bytes have accumulated, instead of waiting for the whole transaction
/// to finish. Keeps peak memory (buffered Arrow arrays plus the encoded
/// Parquet bytes) bounded regardless of transaction size.
const DATA_FILE_FLUSH_BYTES: usize = 256 * 1024 * 1024;

/// Part size for the multipart upload `stage_pending_load` writes staged
/// `Load` keys to — bounds how much of a transaction's `Load` traffic sits
/// buffered in memory before a part actually uploads, same role
/// `DATA_FILE_FLUSH_BYTES` plays for `Store`.
const PENDING_LOADS_CHUNK_BYTES: usize = 16 * 1024 * 1024;

/// Caps each Parquet row group's estimated encoded size, so a single
/// `DATA_FILE_FLUSH_BYTES`-sized file ends up as several smaller row groups
/// rather than one — without this, the parquet crate's default `None`
/// `WriterProperties` only caps row groups by row *count* (1,048,576 by
/// default), so a file with fewer rows than that but still ~256MiB (e.g.
/// wide rows, large `flow_document` text) becomes a single row group. The
/// scan path (`iceberg::scan`) has to fetch a row group's entire compressed
/// bytes in one request before it can decode anything from it, and opens
/// every data file surviving pruning for a batch concurrently (to
/// merge-sort them) — so a handful of large single-row-group files on the
/// read path can multiply memory far beyond this constant's own name.
const MAX_ROW_GROUP_BYTES: usize = 32 * 1024 * 1024;

fn data_file_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_max_row_group_bytes(Some(MAX_ROW_GROUP_BYTES))
        .build()
}

/// Rows per `ArrowWriter::write` call in [`write_in_row_group_chunks`] — small
/// enough that even a single chunk is comfortably under
/// `MAX_ROW_GROUP_BYTES` for this connector's row sizes, so the writer's own
/// proactive byte-based splitting (see that function's doc comment) gets a
/// chance to engage before any single call can overshoot the cap.
const WRITE_CHUNK_ROWS: usize = 10_000;

/// `ArrowWriter::write`'s `max_row_group_bytes` splitting only proactively
/// engages once the current row group already has some buffered rows (it
/// needs an average-row-size estimate to decide how many more rows fit) —
/// on a single `write(&batch)` call with the *entire* accumulated batch, that
/// guard is never true until the whole batch is already written, so the cap
/// is only enforced after the fact, too late to prevent one oversized row
/// group. Writing in modest row-count chunks instead means the second and
/// later chunks always have `buffered_rows > 0`, letting the writer actually
/// split proactively.
fn write_in_row_group_chunks(
    writer: &mut ArrowWriter<&mut Vec<u8>>,
    batch: &RecordBatch,
) -> Result<()> {
    let mut offset = 0;
    while offset < batch.num_rows() {
        let len = WRITE_CHUNK_ROWS.min(batch.num_rows() - offset);
        writer.write(&batch.slice(offset, len))?;
        offset += len;
    }
    Ok(())
}

struct BindingState {
    table: String,
    collection: String,
    state_key: String,
    schema: Arc<Schema>,
    table_schema: serde_json::Value,
    last_column_id: i32,
    key_types: Vec<DataType>,
    key_builders: Vec<ColumnBuilder>,
    key_bound_trackers: Vec<BoundTracker>,
    /// The last key appended this data file, for asserting Store calls keep
    /// arriving in ascending order. Reset whenever `write_and_upload`
    /// flushes the accumulated rows into a new data file.
    last_key: Option<Vec<Bound>>,
    value_builders: Vec<ColumnBuilder>,
    doc_builder: StringBuilder,
    rows: usize,
    /// Running total of bytes buffered for the current (unflushed) data
    /// file: key_packed + values_packed + doc text appended this chunk.
    /// Reset to 0 each time a data file is flushed. A cheap proxy for the
    /// eventual encoded Parquet size, using lengths already in hand.
    estimated_bytes: usize,
    /// Fixed for this binding's whole process lifetime — where
    /// `stage_pending_load` stages this transaction's `Load` keys in object
    /// storage and `flush_pending_loads` reads them back from at `Flush`, so
    /// `Load` never has to hold them in memory or scan while an
    /// `Acknowledge` may still be in flight (see the `Flush` arm's comment).
    pending_loads_path: ObjectPath,
    /// `Some` only while this transaction has staged at least one `Load`
    /// key; taken, finished, and the object deleted by `flush_pending_loads`.
    pending_loads_upload: Option<WriteMultipart>,
    /// The file-path table `scan::load` interns into across however many
    /// chunked calls `flush_pending_loads` makes reading back
    /// `pending_loads_path` this transaction. `Store`'s `_meta.file_path`
    /// (an index into this, not a path) is resolved through it —
    /// deduplicated, so its size is bounded by distinct files touched, not
    /// rows touched.
    load_file_paths: scan::FilePathTable,
    /// The current snapshot's manifest-derived state, cached across
    /// `flush_pending_loads`'s chunked `scan::load` calls — `None` until the
    /// first call populates it, and reset to `None` in the `Flush` arm
    /// whenever it just applied a resolved `Acknowledge`'s new checkpoint,
    /// since that's the only time it can go stale.
    scan_cache: Option<scan::SnapshotScan>,
    /// Positions to mask with a positional delete this transaction, by the
    /// data file_path they were found in (`_meta.file_path`/
    /// `_meta.positional_delete` on the existing document, carried forward
    /// from `Load` through the runtime's document reduction).
    pending_deletes: HashMap<String, Vec<i64>>,
    /// Counts of inserts/updates/deletes `Store` has applied this
    /// transaction, logged and reset at `StartCommit`.
    transaction_counts: TransactionCounts,
    /// Cumulative since connector start (never reset) — logged alongside
    /// `transaction_counts` at `StartCommit` for a running per-collection
    /// total, not just this transaction's.
    total_keys_handled: u64,
    /// Cumulative Parquet bytes written for data files only (not
    /// positional-delete files) since connector start, never reset.
    total_bytes_written: u64,
    checkpoint: BindingCheckpoint,
}

#[derive(Default)]
struct TransactionCounts {
    inserts: u64,
    updates: u64,
    deletes: u64,
}

impl BindingState {
    /// Encodes whatever rows are currently buffered into one Parquet data
    /// file, uploads it, and writes its manifest — a no-op if nothing is
    /// buffered. Called both mid-transaction, once `estimated_bytes`
    /// crosses `DATA_FILE_FLUSH_BYTES`, and at the end of a transaction via
    /// `write_and_upload`, so a single commit may produce several data
    /// files instead of one sized to the whole transaction.
    ///
    /// Does not reset `last_key` — the ascending-key invariant it guards
    /// must hold across the whole transaction, not just within one data
    /// file.
    async fn flush_data_file(&mut self, catalog: &Catalog) -> Result<()> {
        if self.rows == 0 {
            return Ok(());
        }

        let snapshot_id = *self
            .checkpoint
            .pending_snapshot_id
            .get_or_insert_with(|| self.checkpoint.sequence_number + 1);

        let lower_bounds: Vec<(i32, Vec<u8>)> = self
            .key_bound_trackers
            .iter()
            .enumerate()
            .filter_map(|(i, tracker)| {
                tracker.encoded().map(|(lower, _)| (i as i32 + 1, lower))
            })
            .collect();
        let upper_bounds: Vec<(i32, Vec<u8>)> = self
            .key_bound_trackers
            .iter()
            .enumerate()
            .filter_map(|(i, tracker)| {
                tracker.encoded().map(|(_, upper)| (i as i32 + 1, upper))
            })
            .collect();
        for tracker in &mut self.key_bound_trackers {
            tracker.reset();
        }

        let mut columns: Vec<ArrayRef> = self
            .key_builders
            .iter_mut()
            .map(ColumnBuilder::finish)
            .collect();
        columns.extend(self.value_builders.iter_mut().map(ColumnBuilder::finish));
        columns.push(Arc::new(self.doc_builder.finish()));

        let batch = RecordBatch::try_new(self.schema.clone(), columns)?;

        let mut buf = Vec::new();
        let file_metadata = {
            let mut writer = ArrowWriter::try_new(
                &mut buf,
                self.schema.clone(),
                Some(data_file_writer_properties()),
            )?;
            write_in_row_group_chunks(&mut writer, &batch)?;
            writer.close()?
        };

        let bytes = buf.len();
        let rows = self.rows;
        let path = ObjectPath::from(format!(
            "{}/{}/data/{}.parquet",
            catalog.prefix,
            self.table,
            Uuid::new_v4()
        ));

        let started = Instant::now();
        catalog
            .object_store
            .put(&path, PutPayload::from(buf))
            .await?;

        self.total_bytes_written += bytes as u64;

        tracing::info!(
            table = %self.table,
            path = %path,
            rows,
            bytes,
            elapsed_ms = started.elapsed().as_millis(),
            "wrote parquet data file"
        );

        let split_offsets = file_metadata
            .row_groups()
            .iter()
            .filter_map(|rg| rg.file_offset())
            .collect();

        let pending_manifest = manifest::write_manifest(
            catalog,
            &self.table,
            &self.table_schema.to_string(),
            snapshot_id,
            DataFileInfo {
                file_path: catalog.uri(path.as_ref()),
                record_count: rows as i64,
                file_size_in_bytes: bytes as i64,
                split_offsets,
                lower_bounds,
                upper_bounds,
            },
        )
        .await?;

        self.checkpoint.pending_manifests.push(pending_manifest);
        self.rows = 0;
        self.estimated_bytes = 0;

        Ok(())
    }

    async fn write_and_upload(&mut self, catalog: &Catalog) -> Result<()> {
        // Always reset, even if this binding has nothing left to flush this
        // call (e.g. a mid-transaction binding-change flush already handled
        // its rows) — otherwise the ascending-key check below would compare
        // the next transaction's first key against a stale key left over
        // from this one.
        self.last_key = None;

        if self.rows == 0 && self.pending_deletes.is_empty() {
            return Ok(());
        }

        self.flush_data_file(catalog).await?;

        let snapshot_id = *self
            .checkpoint
            .pending_snapshot_id
            .get_or_insert_with(|| self.checkpoint.sequence_number + 1);

        // `mem::take` rather than `drain()`: drain keeps the HashMap's
        // allocated capacity, permanently retaining this binding's
        // largest-ever delete set.
        for (file_path, positions) in std::mem::take(&mut self.pending_deletes) {
            let pending_manifest = write_position_delete_file(
                catalog,
                &self.table,
                &self.table_schema.to_string(),
                snapshot_id,
                &file_path,
                &positions,
            )
            .await?;
            self.checkpoint.pending_manifests.push(pending_manifest);
        }

        Ok(())
    }

    /// Appends `key_packed` to this binding's `Load` staging object in
    /// object storage, starting a multipart upload to it if this is the
    /// first `Load` staged since the last `flush_pending_loads` drained it.
    /// Never scans — see the `Flush` arm's comment for why that has to wait.
    async fn stage_pending_load(&mut self, catalog: &Catalog, key_packed: &[u8]) -> Result<()> {
        if self.pending_loads_upload.is_none() {
            let upload = catalog
                .object_store
                .put_multipart(&self.pending_loads_path)
                .await
                .context("starting Load staging upload")?;
            self.pending_loads_upload = Some(WriteMultipart::new_with_chunk_size(
                upload,
                PENDING_LOADS_CHUNK_BYTES,
            ));
        }
        write_pending_load_record(self.pending_loads_upload.as_mut().unwrap(), key_packed);
        Ok(())
    }
}

/// One binding's `Acknowledge` work, with everything it needs taken out of
/// its `BindingState` by value so it can run on a spawned task independent
/// of the `BindingState`s a connector's main loop keeps processing `Load`
/// against (see `commit_pending_acknowledge`).
struct AckJob {
    state_key: String,
    table: String,
    table_schema: serde_json::Value,
    last_column_id: i32,
    checkpoint: BindingCheckpoint,
}

/// Commits every binding's pending manifests, in order, exactly as the old
/// inline per-binding loop in the `Acknowledge` arm did, then sends
/// `Response::Acknowledged` itself — the whole function runs on a spawned
/// task rather than being `.await`ed inline, so the main loop can keep
/// reading and staging `Load` requests (to object storage, via
/// `BindingState::stage_pending_load`) while this is still in flight.
///
/// Sending `Acknowledged` here, as soon as the commit resolves, rather than
/// leaving it to the main loop, matters because the runtime won't send
/// `Flush` until it's seen `Acknowledged` — if a transaction has no `Load`
/// calls, the main loop has no other reason to come back around and notice
/// this task finished, so waiting for it to do so would deadlock. The
/// `Flush` arm separately joins the returned `JoinHandle` to apply the new
/// checkpoints to `bindings` before evaluating any staged `Load`s, which is
/// what guarantees those loads see this commit's result (read-committed)
/// rather than racing it.
async fn commit_pending_acknowledge(
    catalog: Arc<Catalog>,
    output: Arc<Mutex<Output>>,
    jobs: Vec<AckJob>,
) -> Result<Vec<BindingCheckpoint>> {
    let state_keys: Vec<String> = jobs.iter().map(|job| job.state_key.clone()).collect();

    let mut checkpoints = Vec::with_capacity(jobs.len());
    for job in jobs {
        checkpoints.push(acknowledge_binding(&catalog, job).await?);
    }

    let state = connector_state_json(state_keys.into_iter().zip(checkpoints.iter().cloned()))?;
    output.lock().unwrap().send(Response {
        kind: Some(response::Kind::Acknowledged(Acknowledged {
            state: Some(state),
        })),
        ..Default::default()
    })?;
    tracing::warn!("Acknowledged");
    log_object_store_requests("acknowledge", &catalog.request_counts);

    Ok(checkpoints)
}

async fn acknowledge_binding(catalog: &Catalog, job: AckJob) -> Result<BindingCheckpoint> {
    let AckJob {
        table,
        table_schema,
        last_column_id,
        mut checkpoint,
        ..
    } = job;

    if checkpoint.pending_manifests.is_empty() {
        return Ok(checkpoint);
    }

    tracing::info!(table = %table, "acknowledge: starting");

    let snapshot_id = checkpoint
        .pending_snapshot_id
        .unwrap_or(checkpoint.sequence_number + 1);

    let prior_entries = match &checkpoint.manifest_list {
        Some(path) => manifest_list::read_manifest_list(&catalog.object_store, path).await?,
        None => Vec::new(),
    };
    tracing::info!(table = %table, "acknowledge: read prior manifest list");

    let manifest_list_path = manifest_list::write_manifest_list(
        &catalog.object_store,
        &catalog.prefix,
        &table,
        snapshot_id,
        snapshot_id,
        prior_entries,
        &checkpoint.pending_manifests,
    )
    .await?;
    tracing::info!(table = %table, "acknowledge: wrote new manifest list");

    let data_manifests = checkpoint
        .pending_manifests
        .iter()
        .filter(|m| m.content == CONTENT_DATA);
    let added_files_count: i32 = data_manifests.clone().map(|m| m.added_files_count).sum();
    let added_rows_count: i64 = data_manifests.clone().map(|m| m.added_rows_count).sum();
    let added_files_size: i64 = data_manifests.map(|m| m.added_files_size).sum();
    let added_position_deletes: i64 = checkpoint
        .pending_manifests
        .iter()
        .filter(|m| m.content == CONTENT_DELETES)
        .map(|m| m.added_rows_count)
        .sum();

    let snapshot = build_snapshot(
        snapshot_id,
        checkpoint.snapshot_id,
        snapshot_id,
        catalog.uri(&manifest_list_path),
        added_files_count,
        added_rows_count,
        added_files_size,
        added_position_deletes,
    );

    tracing::info!(table = %table, "acknowledge: calling commit_snapshot");
    commit_snapshot(catalog, &table, table_schema, last_column_id, &snapshot).await?;
    tracing::info!(table = %table, "acknowledge: commit_snapshot returned");

    checkpoint.snapshot_id = Some(snapshot_id);
    checkpoint.sequence_number = snapshot_id;
    checkpoint.manifest_list = Some(manifest_list_path);
    checkpoint.pending_manifests.clear();
    checkpoint.pending_snapshot_id = None;

    Ok(checkpoint)
}

/// Commits a snapshot by calling the connector's own embedded REST catalog
/// server, exercising the same `POST /v1/{prefix}/transactions/commit` path
/// a real client would use. The table's schema is sent along on every
/// commit — the catalog only applies it when the table doesn't exist yet,
/// since schema evolution isn't supported.
async fn commit_snapshot(
    catalog: &Catalog,
    table: &str,
    table_schema: serde_json::Value,
    last_column_id: i32,
    snapshot: &Snapshot,
) -> Result<()> {
    let request = CommitTransactionRequest {
        table_changes: vec![TableChange {
            identifier: TableIdentifier {
                name: table.to_string(),
            },
            updates: vec![
                TableUpdate::AddSchema {
                    schema: table_schema,
                    last_column_id,
                },
                TableUpdate::AddSnapshot {
                    snapshot: snapshot.clone(),
                },
                TableUpdate::SetSnapshotRef {
                    ref_name: "main".to_string(),
                    ref_type: "branch".to_string(),
                    snapshot_id: snapshot.snapshot_id,
                },
            ],
        }],
    };

    let port = server::listen_port();
    let url = format!("http://127.0.0.1:{port}/v1/{CATALOG_PREFIX}/transactions/commit");

    tracing::info!(table, %url, "commit_snapshot: sending request");
    let response = catalog
        .http_client
        .post(&url)
        .json(&request)
        .send()
        .await
        .context("calling embedded catalog commit endpoint")?;
    tracing::info!(table, status = %response.status(), "commit_snapshot: got response");

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("catalog commit failed ({status}): {body}");
    }

    tracing::info!(
        table,
        snapshot_id = snapshot.snapshot_id,
        parent_snapshot_id = ?snapshot.parent_snapshot_id,
        manifest_list = %snapshot.manifest_list,
        summary = ?snapshot.summary,
        "committed snapshot to table metadata"
    );

    Ok(())
}

/// The reserved Iceberg field-ids for a positional-delete file's two
/// columns, per the spec's Appendix A.
const POSITION_DELETE_FILE_PATH_FIELD_ID: i32 = 2147483546;
const POSITION_DELETE_POS_FIELD_ID: i32 = 2147483545;

fn position_delete_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        arrow_field_with_iceberg_id(
            "file_path",
            DataType::Utf8,
            POSITION_DELETE_FILE_PATH_FIELD_ID,
        ),
        arrow_field_with_iceberg_id("pos", DataType::Int64, POSITION_DELETE_POS_FIELD_ID),
    ]))
}

/// Writes a positional-delete Parquet file masking `positions` within
/// `referenced_data_file`, and its manifest entry — one delete file per
/// referenced data file_path.
async fn write_position_delete_file(
    catalog: &Catalog,
    table: &str,
    table_schema_json: &str,
    snapshot_id: i64,
    referenced_data_file: &str,
    positions: &[i64],
) -> Result<crate::iceberg::state::PendingManifest> {
    let schema = position_delete_schema();

    let mut file_path_builder = StringBuilder::new();
    let mut pos_builder = Int64Builder::new();
    for &position in positions {
        file_path_builder.append_value(referenced_data_file);
        pos_builder.append_value(position);
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(file_path_builder.finish()),
            Arc::new(pos_builder.finish()),
        ],
    )?;

    let mut buf = Vec::new();
    {
        let mut writer =
            ArrowWriter::try_new(&mut buf, schema.clone(), Some(data_file_writer_properties()))?;
        write_in_row_group_chunks(&mut writer, &batch)?;
        writer.close()?;
    }

    let file_size_in_bytes = buf.len() as i64;
    let record_count = positions.len() as i64;
    let path = ObjectPath::from(format!(
        "{}/{table}/data/{}-deletes.parquet",
        catalog.prefix,
        Uuid::new_v4()
    ));

    catalog
        .object_store
        .put(&path, PutPayload::from(buf))
        .await?;

    tracing::info!(
        table = %table,
        path = %path,
        record_count,
        referenced_data_file,
        "wrote positional-delete file"
    );

    manifest::write_delete_manifest(
        catalog,
        table,
        table_schema_json,
        snapshot_id,
        DeleteFileInfo {
            file_path: catalog.uri(path.as_ref()),
            record_count,
            file_size_in_bytes,
            referenced_data_file: referenced_data_file.to_string(),
        },
    )
    .await
}

fn binding_state(
    binding: &materialization_spec::Binding,
    checkpoint: BindingCheckpoint,
    object_store_prefix: &str,
) -> Result<BindingState> {
    let field_selection = binding
        .field_selection
        .as_ref()
        .expect("binding must have field selection");
    let collection = binding
        .collection
        .as_ref()
        .expect("binding must have collection spec");

    let mut fields = Vec::new();
    let mut key_types = Vec::new();
    let mut key_builders = Vec::new();
    let mut key_bound_trackers = Vec::new();
    let mut value_builders = Vec::new();
    let mut iceberg_fields = Vec::new();
    let mut field_id = 1;

    for key_field in &field_selection.keys {
        let data_type = arrow_type_for_field(&collection.projections, key_field);
        fields.push(arrow_field_with_iceberg_id(
            key_field,
            data_type.clone(),
            field_id,
        ));
        key_builders.push(ColumnBuilder::new(&data_type));
        key_bound_trackers.push(BoundTracker::new(&data_type));
        key_types.push(data_type.clone());
        iceberg_fields.push(json!({
            "id": field_id,
            "name": key_field,
            "required": false,
            "type": iceberg_type_for_arrow(&data_type),
        }));
        field_id += 1;
    }
    for value_field in &field_selection.values {
        let data_type = arrow_type_for_field(&collection.projections, value_field);
        fields.push(arrow_field_with_iceberg_id(
            value_field,
            data_type.clone(),
            field_id,
        ));
        value_builders.push(ColumnBuilder::new(&data_type));
        iceberg_fields.push(json!({
            "id": field_id,
            "name": value_field,
            "required": false,
            "type": iceberg_type_for_arrow(&data_type),
        }));
        field_id += 1;
    }
    fields.push(arrow_field_with_iceberg_id(
        "flow_document",
        DataType::Utf8,
        field_id,
    ));
    iceberg_fields.push(json!({
        "id": field_id,
        "name": "flow_document",
        "required": false,
        "type": "string",
    }));

    let last_column_id = field_id;
    let table_schema = json!({
        "type": "struct",
        "schema-id": 0,
        "fields": iceberg_fields,
    });

    let table = binding.resource_path[0].clone();
    let pending_loads_path = ObjectPath::from(format!(
        "{object_store_prefix}/{table}/pending-loads/{}.bin",
        Uuid::new_v4()
    ));

    Ok(BindingState {
        table,
        collection: collection.name.clone(),
        state_key: binding.state_key.clone(),
        schema: Arc::new(Schema::new(fields)),
        table_schema,
        last_column_id,
        key_types,
        key_builders,
        key_bound_trackers,
        last_key: None,
        value_builders,
        doc_builder: StringBuilder::new(),
        rows: 0,
        estimated_bytes: 0,
        pending_loads_path,
        pending_loads_upload: None,
        load_file_paths: scan::FilePathTable::default(),
        scan_cache: None,
        pending_deletes: HashMap::new(),
        transaction_counts: TransactionCounts::default(),
        total_keys_handled: 0,
        total_bytes_written: 0,
        checkpoint,
    })
}

/// Builds an Arrow field stamped with the Parquet field-id metadata key that
/// `parquet::arrow::ArrowWriter` reads to assign the physical column's
/// `field_id`, which Iceberg readers require to map data file columns back
/// to schema fields.
pub(crate) fn arrow_field_with_iceberg_id(name: &str, data_type: DataType, field_id: i32) -> Field {
    Field::new(name, data_type, true).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        field_id.to_string(),
    )]))
}

pub(crate) fn arrow_type_for_field(projections: &[Projection], field: &str) -> DataType {
    let types: Vec<&str> = projections
        .iter()
        .find(|p| p.field == field)
        .and_then(|p| p.inference.as_ref())
        .map(|inference| {
            inference
                .types
                .iter()
                .map(String::as_str)
                .filter(|t| *t != "null")
                .collect()
        })
        .unwrap_or_default();

    match types.as_slice() {
        ["integer"] => DataType::Int64,
        ["numeric"] => DataType::Float64,
        ["boolean"] => DataType::Boolean,
        // "string", multi-type, and anything else fall back to a JSON-encoded
        // string column.
        _ => DataType::Utf8,
    }
}

fn iceberg_type_for_arrow(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Int64 => "long",
        DataType::Float64 => "double",
        DataType::Boolean => "boolean",
        _ => "string",
    }
}

enum ColumnBuilder {
    Utf8(StringBuilder),
    Int64(Int64Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
}

impl ColumnBuilder {
    fn new(data_type: &DataType) -> ColumnBuilder {
        match data_type {
            DataType::Int64 => ColumnBuilder::Int64(Int64Builder::new()),
            DataType::Float64 => ColumnBuilder::Float64(Float64Builder::new()),
            DataType::Boolean => ColumnBuilder::Boolean(BooleanBuilder::new()),
            _ => ColumnBuilder::Utf8(StringBuilder::new()),
        }
    }

    fn append(&mut self, element: &Element) -> Result<()> {
        match self {
            ColumnBuilder::Utf8(b) => match element {
                Element::Nil => b.append_null(),
                Element::String(v) => b.append_value(v.as_ref()),
                Element::Bytes(v) => b.append_value(String::from_utf8_lossy(v).as_ref()),
                Element::Int(v) => b.append_value(v.to_string()),
                Element::Float(v) => b.append_value(v.to_string()),
                Element::Double(v) => b.append_value(v.to_string()),
                Element::Bool(v) => b.append_value(v.to_string()),
                other => anyhow::bail!("unexpected tuple element {:?}", other),
            },
            ColumnBuilder::Int64(b) => match element {
                Element::Nil => b.append_null(),
                Element::Int(v) => b.append_value(*v),
                other => anyhow::bail!("unexpected tuple element {:?} for integer column", other),
            },
            ColumnBuilder::Float64(b) => match element {
                Element::Nil => b.append_null(),
                Element::Float(v) => b.append_value(*v as f64),
                Element::Double(v) => b.append_value(*v),
                other => anyhow::bail!("unexpected tuple element {:?} for numeric column", other),
            },
            ColumnBuilder::Boolean(b) => match element {
                Element::Nil => b.append_null(),
                Element::Bool(v) => b.append_value(*v),
                other => anyhow::bail!("unexpected tuple element {:?} for boolean column", other),
            },
        }

        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            ColumnBuilder::Utf8(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Int64(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Float64(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Boolean(b) => Arc::new(b.finish()) as ArrayRef,
        }
    }
}
