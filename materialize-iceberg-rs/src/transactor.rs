use std::collections::HashMap;
use std::sync::Arc;
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
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::PutPayload;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use parquet::arrow::ArrowWriter;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
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
use crate::iceberg::catalog::CatalogState;
use crate::iceberg::catalog::CommitTransactionRequest;
use crate::iceberg::catalog::TableChange;
use crate::iceberg::catalog::TableIdentifier;
use crate::iceberg::catalog::TableUpdate;
use crate::iceberg::manifest::DataFileInfo;
use crate::iceberg::manifest::{self};
use crate::iceberg::manifest_list;
use crate::iceberg::snapshot::Snapshot;
use crate::iceberg::snapshot::build_snapshot;
use crate::iceberg::state::BindingCheckpoint;
use crate::iceberg::state::ConnectorState;
use crate::server;
use crate::server::SharedCatalog;

pub async fn run_transactions(
    input: &mut Input,
    output: &mut Output,
    open: Open,
    catalog: SharedCatalog,
) -> Result<()> {
    let spec = open
        .materialization
        .expect("must have a materialization spec");

    let config: EndpointConfig = serde_json::from_slice(&spec.config_json)?;
    let (object_store, prefix, bucket) = build_object_store(&config.store)?;

    let catalog_state = CatalogState {
        object_store: object_store.clone(),
        prefix: prefix.clone(),
        bucket: bucket.clone(),
    };
    server::set_catalog(&catalog, catalog_state.clone()).await;

    let mut connector_state = ConnectorState::from_json(open.state_json.as_ref())?;

    let mut bindings = spec
        .bindings
        .iter()
        .map(|binding| {
            let checkpoint = connector_state
                .bindings
                .remove(&binding.state_key)
                .unwrap_or_default();
            binding_state(binding, checkpoint)
        })
        .collect::<Result<Vec<_>>>()?;

    for binding in &mut bindings {
        tracing::warn!(binding.state_key, "binding found");
        if binding.checkpoint.snapshot_id.is_none() {
            if let Some(current) = catalog_state.current_snapshot(&binding.table).await? {
                binding.checkpoint.snapshot_id = Some(current.snapshot_id);
                binding.checkpoint.sequence_number = current.sequence_number;
                binding.checkpoint.manifest_list = Some(current.manifest_list);
            }
        }
    }

    output.send(Response {
        kind: Some(response::Kind::Opened(Opened {
            runtime_checkpoint: None,
            ..Default::default()
        })),
        ..Default::default()
    })?;

    loop {
        let request = match input.read()? {
            Some(req) => req,
            None => return Ok(()),
        };

        match request.kind {
            Some(request::Kind::Load(_)) => {
                tracing::warn!("--> Load");
            }
            Some(request::Kind::Flush(_)) => {
                tracing::warn!("--> Flush");
                output.send(Response {
                    kind: Some(response::Kind::Flushed(Flushed { state: None })),
                    ..Default::default()
                })?;
                tracing::warn!("<-- Flushed");
            }
            Some(request::Kind::Store(store)) => {
                tracing::warn!("--> Store");
                let binding = &mut bindings[store.binding as usize];

                if store.delete {
                    tracing::warn!(
                        table = %binding.table,
                        "dropping deleted document: positional deletes are not implemented yet"
                    );
                    continue;
                }

                let elements: Vec<Element> = tuple::unpack(&store.key_packed)?;
                for (builder, element) in binding.key_builders.iter_mut().zip(elements.iter()) {
                    builder.append(element)?;
                }

                let elements: Vec<Element> = tuple::unpack(&store.values_packed)?;
                for (builder, element) in binding.value_builders.iter_mut().zip(elements.iter()) {
                    builder.append(element)?;
                }

                let doc =
                    std::str::from_utf8(&store.doc_json).context("doc_json must be valid UTF-8")?;
                binding.doc_builder.append_value(doc);
                binding.rows += 1;
            }
            Some(request::Kind::StartCommit(_)) => {
                tracing::warn!("--> StartCommit");
                for binding in bindings.iter_mut() {
                    binding
                        .write_and_upload(&object_store, &prefix, &bucket)
                        .await?;
                }

                let state = checkpoint_from_bindings(&bindings)?;
                output.send(Response {
                    kind: Some(response::Kind::StartedCommit(StartedCommit {
                        state: Some(state),
                    })),
                    ..Default::default()
                })?;
                tracing::warn!("<-- StartedCommit");
            }
            Some(request::Kind::Acknowledge(_)) => {
                tracing::warn!("--> Acknowledge");
                for binding in bindings.iter_mut() {
                    binding.acknowledge(&object_store, &prefix, &bucket).await?;
                }

                let state = checkpoint_from_bindings(&bindings)?;
                output.send(Response {
                    kind: Some(response::Kind::Acknowledged(Acknowledged {
                        state: Some(state),
                    })),
                    ..Default::default()
                })?;
                tracing::warn!("<-- Acknowledged");
            }
            _ => {}
        }
    }
}

fn checkpoint_from_bindings(bindings: &[BindingState]) -> Result<flow::ConnectorState> {
    let mut state = ConnectorState::default();
    for binding in bindings {
        state
            .bindings
            .insert(binding.state_key.clone(), binding.checkpoint.clone());
    }

    Ok(flow::ConnectorState {
        updated_json: state.to_json()?.into(),
        merge_patch: false,
    })
}

fn build_object_store(store: &StoreConfig) -> Result<(Arc<dyn ObjectStore>, String, String)> {
    let StoreConfig::S3(s3) = store;

    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&s3.bucket)
        .with_region(&s3.region)
        .with_access_key_id(&s3.credentials.aws_access_key_id)
        .with_secret_access_key(&s3.credentials.aws_secret_access_key)
        // rustfs and MinIO require path-style addressing.
        .with_virtual_hosted_style_request(false);

    if let Some(endpoint) = &s3.endpoint {
        builder = builder.with_endpoint(endpoint).with_allow_http(true);
    }

    Ok((
        Arc::new(builder.build()?),
        s3.prefix.clone(),
        s3.bucket.clone(),
    ))
}

struct BindingState {
    table: String,
    state_key: String,
    schema: Arc<Schema>,
    table_schema: serde_json::Value,
    last_column_id: i32,
    key_builders: Vec<ColumnBuilder>,
    value_builders: Vec<ColumnBuilder>,
    doc_builder: StringBuilder,
    rows: usize,
    checkpoint: BindingCheckpoint,
}

impl BindingState {
    async fn write_and_upload(
        &mut self,
        object_store: &Arc<dyn ObjectStore>,
        prefix: &str,
        bucket: &str,
    ) -> Result<()> {
        if self.rows == 0 {
            return Ok(());
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
            let mut writer = ArrowWriter::try_new(&mut buf, self.schema.clone(), None)?;
            writer.write(&batch)?;
            writer.close()?
        };

        let bytes = buf.len();
        let rows = self.rows;
        let path = ObjectPath::from(format!(
            "{}/{}/data/{}.parquet",
            prefix,
            self.table,
            Uuid::new_v4()
        ));

        let started = Instant::now();
        object_store.put(&path, PutPayload::from(buf)).await?;

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

        let snapshot_id = *self
            .checkpoint
            .pending_snapshot_id
            .get_or_insert_with(|| self.checkpoint.sequence_number + 1);

        let pending_manifest = manifest::write_manifest(
            object_store,
            prefix,
            bucket,
            &self.table,
            &self.table_schema.to_string(),
            snapshot_id,
            DataFileInfo {
                file_path: format!("s3://{bucket}/{path}"),
                record_count: rows as i64,
                file_size_in_bytes: bytes as i64,
                split_offsets,
            },
        )
        .await?;

        self.checkpoint.pending_manifests.push(pending_manifest);

        self.rows = 0;
        Ok(())
    }

    async fn acknowledge(
        &mut self,
        object_store: &Arc<dyn ObjectStore>,
        prefix: &str,
        bucket: &str,
    ) -> Result<()> {
        if self.checkpoint.pending_manifests.is_empty() {
            return Ok(());
        }

        let snapshot_id = self
            .checkpoint
            .pending_snapshot_id
            .unwrap_or(self.checkpoint.sequence_number + 1);

        let prior_entries = match &self.checkpoint.manifest_list {
            Some(path) => manifest_list::read_manifest_list(object_store, path).await?,
            None => Vec::new(),
        };

        let manifest_list_path = manifest_list::write_manifest_list(
            object_store,
            prefix,
            &self.table,
            snapshot_id,
            snapshot_id,
            prior_entries,
            &self.checkpoint.pending_manifests,
        )
        .await?;

        let added_files_count: i32 = self
            .checkpoint
            .pending_manifests
            .iter()
            .map(|m| m.added_files_count)
            .sum();
        let added_rows_count: i64 = self
            .checkpoint
            .pending_manifests
            .iter()
            .map(|m| m.added_rows_count)
            .sum();
        let added_files_size: i64 = self
            .checkpoint
            .pending_manifests
            .iter()
            .map(|m| m.added_files_size)
            .sum();

        let snapshot = build_snapshot(
            snapshot_id,
            self.checkpoint.snapshot_id,
            snapshot_id,
            format!("s3://{bucket}/{manifest_list_path}"),
            added_files_count,
            added_rows_count,
            added_files_size,
        );

        self.commit_snapshot(snapshot).await?;

        self.checkpoint.snapshot_id = Some(snapshot_id);
        self.checkpoint.sequence_number = snapshot_id;
        self.checkpoint.manifest_list = Some(manifest_list_path);
        self.checkpoint.pending_manifests.clear();
        self.checkpoint.pending_snapshot_id = None;

        Ok(())
    }

    /// Commits a snapshot by calling the connector's own embedded REST
    /// catalog server, exercising the same `POST
    /// /v1/{prefix}/transactions/commit` path a real client would use. The
    /// table's schema is sent along on every commit — the catalog only
    /// applies it when the table doesn't exist yet, since schema evolution
    /// isn't supported.
    async fn commit_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        let request = CommitTransactionRequest {
            table_changes: vec![TableChange {
                identifier: TableIdentifier {
                    name: self.table.clone(),
                },
                updates: vec![
                    TableUpdate::AddSchema {
                        schema: self.table_schema.clone(),
                        last_column_id: self.last_column_id,
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

        let response = reqwest::Client::new()
            .post(&url)
            .json(&request)
            .send()
            .await
            .context("calling embedded catalog commit endpoint")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("catalog commit failed ({status}): {body}");
        }

        tracing::info!(
            table = %self.table,
            snapshot_id = snapshot.snapshot_id,
            parent_snapshot_id = ?snapshot.parent_snapshot_id,
            manifest_list = %snapshot.manifest_list,
            summary = ?snapshot.summary,
            "committed snapshot to table metadata"
        );

        Ok(())
    }
}

fn binding_state(
    binding: &materialization_spec::Binding,
    checkpoint: BindingCheckpoint,
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
    let mut key_builders = Vec::new();
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

    Ok(BindingState {
        table: binding.resource_path[0].clone(),
        state_key: binding.state_key.clone(),
        schema: Arc::new(Schema::new(fields)),
        table_schema,
        last_column_id,
        key_builders,
        value_builders,
        doc_builder: StringBuilder::new(),
        rows: 0,
        checkpoint,
    })
}

/// Builds an Arrow field stamped with the Parquet field-id metadata key that
/// `parquet::arrow::ArrowWriter` reads to assign the physical column's
/// `field_id`, which Iceberg readers require to map data file columns back
/// to schema fields.
fn arrow_field_with_iceberg_id(name: &str, data_type: DataType, field_id: i32) -> Field {
    Field::new(name, data_type, true).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        field_id.to_string(),
    )]))
}

fn arrow_type_for_field(projections: &[Projection], field: &str) -> DataType {
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
