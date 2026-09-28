use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use object_store::{
    ObjectStore, ObjectStoreExt, PutPayload, aws::AmazonS3Builder, path::Path as ObjectPath,
};
use parquet::arrow::ArrowWriter;
use proto_flow::{
    flow::{Projection, materialization_spec},
    materialize::{
        Response, request,
        request::Open,
        response,
        response::{Acknowledged, Flushed, Opened, StartedCommit},
    },
};
use tuple::Element;
use uuid::Uuid;

use crate::configuration::{EndpointConfig, Store as StoreConfig};
use crate::{Input, Output};

pub async fn run_transactions(input: &mut Input, output: &mut Output, open: Open) -> Result<()> {
    let spec = open
        .materialization
        .expect("must have a materialization spec");

    let config: EndpointConfig = serde_json::from_slice(&spec.config_json)?;
    let (object_store, prefix) = build_object_store(&config.store)?;

    let mut bindings = spec
        .bindings
        .iter()
        .map(binding_state)
        .collect::<Result<Vec<_>>>()?;

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
            Some(request::Kind::Flush(_)) => {
                output.send(Response {
                    kind: Some(response::Kind::Flushed(Flushed { state: None })),
                    ..Default::default()
                })?;
            }
            Some(request::Kind::Store(store)) => {
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

                let doc =
                    std::str::from_utf8(&store.doc_json).context("doc_json must be valid UTF-8")?;
                binding.doc_builder.append_value(doc);
                binding.rows += 1;
            }
            Some(request::Kind::StartCommit(_)) => {
                for binding in bindings.iter_mut() {
                    binding.write_and_upload(&object_store, &prefix).await?;
                }

                output.send(Response {
                    kind: Some(response::Kind::StartedCommit(StartedCommit { state: None })),
                    ..Default::default()
                })?;
            }
            Some(request::Kind::Acknowledge(_)) => {
                output.send(Response {
                    kind: Some(response::Kind::Acknowledged(Acknowledged { state: None })),
                    ..Default::default()
                })?;
            }
            _ => {}
        }
    }
}

fn build_object_store(store: &StoreConfig) -> Result<(Arc<dyn ObjectStore>, String)> {
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

    Ok((Arc::new(builder.build()?), s3.prefix.clone()))
}

struct BindingState {
    table: String,
    schema: Arc<Schema>,
    key_builders: Vec<KeyBuilder>,
    doc_builder: StringBuilder,
    rows: usize,
}

impl BindingState {
    async fn write_and_upload(
        &mut self,
        object_store: &Arc<dyn ObjectStore>,
        prefix: &str,
    ) -> Result<()> {
        if self.rows == 0 {
            return Ok(());
        }

        let mut columns: Vec<ArrayRef> = self
            .key_builders
            .iter_mut()
            .map(KeyBuilder::finish)
            .collect();
        columns.push(Arc::new(self.doc_builder.finish()));

        let batch = RecordBatch::try_new(self.schema.clone(), columns)?;

        let mut buf = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, self.schema.clone(), None)?;
            writer.write(&batch)?;
            writer.close()?;
        }

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

        self.rows = 0;
        Ok(())
    }
}

fn binding_state(binding: &materialization_spec::Binding) -> Result<BindingState> {
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

    for key_field in &field_selection.keys {
        let data_type = arrow_type_for_key_field(&collection.projections, key_field);
        fields.push(Field::new(key_field.clone(), data_type.clone(), true));
        key_builders.push(KeyBuilder::new(&data_type));
    }
    fields.push(Field::new("flow_document", DataType::Utf8, true));

    Ok(BindingState {
        table: binding.resource_path[0].clone(),
        schema: Arc::new(Schema::new(fields)),
        key_builders,
        doc_builder: StringBuilder::new(),
        rows: 0,
    })
}

fn arrow_type_for_key_field(projections: &[Projection], field: &str) -> DataType {
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

enum KeyBuilder {
    Utf8(StringBuilder),
    Int64(Int64Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
}

impl KeyBuilder {
    fn new(data_type: &DataType) -> KeyBuilder {
        match data_type {
            DataType::Int64 => KeyBuilder::Int64(Int64Builder::new()),
            DataType::Float64 => KeyBuilder::Float64(Float64Builder::new()),
            DataType::Boolean => KeyBuilder::Boolean(BooleanBuilder::new()),
            _ => KeyBuilder::Utf8(StringBuilder::new()),
        }
    }

    fn append(&mut self, element: &Element) -> Result<()> {
        match self {
            KeyBuilder::Utf8(b) => match element {
                Element::Nil => b.append_null(),
                Element::String(v) => b.append_value(v.as_ref()),
                Element::Bytes(v) => b.append_value(String::from_utf8_lossy(v).as_ref()),
                Element::Int(v) => b.append_value(v.to_string()),
                Element::Float(v) => b.append_value(v.to_string()),
                Element::Double(v) => b.append_value(v.to_string()),
                Element::Bool(v) => b.append_value(v.to_string()),
                other => anyhow::bail!("unexpected key tuple element {:?}", other),
            },
            KeyBuilder::Int64(b) => match element {
                Element::Nil => b.append_null(),
                Element::Int(v) => b.append_value(*v),
                other => anyhow::bail!(
                    "unexpected key tuple element {:?} for integer column",
                    other
                ),
            },
            KeyBuilder::Float64(b) => match element {
                Element::Nil => b.append_null(),
                Element::Float(v) => b.append_value(*v as f64),
                Element::Double(v) => b.append_value(*v),
                other => anyhow::bail!(
                    "unexpected key tuple element {:?} for numeric column",
                    other
                ),
            },
            KeyBuilder::Boolean(b) => match element {
                Element::Nil => b.append_null(),
                Element::Bool(v) => b.append_value(*v),
                other => anyhow::bail!(
                    "unexpected key tuple element {:?} for boolean column",
                    other
                ),
            },
        }

        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            KeyBuilder::Utf8(b) => Arc::new(b.finish()) as ArrayRef,
            KeyBuilder::Int64(b) => Arc::new(b.finish()) as ArrayRef,
            KeyBuilder::Float64(b) => Arc::new(b.finish()) as ArrayRef,
            KeyBuilder::Boolean(b) => Arc::new(b.finish()) as ArrayRef,
        }
    }
}
