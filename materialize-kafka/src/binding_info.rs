use crate::configuration::{MessageFormat, SchemaRegistryConfig};
use anyhow::Result;
use doc::{ptr::Token, shape::ObjProperty, Pointer, Shape};
use futures::stream::{self, StreamExt};
use json::schema::{
    self,
    types::{self},
};
use proto_flow::flow::{inference, materialization_spec::Binding, Projection};
use reqwest::Client;
use serde::Deserialize;
use xxhash_rust::xxh3::xxh3_64;

const CONCURRENT_SCHEMA_REQUESTS: usize = 10;

pub struct BindingInfo {
    pub topic: String,
    pub schema: Option<BindingSchema>,
    pub key_ptr: Vec<Pointer>,
    pub field_names: Vec<String>,
}

pub struct BindingSchema {
    pub key_schema: avro::Schema,
    pub key_schema_id: u32,
    pub schema: avro::Schema,
    pub schema_id: u32,
}

pub async fn get_binding_info(
    bindings: &[Binding],
    message_format: &MessageFormat,
    schema_registry_config: Option<&SchemaRegistryConfig>,
) -> Result<Vec<BindingInfo>> {
    let computed = bindings
        .iter()
        .map(binding_info)
        .collect::<Result<Vec<_>>>()?;

    if matches!(message_format, MessageFormat::JSON) {
        return Ok(computed
            .into_iter()
            .map(|binding| BindingInfo {
                topic: binding.topic,
                schema: None,
                key_ptr: binding.key_ptr,
                field_names: binding.field_names,
            })
            .collect());
    }

    let http = Client::new();
    let cfg =
        schema_registry_config.expect("schema registry config must be provided for Avro encoding");

    // Get the schema ID for each binding, registering it if necessary. The
    // subject name is synthesized from a hash of the schema itself, and uses
    // what amounts to a TopicRecordNameStrategy, where the record name is the
    // hash of the schema. In this way a schema change is treated as a new type
    // of record.
    let schema_ids = stream::iter(
        computed
            .iter()
            .map(|binding| {
                let key_schema = binding.key_schema.clone();
                let schema = binding.schema.clone();
                let http = http.clone();
                let SchemaRegistryConfig {
                    endpoint,
                    username,
                    password,
                } = cfg.clone();

                async move {
                    let mut key_and_sch = [0; 2];
                    for (idx, sch) in [key_schema, schema].iter().enumerate() {
                        let subject = subject_for_schema(&binding.topic, &sch.canonical_form());
                        let schema_id = upsert_schema(
                            http.clone(),
                            &endpoint,
                            &username,
                            &password,
                            &subject,
                            &sch.canonical_form(),
                        )
                        .await?;

                        key_and_sch[idx] = schema_id;
                    }

                    Ok((key_and_sch[0], key_and_sch[1]))
                }
            })
            .collect::<Vec<_>>(),
    )
    .buffered(CONCURRENT_SCHEMA_REQUESTS)
    .collect::<Vec<_>>()
    .await
    .into_iter()
    .collect::<Result<Vec<_>>>()?;

    Ok(computed
        .into_iter()
        .zip(schema_ids)
        .map(|(binding, (key_schema_id, schema_id))| BindingInfo {
            topic: binding.topic,
            schema: Some(BindingSchema {
                key_schema: binding.key_schema,
                key_schema_id,
                schema: binding.schema,
                schema_id,
            }),
            key_ptr: binding.key_ptr,
            field_names: binding.field_names,
        })
        .collect())
}

fn subject_for_schema(topic: &str, schema_str: &str) -> String {
    let schema_hash = xxh3_64(schema_str.as_bytes());
    format!("{}-{:016x}", topic, schema_hash)
}

#[derive(Deserialize, Debug)]
struct SchemaId {
    id: u32,
}

async fn upsert_schema(
    http: Client,
    endpoint: &str,
    username: &str,
    password: &str,
    subject: &str,
    schema_str: &str,
) -> Result<u32> {
    let url = format!("{}/subjects/{}/versions?normalize=true", endpoint, subject);

    let res = http
        .post(&url)
        .basic_auth(username, Some(&password))
        .json(&serde_json::json!({
            "schema": schema_str,
            "schemaType": "AVRO"
        }))
        .send()
        .await?;

    if !res.status().is_success() {
        let status = res.status();
        let body = res.text().await?;
        anyhow::bail!(
            "request POST {} failed with status {}: {}",
            url,
            status,
            body
        );
    }

    let res: SchemaId = res.json().await?;
    Ok(res.id)
}

struct ComputedBinding {
    topic: String,
    key_schema: avro::Schema,
    schema: avro::Schema,
    key_ptr: Vec<Pointer>,
    field_names: Vec<String>,
}

fn binding_info(binding: &Binding) -> Result<ComputedBinding> {
    let field_selection = binding.field_selection.to_owned().unwrap();
    let mut fields: Vec<String> = field_selection
        .keys
        .iter()
        .chain(field_selection.values.clone().iter())
        .map(|f| f.to_owned())
        .collect();
    if !field_selection.document.is_empty() {
        fields.push(field_selection.document);
    }

    let collection = binding.collection.to_owned().unwrap();

    let collection_schema = if collection.read_schema_json.is_empty() {
        collection.write_schema_json
    } else {
        collection.read_schema_json
    };

    let json_schema = doc::validation::build_bundle(&collection_schema)?;
    let validator = doc::Validator::new(json_schema)?;
    let collection_shape = Shape::infer(&validator.schemas()[0], validator.schema_index());
    let collection_locations = collection_shape.locations();
    let collection_projections = collection.projections;

    // The fields in the schema must be sorted to match the ordering of
    // constructed values for Avro serialization to work.
    let mut fields_sorted = fields.clone();
    fields_sorted.sort();

    // Create a Shape based on the selected fields and their projections which
    // will be used to generate an appropriate schema.
    let mut shape = Shape::nothing();
    shape.type_ = schema::types::OBJECT;
    shape.object.properties = fields_sorted
        .iter()
        .map(|field| {
            let (shape, is_required) =
                field_to_shape(field, &collection_projections, &collection_locations);

            ObjProperty {
                name: field.clone().into(),
                is_required,
                shape,
            }
        })
        .collect();

    let key_ptr = field_selection
        .keys
        .iter()
        .map(|k| {
            // The selected key fields are treated as unescaped strings. For
            // example, a key field like `some/nested~1key` is taken directly as
            // a property named "some/nested~1key" with that pointer rather than
            // unescaping the ~1 into a nested pointer of ("some", "nested/key").
            let mut ptr = Pointer::empty();
            ptr.push(Token::from_str(k));
            ptr
        })
        .collect::<Vec<Pointer>>();

    let key_schema = avro::key_to_avro(&key_ptr, shape.clone());
    let schema = avro::shape_to_avro(shape);

    Ok(ComputedBinding {
        topic: binding.resource_path[0].to_string(),
        key_schema,
        schema,
        key_ptr,
        field_names: fields,
    })
}

fn field_to_shape(
    field: &str,
    collection_projections: &[Projection],
    collection_locations: &[(Pointer, bool, &Shape, doc::shape::location::Exists)],
) -> (Shape, bool) {
    let ptr = Pointer::from_str(field);

    if let Ok(idx) =
        collection_locations.binary_search_by_key(&ptr, |(loc_ptr, ..)| loc_ptr.clone())
    {
        // This is an actual field from the collection.
        let (_, _, shape, exists) = collection_locations[idx];
        return (
            shape.to_owned(),
            matches!(exists, doc::shape::location::Exists::Must),
        );
    }
    // This must be a synthetic projection like flow_published_at,
    // flow_document, etc. General cases of those are handled below, by
    // conjuring up an appropriate shape for single scalar type. More complex
    // types for synthetic projections aren't handled right now and hopefully
    // never will need to be.
    let projection = match collection_projections
        .binary_search_by_key(&field, |projection| projection.field.as_str())
    {
        Ok(idx) => &collection_projections[idx],
        Err(_) => panic!("projection for field {} not found", field),
    };

    let mut shape = Shape::nothing();
    let inference = projection.inference.to_owned().unwrap();

    if let Some(inf) = &inference.string {
        if !inf.content_encoding.is_empty() {
            shape.string.content_encoding = Some(inf.content_encoding.clone().into());
        }
        if !inf.content_type.is_empty() {
            shape.string.content_encoding = Some(inf.content_type.clone().into());
        }
        if !inf.format.is_empty() {
            shape.string.content_encoding = Some(inf.format.clone().into());
        }
        if inf.max_length > 0 {
            shape.string.max_length = Some(inf.max_length);
        }
    }

    if let Some(inf) = inference.numeric {
        if inf.has_maximum {
            shape.numeric.maximum = Some(inf.maximum.into());
        }
        if inf.has_minimum {
            shape.numeric.minimum = Some(inf.minimum.into());
        }
    }

    if !inference.title.is_empty() {
        shape.title = Some(inference.title.clone().into());
    }

    if !inference.description.is_empty() {
        shape.description = Some(inference.description.clone().into());
    }

    if !inference.default_json.is_empty() {
        let val: serde_json::Value =
            serde_json::from_str(&inference.default_json).expect("default must be valid JSON");
        shape.default = Some(Box::new((val, None)));
    }

    if projection.ptr.is_empty() {
        // Root document projection, which is always serialized as a string.
        shape.type_ = types::STRING;
        return (shape, true);
    }

    shape.type_ = inference.types.iter().fold(types::INVALID, |acc, ty| {
        acc | types::Set::for_type_name(&ty.to_lowercase()).unwrap()
    });

    if !shape.type_.is_single_scalar_type() {
        panic!("synthetic projections that are not a single scalar type are not supported");
    }

    (shape, inference.exists() == inference::Exists::Must)
}
