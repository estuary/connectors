use std::collections::HashMap;

use anyhow::{Context, Result};
use apache_avro::schema::Schema as AvroSchema;
use doc::{
    shape::{schema::to_schema, ObjProperty},
    Shape,
};
use json::schema::{self as JsonSchema, types};
use proto_flow::capture::{request::Discover, response::discovered};
use rdkafka::consumer::Consumer;
use schemars::schema::RootSchema;
use serde_json::json;

use crate::{
    configuration::{EndpointConfig, Resource, SchemaRegistryConfig},
    schema_registry::{
        RegisteredSchema::{Avro, Json, Protobuf},
        SchemaRegistryClient, TopicSchema,
    },
    KAFKA_METADATA_TIMEOUT,
};

static KAFKA_INTERNAL_TOPICS: [&str; 3] = ["__consumer_offsets", "__amazon_msk_canary", "_schemas"];

pub async fn do_discover(req: Discover) -> Result<Vec<discovered::Binding>> {
    let config: EndpointConfig = serde_json::from_slice(&req.config_json)?;
    let consumer = config.to_consumer().await?;

    let meta = consumer
        .fetch_metadata(None, KAFKA_METADATA_TIMEOUT)
        .context("Could not connect to bootstrap server with the provided configuration. This may be due to an incorrect configuration for authentication or bootstrap servers. Double check your configuration and try again.")?;

    let mut all_topics: Vec<String> = meta
        .topics()
        .iter()
        .filter_map(|t| {
            let name = t.name();
            if KAFKA_INTERNAL_TOPICS.contains(&name) {
                None
            } else {
                Some(name.to_string())
            }
        })
        .collect();

    all_topics.sort();

    let registered_schemas = match config.schema_registry {
        SchemaRegistryConfig::ConfluentSchemaRegistry {
            endpoint,
            username,
            password,
        } => {
            let client = SchemaRegistryClient::new(endpoint, username, password);
            client
                .schemas_for_topics(&all_topics)
                .await
                .context("Could not connect to the configured schema registry. Double check your configuration and try again.")?
        }
        SchemaRegistryConfig::NoSchemaRegistry { .. } => HashMap::new(),
    };

    all_topics
        .into_iter()
        .filter_map(|topic| {
            let registered_schema = match registered_schemas.get(&topic) {
                Some(s) => s,
                None => &TopicSchema::default(),
            };

            if matches!(&registered_schema.key, Some(Protobuf))
                || matches!(&registered_schema.value, Some(Protobuf))
            {
                // TODO(whb): At some point we may want to support protobuf
                // schemas.
                return None;
            }

            let (collection_schema, key_ptrs) =
                match topic_schema_to_collection_spec(registered_schema) {
                    Ok(s) => s,
                    Err(e) => return Some(Err(e)),
                };

            Some(Ok(discovered::Binding {
                recommended_name: topic.to_owned(),
                resource_config_json: serde_json::to_string(&Resource {
                    topic: topic.to_owned(),
                })
                .expect("resource config must serialize").into(),
                document_schema_json: serde_json::to_string(&collection_schema)
                    .expect("document schema must serialize").into(),
                key: key_ptrs,
                resource_path: vec![topic.to_owned()],
                ..Default::default()
            }))
        })
        .collect::<Result<Vec<_>>>()
}

fn topic_schema_to_collection_spec(
    topic_schema: &TopicSchema,
) -> Result<(RootSchema, Vec<String>)> {
    let mut collection_key = vec!["/_meta/partition".to_string(), "/_meta/offset".to_string()];
    let doc_schema_json = json!({
        "x-infer-schema": true,
        "if": {
            "properties": {
              "_meta": {
                "properties": {
                  "op": {
                    "const": "d"
                  }
                }
              }
            }
          },
        "then": {
            "reduce": {
                "delete": true,
                "strategy": "merge"
            }
        },
        "type": "object",
        "properties": {
            "_meta": {
                "type": "object",
                "properties": {
                    "topic": {
                        "description": "The topic the message was read from",
                        "type": "string",
                    },
                    "partition": {
                        "description": "The partition the message was read from",
                        "type": "integer",
                    },
                    "offset": {
                        "description": "The offset of the message within the partition",
                        "type": "integer",
                    },
                    "op": {
                        "enum": ["c", "u", "d"],
                        "description": "Change operation type: 'c' Create/Insert, 'u' Update, 'd' Delete.",
                    }
                },
                "required": ["offset", "op", "partition", "topic"]
            }
        },
        "required": ["_meta"]
    });

    let mut collection_schema: RootSchema = serde_json::from_value(doc_schema_json).unwrap();

    let mut key_shape = match &topic_schema.key {
        Some(Avro(schema)) => avro_key_schema_to_shape(schema)?,
        Some(Json(schema)) => json_key_schema_to_shape(schema)?,
        Some(Protobuf) => todo!("protobuf schemas are not yet supported"),
        None => Shape::nothing(),
    };

    if usable_key_shape(&key_shape) {
        if key_shape.type_ != JsonSchema::types::OBJECT {
            // The topic key is a single value not part of a record, and
            // that cannot be represented as a JSON document. There has
            // to be a key/value pair in the document, so transmute this
            // unnamed scalar value into a document with a synthetic key
            // to reference its value.
            let scalar_shape = key_shape.clone();
            key_shape = Shape::nothing();
            key_shape.type_ = JsonSchema::types::OBJECT;
            key_shape.object.properties = vec![ObjProperty {
                name: "_key".into(),
                is_required: true,
                shape: scalar_shape,
            }];
        }

        collection_key = key_shape
            .locations()
            .into_iter()
            .filter(|(ptr, _, shape, ..)| {
                !shape.type_.overlaps(JsonSchema::types::OBJECT) && ptr.to_string() != "/*"
            })
            .map(|(ptr, ..)| ptr.to_string())
            .collect();

        let mut key_schema = to_schema(key_shape);

        collection_schema
            .schema
            .object()
            .properties
            .append(&mut key_schema.schema.object().properties);

        collection_schema
            .schema
            .object()
            .required
            .append(&mut key_schema.schema.object().required);
    }

    Ok((collection_schema, collection_key))
}

fn avro_key_schema_to_shape(schema: &AvroSchema) -> Result<Shape> {
    let mut shape = Shape::nothing();

    shape.type_ = match schema {
        AvroSchema::Boolean => JsonSchema::types::BOOLEAN,
        AvroSchema::Int | AvroSchema::Long => JsonSchema::types::INTEGER,
        AvroSchema::String => JsonSchema::types::STRING,
        AvroSchema::Bytes | AvroSchema::Fixed(_) => {
            shape.string.content_encoding = Some("base64".into());
            JsonSchema::types::STRING
        }
        AvroSchema::Decimal(_) | AvroSchema::BigDecimal => {
            shape.string.format = Some(JsonSchema::formats::Format::Number);
            JsonSchema::types::STRING
        }
        AvroSchema::Uuid => {
            shape.string.format = Some(JsonSchema::formats::Format::Uuid);
            JsonSchema::types::STRING
        }
        AvroSchema::Date => {
            shape.string.format = Some(JsonSchema::formats::Format::Date);
            JsonSchema::types::STRING
        }
        AvroSchema::TimeMillis | AvroSchema::TimeMicros => {
            shape.string.format = Some(JsonSchema::formats::Format::Time);
            JsonSchema::types::STRING
        }
        AvroSchema::TimestampMillis
        | AvroSchema::TimestampMicros
        | AvroSchema::TimestampNanos
        | AvroSchema::LocalTimestampMillis
        | AvroSchema::LocalTimestampMicros
        | AvroSchema::LocalTimestampNanos => {
            shape.string.format = Some(JsonSchema::formats::Format::DateTime);
            JsonSchema::types::STRING
        }
        AvroSchema::Duration => {
            shape.string.format = Some(JsonSchema::formats::Format::Duration);
            JsonSchema::types::STRING
        }
        AvroSchema::Enum(enum_schema) => {
            shape.enum_ = Some(
                enum_schema
                    .symbols
                    .iter()
                    .map(|s| s.to_string().into())
                    .collect(),
            );
            JsonSchema::types::STRING
        }
        AvroSchema::Record(record_schema) => {
            shape.object.properties = record_schema
                .fields
                .iter()
                .map(|field| {
                    let mut field_shape = avro_key_schema_to_shape(&field.schema)?;

                    if let Some(doc) = &field.doc {
                        field_shape.description = Some(doc.to_string().into());
                    }
                    if let Some(default) = &field.default {
                        field_shape.default = Some((default.to_owned(), None).into());
                    }

                    Ok(ObjProperty {
                        name: field.name.clone().into(),
                        is_required: field.default.is_none(),
                        shape: field_shape,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            JsonSchema::types::OBJECT
        }

        // Schemas that allow 'null' are not schematized as keys since
        // nullable keys are not very useful in practice.
        AvroSchema::Null => JsonSchema::types::INVALID,

        // Similarly, schemas with multiple types or a single type with an
        // additional explicit 'null' are not very useful in practice,
        // although technically allowed by Flow, so they won't be
        // schematized.
        AvroSchema::Union(_) => JsonSchema::types::INVALID,

        // We could perhaps treat floating points as string-encoded numbers,
        // but if that's what they were then they should probably specified
        // as decimals. And this is more consistent with what is achievable
        // with JSON schemas.
        AvroSchema::Float => JsonSchema::types::INVALID,
        AvroSchema::Double => JsonSchema::types::INVALID,

        // Arrays and maps just don't make any sense as key schemas.
        AvroSchema::Array(_) => JsonSchema::types::INVALID,
        AvroSchema::Map(_) => JsonSchema::types::INVALID,

        AvroSchema::Ref { name } => anyhow::bail!("Avro key schema contains reference {}", name),
    };

    Ok(shape)
}

fn json_key_schema_to_shape(schema: &serde_json::Value) -> Result<Shape> {
    let json_schema = doc::validation::build_bundle(schema.to_string().as_bytes())?;
    let validator = doc::Validator::new(json_schema)?;
    Ok(doc::Shape::infer(
        &validator.schemas()[0],
        validator.schema_index(),
    ))
}

fn usable_key_shape(shape: &Shape) -> bool {
    // Schemas may be valid keys if all the properties are keyable and
    // non-nullable, including nested properties. Non-nullable here means they
    // can't be an explicit null, and either must exist or have a default value.
    shape
        .locations()
        .iter()
        .all(|(ptr, pattern, shape, exists)| {
            if ptr.to_string() == "/*" && shape.type_ == types::INVALID {
                // This represents an "additionalProperties: false"
                // configuration, which must be the case for a valid key schema.
                return true;
            }

            !pattern
                && (exists.must() || shape.default.is_some())
                && !shape.type_.overlaps(JsonSchema::types::NULL)
                && (shape.type_.is_keyable_type() || shape.type_ == types::OBJECT)
        })
}

#[cfg(test)]
mod tests {
    use insta::assert_snapshot;

    use super::*;

    #[test]
    fn test_topic_schema_to_collection_spec() {
        let test_cases = [
            (
                "no key",
                &TopicSchema {
                    key: None,
                    ..Default::default()
                },
            ),
            (
                "single scalar avro key",
                &TopicSchema {
                    key: Some(Avro(
                        apache_avro::Schema::parse(&json!({"type": "string"})).unwrap(),
                    )),
                    ..Default::default()
                },
            ),
            (
                "single nullable scalar avro key",
                &TopicSchema {
                    key: Some(Avro(
                        apache_avro::Schema::parse(&json!({"type": ["null", "string"]})).unwrap(),
                    )),
                    ..Default::default()
                },
            ),
            (
                "single non-scalar avro key",
                &TopicSchema {
                    key: Some(Avro(
                        apache_avro::Schema::parse(&json!({"type": "array", "items": "string"}))
                            .unwrap(),
                    )),
                    ..Default::default()
                },
            ),
            (
                "avro record with scalar compound key",
                &TopicSchema {
                    key: Some(Avro(
                        apache_avro::Schema::parse(&json!({
                            "type": "record",
                            "name": "someRecord",
                            "fields": [
                                {"name": "firstKey", "type": "string", "doc": "the first key field"},
                                {"name": "secondKey", "type": "long", "doc": "the second key field"},
                                {
                                    "name": "thirdKey",
                                    "type": "enum",
                                    "symbols": ["a", "b", "c"],
                                    "default": "a",
                                    "doc": "the third key field, which is an enum with a default value",
                                },
                            ],
                        }))
                        .unwrap(),
                    )),
                    ..Default::default()
                },
            ),
            (
                "nested avro record with scalars",
                &TopicSchema {
                    key: Some(Avro(
                        apache_avro::Schema::parse(&json!({
                            "type": "record",
                            "name": "someRecord",
                            "fields": [
                                {"name": "firstKey", "type": "string", "doc": "the first key field"},
                                {
                                    "name": "nestedRecord",
                                    "type": {
                                        "type": "record",
                                        "name": "NestedRecord",
                                        "fields": [
                                            {"name": "secondKeyNested", "type": "long", "doc": "the second key field"},
                                            {"name": "thirdKeyNested", "type": "bytes", "doc": "the third key field"},
                                        ]
                                    }
                                },
                            ],
                        }))
                        .unwrap(),
                    )),
                    ..Default::default()
                },
            ),
            (
                "single scalar json key",
                &TopicSchema {
                    key: Some(Json(json!({"type": "string"}))),
                    ..Default::default()
                },
            ),
            (
                "single nullable scalar json key",
                &TopicSchema {
                    key: Some(Json(json!({"type": ["null", "string"]}))),
                    ..Default::default()
                },
            ),
            (
                "nested json object",
                &TopicSchema {
                    key: Some(Json(json!({
                        "type": "object",
                        "properties": {
                            "firstKey": {"type": "string"},
                            "nestedObject": {
                                "type": "object",
                                "properties": {
                                    "secondKeyNested": {"type": "integer"},
                                    "thirdKeyNested": {"type": "boolean"},
                                },
                                "required": ["secondKeyNested", "thirdKeyNested"],
                            },
                        },
                        "required": ["firstKey", "nestedObject"],
                        "additionalProperties": false
                    }))),
                    ..Default::default()
                },
            ),
        ];

        for (name, input) in test_cases {
            let (discovered_schema, discovered_key) =
                topic_schema_to_collection_spec(input).unwrap();
            let mut snap = String::new();
            snap.push_str(&serde_json::to_string(&discovered_key).unwrap());
            snap.push_str("\n");
            snap.push_str(&serde_json::to_string_pretty(&discovered_schema).unwrap());
            assert_snapshot!(name, snap);
        }
    }

    #[test]
    fn test_avro_key_schema_to_shape() {
        let test_cases = [
            (
                vec![json!({"type": "boolean"})],
                Some(json!({"type": "boolean"})),
            ),
            (
                vec![json!({"type": "int"}), json!({"type": "long"})],
                Some(json!({"type": "integer"})),
            ),
            (
                vec![json!({"type": "string"})],
                Some(json!({"type": "string"})),
            ),
            (
                vec![
                    json!({"type": "bytes"}),
                    json!({"type": "fixed", "name": "foo", "size": 10}),
                ],
                Some(json!({"type": "string", "contentEncoding": "base64"})),
            ),
            (
                vec![
                    json!({"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}),
                    json!({"type": "bytes", "logicalType": "big-decimal", "precision": 4, "scale": 2}),
                ],
                Some(json!({"type": "string", "format": "number"})),
            ),
            (
                vec![json!({"type": "string", "logicalType": "uuid"})],
                Some(json!({"type": "string", "format": "uuid"})),
            ),
            (
                vec![json!({"type": "int", "logicalType": "date"})],
                Some(json!({"type": "string", "format": "date"})),
            ),
            (
                vec![
                    json!({"type": "int", "logicalType": "time-millis"}),
                    json!({"type": "long", "logicalType": "time-micros"}),
                ],
                Some(json!({"type": "string", "format": "time"})),
            ),
            (
                vec![
                    json!({"type": "long", "logicalType": "timestamp-millis"}),
                    json!({"type": "long", "logicalType": "timestamp-micros"}),
                    json!({"type": "long", "logicalType": "local-timestamp-millis"}),
                    json!({"type": "long", "logicalType": "local-timestamp-micros"}),
                    // TODO(whb): These nanosecond timestamps are not parsed by
                    // the Avro library and I'm not sure if they are really part
                    // of the spec.
                    // json!({"type": "long", "logicalType": "timestamp-nanos"})),
                    // json!({"type": "long", "logicalType": "local-timestamp-nanos"})),
                ],
                Some(json!({"type": "string", "format": "date-time"})),
            ),
            (
                vec![
                    json!({"type": "fixed", "name": "foo", "size": 12, "logicalType": "duration"}),
                ],
                Some(json!({"type": "string", "format": "duration"})),
            ),
            (
                vec![json!({"type": "enum", "name": "foo", "symbols": ["a", "b", "c"]})],
                Some(json!({"type": "string", "enum": ["a", "b", "c"]})),
            ),
            (
                vec![
                    json!({"type": "null"}),
                    json!({"type": ["null", "long"]}),
                    json!({"type": "float"}),
                    json!({"type": "double"}),
                    json!({"type": "array", "items": "string"}),
                    json!({"type": "map", "values": "string"}),
                ],
                None,
            ),
        ];

        for (schema_jsons, want) in test_cases {
            for schema_json in schema_jsons {
                let shape =
                    avro_key_schema_to_shape(&AvroSchema::parse(&schema_json).unwrap()).unwrap();
                if usable_key_shape(&shape) {
                    assert_eq!(
                        serde_json::to_value(&to_schema(shape).schema).unwrap(),
                        serde_json::to_value(&want.clone().unwrap()).unwrap()
                    )
                } else {
                    assert!(want.is_none())
                }
            }
        }
    }
}
