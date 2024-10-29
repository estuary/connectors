use std::collections::HashMap;

use anyhow::Result;
use apache_avro::schema::Schema as AvroSchema;
use doc::{
    shape::{schema::to_schema, ObjProperty},
    Shape,
};
use json::schema as JsonSchema;
use proto_flow::capture::{request::Discover, response::discovered};
use rdkafka::consumer::Consumer;
use schemars::schema::RootSchema;
use serde_json::json;

use crate::{
    configuration::{EndpointConfig, Resource},
    schema_registry::{
        RegisteredSchema::Avro, RegisteredSchema::Json, RegisteredSchema::Protobuf,
        SchemaRegistryClient, TopicSchema,
    },
    KAFKA_TIMEOUT,
};

static KAFKA_INTERNAL_TOPICS: [&str; 3] = ["__consumer_offsets", "__amazon_msk_canary", "_schemas"];

pub async fn do_discover(req: Discover) -> Result<Vec<discovered::Binding>> {
    let config: EndpointConfig = serde_json::from_str(&req.config_json)?;
    let consumer = config.to_consumer().await?;

    let meta = consumer.fetch_metadata(None, KAFKA_TIMEOUT)?;

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
        Some(cfg) => {
            let client = SchemaRegistryClient::new(cfg.endpoint, cfg.username, cfg.password);
            client.schemas_for_topics(&all_topics).await?
        }
        None => HashMap::new(),
    };

    Ok(all_topics
        .into_iter()
        .filter_map(|topic| {
            let registered_schema = match registered_schemas.get(&topic) {
                Some(s) => s,
                None => &TopicSchema::default(),
            };

            match (&registered_schema.key, &registered_schema.value) {
                (None, None) => (),
                (Some(Avro(_)), Some(Avro(_))) => (),
                (None, Some(Avro(_))) => (),
                (Some(Avro(_)), None) => (),
                // TODO(whb): Support JSON and Protobuf.
                _ => return None,
            };

            let (collection_schema, key_ptrs) = topic_schema_to_collection_spec(registered_schema);

            Some(discovered::Binding {
                recommended_name: topic.to_owned(),
                resource_config_json: serde_json::to_string(&Resource {
                    topic: topic.to_owned(),
                })
                .expect("resource config must serialize"),
                document_schema_json: serde_json::to_string(&collection_schema)
                    .expect("document schema must serialize"),
                key: key_ptrs,
                resource_path: vec![topic.to_owned()],
                ..Default::default()
            })
        })
        .collect())
}

fn topic_schema_to_collection_spec(topic_schema: &TopicSchema) -> (RootSchema, Vec<String>) {
    let mut collection_key = vec!["/_meta/partition".to_string(), "/_meta/offset".to_string()];
    let doc_schema_json = json!({
        "x-infer-schema": true,
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
                    }
                },
                "required": ["offset", "partition"]
            }
        },
        "required": ["_meta"]
    });

    let mut collection_schema: RootSchema = serde_json::from_value(doc_schema_json).unwrap();

    if let Some(mut key_shape) = match &topic_schema.key {
        Some(Avro(schema)) => avro_key_schema_to_shape(schema),
        Some(Json(_)) => todo!(),
        Some(Protobuf) => todo!(),
        None => None,
    } {
        if !key_shape.type_.overlaps(JsonSchema::types::OBJECT) {
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
            .filter(|(_, _, shape, ..)| !shape.type_.overlaps(JsonSchema::types::OBJECT))
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

    (collection_schema, collection_key)
}

fn avro_key_schema_to_shape(schema: &AvroSchema) -> Option<Shape> {
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

                    Some(ObjProperty {
                        name: field.name.clone().into(),
                        is_required: field.default.is_none(),
                        shape: field_shape,
                    })
                })
                .collect::<Option<Vec<_>>>()?;
            JsonSchema::types::OBJECT
        }

        // Schemas that allow 'null' are not schematized as keys since
        // nullable keys are not very useful in practice.
        AvroSchema::Null => return None,

        // Similarly, schemas with multiple types or a single type with an
        // additional explicit 'null' are not very useful in practice,
        // although technically allowed by Flow, so they won't be
        // schematized.
        AvroSchema::Union(_) => return None,

        // We could perhaps treat floating points as string-encoded numbers,
        // but if that's what they were then they should probably specified
        // as decimals. And this is more consistent with what is achievable
        // with JSON schemas.
        AvroSchema::Float => return None,
        AvroSchema::Double => return None,

        // Arrays and maps just don't make any sense as key schemas.
        AvroSchema::Array(_) => return None,
        AvroSchema::Map(_) => return None,

        AvroSchema::Ref { name } => panic!("key schema contains ref {}", name),
    };

    Some(shape)
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
                                    "type": "record",
                                    "fields": [
                                        {"name": "secondKeyNested", "type": "long", "doc": "the second key field"},
                                        {"name": "thirdKeyNested", "type": "bytes", "doc": "the third key field"},
                                    ],
                                },
                            ],
                        }))
                        .unwrap(),
                    )),
                    ..Default::default()
                },
            ),
        ];

        for (name, input) in test_cases {
            let (discovered_schema, discovered_key) = topic_schema_to_collection_spec(input);
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
                match avro_key_schema_to_shape(&AvroSchema::parse(&schema_json).unwrap()) {
                    Some(shape) => {
                        assert_eq!(
                            serde_json::to_value(&to_schema(shape).schema).unwrap(),
                            serde_json::to_value(&want.clone().unwrap()).unwrap()
                        )
                    }
                    None => assert!(want.is_none()),
                }
            }
        }
    }
}
