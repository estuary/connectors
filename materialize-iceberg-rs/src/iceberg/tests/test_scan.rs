use super::*;
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::datatypes::Schema;
use proto_flow::flow::Inference;
use proto_flow::flow::Projection;

use crate::configuration::EndpointConfig;
use crate::transactor;

/// Builds the Arrow schema for `collection`'s key and value fields (plus a
/// trailing `flow_document` column), read from a Flow catalog source file's
/// `collections` section, the same way `binding_state()` in
/// `transactor.rs` builds one from a real materialization binding — just
/// fed by this file's JSON-Schema `properties`/`key` directly, rather than
/// by a built `CollectionSpec`/`FieldSelection`. Value fields are every
/// other schema property besides the key(s) and `flow_document`, in
/// ascending sorted order, matching how `FieldSelection.values` is
/// documented to be ordered.
fn load_schema(flow_yaml_path: &str, collection: &str) -> (Schema, usize) {
    let yaml = std::fs::read_to_string(flow_yaml_path).unwrap();
    let doc: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();

    let collection_spec = &doc["collections"][collection];
    let properties = collection_spec["schema"]["properties"]
        .as_mapping()
        .unwrap();

    let key_fields: Vec<String> = collection_spec["key"]
        .as_sequence()
        .unwrap()
        .iter()
        .map(|ptr| ptr.as_str().unwrap().trim_start_matches('/').to_string())
        .collect();

    let mut value_fields: Vec<String> = properties
        .keys()
        .filter_map(|k| k.as_str())
        .map(str::to_string)
        .filter(|name| name != "flow_document" && !key_fields.contains(name))
        .collect();
    value_fields.sort();

    let to_projection = |field: &str| -> Projection {
        let json_type = properties[field]["type"].as_str().unwrap().to_string();
        Projection {
            field: field.to_string(),
            ptr: format!("/{field}"),
            inference: Some(Inference {
                types: vec![json_type],
                ..Default::default()
            }),
            ..Default::default()
        }
    };
    let projections: Vec<Projection> = key_fields
        .iter()
        .chain(value_fields.iter())
        .map(|field| to_projection(field))
        .collect();

    let mut fields = Vec::new();
    let mut field_id = 1;
    for field in key_fields.iter().chain(value_fields.iter()) {
        let data_type = transactor::arrow_type_for_field(&projections, field);
        fields.push(transactor::arrow_field_with_iceberg_id(
            field, data_type, field_id,
        ));
        field_id += 1;
    }
    fields.push(transactor::arrow_field_with_iceberg_id(
        "flow_document",
        DataType::Utf8,
        field_id,
    ));

    (Schema::new(fields), key_fields.len())
}

// #[tokio::test]
// async fn test_load() {
//     tracing_subscriber::fmt()
//         .with_test_writer()
//         .with_level(true)
//         .init();
//     let yaml = std::fs::read_to_string("../iceberg-config.yaml").unwrap();
//     let config: EndpointConfig = serde_yaml::from_str(&yaml).unwrap();
//     let (object_store, prefix, bucket) = transactor::build_object_store(&config.store).unwrap();

//     let catalog = Arc::new(Catalog {
//         object_store: object_store.clone(),
//         prefix: prefix.clone(),
//         bucket: bucket.clone(),
//     });

//     let (schema, num_keys) = load_schema("src/iceberg/tests/collections.yaml", "test/collection");

//     let requested_keys: Vec<Vec<Element>> = ["1", "4"]
//         .into_iter()
//         .map(|key| vec![Element::String(key.into())])
//         .collect();

//     let docs = load(&catalog, "lima_webhook-data", &schema, num_keys, &requested_keys).await.unwrap();

//     for doc in docs {
//         let value: serde_json::Value = serde_json::from_slice(&doc.doc_json).unwrap();
//         println!("{}", serde_json::to_string_pretty(&value).unwrap());
//     }
// }
