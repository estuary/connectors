use super::elastic_search_data_types::{ESBasicType, ESFieldType, ESTypeOverride};
use super::errors::*;

use doc::inference::{ArrayShape, ObjShape, Shape};
use doc::Annotation;
use flow_json::schema::{self, index::IndexBuilder, types};
use serde_json::json;
use std::collections::HashMap;

pub const DEFAULT_IGNORE_ABOVE: u16 = 256;

pub fn build_elastic_schema(
    schema_uri: url::Url,
    schema_json: &[u8],
) -> Result<ESFieldType, Error> {
    build_elastic_schema_with_overrides(schema_uri, schema_json, Vec::new())
}

pub fn build_elastic_schema_with_overrides(
    schema_uri: url::Url,
    schema_json: &[u8],
    es_type_overrides: Vec<ESTypeOverride>,
) -> Result<ESFieldType, Error> {
    let schema = match serde_json::from_slice(schema_json) {
        Ok(v) => v,
        Err(e) => return Err(Error::SchemaJsonParsing(e)),
    };

    let schema = schema::build::build_schema::<Annotation>(schema_uri.clone(), &schema).unwrap();
    let mut index = IndexBuilder::new();
    index.add(&schema).unwrap();
    index.verify_references().unwrap();
    let index = index.into_index();

    let shape = Shape::infer(index.must_fetch(&schema_uri).unwrap(), &index);

    let mut built = build_from_shape(&shape)?;
    for es_override in es_type_overrides {
        built = built.apply_type_override(es_override)?;
    }

    if let ESFieldType::Basic(_) = built {
        // TODO(jixiang): check if array and basic types are allowed in the root of elastic mapping defs.
        return Err(Error::schema_error(
            UNSUPPORTED_NON_ARRAY_OR_OBJECTS,
            &shape,
        ));
    }

    Ok(built)
}

fn build_from_shape(shape: &Shape) -> Result<ESFieldType, Error> {
    let mut fields = Vec::new();

    if shape.type_.overlaps(types::OBJECT) {
        fields.push(build_from_object(&shape.object)?);
    }
    if shape.type_.overlaps(types::ARRAY) {
        fields.push(build_from_array(&shape.array)?);
    }
    if shape.type_.overlaps(types::BOOLEAN) {
        fields.push(ESFieldType::Basic(ESBasicType::Boolean));
    }
    if shape.type_.overlaps(types::FRACTIONAL) {
        fields.push(ESFieldType::Basic(ESBasicType::Double));
    } else if shape.type_.overlaps(types::INTEGER) {
        fields.push(ESFieldType::Basic(ESBasicType::Long));
    }
    if shape.type_.overlaps(types::STRING) {
        // TODO(jixiang): should dual_text be turned on by default?
        fields.push(ESFieldType::Basic(ESBasicType::Keyword {
            ignore_above: DEFAULT_IGNORE_ABOVE,
            dual_text: false,
        }));
    }

    if fields.is_empty() {
        Ok(ESFieldType::Basic(ESBasicType::Null))
    } else if fields.len() == 1 {
        Ok(fields.pop().unwrap())
    } else {
        return Err(Error::schema_error(
            UNSUPPORTED_MULTIPLE_OR_UNSPECIFIED_TYPES,
            &shape,
        ));
    }
}

fn build_from_object(shape: &ObjShape) -> Result<ESFieldType, Error> {
    if !shape.additional.is_none() {
        return Err(Error::schema_error(
            UNSUPPORTED_OBJECT_ADDITIONAL_FIELDS,
            shape,
        ));
    }

    let mut es_properties = HashMap::new();
    for prop in &shape.properties {
        match build_from_shape(&prop.shape) {
            Ok(v) => es_properties.insert(prop.name.clone(), v),
            Err(e) => return Err(e),
        };
    }

    return Ok(ESFieldType::Object {
        properties: es_properties,
    });
}

fn build_from_array(shape: &ArrayShape) -> Result<ESFieldType, Error> {
    if !shape.tuple.is_empty() {
        return Err(Error::schema_error(UNSUPPORTED_TUPLE, shape));
    }

    return match &shape.additional {
        None => Err(Error::schema_error(
            UNSUPPORTED_MULTIPLE_OR_UNSPECIFIED_TYPES,
            shape,
        )),
        // In Elastic search, the schema of an array is the same as the schema of its items.
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/array.html
        Some(shape) => build_from_shape(shape),
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_url() -> url::Url {
        url::Url::parse("http://test/dummy_schema").unwrap()
    }

    fn check_schema_error_error(actual_error: &Error, expected_error_message: &str) {
        assert!(matches!(actual_error, Error::UnSupportedError { .. }));
        if let Error::UnSupportedError(actual_error_message) = actual_error {
            assert!(actual_error_message.contains(expected_error_message))
        }
    }

    fn run_and_check_override_error(
        pointer: &str,
        schema_json: &[u8],
        expected_error_message: &str,
    ) {
        let actual_error = build_elastic_schema_with_overrides(
            test_url(),
            schema_json,
            vec![ESTypeOverride {
                pointer: pointer.to_string(),
                es_type: ESBasicType::Boolean,
            }],
        )
        .unwrap_err();

        assert!(matches!(actual_error, Error::OverridePointerError { .. }));
        if let Error::OverridePointerError(actual_error_message) = actual_error {
            assert!(actual_error_message.contains(expected_error_message));
        }
    }

    #[test]
    fn test_build_elastic_search_schema_with_error() {
        assert!(matches!(
            build_elastic_schema(test_url(), b"A bad json schema").unwrap_err(),
            Error::SchemaJsonParsing { .. }
        ));

        let empty_schema_json = b" { } ";
        check_schema_error_error(
            &build_elastic_schema(test_url(), empty_schema_json).unwrap_err(),
            UNSUPPORTED_MULTIPLE_OR_UNSPECIFIED_TYPES,
        );

        let multiple_types_schema_json = br#"{"type": ["integer", "string"]}"#;
        check_schema_error_error(
            &build_elastic_schema(test_url(), multiple_types_schema_json).unwrap_err(),
            UNSUPPORTED_MULTIPLE_OR_UNSPECIFIED_TYPES,
        );

        let int_schema_json = br#"{"type": "integer"}"#;
        check_schema_error_error(
            &build_elastic_schema(test_url(), int_schema_json).unwrap_err(),
            UNSUPPORTED_NON_ARRAY_OR_OBJECTS,
        );

        let multiple_field_types_schema_json = br#" { "type": "object", "properties": { "mul_type": {"type": ["boolean", "integer"] } } }"#;
        check_schema_error_error(
            &build_elastic_schema(test_url(), multiple_field_types_schema_json).unwrap_err(),
            UNSUPPORTED_MULTIPLE_OR_UNSPECIFIED_TYPES,
        );

        let object_additional_field_schema_json = br#"
          {"type": "object", "additionalProperties": {"type": "integer"}, "properties": {"int": {"type": "integer"}}}
        "#;
        check_schema_error_error(
            &build_elastic_schema(test_url(), object_additional_field_schema_json).unwrap_err(),
            UNSUPPORTED_OBJECT_ADDITIONAL_FIELDS,
        );

        let tuple_field_schema_json =
            br#"{"type": "array", "items": [{"type": "string"}, {"type": "integer"}]}"#;
        check_schema_error_error(
            &build_elastic_schema(test_url(), tuple_field_schema_json).unwrap_err(),
            UNSUPPORTED_TUPLE,
        );

        let simple_array_schema_json = br#"{"type": "array", "items": {"type": "string"}}"#;
        check_schema_error_error(
            &build_elastic_schema(test_url(), simple_array_schema_json).unwrap_err(),
            UNSUPPORTED_NON_ARRAY_OR_OBJECTS,
        );
    }

    #[test]
    fn test_build_elastic_search_schema_all_types() {
        let schema_json = br#"
        {
            "properties":{
                "str": {"type": "string"},
                "str_or_null": {"type": ["string", "null"] },
                "int": {"type": "integer"},
                "int_or_null": {"type": ["integer", "null"] },
                "num": {"type": "number"},
                "num_or_null": {"type": ["number", "null"] },
                "bool": {"type": "boolean"},
                "bool_or_null": {"type": ["boolean", "null"]},
                "enum": {"enum": [1,2,3]},
                "array_of_ints": {"type": "array", "items": {"type": "integer"}},
                "array_of_objs": {"type": "array", "items": {"type": "object", "properties": {"arr_field": {"type": "string"}}}},
                "nested": {"type": "object", "required": [], "properties": {"nested_field": {"type": ["null", "integer"]}}}

            },
            "required":["str"],
            "type":"object"
        }
        "#;

        let actual = build_elastic_schema(test_url(), schema_json)
            .unwrap()
            .render();
        assert_eq!(
            actual,
            json!({"properties": {
                    "array_of_ints": {"type": "long"},
                    "array_of_objs": {"properties": {"arr_field":{"type": "keyword", "ignore_above": 256}}},
                    "bool":{"type": "boolean"},
                    "bool_or_null": {"type": "boolean"},
                    "enum": {"type": "long"},
                    "int": {"type": "long"},
                    "int_or_null": {"type": "long"},
                    "nested":{"properties": {"nested_field": {"type": "long"}}},
                    "num": {"type": "double"},
                    "num_or_null": {"type": "double"},
                    "str": {"type": "keyword", "ignore_above": 256},
                    "str_or_null": {"type": "keyword", "ignore_above": 256}
                }
            })
        );
    }

    #[test]
    fn test_build_elastic_search_schema_with_reference() {
        let schema_json = br#"{
            "$defs": {
                "__flowInline1":{
                    "$defs":{
                        "anAnchor": {
                            "$anchor": "AnAnchor",
                            "properties": {
                                "one":{"type": "string"},
                                "two":{"type": "integer"}
                            },
                            "required":["one"],
                            "type":"object"
                        }
                    },
                    "$id": "test://example/int-string.schema",
                    "properties": {
                        "bit": { "type": "boolean" },
                        "int": { "type": "integer" },
                        "str": { "type": "string" }
                    },
                    "required": ["int", "str", "bit"], "type": "object"
                }
            },
            "$id": "test://example/int-string-len.schema",
            "$ref": "test://example/int-string.schema",
            "properties": {
                "arr":{
                    "items":{"$ref": "int-string.schema#AnAnchor"},
                    "type":"array"
                },
                "len":{"type": "integer"}
            },
            "required":["len"]
        }"#;

        let actual = build_elastic_schema(
            url::Url::parse("test://example/int-string-len.schema").unwrap(),
            schema_json,
        )
        .unwrap()
        .render();

        assert_eq!(
            actual,
            json!({"properties": {
                    "arr":{
                        "properties": {
                          "one": {"type": "keyword", "ignore_above": 256},
                          "two": {"type": "long"}
                        }
                    },
                    "bit": { "type": "boolean" },
                    "int": { "type": "long" },
                    "len": { "type": "long" },
                    "str": { "type": "keyword", "ignore_above": 256}
                }
            })
        );
    }

    #[test]
    fn test_build_elastic_search_schema_with_override() {
        let schema_json = br#"
        {
            "properties":{
                "str": {"type": "string"},
                "enum": {"enum": [1,2,3]},
                "array_of_ints": {"type": "array", "items": {"type": "integer"}},
                "array_of_objs": {"type": "array", "items": {"type": "object", "properties": {"arr_field": {"type": "string"}}}},
                "nested": {"type": "object", "required": [], "properties": {"nested_field": {"type": ["null", "integer"]}}}
            },
            "required":["str"],
            "type":"object"
        }
        "#;

        run_and_check_override_error("", schema_json, POINTER_EMPTY);
        run_and_check_override_error("/missing_field", schema_json, POINTER_MISSING_FIELD);
        run_and_check_override_error(
            "/nested/nested_field/aa",
            schema_json,
            POINTER_WRONG_FIELD_TYPE,
        );

        let actual = build_elastic_schema_with_overrides(
            test_url(),
            schema_json,
            vec![
                ESTypeOverride {
                    pointer: "/str".to_string(),
                    es_type: ESBasicType::Date {
                        format: "testing_date_format".to_string(),
                    },
                },
                ESTypeOverride {
                    pointer: "enum".to_string(),
                    es_type: ESBasicType::Boolean,
                },
                ESTypeOverride {
                    pointer: "array_of_ints".to_string(),
                    es_type: ESBasicType::Boolean,
                },
                ESTypeOverride {
                    pointer: "/array_of_objs/arr_field".to_string(),
                    es_type: ESBasicType::Boolean,
                },
                ESTypeOverride {
                    pointer: "/nested/nested_field".to_string(),
                    es_type: ESBasicType::Keyword {
                        ignore_above: 300,
                        dual_text: true,
                    },
                },
            ],
        )
        .unwrap()
        .render();
        assert_eq!(
            actual,
            json!({"properties": {
                    "array_of_ints": { "type": "boolean" },
                    "array_of_objs": { "properties": {"arr_field": {"type": "boolean"}}},
                    "enum": {"type": "boolean"},
                    "nested":{
                         "properties":{
                             "nested_field": {
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                        "ignore_above": 300
                                    }
                                }
                            }
                        }
                    },
                    "str": {"type": "date", "format": "testing_date_format"},
                }
            })
        );
    }
}
