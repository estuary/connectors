use std::fmt;

pub const UNSUPPORTED_MULTIPLE_OR_UNSPECIFIED_TYPES: &str =
    "multiple non-trivial data types or unspecified data types are not supported";
pub const UNSUPPORTED_NON_ARRAY_OR_OBJECTS: &str =
    "data types other than objects or arrays of objects are not supported";
pub const UNSUPPORTED_OBJECT_ADDITIONAL_FIELDS: &str =
    "additional properties on an object are not supported";
pub const UNSUPPORTED_TUPLE: &str = "Tuple is not supported";

pub const POINTER_EMPTY: &str = "empty path";
pub const POINTER_MISSING_FIELD: &str = "pointer of a non-existing field";
pub const POINTER_WRONG_FIELD_TYPE: &str =
    "non-leaf field on json path is a basic type (int, bool..)";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("bad schema json")]
    SchemaJsonParsing(#[from] serde_json::Error),
    #[error("unsupported Flow schema in elastic search, details: {0}")]
    UnSupportedError(String),
    #[error("unable to override elastic search schema, details: {0}")]
    OverridePointerError(String),
}

impl Error {
    pub fn schema_error(error_message: &str, debug_comp: &dyn fmt::Debug) -> Self {
        Error::UnSupportedError(format!(
            "{}. affected component {:?}",
            error_message, debug_comp
        ))
    }

    pub fn override_error(error_message: &str, pointer: &String) -> Self {
        Error::OverridePointerError(format!("{}. Pointer:{}", error_message, pointer))
    }
}
