#![allow(dead_code)]

use std::marker::PhantomData;

use chrono::{DateTime, Utc};
use schemars::{schema_for, JsonSchema};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{json, Value};

#[derive(Debug)]
pub struct ConnectionSpecification<C> {
    config_type: PhantomData<C>,
}

impl<C> Default for ConnectionSpecification<C> {
    fn default() -> Self {
        Self {
            config_type: PhantomData,
        }
    }
}

impl<C: JsonSchema> Serialize for ConnectionSpecification<C> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        schema_for!(C).serialize(serializer)
    }
}

#[derive(Debug, Serialize)]
pub enum DestinationSyncMode {
    Append,
    AppendDedup,
    Overwrite,
}

#[derive(Serialize, Debug)]
pub struct Spec<C: JsonSchema> {
    #[serde(rename = "connectionSpecification")]
    connection_specification: ConnectionSpecification<C>,
    #[serde(rename = "documentationUrl")]
    documentation_url: String,
    #[serde(rename = "supportsIncremental")]
    supports_incremental: bool,
    supported_destination_sync_modes: Vec<DestinationSyncMode>,
}

impl<C: JsonSchema> Spec<C> {
    pub fn new(
        documentation_url: impl Into<String>,
        incremental: bool,
        sync_modes: Vec<DestinationSyncMode>,
    ) -> Self {
        Self {
            connection_specification: ConnectionSpecification::default(),
            supports_incremental: incremental,
            supported_destination_sync_modes: sync_modes,
            documentation_url: documentation_url.into(),
        }
    }
}

#[derive(Serialize, Debug)]
pub struct ConnectionStatus {
    status: String,
    message: String,
}

impl ConnectionStatus {
    pub fn new(status: String, message: String) -> Self {
        Self { status, message }
    }
}

#[derive(Deserialize, Debug)]
pub struct Stream {
    pub name: String,
}

impl Stream {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl Serialize for Stream {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Stream", 2)?;
        state.serialize_field("name", &self.name)?;
        // state.serialize_field("json_schema", &json!({"type": "object"}))?;
        state.serialize_field(
            "json_schema",
            &json!({
                "x-infer-schema": true,
                "type": "object",
                "properties": {
                    "_meta": {
                        "type": "object",
                        "properties": {
                            "partition": {
                                "description": "The partition the message was read from",
                                "type": "integer",
                            },
                            "offset": {
                                "description": "The offset of the message within the partition",
                                "type": "integer",
                            }
                        },
                        "required": ["partition", "offset"]
                    }
                },
                "required": ["_meta"]
            }),
        )?;
        state.serialize_field("supported_sync_modes", &vec!["incremental"])?;
        state.serialize_field(
            "source_defined_primary_key",
            &vec![vec!["_meta/partition"], vec!["_meta/offset"]],
        )?;
        state.end()
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Catalog {
    pub streams: Vec<Stream>,
}

impl Catalog {
    pub fn new(streams: Vec<Stream>) -> Self {
        Self { streams }
    }
}

#[derive(Serialize, Debug)]
pub struct Record {
    pub stream: String,
    pub data: RecordData,
    #[serde(with = "chrono::serde::ts_seconds")]
    pub emitted_at: DateTime<Utc>,
    pub namespace: String,
}

#[derive(Serialize, Debug)]
pub struct RecordData {
    data: Value,
    #[serde(rename = "_meta")]
    meta: RecordMeta,
}

#[derive(Serialize, Debug)]
struct RecordMeta {
    partition: i32,
    offset: i64,
}

impl Record {
    pub fn new(
        stream: String,
        payload: Value,
        partition: i32,
        offset: i64,
        emitted_at: DateTime<Utc>,
        namespace: String,
    ) -> Self {
        Self {
            stream,
            data: RecordData {
                data: payload,
                meta: RecordMeta { partition, offset },
            },
            emitted_at,
            namespace,
        }
    }
}

#[derive(Serialize, Debug)]
pub struct State {
    data: Value,
}

impl State {
    pub fn new(data: Value) -> Self {
        Self { data }
    }
}

///  Defines a few functions used to construct a valid `Envelope` payload.
pub trait Message: Serialize {
    fn type_id(&self) -> &'static str;

    fn data_key(&self) -> &'static str;
}

/// Quickly implements the `Message` trait on a type given the type_id and
/// data_key.
///
/// Example Expansion:
/// ```
/// impl_message!(ConnectionStatus, "CONNECTION_STATUS", "connectionStatus");
/// ```
/// expands to:
/// ```
/// impl Message for ConnectionStatus {
///     fn type_id(&self) -> &'static str {
///         "CONNECTION_STATUS"
///     }
///     fn data_key(&self) -> &'static str {
///         "connectionStatus"
///     }
/// }
/// ```
macro_rules! impl_message {
    ($name:tt, $type_id:literal, $data_key:literal) => {
        impl Message for $name {
            fn type_id(&self) -> &'static str {
                $type_id
            }
            fn data_key(&self) -> &'static str {
                $data_key
            }
        }
    };
}

impl<C: JsonSchema + Serialize> Message for Spec<C> {
    fn type_id(&self) -> &'static str {
        "SPEC"
    }

    fn data_key(&self) -> &'static str {
        "spec"
    }
}

impl_message!(ConnectionStatus, "CONNECTION_STATUS", "connectionStatus");
impl_message!(Catalog, "CATALOG", "catalog");
impl_message!(Record, "RECORD", "record");
impl_message!(State, "STATE", "state");

/// The `Envelope` wraps the Airbyte message with a type descriptor and unique
/// key. To conform with the Airbyte spec, all messages should be wrapped in an
/// `Envelope` before being serialized.
///
/// Use `impl_message!` on your `Message` type to specify a few values necessary
/// for serializing a valid `Envelope`.
#[derive(Debug)]
pub struct Envelope<T: Message> {
    pub message: T,
}

impl<T: Message> From<T> for Envelope<T> {
    fn from(t: T) -> Self {
        Envelope { message: t }
    }
}

impl<T: Message> Serialize for Envelope<T> {
    /// Wraps a `Message` with the proper fields.
    ///
    /// Example:
    /// ```
    /// serde_json::to_string(&Spec::new(true, vec![]).into())
    /// ```
    /// serializes to:
    /// ```json
    /// { "type": "SPEC", "spec": {...omitted...} }
    /// ```
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("Message", 2)?;
        s.serialize_field("type", self.message.type_id())?;
        s.serialize_field(self.message.data_key(), &self.message)?;
        s.end()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[derive(JsonSchema, Serialize)]
    struct SampleConfig {
        foo: String,
    }

    #[test]
    fn serialize_spec_test() {
        let spec: Envelope<Spec<SampleConfig>> =
            Spec::new("example.com", true, vec![DestinationSyncMode::Append]).into();

        let serialized = serde_json::to_string(&spec).expect("to serialize spec to json");
        // TODO: replace with serde_test assertions instead.
        assert_ne!("", serialized);
    }

    #[test]
    fn serialize_connection_status_test() {
        let status: Envelope<ConnectionStatus> =
            ConnectionStatus::new("SUCCEEDED".to_owned(), "Yay".to_owned()).into();

        let serialized = serde_json::to_string(&status).expect("to serialize connection to json");
        assert_ne!("", serialized);
    }

    #[test]
    fn serialize_catalog_test() {
        let stream = Stream::new("First Stream".to_owned());
        let catalog: Envelope<Catalog> = Catalog::new(vec![stream]).into();

        let serialized = serde_json::to_string(&catalog).expect("to serialize catalog to json");
        assert_ne!("", serialized);
    }

    #[test]
    fn serialize_record_test() {
        let record: Envelope<Record> = Record::new(
            "First Stream".to_owned(),
            json!({"some": "value"}),
            1,
            0,
            Utc::now(),
            "ns".to_owned(),
        )
        .into();

        let serialized = serde_json::to_string(&record).expect("to serialize record to json");
        assert_ne!("", serialized);
    }

    #[test]
    fn serialize_state_test() {
        let state: Envelope<State> = State::new(json!({"foo": "bar"})).into();

        let serialized = serde_json::to_string(&state).expect("to serialize connection to json");
        assert_ne!("", serialized);
    }
}
