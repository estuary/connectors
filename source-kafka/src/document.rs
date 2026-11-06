//! Streaming serialization of captured documents.
//!
//! [`MergeSerializer`] wraps another [`Serializer`]. When the value being
//! serialized is a JSON object, it forwards the object's fields to the inner
//! serializer, then appends the Kafka message key's fields and a trailing
//! `_meta` object.
//!
//! This lets `pull.rs` serialize a protobuf `DynamicMessage` straight to output
//! bytes without first materializing it as a `serde_json::Value`, skipping the
//! intermediate DOM and the per-field allocations it requires.

use std::marker::PhantomData;

use serde::ser::{Error as _, Impossible, Serialize, SerializeMap, Serializer};
use serde_json::{Map, Value};

const EXPECT_OBJECT: &str = "captured document must serialize as a JSON object";
const KEY_NOT_STRING: &str = "captured document field name must be a string";

/// Generates `Serializer` methods that reject the value. Our top-level document
/// must be a JSON object, so any other kind is an error.
macro_rules! reject_scalars {
    ($msg:expr; $($method:ident($ty:ty)),* $(,)?) => {
        $(
            fn $method(self, _v: $ty) -> Result<Self::Ok, Self::Error> {
                Err(Self::Error::custom($msg))
            }
        )*
    };
}

/// A [`Serializer`] that emits a JSON object equal to the serialized payload
/// with `key` fields merged over it and a trailing `_meta` field appended.
pub struct MergeSerializer<'a, S, M> {
    inner: S,
    key: Option<&'a Map<String, Value>>,
    meta: &'a M,
}

impl<'a, S, M> MergeSerializer<'a, S, M> {
    pub fn new(inner: S, key: Option<&'a Map<String, Value>>, meta: &'a M) -> Self {
        Self { inner, key, meta }
    }
}

impl<'a, S, M> Serializer for MergeSerializer<'a, S, M>
where
    S: Serializer,
    M: Serialize,
{
    type Ok = S::Ok;
    type Error = S::Error;
    type SerializeSeq = Impossible<S::Ok, S::Error>;
    type SerializeTuple = Impossible<S::Ok, S::Error>;
    type SerializeTupleStruct = Impossible<S::Ok, S::Error>;
    type SerializeTupleVariant = Impossible<S::Ok, S::Error>;
    type SerializeMap = MergeMap<'a, S::SerializeMap, M>;
    type SerializeStruct = Impossible<S::Ok, S::Error>;
    type SerializeStructVariant = Impossible<S::Ok, S::Error>;

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        // Always pass `None`: the incoming hint doesn't account for the key and
        // `_meta` fields we append, and a hint of `Some(0)` makes serde_json
        // emit a closed `{}` up front, stranding our appended fields outside it.
        Ok(MergeMap {
            inner: self.inner.serialize_map(None)?,
            key: self.key,
            meta: self.meta,
            name_buf: String::new(),
        })
    }

    reject_scalars! {
        EXPECT_OBJECT;
        serialize_bool(bool),
        serialize_i8(i8),
        serialize_i16(i16),
        serialize_i32(i32),
        serialize_i64(i64),
        serialize_u8(u8),
        serialize_u16(u16),
        serialize_u32(u32),
        serialize_u64(u64),
        serialize_f32(f32),
        serialize_f64(f64),
        serialize_char(char),
        serialize_str(&str),
        serialize_bytes(&[u8]),
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(Self::Error::custom(EXPECT_OBJECT))
    }
    fn serialize_some<T: ?Sized + Serialize>(self, _v: &T) -> Result<Self::Ok, Self::Error> {
        Err(Self::Error::custom(EXPECT_OBJECT))
    }
    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(Self::Error::custom(EXPECT_OBJECT))
    }
    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(Self::Error::custom(EXPECT_OBJECT))
    }
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(Self::Error::custom(EXPECT_OBJECT))
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _v: &T,
    ) -> Result<Self::Ok, Self::Error> {
        Err(Self::Error::custom(EXPECT_OBJECT))
    }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _v: &T,
    ) -> Result<Self::Ok, Self::Error> {
        Err(Self::Error::custom(EXPECT_OBJECT))
    }
    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(Self::Error::custom(EXPECT_OBJECT))
    }
    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(Self::Error::custom(EXPECT_OBJECT))
    }
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(Self::Error::custom(EXPECT_OBJECT))
    }
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(Self::Error::custom(EXPECT_OBJECT))
    }
    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(Self::Error::custom(EXPECT_OBJECT))
    }
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(Self::Error::custom(EXPECT_OBJECT))
    }
}

/// The map state returned by [`MergeSerializer::serialize_map`]. It forwards
/// payload entries (dropping any whose name the key overrides), then on `end`
/// appends the key fields and the `_meta` object.
pub struct MergeMap<'a, SM, M> {
    inner: SM,
    key: Option<&'a Map<String, Value>>,
    meta: &'a M,
    // Reused across entries so recognizing overridden field names doesn't
    // allocate per field.
    name_buf: String,
}

impl<'a, SM, M> SerializeMap for MergeMap<'a, SM, M>
where
    SM: SerializeMap,
    M: Serialize,
{
    type Ok = SM::Ok;
    type Error = SM::Error;

    fn serialize_entry<K, V>(&mut self, key: &K, value: &V) -> Result<(), Self::Error>
    where
        K: ?Sized + Serialize,
        V: ?Sized + Serialize,
    {
        // Extract the field name so the payload's copy can be dropped when the
        // key overrides it or it collides with the reserved `_meta`.
        self.name_buf.clear();
        key.serialize(KeyExtractor::<Self::Error> {
            out: &mut self.name_buf,
            _err: PhantomData,
        })?;

        let overridden_by_key = match self.key {
            Some(key_fields) => key_fields.contains_key(self.name_buf.as_str()),
            None => false,
        };
        if self.name_buf == "_meta" || overridden_by_key {
            // `_meta` is reserved for our metadata. A key field of the same name
            // overrides the payload.
            return Ok(());
        }
        self.inner.serialize_entry(self.name_buf.as_str(), value)
    }

    // prost-reflect and serde_json both serialize objects via `serialize_entry`,
    // so these split methods aren't exercised by our payloads. Forward them
    // plainly to keep the impl total.
    fn serialize_key<K: ?Sized + Serialize>(&mut self, key: &K) -> Result<(), Self::Error> {
        self.inner.serialize_key(key)
    }
    fn serialize_value<V: ?Sized + Serialize>(&mut self, value: &V) -> Result<(), Self::Error> {
        self.inner.serialize_value(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        let mut inner = self.inner;
        // The key is merged in last, so a key field named `_meta`
        // overrides ours. Emit ours only if the key didn't, keeping exactly one.
        let mut key_has_meta = false;
        if let Some(key_fields) = self.key {
            for (name, value) in key_fields {
                if name == "_meta" {
                    key_has_meta = true;
                }
                inner.serialize_entry(name, value)?;
            }
        }
        if !key_has_meta {
            inner.serialize_entry("_meta", self.meta)?;
        }
        inner.end()
    }
}

/// A [`Serializer`] whose only job is to capture a string map key into `out`.
/// JSON object keys and protobuf field names are always strings, so anything
/// else is an error.
struct KeyExtractor<'a, E> {
    out: &'a mut String,
    _err: PhantomData<fn() -> E>,
}

impl<'a, E> Serializer for KeyExtractor<'a, E>
where
    E: serde::ser::Error,
{
    type Ok = ();
    type Error = E;
    type SerializeSeq = Impossible<(), E>;
    type SerializeTuple = Impossible<(), E>;
    type SerializeTupleStruct = Impossible<(), E>;
    type SerializeTupleVariant = Impossible<(), E>;
    type SerializeMap = Impossible<(), E>;
    type SerializeStruct = Impossible<(), E>;
    type SerializeStructVariant = Impossible<(), E>;

    fn serialize_str(self, v: &str) -> Result<(), E> {
        self.out.push_str(v);
        Ok(())
    }

    reject_scalars! {
        KEY_NOT_STRING;
        serialize_bool(bool),
        serialize_i8(i8),
        serialize_i16(i16),
        serialize_i32(i32),
        serialize_i64(i64),
        serialize_u8(u8),
        serialize_u16(u16),
        serialize_u32(u32),
        serialize_u64(u64),
        serialize_f32(f32),
        serialize_f64(f64),
        serialize_char(char),
        serialize_bytes(&[u8]),
    }

    fn serialize_none(self) -> Result<(), E> {
        Err(E::custom(KEY_NOT_STRING))
    }
    fn serialize_some<T: ?Sized + Serialize>(self, _v: &T) -> Result<(), E> {
        Err(E::custom(KEY_NOT_STRING))
    }
    fn serialize_unit(self) -> Result<(), E> {
        Err(E::custom(KEY_NOT_STRING))
    }
    fn serialize_unit_struct(self, _name: &'static str) -> Result<(), E> {
        Err(E::custom(KEY_NOT_STRING))
    }
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
    ) -> Result<(), E> {
        Err(E::custom(KEY_NOT_STRING))
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _v: &T,
    ) -> Result<(), E> {
        Err(E::custom(KEY_NOT_STRING))
    }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _v: &T,
    ) -> Result<(), E> {
        Err(E::custom(KEY_NOT_STRING))
    }
    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, E> {
        Err(E::custom(KEY_NOT_STRING))
    }
    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, E> {
        Err(E::custom(KEY_NOT_STRING))
    }
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, E> {
        Err(E::custom(KEY_NOT_STRING))
    }
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, E> {
        Err(E::custom(KEY_NOT_STRING))
    }
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, E> {
        Err(E::custom(KEY_NOT_STRING))
    }
    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, E> {
        Err(E::custom(KEY_NOT_STRING))
    }
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, E> {
        Err(E::custom(KEY_NOT_STRING))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Serialize `payload` through a `MergeSerializer` into a JSON string, the
    /// way `pull.rs` will drive it for a protobuf message.
    fn merge(payload: &Value, key: Option<Value>, meta: &Value) -> String {
        let key_map = key.map(|v| v.as_object().unwrap().clone());
        let mut buf = Vec::new();
        {
            let mut inner = serde_json::Serializer::new(&mut buf);
            payload
                .serialize(MergeSerializer::new(&mut inner, key_map.as_ref(), meta))
                .unwrap();
        }
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn appends_meta_with_no_key() {
        let out = merge(&json!({"a": 1, "b": "x"}), None, &json!({"op": "u"}));
        assert_eq!(out, r#"{"a":1,"b":"x","_meta":{"op":"u"}}"#);
    }

    #[test]
    fn merges_non_colliding_key() {
        let out = merge(
            &json!({"a": 1}),
            Some(json!({"id": 7})),
            &json!({"op": "c"}),
        );
        assert_eq!(out, r#"{"a":1,"id":7,"_meta":{"op":"c"}}"#);
    }

    #[test]
    fn key_overrides_colliding_payload_field() {
        // The payload's `id` is dropped in favor of the key's `id`, and no
        // duplicate key is emitted.
        let out = merge(
            &json!({"id": "payload", "a": 1}),
            Some(json!({"id": "key"})),
            &json!({"op": "u"}),
        );
        assert_eq!(out, r#"{"a":1,"id":"key","_meta":{"op":"u"}}"#);
    }

    #[test]
    fn empty_payload_still_gets_meta() {
        let out = merge(&json!({}), None, &json!({"op": "d"}));
        assert_eq!(out, r#"{"_meta":{"op":"d"}}"#);
    }

    #[test]
    fn payload_meta_is_dropped_in_favor_of_ours() {
        // A payload field literally named `_meta` is reserved for our metadata:
        // it's dropped, and exactly one `_meta` (ours) is emitted.
        let out = merge(
            &json!({"_meta": {"stale": true}, "a": 1}),
            None,
            &json!({"op": "u"}),
        );
        assert_eq!(out, r#"{"a":1,"_meta":{"op":"u"}}"#);
    }

    #[test]
    fn key_meta_overrides_ours() {
        // Matches the previous merge behavior: the key is merged in last, so a
        // key field named `_meta` overrides our metadata. Still exactly one
        // `_meta`, and no duplicate.
        let out = merge(
            &json!({"a": 1}),
            Some(json!({"_meta": {"stale": true}, "id": 7})),
            &json!({"op": "c"}),
        );
        assert_eq!(out, r#"{"a":1,"_meta":{"stale":true},"id":7}"#);
    }
}

/// Differential tests comparing the streaming [`MergeSerializer`] path against
/// the `serde_json::Value` DOM merge path it replaced, over a corpus of
/// `DynamicMessage`s decoded from wire bytes like production payloads. The one
/// intentional difference, f32 representation, is pinned by its own test.
#[cfg(test)]
mod differential {
    use super::MergeSerializer;
    use crate::protobuf::decode_protobuf_message;
    use prost_reflect::{
        bytes::Bytes, prost::Message as _, DescriptorPool, DynamicMessage, MapKey,
        SerializeOptions, Value as PValue,
    };
    use serde_json::{json, Map, Value};
    use std::collections::{BTreeMap, HashMap, HashSet};

    fn pool() -> DescriptorPool {
        let file_descriptor_set = protox::compile(
            ["differential_corpus.proto"],
            [concat!(env!("CARGO_MANIFEST_DIR"), "/src/testdata")],
        )
        .expect("corpus proto must compile");
        DescriptorPool::from_file_descriptor_set(file_descriptor_set)
            .expect("corpus descriptors must load")
    }

    fn serialize_options() -> SerializeOptions {
        SerializeOptions::new().use_proto_field_name(true)
    }

    /// Replica of the merge path this connector used before streaming
    /// serialization: build a `serde_json::Value` DOM, insert `_meta`, then
    /// append the key fields over it. This is the oracle the streaming output
    /// is compared against.
    fn merge_via_value_dom(
        message: &DynamicMessage,
        key: Option<&Map<String, Value>>,
        meta: &Value,
    ) -> Value {
        let mut doc: Value = message
            .serialize_with_options(serde_json::value::Serializer, &serialize_options())
            .expect("oracle serialization must succeed");
        let captured = doc
            .as_object_mut()
            .expect("corpus payloads serialize as objects");
        captured.insert("_meta".to_string(), meta.clone());
        if let Some(key_fields) = key {
            captured.append(&mut key_fields.clone());
        }
        doc
    }

    /// The streaming path exactly as `pull.rs` drives it for protobuf payloads.
    fn merge_via_streaming(
        message: &DynamicMessage,
        key: Option<&Map<String, Value>>,
        meta: &Value,
    ) -> Result<Vec<u8>, serde_json::Error> {
        let mut buf = Vec::new();
        {
            let mut ser = serde_json::Serializer::new(&mut buf);
            message.serialize_with_options(
                MergeSerializer::new(&mut ser, key, meta),
                &serialize_options(),
            )?;
        }
        Ok(buf)
    }

    /// Extracts the top-level object keys from serialized JSON without
    /// collapsing duplicates, which parsing to a `serde_json::Value` would
    /// silently do.
    fn top_level_keys(bytes: &[u8]) -> Vec<String> {
        struct Keys;
        impl<'de> serde::de::Visitor<'de> for Keys {
            type Value = Vec<String>;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a JSON object")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Vec<String>, A::Error> {
                let mut keys = Vec::new();
                while let Some(key) = map.next_key::<String>()? {
                    map.next_value::<serde::de::IgnoredAny>()?;
                    keys.push(key);
                }
                Ok(keys)
            }
        }
        use serde::de::Deserializer as _;
        serde_json::Deserializer::from_slice(bytes)
            .deserialize_map(Keys)
            .expect("streamed document must be a JSON object")
    }

    fn message_of(pool: &DescriptorPool, name: &str, fields: Vec<(&str, PValue)>) -> DynamicMessage {
        let desc = pool
            .get_message_by_name(name)
            .unwrap_or_else(|| panic!("missing descriptor {name}"));
        let mut msg = DynamicMessage::new(desc.clone());
        for (field, value) in fields {
            msg.set_field_by_name(field, value);
        }
        // Round-trip through wire bytes so the tested message is decoded the
        // same way production decodes registry payloads.
        decode_protobuf_message(&desc, &msg.encode_to_vec()).unwrap()
    }

    fn everything(pool: &DescriptorPool, fields: Vec<(&str, PValue)>) -> DynamicMessage {
        message_of(pool, "differential.Everything", fields)
    }

    /// Builds a corpus sub-message from its prost-types counterpart by
    /// decoding its encoded bytes under the pool's descriptor.
    fn from_prost<T: prost_reflect::prost::Message>(
        pool: &DescriptorPool,
        name: &str,
        value: &T,
    ) -> DynamicMessage {
        let desc = pool
            .get_message_by_name(name)
            .unwrap_or_else(|| panic!("missing descriptor {name}"));
        decode_protobuf_message(&desc, &value.encode_to_vec()).unwrap()
    }

    fn wrapper(pool: &DescriptorPool, name: &str, value: PValue) -> PValue {
        let desc = pool
            .get_message_by_name(name)
            .unwrap_or_else(|| panic!("missing descriptor {name}"));
        let mut msg = DynamicMessage::new(desc);
        msg.set_field_by_name("value", value);
        PValue::Message(msg)
    }

    fn nested(pool: &DescriptorPool, name: &str, ratio: f32, samples: Vec<f64>) -> DynamicMessage {
        message_of(
            pool,
            "differential.Nested",
            vec![
                ("name", PValue::String(name.to_string())),
                ("ratio", PValue::F32(ratio)),
                (
                    "samples",
                    PValue::List(samples.into_iter().map(PValue::F64).collect()),
                ),
            ],
        )
    }

    fn pv(kind: prost_types::value::Kind) -> prost_types::Value {
        prost_types::Value { kind: Some(kind) }
    }

    /// Corpus messages. f32 values here are limited to ones whose f64 widening
    /// prints identically, like 0.5 and -1.25. The divergent representations
    /// are covered by `f32_representation_differs_from_value_dom_path`.
    fn corpus(pool: &DescriptorPool) -> Vec<(&'static str, DynamicMessage)> {
        use prost_types::value::Kind;

        let any = prost_types::Any {
            type_url: "type.googleapis.com/differential.Nested".to_string(),
            value: nested(pool, "wrapped", 0.5, vec![1.5]).encode_to_vec(),
        };

        let deep_struct = prost_types::Struct {
            fields: BTreeMap::from([(
                // A nested `_meta` key is below the top level and must survive.
                "_meta".to_string(),
                pv(Kind::StringValue("kept".to_string())),
            )]),
        };
        let struct_top = prost_types::Struct {
            fields: BTreeMap::from([
                ("s".to_string(), pv(Kind::StringValue("payload".to_string()))),
                ("n".to_string(), pv(Kind::NumberValue(1.5))),
                ("flag".to_string(), pv(Kind::BoolValue(true))),
                ("nil".to_string(), pv(Kind::NullValue(0))),
                // As the whole payload this Struct's `_meta` key is top-level
                // and dropped in favor of the connector's. As the wkt_struct
                // field value it's nested and survives.
                ("_meta".to_string(), pv(Kind::StringValue("from-payload".to_string()))),
                ("deep".to_string(), pv(Kind::StructValue(deep_struct))),
            ]),
        };

        vec![
            ("empty", everything(pool, vec![])),
            (
                "everything",
                everything(
                    pool,
                    vec![
                        ("f_double", PValue::F64(0.1)),
                        ("f_float", PValue::F32(0.5)),
                        ("f_int32", PValue::I32(i32::MIN)),
                        ("f_int64", PValue::I64(i64::MAX)),
                        ("f_uint32", PValue::U32(u32::MAX)),
                        ("f_uint64", PValue::U64(u64::MAX)),
                        ("f_sint32", PValue::I32(-42)),
                        ("f_sint64", PValue::I64(i64::MIN)),
                        ("f_fixed32", PValue::U32(7)),
                        ("f_fixed64", PValue::U64(8)),
                        ("f_sfixed32", PValue::I32(-7)),
                        ("f_sfixed64", PValue::I64(-8)),
                        ("f_bool", PValue::Bool(true)),
                        (
                            "f_string",
                            PValue::String("héllo \"world\"\n☃".to_string()),
                        ),
                        (
                            "f_bytes",
                            PValue::Bytes(Bytes::from_static(&[0x00, 0xFF, 0x9F, 0x92, 0x96])),
                        ),
                        ("f_enum", PValue::EnumNumber(2)),
                        (
                            "f_message",
                            PValue::Message(nested(pool, "inner", -1.25, vec![1.5, -2.5])),
                        ),
                        (
                            "r_string",
                            PValue::List(vec![
                                PValue::String("first".to_string()),
                                PValue::String("second".to_string()),
                            ]),
                        ),
                        (
                            "r_float",
                            PValue::List(vec![
                                PValue::F32(0.5),
                                PValue::F32(-1.25),
                                PValue::F32(16_777_216.0),
                            ]),
                        ),
                        (
                            "r_message",
                            PValue::List(vec![
                                PValue::Message(nested(pool, "a", 0.5, vec![])),
                                PValue::Message(nested(pool, "b", -1.25, vec![0.25])),
                            ]),
                        ),
                        (
                            "m_string",
                            PValue::Map(HashMap::from([
                                (
                                    MapKey::String("k1".to_string()),
                                    PValue::String("v1".to_string()),
                                ),
                                (
                                    MapKey::String("k2".to_string()),
                                    PValue::String("v2".to_string()),
                                ),
                                (
                                    MapKey::String("k3".to_string()),
                                    PValue::String("v3".to_string()),
                                ),
                            ])),
                        ),
                        (
                            "m_int32",
                            PValue::Map(HashMap::from([
                                (
                                    MapKey::I32(1),
                                    PValue::Message(nested(pool, "one", 0.5, vec![])),
                                ),
                                (
                                    MapKey::I32(-2),
                                    PValue::Message(nested(pool, "neg", 0.5, vec![])),
                                ),
                            ])),
                        ),
                        (
                            "m_int64",
                            PValue::Map(HashMap::from([
                                (MapKey::I64(-5), PValue::String("neg".to_string())),
                                (MapKey::I64(5), PValue::String("pos".to_string())),
                            ])),
                        ),
                        (
                            "m_uint64",
                            PValue::Map(HashMap::from([(
                                MapKey::U64(u64::MAX),
                                PValue::String("max".to_string()),
                            )])),
                        ),
                        (
                            "m_bool",
                            PValue::Map(HashMap::from([
                                (MapKey::Bool(true), PValue::String("t".to_string())),
                                (MapKey::Bool(false), PValue::String("f".to_string())),
                            ])),
                        ),
                        // Explicit presence at the default value must still be
                        // emitted.
                        ("opt_int32", PValue::I32(0)),
                        ("one_string", PValue::String("chosen".to_string())),
                        (
                            "wkt_timestamp",
                            PValue::Message(from_prost(
                                pool,
                                "google.protobuf.Timestamp",
                                &prost_types::Timestamp {
                                    seconds: 1_730_233_606,
                                    nanos: 123_000_000,
                                },
                            )),
                        ),
                        (
                            "wkt_duration",
                            PValue::Message(from_prost(
                                pool,
                                "google.protobuf.Duration",
                                &prost_types::Duration {
                                    seconds: 3600,
                                    nanos: 500_000_000,
                                },
                            )),
                        ),
                        (
                            "wkt_struct",
                            PValue::Message(from_prost(
                                pool,
                                "google.protobuf.Struct",
                                &struct_top.clone(),
                            )),
                        ),
                        (
                            "wkt_value",
                            PValue::Message(from_prost(
                                pool,
                                "google.protobuf.Value",
                                &pv(Kind::ListValue(prost_types::ListValue {
                                    values: vec![
                                        pv(Kind::NumberValue(1.0)),
                                        pv(Kind::StringValue("two".to_string())),
                                    ],
                                })),
                            )),
                        ),
                        (
                            "wkt_list",
                            PValue::Message(from_prost(
                                pool,
                                "google.protobuf.ListValue",
                                &prost_types::ListValue {
                                    values: vec![pv(Kind::NullValue(0)), pv(Kind::BoolValue(false))],
                                },
                            )),
                        ),
                        (
                            "wrap_double",
                            wrapper(pool, "google.protobuf.DoubleValue", PValue::F64(2.5)),
                        ),
                        (
                            "wrap_float",
                            wrapper(pool, "google.protobuf.FloatValue", PValue::F32(-1.25)),
                        ),
                        (
                            "wrap_int64",
                            wrapper(pool, "google.protobuf.Int64Value", PValue::I64(i64::MIN)),
                        ),
                        (
                            "wrap_uint64",
                            wrapper(pool, "google.protobuf.UInt64Value", PValue::U64(u64::MAX)),
                        ),
                        (
                            "wrap_bool",
                            wrapper(pool, "google.protobuf.BoolValue", PValue::Bool(false)),
                        ),
                        (
                            "wrap_string",
                            wrapper(pool, "google.protobuf.StringValue", PValue::String(String::new())),
                        ),
                        (
                            "wrap_bytes",
                            wrapper(
                                pool,
                                "google.protobuf.BytesValue",
                                PValue::Bytes(Bytes::from_static(b"hi")),
                            ),
                        ),
                        (
                            "wkt_mask",
                            PValue::Message(from_prost(
                                pool,
                                "google.protobuf.FieldMask",
                                &prost_types::FieldMask {
                                    paths: vec!["a.b".to_string(), "c".to_string()],
                                },
                            )),
                        ),
                        ("f_null", PValue::EnumNumber(0)),
                        (
                            "f_any",
                            PValue::Message(from_prost(pool, "google.protobuf.Any", &any.clone())),
                        ),
                        ("_meta", PValue::String("payload-meta-dropped".to_string())),
                    ],
                ),
            ),
            (
                "float_specials",
                everything(
                    pool,
                    vec![
                        ("f_float", PValue::F32(f32::NAN)),
                        ("f_double", PValue::F64(f64::NEG_INFINITY)),
                        (
                            "r_float",
                            PValue::List(vec![
                                PValue::F32(f32::NAN),
                                PValue::F32(f32::INFINITY),
                                PValue::F32(f32::NEG_INFINITY),
                            ]),
                        ),
                        (
                            "f_message",
                            PValue::Message(nested(
                                pool,
                                "specials",
                                0.5,
                                vec![f64::NAN, f64::INFINITY, f64::NEG_INFINITY],
                            )),
                        ),
                    ],
                ),
            ),
            (
                "unknown_enum",
                everything(pool, vec![("f_enum", PValue::EnumNumber(42))]),
            ),
            (
                "meta_collision",
                everything(
                    pool,
                    vec![
                        ("_meta", PValue::String("stale".to_string())),
                        ("f_string", PValue::String("x".to_string())),
                    ],
                ),
            ),
            (
                "struct_top",
                from_prost(pool, "google.protobuf.Struct", &struct_top),
            ),
            (
                // An empty Struct's serializer passes `Some(0)` as the map
                // length hint. MergeSerializer must ignore it or the appended
                // fields would land outside a closed `{}`.
                "struct_top_empty",
                from_prost(
                    pool,
                    "google.protobuf.Struct",
                    &prost_types::Struct {
                        fields: BTreeMap::new(),
                    },
                ),
            ),
            ("any_top", from_prost(pool, "google.protobuf.Any", &any)),
        ]
    }

    fn key_variants() -> Vec<(&'static str, Option<Map<String, Value>>)> {
        let obj = |v: Value| Some(v.as_object().unwrap().clone());
        vec![
            ("no-key", None),
            ("plain-key", obj(json!({"id": 7, "region": "us"}))),
            // Collides with Everything's f_string/f_float/m_string and
            // struct_top's s. Appended as plain fields everywhere else.
            (
                "colliding-key",
                obj(json!({
                    "f_string": "key-wins",
                    "f_float": 9.5,
                    "m_string": {"replaced": true},
                    "s": "key-wins",
                })),
            ),
            ("meta-key", obj(json!({"_meta": {"source": "key"}, "id": 7}))),
        ]
    }

    fn meta() -> Value {
        json!({"topic": "t", "partition": 3, "offset": 42, "op": "u"})
    }

    #[test]
    fn corpus_equivalence() {
        let pool = pool();
        let meta = meta();
        for (label, message) in corpus(&pool) {
            for (key_label, key) in key_variants() {
                let oracle = merge_via_value_dom(&message, key.as_ref(), &meta);
                let streamed = merge_via_streaming(&message, key.as_ref(), &meta)
                    .unwrap_or_else(|err| panic!("{label}/{key_label}: streaming failed: {err}"));

                let keys = top_level_keys(&streamed);
                let unique: HashSet<&String> = keys.iter().collect();
                assert_eq!(
                    unique.len(),
                    keys.len(),
                    "{label}/{key_label}: duplicate top-level keys in {}",
                    String::from_utf8_lossy(&streamed),
                );
                let oracle_keys: HashSet<&String> = oracle.as_object().unwrap().keys().collect();
                assert_eq!(
                    unique, oracle_keys,
                    "{label}/{key_label}: top-level key sets differ",
                );

                let parsed: Value = serde_json::from_slice(&streamed).unwrap();
                assert_eq!(parsed, oracle, "{label}/{key_label}: documents differ");
            }
        }
    }

    /// Pins the parsed form of a fully-populated document so dependency
    /// upgrades that change leaf encoding are caught. The parsed, key-sorted
    /// form is snapshotted because raw bytes are nondeterministic: protobuf
    /// map fields iterate in HashMap order.
    #[test]
    fn everything_document_snapshot() {
        let pool = pool();
        let (_, message) = corpus(&pool).remove(1);
        let streamed = merge_via_streaming(&message, None, &meta()).unwrap();
        let parsed: Value = serde_json::from_slice(&streamed).unwrap();
        insta::assert_json_snapshot!("everything_document", parsed);
    }

    /// The one intentional difference between the paths. The DOM path widened
    /// f32 to f64 and printed the widened value. The streaming path prints the
    /// shortest representation that round-trips the f32, which is the
    /// proto3-canonical JSON form. Both denote the same underlying f32.
    #[test]
    fn f32_representation_differs_from_value_dom_path() {
        let pool = pool();
        let meta = json!({"op": "u"});
        let cases = [
            (0.1f32, "0.1", "0.10000000149011612"),
            (1.1f32, "1.1", "1.100000023841858"),
            (1e-45f32, "1e-45", "1.401298464324817e-45"),
        ];
        for (value, streamed_repr, dom_repr) in cases {
            let message = everything(&pool, vec![("f_float", PValue::F32(value))]);

            let streamed = String::from_utf8(
                merge_via_streaming(&message, None, &meta).unwrap(),
            )
            .unwrap();
            assert_eq!(
                streamed,
                format!(r#"{{"f_float":{streamed_repr},"_meta":{{"op":"u"}}}}"#),
            );

            let dom = serde_json::to_string(&merge_via_value_dom(&message, None, &meta)).unwrap();
            assert_eq!(
                dom,
                format!(r#"{{"_meta":{{"op":"u"}},"f_float":{dom_repr}}}"#),
            );

            // Both representations round-trip to the same f32.
            let streamed_parsed: Value = serde_json::from_str(&streamed).unwrap();
            let dom_parsed: Value = serde_json::from_str(&dom).unwrap();
            assert_eq!(streamed_parsed["f_float"].as_f64().unwrap() as f32, value);
            assert_eq!(dom_parsed["f_float"].as_f64().unwrap() as f32, value);
        }
    }

    /// A payload that serializes as a JSON string rather than an object (here
    /// a top-level google.protobuf.Timestamp) must produce a clean error. The
    /// replaced DOM path panicked on `as_object_mut().unwrap()` for these.
    #[test]
    fn non_object_payload_errors_cleanly() {
        let pool = pool();
        let message = from_prost(
            &pool,
            "google.protobuf.Timestamp",
            &prost_types::Timestamp {
                seconds: 1,
                nanos: 0,
            },
        );
        let err = merge_via_streaming(&message, None, &json!({"op": "u"})).unwrap_err();
        assert!(
            err.to_string().contains("must serialize as a JSON object"),
            "unexpected error: {err}",
        );
    }
}
