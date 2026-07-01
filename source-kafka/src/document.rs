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
