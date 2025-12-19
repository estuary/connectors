//! Lossy UTF-8 deserializer wrapper for handling invalid UTF-8 in BSON strings.
//!
//! This module provides `Utf8LossyDeserializer`, a serde `Deserializer` wrapper that
//! handles invalid UTF-8 sequences in BSON strings by replacing them with the Unicode
//! replacement character (U+FFFD).

use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde::Deserializer;

/// The special name used by the bson crate to signal lossy UTF-8 handling.
/// When the bson deserializer sees this name in `deserialize_newtype_struct`,
/// it sets an internal flag to use lossy UTF-8 conversion for strings.
const BSON_UTF8_LOSSY_NEWTYPE: &str = "$__bson_private_utf8_lossy";

/// A deserializer wrapper that handles invalid UTF-8 sequences in strings
/// by replacing them with the Unicode replacement character (U+FFFD).
///
/// This works by intercepting `deserialize_any` calls and routing them through
/// the bson crate's `Utf8Lossy` mechanism, which tells the bson deserializer
/// to use lossy UTF-8 conversion for string values.
pub struct Utf8LossyDeserializer<D> {
    inner: D,
}

impl<D> Utf8LossyDeserializer<D> {
    /// Create a new Utf8LossyDeserializer wrapping the given deserializer.
    pub fn new(inner: D) -> Self {
        Self { inner }
    }
}

/// A visitor that triggers lossy UTF-8 mode and then delegates to the original visitor.
///
/// When the bson deserializer calls `visit_newtype_struct` on this visitor,
/// it passes a deserializer with the `utf8_lossy` flag already set.
/// We then use that deserializer with `deserialize_any` to continue the actual deserialization.
struct TriggerLossyVisitor<V> {
    inner: V,
}

impl<'de, V: Visitor<'de>> Visitor<'de> for TriggerLossyVisitor<V> {
    type Value = V::Value;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.inner.expecting(formatter)
    }

    fn visit_newtype_struct<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        // The deserializer now has utf8_lossy=true, use it for the actual deserialization
        deserializer.deserialize_any(self.inner)
    }
}

/// Wrapper visitor that delegates to the inner visitor while maintaining lossy context.
struct Utf8LossyVisitor<V> {
    inner: V,
}

impl<'de, V: Visitor<'de>> Visitor<'de> for Utf8LossyVisitor<V> {
    type Value = V::Value;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.inner.expecting(formatter)
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Self::Value, E> {
        self.inner.visit_bool(v)
    }

    fn visit_i8<E: de::Error>(self, v: i8) -> Result<Self::Value, E> {
        self.inner.visit_i8(v)
    }

    fn visit_i16<E: de::Error>(self, v: i16) -> Result<Self::Value, E> {
        self.inner.visit_i16(v)
    }

    fn visit_i32<E: de::Error>(self, v: i32) -> Result<Self::Value, E> {
        self.inner.visit_i32(v)
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
        self.inner.visit_i64(v)
    }

    fn visit_i128<E: de::Error>(self, v: i128) -> Result<Self::Value, E> {
        self.inner.visit_i128(v)
    }

    fn visit_u8<E: de::Error>(self, v: u8) -> Result<Self::Value, E> {
        self.inner.visit_u8(v)
    }

    fn visit_u16<E: de::Error>(self, v: u16) -> Result<Self::Value, E> {
        self.inner.visit_u16(v)
    }

    fn visit_u32<E: de::Error>(self, v: u32) -> Result<Self::Value, E> {
        self.inner.visit_u32(v)
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
        self.inner.visit_u64(v)
    }

    fn visit_u128<E: de::Error>(self, v: u128) -> Result<Self::Value, E> {
        self.inner.visit_u128(v)
    }

    fn visit_f32<E: de::Error>(self, v: f32) -> Result<Self::Value, E> {
        self.inner.visit_f32(v)
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Self::Value, E> {
        self.inner.visit_f64(v)
    }

    fn visit_char<E: de::Error>(self, v: char) -> Result<Self::Value, E> {
        self.inner.visit_char(v)
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
        self.inner.visit_str(v)
    }

    fn visit_borrowed_str<E: de::Error>(self, v: &'de str) -> Result<Self::Value, E> {
        self.inner.visit_borrowed_str(v)
    }

    fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
        self.inner.visit_string(v)
    }

    fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
        self.inner.visit_bytes(v)
    }

    fn visit_borrowed_bytes<E: de::Error>(self, v: &'de [u8]) -> Result<Self::Value, E> {
        self.inner.visit_borrowed_bytes(v)
    }

    fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Self::Value, E> {
        self.inner.visit_byte_buf(v)
    }

    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        self.inner.visit_none()
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        self.inner.visit_some(Utf8LossyDeserializer::new(deserializer))
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        self.inner.visit_unit()
    }

    fn visit_newtype_struct<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        self.inner.visit_newtype_struct(Utf8LossyDeserializer::new(deserializer))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<Self::Value, A::Error> {
        self.inner.visit_seq(Utf8LossySeqAccess { inner: seq })
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
        self.inner.visit_map(Utf8LossyMapAccess { inner: map })
    }

    fn visit_enum<A: de::EnumAccess<'de>>(self, data: A) -> Result<Self::Value, A::Error> {
        self.inner.visit_enum(Utf8LossyEnumAccess { inner: data })
    }
}

/// Wrapper for SeqAccess that applies lossy UTF-8 handling to elements.
struct Utf8LossySeqAccess<A> {
    inner: A,
}

impl<'de, A: SeqAccess<'de>> SeqAccess<'de> for Utf8LossySeqAccess<A> {
    type Error = A::Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error> {
        self.inner.next_element_seed(Utf8LossySeed { inner: seed })
    }

    fn size_hint(&self) -> Option<usize> {
        self.inner.size_hint()
    }
}

/// Wrapper for MapAccess that applies lossy UTF-8 handling to keys and values.
struct Utf8LossyMapAccess<A> {
    inner: A,
}

impl<'de, A: MapAccess<'de>> MapAccess<'de> for Utf8LossyMapAccess<A> {
    type Error = A::Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error> {
        self.inner.next_key_seed(Utf8LossySeed { inner: seed })
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value, Self::Error> {
        self.inner.next_value_seed(Utf8LossySeed { inner: seed })
    }

    fn size_hint(&self) -> Option<usize> {
        self.inner.size_hint()
    }
}

/// Wrapper for EnumAccess that applies lossy UTF-8 handling.
struct Utf8LossyEnumAccess<A> {
    inner: A,
}

impl<'de, A: de::EnumAccess<'de>> de::EnumAccess<'de> for Utf8LossyEnumAccess<A> {
    type Error = A::Error;
    type Variant = Utf8LossyVariantAccess<A::Variant>;

    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error> {
        let (value, variant) = self.inner.variant_seed(Utf8LossySeed { inner: seed })?;
        Ok((value, Utf8LossyVariantAccess { inner: variant }))
    }
}

/// Wrapper for VariantAccess that applies lossy UTF-8 handling.
struct Utf8LossyVariantAccess<A> {
    inner: A,
}

impl<'de, A: de::VariantAccess<'de>> de::VariantAccess<'de> for Utf8LossyVariantAccess<A> {
    type Error = A::Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        self.inner.unit_variant()
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value, Self::Error> {
        self.inner.newtype_variant_seed(Utf8LossySeed { inner: seed })
    }

    fn tuple_variant<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.tuple_variant(len, Utf8LossyVisitor { inner: visitor })
    }

    fn struct_variant<V: Visitor<'de>>(self, fields: &'static [&'static str], visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.struct_variant(fields, Utf8LossyVisitor { inner: visitor })
    }
}

/// Wrapper for DeserializeSeed that applies lossy UTF-8 handling.
struct Utf8LossySeed<S> {
    inner: S,
}

impl<'de, S: DeserializeSeed<'de>> DeserializeSeed<'de> for Utf8LossySeed<S> {
    type Value = S::Value;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        self.inner.deserialize(Utf8LossyDeserializer::new(deserializer))
    }
}

impl<'de, D: Deserializer<'de>> Deserializer<'de> for Utf8LossyDeserializer<D> {
    type Error = D::Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        // Trigger the bson crate's lossy UTF-8 mode by calling deserialize_newtype_struct
        // with the special name. The bson deserializer will set its internal utf8_lossy flag
        // and then call our visitor's visit_newtype_struct with itself (flag set).
        self.inner.deserialize_newtype_struct(
            BSON_UTF8_LOSSY_NEWTYPE,
            TriggerLossyVisitor { inner: visitor },
        )
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_bool(Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_i8(Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_i16(Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_i32(Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_i64(Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_u8(Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_u16(Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_u32(Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_u64(Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_f32(Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_f64(Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_char(Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_str(Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_string(Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_bytes(Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_byte_buf(Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_option(Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_unit(Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(self, name: &'static str, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_unit_struct(name, Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(self, name: &'static str, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_newtype_struct(name, Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_seq(Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_tuple(len, Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(self, name: &'static str, len: usize, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_tuple_struct(name, len, Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_map(Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_struct<V: Visitor<'de>>(self, name: &'static str, fields: &'static [&'static str], visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_struct(name, fields, Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_enum<V: Visitor<'de>>(self, name: &'static str, variants: &'static [&'static str], visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_enum(name, variants, Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_identifier(Utf8LossyVisitor { inner: visitor })
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_ignored_any(Utf8LossyVisitor { inner: visitor })
    }

    fn is_human_readable(&self) -> bool {
        self.inner.is_human_readable()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serializer::SanitizingSerializer;

    #[test]
    fn test_invalid_utf8_in_string_field() {
        // Construct a BSON document with invalid UTF-8 in a string field.
        // BSON format:
        // - 4 bytes: document length (little-endian)
        // - elements...
        // - 1 byte: null terminator
        //
        // String element format:
        // - 1 byte: type (0x02 for string)
        // - cstring: field name (null-terminated)
        // - 4 bytes: string length including null terminator (little-endian)
        // - bytes: string data
        // - 1 byte: null terminator

        // Create a document: {"name": "hello\x80world"}
        // 0x80 is an invalid UTF-8 byte (continuation byte without leading byte)
        let mut bson_bytes = Vec::new();

        // We'll build the document and calculate the length at the end
        let mut doc_content = Vec::new();

        // String element for "name" field with invalid UTF-8
        doc_content.push(0x02); // string type
        doc_content.extend_from_slice(b"name\0"); // field name

        // String value: "hello\x80world" (12 bytes including null terminator)
        let invalid_string = b"hello\x80world\0";
        let str_len = invalid_string.len() as u32;
        doc_content.extend_from_slice(&str_len.to_le_bytes());
        doc_content.extend_from_slice(invalid_string);

        // Document null terminator
        doc_content.push(0x00);

        // Calculate total document length (4 bytes for length + content)
        let doc_len = (4 + doc_content.len()) as u32;
        bson_bytes.extend_from_slice(&doc_len.to_le_bytes());
        bson_bytes.extend_from_slice(&doc_content);

        // Transcode using Utf8LossyDeserializer to handle invalid UTF-8
        let raw_doc = bson::raw::RawDocument::from_bytes(&bson_bytes).unwrap();
        let deserializer = bson::RawDeserializer::new(raw_doc.as_bytes()).unwrap();
        let lossy_deserializer = Utf8LossyDeserializer::new(deserializer);
        let mut buf = Vec::new();
        let mut serializer = SanitizingSerializer::new(&mut buf, false);
        let result = serde_transcode::transcode(lossy_deserializer, &mut serializer);

        // With Utf8LossyDeserializer, transcoding should succeed
        assert!(result.is_ok(), "Expected success but got error: {:?}", result);

        // The invalid UTF-8 byte should be replaced with the Unicode replacement character
        let output = String::from_utf8(buf).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();

        // U+FFFD is the replacement character (displayed as �)
        assert_eq!(parsed["name"], "hello\u{FFFD}world");
    }
}
