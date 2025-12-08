//! Custom Serde Serializer that performs sanitization during transcoding.
//!
//! This serializer wraps `serde_json::Serializer` and intercepts extended JSON patterns
//! from BSON, converting them inline to sanitized output without intermediate allocation.

use chrono::{TimeZone, Utc};
use serde::ser::{
    self, Serialize, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant,
    SerializeTuple, SerializeTupleStruct, SerializeTupleVariant,
};
use serde::Serializer;
use std::io::Write;

const MIN_TIME_MILLIS: i64 = -62167219200000; // year 0
const MAX_TIME_MILLIS: i64 = 253402300799999; // year 9999

/// Convert milliseconds since epoch to RFC3339 format string.
/// Clamps the value to the valid range (year 0 to year 9999).
fn millis_to_rfc3339(millis: i64) -> String {
    let clamped = millis.clamp(MIN_TIME_MILLIS, MAX_TIME_MILLIS);
    let dt = Utc.timestamp_millis_opt(clamped).single().unwrap();
    dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

#[derive(Debug)]
pub struct Error(String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for Error {}

impl ser::Error for Error {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Error(msg.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error(e.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error(e.to_string())
    }
}

/// State for detecting and collecting extended JSON patterns in maps.
#[derive(Debug)]
enum MapState {
    /// Normal map, no extended JSON detected
    Normal,
    /// Detected $oid key, collecting value
    ObjectId,
    /// Detected $date key, collecting value
    DateTime,
    /// Detected $numberDecimal key (human-readable format), collecting value
    Decimal128,
    /// Detected $numberDecimalBytes key (non-human-readable format), collecting raw bytes
    Decimal128Bytes,
    /// Detected $binary key (human-readable format), collecting value
    Binary,
}

/// A Serde Serializer that performs sanitization inline during transcoding.
///
/// This serializer wraps a `serde_json::Serializer` and intercepts extended JSON
/// patterns from BSON (like `{"$oid": "..."}`, `{"$date": {...}}`, etc.),
/// converting them to sanitized string values.
pub struct SanitizingSerializer<W: Write> {
    writer: W,
    /// Whether this is the root document (for _id handling)
    is_root: bool,
}

impl<W: Write> SanitizingSerializer<W> {
    /// Create a new sanitizing serializer that writes to the given writer.
    /// Set `is_root` to true for the top-level document to enable _id stringification.
    pub fn new(writer: W, is_root: bool) -> Self {
        Self { writer, is_root }
    }
}

impl<'a, W: Write> Serializer for &'a mut SanitizingSerializer<W> {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = SanitizingSerializeSeq<'a, W>;
    type SerializeTuple = SanitizingSerializeTuple<'a, W>;
    type SerializeTupleStruct = SanitizingSerializeTupleStruct<'a, W>;
    type SerializeTupleVariant = SanitizingSerializeTupleVariant<'a, W>;
    type SerializeMap = SanitizingSerializeMap<'a, W>;
    type SerializeStruct = SanitizingSerializeStruct<'a, W>;
    type SerializeStructVariant = SanitizingSerializeStructVariant<'a, W>;

    // Mark as human-readable so BSON serializes extended JSON in string format
    // (e.g., $numberDecimal instead of $numberDecimalBytes)
    fn is_human_readable(&self) -> bool {
        true
    }

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        Ok(serde_json::Serializer::new(&mut self.writer).serialize_bool(v)?)
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        Ok(serde_json::Serializer::new(&mut self.writer).serialize_i8(v)?)
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        Ok(serde_json::Serializer::new(&mut self.writer).serialize_i16(v)?)
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        Ok(serde_json::Serializer::new(&mut self.writer).serialize_i32(v)?)
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        Ok(serde_json::Serializer::new(&mut self.writer).serialize_i64(v)?)
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        Ok(serde_json::Serializer::new(&mut self.writer).serialize_u8(v)?)
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        Ok(serde_json::Serializer::new(&mut self.writer).serialize_u16(v)?)
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        Ok(serde_json::Serializer::new(&mut self.writer).serialize_u32(v)?)
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(serde_json::Serializer::new(&mut self.writer).serialize_u64(v)?)
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        self.serialize_f64(v as f64)
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        if v.is_nan() {
            Ok(self.writer.write_all(b"\"NaN\"")?)
        } else if v.is_infinite() {
            if v.is_sign_positive() {
                Ok(self.writer.write_all(b"\"Infinity\"")?)
            } else {
                Ok(self.writer.write_all(b"\"-Infinity\"")?)
            }
        } else {
            Ok(serde_json::Serializer::new(&mut self.writer).serialize_f64(v)?)
        }
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        Ok(serde_json::Serializer::new(&mut self.writer).serialize_char(v)?)
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(serde_json::Serializer::new(&mut self.writer).serialize_str(v)?)
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        use base64::{engine::general_purpose::STANDARD, Engine};
        Ok(serde_json::Serializer::new(&mut self.writer).serialize_str(&STANDARD.encode(v))?)
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.serialize_unit()
    }

    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(serde_json::Serializer::new(&mut self.writer).serialize_unit()?)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        self.writer.write_all(b"{")?;
        serde_json::Serializer::new(&mut self.writer).serialize_str(variant)?;
        self.writer.write_all(b":")?;
        value.serialize(&mut *self)?;
        self.writer.write_all(b"}")?;
        Ok(())
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        self.writer.write_all(b"[")?;
        Ok(SanitizingSerializeSeq {
            serializer: self,
            first: true,
            len,
        })
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.writer.write_all(b"[")?;
        Ok(SanitizingSerializeTuple {
            serializer: self,
            first: true,
            len,
        })
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.writer.write_all(b"[")?;
        Ok(SanitizingSerializeTupleStruct {
            serializer: self,
            first: true,
            len,
        })
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        self.writer.write_all(b"{")?;
        serde_json::Serializer::new(&mut self.writer).serialize_str(variant)?;
        self.writer.write_all(b":[")?;
        Ok(SanitizingSerializeTupleVariant {
            serializer: self,
            first: true,
            len,
        })
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(SanitizingSerializeMap::new(self))
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.writer.write_all(b"{")?;
        Ok(SanitizingSerializeStruct {
            serializer: self,
            first: true,
        })
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        self.writer.write_all(b"{")?;
        serde_json::Serializer::new(&mut self.writer).serialize_str(variant)?;
        self.writer.write_all(b":{")?;
        Ok(SanitizingSerializeStructVariant {
            serializer: self,
            first: true,
        })
    }
}

pub struct SanitizingSerializeSeq<'a, W: Write> {
    serializer: &'a mut SanitizingSerializer<W>,
    first: bool,
    #[allow(dead_code)]
    len: Option<usize>,
}

impl<'a, W: Write> SerializeSeq for SanitizingSerializeSeq<'a, W> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        if !self.first {
            self.serializer.writer.write_all(b",")?;
        }
        self.first = false;
        value.serialize(&mut *self.serializer)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.serializer.writer.write_all(b"]")?;
        Ok(())
    }
}

pub struct SanitizingSerializeTuple<'a, W: Write> {
    serializer: &'a mut SanitizingSerializer<W>,
    first: bool,
    #[allow(dead_code)]
    len: usize,
}

impl<'a, W: Write> SerializeTuple for SanitizingSerializeTuple<'a, W> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        if !self.first {
            self.serializer.writer.write_all(b",")?;
        }
        self.first = false;
        value.serialize(&mut *self.serializer)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.serializer.writer.write_all(b"]")?;
        Ok(())
    }
}

pub struct SanitizingSerializeTupleStruct<'a, W: Write> {
    serializer: &'a mut SanitizingSerializer<W>,
    first: bool,
    #[allow(dead_code)]
    len: usize,
}

impl<'a, W: Write> SerializeTupleStruct for SanitizingSerializeTupleStruct<'a, W> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        if !self.first {
            self.serializer.writer.write_all(b",")?;
        }
        self.first = false;
        value.serialize(&mut *self.serializer)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.serializer.writer.write_all(b"]")?;
        Ok(())
    }
}

pub struct SanitizingSerializeTupleVariant<'a, W: Write> {
    serializer: &'a mut SanitizingSerializer<W>,
    first: bool,
    #[allow(dead_code)]
    len: usize,
}

impl<'a, W: Write> SerializeTupleVariant for SanitizingSerializeTupleVariant<'a, W> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        if !self.first {
            self.serializer.writer.write_all(b",")?;
        }
        self.first = false;
        value.serialize(&mut *self.serializer)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.serializer.writer.write_all(b"]}")?;
        Ok(())
    }
}

/// State machine for map serialization with extended JSON detection.
pub struct SanitizingSerializeMap<'a, W: Write> {
    serializer: &'a mut SanitizingSerializer<W>,
    /// Current state of extended JSON detection
    state: MapState,
    /// Number of entries written so far
    entries: usize,
    /// Buffer for collecting values when in extended JSON mode
    buffer: Vec<u8>,
    /// Whether we've started writing to output (the opening brace)
    started_output: bool,
    /// The key we're currently processing (for _id handling)
    current_key: Option<String>,
    /// Whether this is a root-level map (for _id handling)
    is_root: bool,
    /// Pending key to write if we determine this is a normal map
    pending_key: Option<String>,
    /// Whether we emitted a sanitized value instead of a map
    emitted_sanitized: bool,
}

impl<'a, W: Write> SanitizingSerializeMap<'a, W> {
    fn new(serializer: &'a mut SanitizingSerializer<W>) -> Self {
        let is_root = serializer.is_root;
        // After creating one map, subsequent maps are not root
        serializer.is_root = false;
        Self {
            serializer,
            state: MapState::Normal,
            entries: 0,
            buffer: Vec::new(),
            started_output: false,
            current_key: None,
            is_root,
            pending_key: None,
            emitted_sanitized: false,
        }
    }

    /// Ensure the map opening brace and any pending keys are written
    fn ensure_started(&mut self) -> Result<(), Error> {
        if !self.started_output {
            self.serializer.writer.write_all(b"{")?;
            self.started_output = true;
        }
        Ok(())
    }

    /// Write comma separator if needed
    fn write_separator(&mut self) -> Result<(), Error> {
        if self.entries > 0 {
            self.serializer.writer.write_all(b",")?;
        }
        Ok(())
    }

    /// Flush pending key if any
    fn flush_pending_key(&mut self) -> Result<(), Error> {
        if let Some(key) = self.pending_key.take() {
            self.ensure_started()?;
            self.write_separator()?;
            serde_json::Serializer::new(&mut self.serializer.writer).serialize_str(&key)?;
            self.serializer.writer.write_all(b":")?;
        }
        Ok(())
    }
}

impl<'a, W: Write> SerializeMap for SanitizingSerializeMap<'a, W> {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), Self::Error> {
        let mut key_buf = Vec::new();
        key.serialize(&mut serde_json::Serializer::new(&mut key_buf))?;
        // Remove quotes from JSON string
        let key_str = String::from_utf8_lossy(&key_buf);
        let key_str = key_str.trim_matches('"').to_string();

        self.current_key = Some(key_str.clone());

        // Check for extended JSON patterns on first key only
        if self.entries == 0 && self.pending_key.is_none() {
            match key_str.as_str() {
                "$oid" => {
                    self.state = MapState::ObjectId;
                    return Ok(());
                }
                "$date" => {
                    self.state = MapState::DateTime;
                    return Ok(());
                }
                "$numberDecimal" => {
                    self.state = MapState::Decimal128;
                    return Ok(());
                }
                "$numberDecimalBytes" => {
                    // Non-human-readable format: raw bytes array
                    self.state = MapState::Decimal128Bytes;
                    return Ok(());
                }
                "$binary" => {
                    self.state = MapState::Binary;
                    return Ok(());
                }
                _ => {}
            }
        }

        // Normal key handling
        self.flush_pending_key()?;
        self.pending_key = Some(key_str);
        Ok(())
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        match &self.state {
            MapState::ObjectId => {
                // Collect the $oid value and emit as string
                self.buffer.clear();
                value.serialize(&mut serde_json::Serializer::new(&mut self.buffer))?;
                // The value should be a JSON string like "507f1f77bcf86cd799439011"
                // Just write it directly (it's already quoted)
                self.serializer.writer.write_all(&self.buffer)?;
                self.emitted_sanitized = true;
                self.state = MapState::Normal;
                return Ok(());
            }
            MapState::DateTime => {
                // Collect the $date value - could be {"$numberLong": "millis"} or just a number
                self.buffer.clear();
                value.serialize(&mut serde_json::Serializer::new(&mut self.buffer))?;
                // Parse the value to extract milliseconds
                let millis = extract_datetime_millis(&self.buffer);
                let rfc3339 = millis_to_rfc3339(millis);
                serde_json::Serializer::new(&mut self.serializer.writer).serialize_str(&rfc3339)?;
                self.emitted_sanitized = true;
                self.state = MapState::Normal;
                return Ok(());
            }
            MapState::Decimal128 => {
                // Collect the $numberDecimal value and emit as string
                self.buffer.clear();
                value.serialize(&mut serde_json::Serializer::new(&mut self.buffer))?;
                // The value should be a JSON string like "123.456"
                // Just write it directly (it's already quoted)
                self.serializer.writer.write_all(&self.buffer)?;
                self.emitted_sanitized = true;
                self.state = MapState::Normal;
                return Ok(());
            }
            MapState::Decimal128Bytes => {
                // Non-human-readable format: raw bytes array [u8; 16]
                // Convert to Decimal128 and then to string
                self.buffer.clear();
                value.serialize(&mut serde_json::Serializer::new(&mut self.buffer))?;
                let decimal_str = extract_decimal128_from_bytes(&self.buffer);
                serde_json::Serializer::new(&mut self.serializer.writer).serialize_str(&decimal_str)?;
                self.emitted_sanitized = true;
                self.state = MapState::Normal;
                return Ok(());
            }
            MapState::Binary => {
                // Collect the $binary value and extract base64
                self.buffer.clear();
                value.serialize(&mut serde_json::Serializer::new(&mut self.buffer))?;
                // Parse {"base64": "...", "subType": "..."}
                let base64 = extract_binary_base64(&self.buffer);
                serde_json::Serializer::new(&mut self.serializer.writer).serialize_str(&base64)?;
                self.emitted_sanitized = true;
                self.state = MapState::Normal;
                return Ok(());
            }
            MapState::Normal => {}
        }

        // Check if this is the _id field at root level
        let is_id_field = self.is_root && self.current_key.as_deref() == Some("_id");

        if is_id_field {
            self.buffer.clear();

            let mut id_serializer = SanitizingSerializer::new(&mut self.buffer, false);
            value.serialize(&mut id_serializer)?;
            let id_str = stringify_id(&self.buffer);

            self.flush_pending_key()?;
            serde_json::Serializer::new(&mut self.serializer.writer).serialize_str(&id_str)?;
            self.entries += 1;
        } else {
            self.flush_pending_key()?;
            value.serialize(&mut *self.serializer)?;
            self.entries += 1;
        }
        Ok(())
    }

    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        // If we emitted a sanitized value (like converting $oid to string),
        // don't write the closing brace - we already emitted the complete value
        if self.emitted_sanitized {
            return Ok(());
        }
        // If we never wrote anything, this might be an empty map or we have pending data
        self.ensure_started()?;
        self.serializer.writer.write_all(b"}")?;
        Ok(())
    }
}

/// Extract milliseconds from datetime JSON value
fn extract_datetime_millis(buf: &[u8]) -> i64 {
    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(buf) {
        match v {
            serde_json::Value::Number(n) => n.as_i64().unwrap_or(0),
            serde_json::Value::Object(obj) => {
                // {"$numberLong": "millis"}
                if let Some(serde_json::Value::String(s)) = obj.get("$numberLong") {
                    s.parse().unwrap_or(0)
                } else {
                    0
                }
            }
            _ => 0,
        }
    } else {
        0
    }
}

/// Extract base64 string from binary JSON value
fn extract_binary_base64(buf: &[u8]) -> String {
    // Try to parse as JSON {"base64": "...", "subType": "..."}
    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(buf) {
        if let serde_json::Value::Object(obj) = v {
            if let Some(serde_json::Value::String(b64)) = obj.get("base64") {
                return b64.clone();
            }
        }
    }
    String::new()
}

/// Extract Decimal128 string from raw bytes array
fn extract_decimal128_from_bytes(buf: &[u8]) -> String {
    // Parse JSON array of bytes: [u8; 16]
    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(buf) {
        if let serde_json::Value::Array(arr) = v {
            if arr.len() == 16 {
                let mut bytes = [0u8; 16];
                for (i, val) in arr.iter().enumerate() {
                    if let Some(n) = val.as_u64() {
                        bytes[i] = n as u8;
                    }
                }
                let decimal = bson::Decimal128::from_bytes(bytes);
                return decimal.to_string();
            }
        }
    }
    "0".to_string()
}

/// Convert _id serde_json::Value to string.
/// Handles String, ObjectId, and Binary types.
/// Exported for benchmarking purposes.
pub fn id_to_string(value: serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s,
        serde_json::Value::Object(mut obj) => {
            // ObjectId: {"$oid": "hexstring"}
            if let Some(serde_json::Value::String(hex)) = obj.remove("$oid") {
                return hex;
            }
            // Binary: {"$binary": {"base64": "...", "subType": "..."}}
            if let Some(serde_json::Value::Object(bin)) = obj.remove("$binary") {
                if let Some(serde_json::Value::String(b64)) = bin.get("base64") {
                    return b64.clone();
                }
            }
            // Fallback: JSON-serialize the object
            serde_json::to_string(&serde_json::Value::Object(obj)).unwrap_or_default()
        }
        other => serde_json::to_string(&other).unwrap_or_default(),
    }
}

/// Stringify an _id value from JSON bytes for the root document.
/// Parses the JSON and extracts string representation from common _id types.
pub fn stringify_id(buf: &[u8]) -> String {
    serde_json::from_slice(buf)
        .map(id_to_string)
        .unwrap_or_default()
}

pub struct SanitizingSerializeStruct<'a, W: Write> {
    serializer: &'a mut SanitizingSerializer<W>,
    first: bool,
}

impl<'a, W: Write> SerializeStruct for SanitizingSerializeStruct<'a, W> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        if !self.first {
            self.serializer.writer.write_all(b",")?;
        }
        self.first = false;
        serde_json::Serializer::new(&mut self.serializer.writer).serialize_str(key)?;
        self.serializer.writer.write_all(b":")?;
        value.serialize(&mut *self.serializer)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.serializer.writer.write_all(b"}")?;
        Ok(())
    }
}

pub struct SanitizingSerializeStructVariant<'a, W: Write> {
    serializer: &'a mut SanitizingSerializer<W>,
    first: bool,
}

impl<'a, W: Write> SerializeStructVariant for SanitizingSerializeStructVariant<'a, W> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        if !self.first {
            self.serializer.writer.write_all(b",")?;
        }
        self.first = false;
        serde_json::Serializer::new(&mut self.serializer.writer).serialize_str(key)?;
        self.serializer.writer.write_all(b":")?;
        value.serialize(&mut *self.serializer)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.serializer.writer.write_all(b"}}")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn serialize_to_string<T: Serialize>(value: &T, is_root: bool) -> String {
        let mut buf = Vec::new();
        let mut serializer = SanitizingSerializer::new(&mut buf, is_root);
        value.serialize(&mut serializer).unwrap();
        String::from_utf8(buf).unwrap()
    }

    /// Test integration with actual BSON transcoding
    fn transcode_bson_to_sanitized_json(doc: &bson::Document, is_root: bool) -> String {
        let bson_bytes = doc.to_vec().unwrap();
        let raw_doc = bson::raw::RawDocument::from_bytes(&bson_bytes).unwrap();
        let deserializer = bson::RawDeserializer::new(raw_doc.as_bytes()).unwrap();
        let mut buf = Vec::new();
        let mut serializer = SanitizingSerializer::new(&mut buf, is_root);
        serde_transcode::transcode(deserializer, &mut serializer).unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn test_primitives() {
        assert_eq!(serialize_to_string(&true, false), "true");
        assert_eq!(serialize_to_string(&false, false), "false");
        assert_eq!(serialize_to_string(&42i32, false), "42");
        assert_eq!(serialize_to_string(&3.14f64, false), "3.14");
        assert_eq!(serialize_to_string(&"hello", false), "\"hello\"");
    }

    #[test]
    fn test_nan_infinity() {
        assert_eq!(serialize_to_string(&f64::NAN, false), "\"NaN\"");
        assert_eq!(serialize_to_string(&f64::INFINITY, false), "\"Infinity\"");
        assert_eq!(serialize_to_string(&f64::NEG_INFINITY, false), "\"-Infinity\"");
    }

    #[test]
    fn test_string_escaping() {
        assert_eq!(serialize_to_string(&"hello\nworld", false), "\"hello\\nworld\"");
        assert_eq!(serialize_to_string(&"tab\there", false), "\"tab\\there\"");
        assert_eq!(serialize_to_string(&"quote\"here", false), "\"quote\\\"here\"");
    }

    #[test]
    fn test_array() {
        let arr = vec![1, 2, 3];
        assert_eq!(serialize_to_string(&arr, false), "[1,2,3]");
    }

    #[test]
    fn test_simple_map() {
        use std::collections::HashMap;
        let mut map = HashMap::new();
        map.insert("key", "value");
        let result = serialize_to_string(&map, false);
        assert_eq!(result, "{\"key\":\"value\"}");
    }

    #[test]
    fn test_object_id_sanitization() {
        use std::collections::HashMap;
        let mut map = HashMap::new();
        map.insert("$oid", "507f1f77bcf86cd799439011");
        let result = serialize_to_string(&map, false);
        assert_eq!(result, "\"507f1f77bcf86cd799439011\"");
    }

    #[test]
    fn test_decimal128_sanitization() {
        use std::collections::HashMap;
        let mut map = HashMap::new();
        map.insert("$numberDecimal", "123.456");
        let result = serialize_to_string(&map, false);
        assert_eq!(result, "\"123.456\"");
    }

    #[test]
    fn test_datetime_sanitization() {
        use serde_json::json;
        let value = json!({"$date": {"$numberLong": "1234567890000"}});
        let result = serialize_to_string(&value, false);
        assert_eq!(result, "\"2009-02-13T23:31:30.000Z\"");
    }

    #[test]
    fn test_binary_sanitization() {
        use serde_json::json;
        let value = json!({"$binary": {"base64": "SGVsbG8gV29ybGQ=", "subType": "00"}});
        let result = serialize_to_string(&value, false);
        assert_eq!(result, "\"SGVsbG8gV29ybGQ=\"");
    }

    #[test]
    fn test_nested_document() {
        use serde_json::json;
        let value = json!({
            "name": "test",
            "created": {"$date": {"$numberLong": "1234567890000"}},
            "ref": {"$oid": "507f1f77bcf86cd799439011"}
        });
        let result = serialize_to_string(&value, false);
        // Note: HashMap ordering is not guaranteed, so we parse and compare
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["name"], "test");
        assert_eq!(parsed["created"], "2009-02-13T23:31:30.000Z");
        assert_eq!(parsed["ref"], "507f1f77bcf86cd799439011");
    }

    #[test]
    fn test_id_stringification_at_root() {
        use serde_json::json;
        let value = json!({
            "_id": {"$oid": "507f1f77bcf86cd799439011"},
            "name": "test"
        });
        let result = serialize_to_string(&value, true);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["_id"], "507f1f77bcf86cd799439011");
        assert_eq!(parsed["name"], "test");
    }

    #[test]
    fn test_id_not_stringified_when_not_root() {
        use serde_json::json;
        let value = json!({
            "_id": {"$oid": "507f1f77bcf86cd799439011"},
            "name": "test"
        });
        let result = serialize_to_string(&value, false);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        // When not root, _id should remain as extended JSON (but $oid itself gets sanitized)
        // Actually, the nested {"$oid": ...} will be sanitized anyway
        assert_eq!(parsed["_id"], "507f1f77bcf86cd799439011");
    }

    #[test]
    fn test_binary_id_stringification_at_root() {
        use bson::{doc, Binary, spec::BinarySubtype};

        // Create a document with Binary _id
        let binary_id = Binary {
            subtype: BinarySubtype::Generic,
            bytes: b"pk val 0".to_vec(),
        };
        let doc = doc! {
            "_id": binary_id,
            "name": "test"
        };

        let result = transcode_bson_to_sanitized_json(&doc, true);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();

        // Binary _id should be stringified as base64
        // "pk val 0" in base64 is "cGsgdmFsIDA="
        assert_eq!(parsed["_id"], "cGsgdmFsIDA=");
        assert_eq!(parsed["name"], "test");
    }

    // ==========================================================================
    // Integration tests with actual BSON data
    // ==========================================================================

    #[test]
    fn test_bson_integration_simple() {
        use bson::{doc, oid::ObjectId};
        let oid = ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap();
        let doc = doc! {
            "_id": oid,
            "name": "test",
            "count": 42
        };
        let result = transcode_bson_to_sanitized_json(&doc, true);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["_id"], "507f1f77bcf86cd799439011");
        assert_eq!(parsed["name"], "test");
        assert_eq!(parsed["count"], 42);
    }

    #[test]
    fn test_bson_integration_datetime() {
        use bson::{doc, DateTime};
        let dt = DateTime::from_millis(1234567890000);
        let doc = doc! {
            "created_at": dt,
            "name": "test"
        };
        let result = transcode_bson_to_sanitized_json(&doc, false);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["created_at"], "2009-02-13T23:31:30.000Z");
        assert_eq!(parsed["name"], "test");
    }

    #[test]
    fn test_bson_integration_decimal128() {
        use bson::{doc, Decimal128};
        use std::str::FromStr;

        let decimal = Decimal128::from_str("123.456").unwrap();
        let doc = doc! {
            "price": decimal,
            "name": "product"
        };

        // New approach
        let result = transcode_bson_to_sanitized_json(&doc, false);

        // Our serializer converts $numberDecimalBytes (raw format) to string
        let new_parsed: serde_json::Value = serde_json::from_str(&result).unwrap();

        // New approach should convert $numberDecimalBytes to string
        assert_eq!(new_parsed["price"], "123.456");
        assert_eq!(new_parsed["name"], "product");
    }

    #[test]
    fn test_bson_integration_binary() {
        use bson::{doc, Binary, spec::BinarySubtype};

        let binary = Binary {
            subtype: BinarySubtype::Generic,
            bytes: b"Hello World".to_vec(),
        };
        let doc = doc! {
            "data": binary,
            "name": "test"
        };

        // New approach: our serializer should convert raw bytes via serialize_bytes to base64
        let result = transcode_bson_to_sanitized_json(&doc, false);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();

        // BSON Binary with RawDeserializer produces raw bytes via serialize_bytes
        // Our serializer converts bytes to base64 string
        // "Hello World" in base64 is "SGVsbG8gV29ybGQ="
        assert_eq!(parsed["data"], "SGVsbG8gV29ybGQ=");
        assert_eq!(parsed["name"], "test");
    }

    #[test]
    fn test_bson_integration_nested_document() {
        use bson::{doc, oid::ObjectId, DateTime};
        let oid = ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap();
        let dt = DateTime::from_millis(1234567890000);
        let doc = doc! {
            "_id": oid,
            "name": "test",
            "metadata": {
                "created_at": dt,
                "tags": ["a", "b", "c"]
            }
        };
        let result = transcode_bson_to_sanitized_json(&doc, true);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["_id"], "507f1f77bcf86cd799439011");
        assert_eq!(parsed["name"], "test");
        assert_eq!(parsed["metadata"]["created_at"], "2009-02-13T23:31:30.000Z");
        assert_eq!(parsed["metadata"]["tags"], serde_json::json!(["a", "b", "c"]));
    }

    #[test]
    fn test_bson_integration_array_of_object_ids() {
        use bson::{doc, oid::ObjectId};
        let oid1 = ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap();
        let oid2 = ObjectId::parse_str("507f1f77bcf86cd799439012").unwrap();
        let doc = doc! {
            "refs": [oid1, oid2]
        };
        let result = transcode_bson_to_sanitized_json(&doc, false);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["refs"][0], "507f1f77bcf86cd799439011");
        assert_eq!(parsed["refs"][1], "507f1f77bcf86cd799439012");
    }

    // ==========================================================================
    // id_to_string tests (moved from sanitize.rs)
    // ==========================================================================

    #[test]
    fn test_id_to_string_string() {
        use serde_json::json;
        let input = json!("my-string-id");
        assert_eq!(id_to_string(input), "my-string-id");
    }

    #[test]
    fn test_id_to_string_object_id() {
        use serde_json::json;
        let input = json!({"$oid": "507f1f77bcf86cd799439011"});
        assert_eq!(id_to_string(input), "507f1f77bcf86cd799439011");
    }

    #[test]
    fn test_id_to_string_binary() {
        use serde_json::json;
        let input = json!({"$binary": {"base64": "cGsgdmFsIDA=", "subType": "00"}});
        assert_eq!(id_to_string(input), "cGsgdmFsIDA=");
    }
}
