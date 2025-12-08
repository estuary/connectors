//! Change event processing for MongoDB change streams.
//!
//! This module handles parsing and transforming MongoDB change events
//! into the output JSON format, including support for split events.

use bson::raw::{RawBsonRef, RawDocument};

use crate::serializer::{stringify_id, SanitizingSerializer};
use crate::{Error, Result};

/// Change event with borrowed references (zero-copy).
pub struct ChangeEvent<'a> {
    pub operation_type: &'a str,
    pub database: &'a str,
    pub collection: &'a str,
    pub full_document: Option<&'a RawDocument>,
    pub full_document_before_change: Option<&'a RawDocument>,
    pub document_key_id: RawBsonRef<'a>,
}

impl<'a> ChangeEvent<'a> {
    /// Create a ChangeEvent from a raw BSON document.
    pub fn from_raw(doc: &'a RawDocument) -> Result<Self> {
        let op_type = doc
            .get_str("operationType")
            .map_err(|_| Error::MissingField("operationType"))?;
        let ns = doc
            .get_document("ns")
            .map_err(|_| Error::MissingField("ns"))?;
        let db = ns
            .get_str("db")
            .map_err(|_| Error::MissingField("ns.db"))?;
        let coll = ns
            .get_str("coll")
            .map_err(|_| Error::MissingField("ns.coll"))?;
        let full_doc = doc.get_document("fullDocument").ok();
        let before = doc.get_document("fullDocumentBeforeChange").ok();
        let doc_key = doc
            .get_document("documentKey")
            .map_err(|_| Error::MissingField("documentKey"))?;
        let id = doc_key
            .get("_id")
            .map_err(|_| Error::MissingField("documentKey._id"))?
            .ok_or(Error::MissingField("documentKey._id"))?;

        Ok(Self {
            operation_type: op_type,
            database: db,
            collection: coll,
            full_document: full_doc,
            full_document_before_change: before,
            document_key_id: id,
        })
    }

    /// Create a ChangeEvent from accumulated split event fragments.
    /// Fields are looked up lazily across all fragments.
    pub fn from_accumulator(acc: &'a FragmentAccumulator) -> Result<Self> {
        let op_type = acc.get_str("operationType")?;
        let ns = acc.get_document("ns")?;
        let db = ns
            .get_str("db")
            .map_err(|_| Error::MissingField("ns.db"))?;
        let coll = ns
            .get_str("coll")
            .map_err(|_| Error::MissingField("ns.coll"))?;
        let full_doc = acc.get_document_opt("fullDocument");
        let before = acc.get_document_opt("fullDocumentBeforeChange");
        let doc_key = acc.get_document("documentKey")?;
        let id = doc_key
            .get("_id")
            .map_err(|_| Error::MissingField("documentKey._id"))?
            .ok_or(Error::MissingField("documentKey._id"))?;

        Ok(Self {
            operation_type: op_type,
            database: db,
            collection: coll,
            full_document: full_doc,
            full_document_before_change: before,
            document_key_id: id,
        })
    }
}

/// Check if a document is a split event fragment, returns (fragment, of)
pub fn get_split_info(doc: &RawDocument) -> Option<(i32, i32)> {
    if let Ok(split_doc) = doc.get_document("splitEvent") {
        let fragment = split_doc.get_i32("fragment").ok()?;
        let of = split_doc.get_i32("of").ok()?;
        Some((fragment, of))
    } else {
        None
    }
}

/// State for accumulating split event fragments.
/// Stores raw bytes for each fragment to enable zero-copy field access via RawDocument.
pub struct FragmentAccumulator {
    /// Raw bytes for each fragment (kept alive for RawDocument access)
    fragments: Vec<Vec<u8>>,
    /// The current fragment number we've received
    current_fragment: i32,
    /// The total number of fragments expected
    total_fragments: i32,
}

impl FragmentAccumulator {
    /// Start accumulating a new split event
    pub fn new(first_doc_bytes: Vec<u8>, total_fragments: i32) -> Self {
        Self {
            fragments: vec![first_doc_bytes],
            current_fragment: 1,
            total_fragments,
        }
    }

    /// Add a fragment to the accumulator. Returns true if all fragments are received.
    pub fn add_fragment(&mut self, doc_bytes: Vec<u8>, fragment: i32, of: i32) -> Result<bool> {
        let expected_fragment = self.current_fragment + 1;

        if fragment != expected_fragment {
            return Err(Error::Fragment(format!(
                "expected fragment {} but got {}",
                expected_fragment, fragment
            )));
        }
        if of != self.total_fragments {
            return Err(Error::Fragment(format!(
                "fragment count mismatch: expected {} but got {}",
                self.total_fragments, of
            )));
        }

        self.fragments.push(doc_bytes);
        self.current_fragment = fragment;

        Ok(self.current_fragment == self.total_fragments)
    }

    /// Lazily get a string field from any fragment that has the key
    pub fn get_str(&self, field: &'static str) -> Result<&str> {
        for bytes in &self.fragments {
            let doc = RawDocument::from_bytes(bytes)?;
            if let Ok(s) = doc.get_str(field) {
                return Ok(s);
            }
        }
        Err(Error::MissingField(field))
    }

    /// Lazily get a raw field from any fragment that has the key
    pub fn get_document(&self, field: &'static str) -> Result<&RawDocument> {
        for bytes in &self.fragments {
            let doc = RawDocument::from_bytes(bytes)?;
            if let Ok(d) = doc.get_document(field) {
                return Ok(d);
            }
        }
        Err(Error::MissingField(field))
    }

    /// Lazily get an optional raw field from any fragment that has the key
    pub fn get_document_opt(&self, field: &str) -> Option<&RawDocument> {
        for bytes in &self.fragments {
            if let Ok(doc) = RawDocument::from_bytes(bytes) {
                if let Ok(d) = doc.get_document(field) {
                    return Some(d);
                }
            }
        }
        None
    }
}

pub fn is_document_operation(op_type: &str) -> bool {
    matches!(op_type, "insert" | "update" | "replace" | "delete")
}

/// Build output document for a change event and write directly to buffer.
pub fn process_change_event(event: &ChangeEvent, buf: &mut Vec<u8>) -> Result<()> {
    buf.clear();

    // Capture the before document reference for the closure
    let before_doc = event.full_document_before_change;

    match event.operation_type {
        "insert" | "update" | "replace" => {
            let full_doc = event
                .full_document
                .ok_or(Error::MissingField("fullDocument"))?;

            buf.push(b'{');

            let mut doc_buf = Vec::new();
            bson_to_sanitized_json_bytes(full_doc, true, &mut doc_buf)?;

            if doc_buf.len() < 2 || doc_buf[0] != b'{' && doc_buf[doc_buf.len() - 1] != b'}' {
                return Err(Error::ExpectedObject(String::from_utf8_lossy(&doc_buf).to_string()));
            }

            // We need to strip the outer braces and inject _meta
            buf.extend_from_slice(&doc_buf[1..doc_buf.len() - 1]);

            // Add _meta field
            buf.extend_from_slice(b",\"_meta\":");
            build_meta(
                buf,
                event.operation_type,
                event.database,
                event.collection,
                before_doc.map(|doc| |b: &mut Vec<u8>| bson_to_sanitized_json_bytes(doc, true, b)),
            )?;

            buf.push(b'}');
        }
        "delete" => {
            let mut id_buf = Vec::new();
            let id_str = stringify_raw_id(event.document_key_id, &mut id_buf)?;

            buf.push(b'{');
            buf.extend_from_slice(b"\"_id\":");
            serde_json::to_writer(&mut *buf, &id_str)?;
            buf.extend_from_slice(b",\"_meta\":");
            build_meta(
                buf,
                event.operation_type,
                event.database,
                event.collection,
                before_doc.map(|doc| |b: &mut Vec<u8>| bson_to_sanitized_json_bytes(doc, true, b)),
            )?;
            buf.push(b'}');
        }
        _ => return Err(Error::UnknownOperation(event.operation_type.to_string())),
    }

    Ok(())
}

/// Process a raw document (not a change event) and write sanitized JSON to buffer.
/// This is used for backfill documents where we receive raw BSON documents
/// with database/collection metadata.
///
/// Output format:
/// {
///   "_id": "<stringified>",
///   ...other fields sanitized...,
///   "_meta": {"op": "c", "source": {"db": "...", "collection": "...", "snapshot": true}}
/// }
pub fn process_raw_document(
    doc: &RawDocument,
    database: &str,
    collection: &str,
    buf: &mut Vec<u8>,
) -> Result<()> {
    use serde::de::Error as DeError;

    buf.clear();
    buf.push(b'{');

    let deserializer = bson::RawDeserializer::new(doc.as_bytes())?;
    let mut temp_buf = Vec::new();
    let mut serializer = SanitizingSerializer::new(&mut temp_buf, true); // true = stringify _id
    serde_transcode::transcode(deserializer, &mut serializer)
        .map_err(|e| Error::Json(serde_json::Error::custom(&e.to_string())))?;

    if temp_buf.len() < 2 || temp_buf[0] != b'{' && temp_buf[temp_buf.len() - 1] != b'}' {
        return Err(Error::ExpectedObject(String::from_utf8_lossy(&temp_buf).to_string()));
    }

    // We need to strip the outer braces and append our _meta field
    buf.extend_from_slice(&temp_buf[1..temp_buf.len() - 1]);

    // Add _meta field
    buf.extend_from_slice(b",\"_meta\":");
    write_meta_start(buf, "c", database, collection);
    buf.extend_from_slice(b",\"snapshot\":true}");

    buf.push(b'}');
    Ok(())
}

/// Transcode a raw BSON document directly to sanitized JSON bytes.
/// This combines BSON parsing, extended JSON conversion, and sanitization in a single pass.
fn bson_to_sanitized_json_bytes(doc: &RawDocument, is_root: bool, buf: &mut Vec<u8>) -> Result<()> {
    let deserializer = bson::RawDeserializer::new(doc.as_bytes())?;
    let mut serializer = SanitizingSerializer::new(buf, is_root);
    serde_transcode::transcode(deserializer, &mut serializer)?;
    Ok(())
}

/// Stringify a raw BSON _id value using the same path as SanitizingSerializer.
/// Converts the value to sanitized JSON, then extracts the string representation.
fn stringify_raw_id(value: RawBsonRef, buf: &mut Vec<u8>) -> Result<String> {
    let bson_value: bson::Bson = value.try_into().map_err(|e: bson::error::Error| e)?;
    buf.clear();
    let mut serializer = SanitizingSerializer::new(&mut *buf, false);
    serde::Serialize::serialize(&bson_value, &mut serializer)?;
    Ok(stringify_id(buf))
}

/// Escapes quote and backslash characters.
fn escape_json_to_buf(buf: &mut Vec<u8>, s: &str) {
    for b in s.as_bytes() {
        match *b {
            b'"' => buf.extend_from_slice(b"\\\""),
            b'\\' => buf.extend_from_slice(b"\\\\"),
            b => buf.push(b),
        }
    }
}

/// Map operation type to short form. Returns None for unknown operations.
fn operation_to_short(op_type: &str) -> Option<&'static str> {
    match op_type {
        "insert" => Some("c"),
        "update" | "replace" => Some("u"),
        "delete" => Some("d"),
        _ => None,
    }
}

/// Build the common parts of _meta JSON: {"op":"X","source":{"db":"...","collection":"..."}}
/// Does NOT close the object - caller must add any additional fields and the closing brace.
fn write_meta_start(buf: &mut Vec<u8>, op: &str, database: &str, collection: &str) {
    buf.push(b'{');
    buf.extend_from_slice(b"\"op\":\"");
    buf.extend_from_slice(op.as_bytes());
    buf.extend_from_slice(b"\",\"source\":{\"db\":\"");
    escape_json_to_buf(buf, database);
    buf.extend_from_slice(b"\",\"collection\":\"");
    escape_json_to_buf(buf, collection);
    buf.extend_from_slice(b"\"}");
}

/// Build _meta object and write to buffer.
/// The `serialize_before` closure is called if present to serialize the "before" document.
fn build_meta<F>(
    buf: &mut Vec<u8>,
    op_type: &str,
    database: &str,
    collection: &str,
    serialize_before: Option<F>,
) -> Result<()>
where
    F: FnOnce(&mut Vec<u8>) -> Result<()>,
{
    let op =
        operation_to_short(op_type).ok_or_else(|| Error::UnknownOperation(op_type.to_string()))?;

    write_meta_start(buf, op, database, collection);

    if let Some(serialize) = serialize_before {
        buf.extend_from_slice(b",\"before\":");
        serialize(buf)?;
    }

    buf.push(b'}');
    Ok(())
}
