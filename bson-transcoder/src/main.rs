mod sanitize;

use bson::raw::{RawBsonRef, RawDocument};
use bson::Document;
use serde_json::{Map, Value};
use std::io::{self, BufReader, ErrorKind, Read, Write};
use thiserror::Error;

use sanitize::{id_to_string, sanitize_document};

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("BSON error: {0}")]
    Bson(#[from] bson::error::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Missing field: {0}")]
    MissingField(&'static str),
    #[error("Expected object")]
    ExpectedObject,
    #[error("Unknown operation: {0}")]
    UnknownOperation(String),
    #[error("Fragment error: {0}")]
    Fragment(String),
}

type Result<T> = std::result::Result<T, Error>;

/// Change event with borrowed references (for non-split events, zero-copy)
struct ChangeEvent<'a> {
    operation_type: &'a str,
    database: &'a str,
    collection: &'a str,
    full_document: Option<&'a RawDocument>,
    full_document_before_change: Option<&'a RawDocument>,
    document_key_id: RawBsonRef<'a>,
}

impl<'a> ChangeEvent<'a> {
    fn from_raw(doc: &'a RawDocument) -> Result<Self> {
        let op_type = doc
            .get_str("operationType")
            .map_err(|_| Error::MissingField("operationType"))?;
        let ns = doc
            .get_document("ns")
            .map_err(|_| Error::MissingField("ns"))?;
        let db = ns.get_str("db").map_err(|_| Error::MissingField("ns.db"))?;
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
}

/// Change event with owned data (for split/merged events)
struct ChangeEventOwned {
    operation_type: String,
    database: String,
    collection: String,
    full_document: Option<Document>,
    full_document_before_change: Option<Document>,
    document_key: Document,
}

impl ChangeEventOwned {
    fn from_document(doc: &Document) -> Result<Self> {
        let operation_type = doc
            .get_str("operationType")
            .map_err(|_| Error::MissingField("operationType"))?
            .to_string();

        let ns = doc
            .get_document("ns")
            .map_err(|_| Error::MissingField("ns"))?;
        let database = ns
            .get_str("db")
            .map_err(|_| Error::MissingField("ns.db"))?
            .to_string();
        let collection = ns
            .get_str("coll")
            .map_err(|_| Error::MissingField("ns.coll"))?
            .to_string();

        let full_document = doc.get_document("fullDocument").ok().cloned();
        let full_document_before_change = doc.get_document("fullDocumentBeforeChange").ok().cloned();

        let document_key = doc
            .get_document("documentKey")
            .map_err(|_| Error::MissingField("documentKey"))?
            .clone();

        Ok(Self {
            operation_type,
            database,
            collection,
            full_document,
            full_document_before_change,
            document_key,
        })
    }
}

/// Read a length-prefixed BSON document from the reader.
/// Returns None on EOF.
fn read_bson_document(reader: &mut impl Read) -> Result<Option<Vec<u8>>> {
    // Read 4-byte length (little-endian)
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_le_bytes(len_buf) as usize;

    // Read full document (length includes the 4-byte prefix)
    let mut doc = vec![0u8; len];
    doc[0..4].copy_from_slice(&len_buf);
    reader.read_exact(&mut doc[4..])?;
    Ok(Some(doc))
}

/// Check if a document is a split event fragment, returns (fragment, of)
fn get_split_info(doc: &RawDocument) -> Option<(i32, i32)> {
    if let Ok(split_doc) = doc.get_document("splitEvent") {
        let fragment = split_doc.get_i32("fragment").ok()?;
        let of = split_doc.get_i32("of").ok()?;
        Some((fragment, of))
    } else {
        None
    }
}

/// State for accumulating split event fragments
struct FragmentAccumulator {
    /// The merged document being built from fragments
    merged: Document,
    /// The current fragment number we've received
    current_fragment: i32,
    /// The total number of fragments expected
    total_fragments: i32,
}

impl FragmentAccumulator {
    /// Start accumulating a new split event
    fn new(first_doc_bytes: &[u8], total_fragments: i32) -> Result<Self> {
        let first_doc = Document::from_reader(&mut &first_doc_bytes[..])?;
        let mut merged = Document::new();

        // Merge fields from first fragment (excluding splitEvent)
        for (key, value) in first_doc.iter() {
            if key != "splitEvent" {
                merged.insert(key.to_string(), value.clone());
            }
        }

        Ok(Self {
            merged,
            current_fragment: 1,
            total_fragments,
        })
    }

    /// Add a fragment to the accumulator. Returns true if all fragments are received.
    fn add_fragment(&mut self, doc_bytes: &[u8], fragment: i32, of: i32) -> Result<bool> {
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

        // Merge fields from this fragment (excluding splitEvent)
        let fragment_doc = Document::from_reader(&mut &doc_bytes[..])?;
        for (key, value) in fragment_doc.iter() {
            if key != "splitEvent" {
                self.merged.insert(key.to_string(), value.clone());
            }
        }

        self.current_fragment = fragment;

        // Return true if we've received all fragments
        Ok(self.current_fragment == self.total_fragments)
    }

    /// Take the merged document (consumes the accumulator)
    fn take(self) -> Document {
        self.merged
    }
}

/// Transcode a raw BSON document to a serde_json::Value.
/// This produces Extended JSON format.
fn bson_to_json(doc: &RawDocument) -> Result<Value> {
    let deserializer = bson::RawDeserializer::new(doc.as_bytes())?;
    let value: Value = serde_transcode::transcode(deserializer, serde_json::value::Serializer)?;
    Ok(value)
}

/// Transcode a raw BSON value to a serde_json::Value.
fn bson_value_to_json(value: RawBsonRef) -> Result<Value> {
    let bson_value: bson::Bson = value.try_into().map_err(|e: bson::error::Error| e)?;
    let json_value = serde_json::to_value(&bson_value)?;
    Ok(json_value)
}

/// Transcode an owned BSON Document to a serde_json::Value using serde_transcode.
fn document_to_json(doc: &Document) -> Result<Value> {
    let bson_bytes = doc.to_vec()?;
    let deserializer = bson::RawDeserializer::new(&bson_bytes)?;
    let value: Value = serde_transcode::transcode(deserializer, serde_json::value::Serializer)?;
    Ok(value)
}

/// Transcode an owned BSON value to a serde_json::Value.
fn owned_bson_value_to_json(value: &bson::Bson) -> Result<Value> {
    let json_value = serde_json::to_value(value)?;
    Ok(json_value)
}

// ============================================================================
// Non-split event processing (zero-copy, optimal performance)
// ============================================================================

fn build_output_document(event: &ChangeEvent) -> Result<Value> {
    let mut output = match event.operation_type {
        "insert" | "update" | "replace" => {
            let full_doc = event
                .full_document
                .ok_or(Error::MissingField("fullDocument"))?;
            let json = bson_to_json(full_doc)?;
            let obj = json.as_object().ok_or(Error::ExpectedObject)?;
            sanitize_document(obj.clone())
        }
        "delete" => {
            let id_json = bson_value_to_json(event.document_key_id)?;
            let id_str = id_to_string(id_json);
            let mut obj = Map::new();
            obj.insert("_id".to_string(), Value::String(id_str));
            obj
        }
        _ => return Err(Error::UnknownOperation(event.operation_type.to_string())),
    };

    // Build _meta field
    let meta = build_meta(event)?;
    output.insert("_meta".to_string(), meta);

    Ok(Value::Object(output))
}

fn build_meta(event: &ChangeEvent) -> Result<Value> {
    let op = match event.operation_type {
        "insert" => "c",
        "update" | "replace" => "u",
        "delete" => "d",
        _ => return Err(Error::UnknownOperation(event.operation_type.to_string())),
    };

    let mut meta = Map::new();
    meta.insert("op".to_string(), Value::String(op.to_string()));

    let mut source = Map::new();
    source.insert("db".to_string(), Value::String(event.database.to_string()));
    source.insert(
        "collection".to_string(),
        Value::String(event.collection.to_string()),
    );
    meta.insert("source".to_string(), Value::Object(source));

    // Handle fullDocumentBeforeChange if present
    if let Some(before) = event.full_document_before_change {
        let before_json = bson_to_json(before)?;
        if let Some(before_obj) = before_json.as_object() {
            let sanitized_before = sanitize_document(before_obj.clone());
            meta.insert("before".to_string(), Value::Object(sanitized_before));
        }
    }

    Ok(Value::Object(meta))
}

// ============================================================================
// Split/merged event processing (uses owned Document)
// ============================================================================

fn build_output_document_owned(event: &ChangeEventOwned) -> Result<Value> {
    let mut output = match event.operation_type.as_str() {
        "insert" | "update" | "replace" => {
            let full_doc = event
                .full_document
                .as_ref()
                .ok_or(Error::MissingField("fullDocument"))?;
            let json = document_to_json(full_doc)?;
            let obj = json.as_object().ok_or(Error::ExpectedObject)?;
            sanitize_document(obj.clone())
        }
        "delete" => {
            let id = event
                .document_key
                .get("_id")
                .ok_or(Error::MissingField("documentKey._id"))?;
            let id_json = owned_bson_value_to_json(id)?;
            let id_str = id_to_string(id_json);
            let mut obj = Map::new();
            obj.insert("_id".to_string(), Value::String(id_str));
            obj
        }
        _ => return Err(Error::UnknownOperation(event.operation_type.clone())),
    };

    // Build _meta field
    let meta = build_meta_owned(event)?;
    output.insert("_meta".to_string(), meta);

    Ok(Value::Object(output))
}

fn build_meta_owned(event: &ChangeEventOwned) -> Result<Value> {
    let op = match event.operation_type.as_str() {
        "insert" => "c",
        "update" | "replace" => "u",
        "delete" => "d",
        _ => return Err(Error::UnknownOperation(event.operation_type.clone())),
    };

    let mut meta = Map::new();
    meta.insert("op".to_string(), Value::String(op.to_string()));

    let mut source = Map::new();
    source.insert("db".to_string(), Value::String(event.database.clone()));
    source.insert(
        "collection".to_string(),
        Value::String(event.collection.clone()),
    );
    meta.insert("source".to_string(), Value::Object(source));

    // Handle fullDocumentBeforeChange if present
    if let Some(before) = &event.full_document_before_change {
        let before_json = document_to_json(before)?;
        if let Some(before_obj) = before_json.as_object() {
            let sanitized_before = sanitize_document(before_obj.clone());
            meta.insert("before".to_string(), Value::Object(sanitized_before));
        }
    }

    Ok(Value::Object(meta))
}

// ============================================================================
// Event filtering and skip response
// ============================================================================

/// Check if an operation type should be processed (produces a document)
fn is_document_operation(op_type: &str) -> bool {
    matches!(op_type, "insert" | "update" | "replace" | "delete")
}

/// Build a skip response for events that should be filtered out
fn build_skip_response(reason: &str, db: Option<&str>, collection: Option<&str>) -> Value {
    let mut obj = Map::new();
    obj.insert("_skip".to_string(), Value::Bool(true));
    obj.insert("reason".to_string(), Value::String(reason.to_string()));
    if let Some(db) = db {
        obj.insert("db".to_string(), Value::String(db.to_string()));
    }
    if let Some(coll) = collection {
        obj.insert("collection".to_string(), Value::String(coll.to_string()));
    }
    Value::Object(obj)
}

/// Build a skip response for a split event fragment
fn build_fragment_skip_response(fragment: i32, of: i32) -> Value {
    let mut obj = Map::new();
    obj.insert("_skip".to_string(), Value::Bool(true));
    obj.insert(
        "reason".to_string(),
        Value::String(format!("split event fragment {}/{}", fragment, of)),
    );
    obj.insert("_fragment".to_string(), Value::Number(fragment.into()));
    obj.insert("_fragmentOf".to_string(), Value::Number(of.into()));
    Value::Object(obj)
}

/// Process a non-split event (zero-copy path)
fn process_event(raw_doc: &RawDocument) -> Result<Value> {
    // Get operation type first to filter
    let op_type = raw_doc
        .get_str("operationType")
        .map_err(|_| Error::MissingField("operationType"))?;

    // Get namespace for logging/skip response
    let (db, coll) = if let Ok(ns) = raw_doc.get_document("ns") {
        (ns.get_str("db").ok(), ns.get_str("coll").ok())
    } else {
        (None, None)
    };

    // Filter non-document operations
    if !is_document_operation(op_type) {
        return Ok(build_skip_response(
            &format!("operation type: {}", op_type),
            db,
            coll,
        ));
    }

    // For non-delete operations, check if fullDocument is present
    if op_type != "delete" {
        if raw_doc.get_document("fullDocument").is_err() {
            return Ok(build_skip_response("no fullDocument", db, coll));
        }
    }

    // Parse and build output document
    let event = ChangeEvent::from_raw(raw_doc)?;
    build_output_document(&event)
}

/// Process a split/merged event (owned path)
fn process_event_owned(doc: &Document) -> Result<Value> {
    // Get operation type first to filter
    let op_type = doc
        .get_str("operationType")
        .map_err(|_| Error::MissingField("operationType"))?;

    // Get namespace for logging/skip response
    let (db, coll) = if let Ok(ns) = doc.get_document("ns") {
        (
            ns.get_str("db").ok().map(|s| s.to_string()),
            ns.get_str("coll").ok().map(|s| s.to_string()),
        )
    } else {
        (None, None)
    };

    // Filter non-document operations
    if !is_document_operation(op_type) {
        return Ok(build_skip_response(
            &format!("operation type: {}", op_type),
            db.as_deref(),
            coll.as_deref(),
        ));
    }

    // For non-delete operations, check if fullDocument is present
    if op_type != "delete" {
        if doc.get_document("fullDocument").is_err() {
            return Ok(build_skip_response(
                "no fullDocument",
                db.as_deref(),
                coll.as_deref(),
            ));
        }
    }

    // Parse and build output document
    let event = ChangeEventOwned::from_document(doc)?;
    build_output_document_owned(&event)
}

// ============================================================================
// Main loop
// ============================================================================

fn main() -> Result<()> {
    let stdin = io::stdin().lock();
    let mut stdout = io::stdout().lock();
    let mut reader = BufReader::new(stdin);

    // State for accumulating split event fragments
    let mut accumulator: Option<FragmentAccumulator> = None;

    while let Some(doc_bytes) = read_bson_document(&mut reader)? {
        let raw_doc = RawDocument::from_bytes(&doc_bytes)?;

        // Check if this is a split event fragment
        let output = if let Some((fragment, of)) = get_split_info(raw_doc) {
            if fragment == 1 {
                // First fragment of a new split event
                if accumulator.is_some() {
                    return Err(Error::Fragment(
                        "received new split event while still accumulating previous".to_string(),
                    ));
                }
                accumulator = Some(FragmentAccumulator::new(&doc_bytes, of)?);
                // Emit a skip response indicating we're waiting for more fragments
                build_fragment_skip_response(fragment, of)
            } else {
                // Subsequent fragment
                let acc = accumulator.as_mut().ok_or_else(|| {
                    Error::Fragment(format!(
                        "received fragment {} without first fragment",
                        fragment
                    ))
                })?;

                let complete = acc.add_fragment(&doc_bytes, fragment, of)?;

                if complete {
                    // All fragments received, process the merged document
                    let merged_doc = accumulator.take().unwrap().take();
                    let mut output = process_event_owned(&merged_doc)?;
                    // Mark this as completing a split event so Go knows to yield
                    if let Some(obj) = output.as_object_mut() {
                        obj.insert("_completedSplitEvent".to_string(), Value::Bool(true));
                    }
                    output
                } else {
                    // Still waiting for more fragments
                    build_fragment_skip_response(fragment, of)
                }
            }
        } else {
            // Non-split event
            if accumulator.is_some() {
                return Err(Error::Fragment(
                    "received non-split event while accumulating fragments".to_string(),
                ));
            }
            // Use zero-copy path for optimal performance
            process_event(raw_doc)?
        };

        serde_json::to_writer(&mut stdout, &output)?;
        stdout.write_all(b"\n")?;
        stdout.flush()?;
    }

    Ok(())
}
