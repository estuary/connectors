//! Binary protocol for communication between Go and the transcoder.
//!
//! This module handles reading input messages and writing output messages
//! using a length-prefixed binary protocol.

use std::io::{self, ErrorKind, Read, Write};

use crate::{Error, Result, PAYLOAD_BUFFER};

// Binary protocol output message type discriminators
const MSG_TYPE_SKIP: u8 = 0x01;
const MSG_TYPE_DOCUMENT: u8 = 0x02;

// Binary protocol input message type discriminators
const INPUT_TYPE_CHANGE_EVENT: u8 = 0x01;
const INPUT_TYPE_RAW_DOCUMENT: u8 = 0x02;

// Document message flags
const FLAG_COMPLETED_SPLIT_EVENT: u8 = 0x01;

/// Result of reading an input message from the protocol stream.
pub enum ReadResult {
    /// A change stream event BSON document
    ChangeEvent(Vec<u8>),
    /// A raw document with metadata (bson_bytes, database, collection)
    /// from backfill or batch captures
    RawDocument(Vec<u8>, String, String),
    /// A flush signal (len == 0) was received
    Flush,
    /// End of file
    Eof,
}

/// Read a message from the input stream.
///
/// Protocol:
/// - Length = 0: Flush signal
/// - Length > 0: [len: u32 LE][type: u8][payload...]
///
/// Message types:
/// - INPUT_TYPE_CHANGE_EVENT (0x01): [type][bson_doc]
/// - INPUT_TYPE_RAW_DOCUMENT (0x02): [type][db_len: u8][db][coll_len: u8][coll][bson_doc]
pub fn read_input_message(reader: &mut impl Read) -> Result<ReadResult> {
    // Read 4-byte length (little-endian)
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(ReadResult::Eof),
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_le_bytes(len_buf) as usize;

    // len == 0 is a signal to flush stdout
    if len == 0 {
        return Ok(ReadResult::Flush);
    }

    // Read message type byte
    let mut type_buf = [0u8; 1];
    reader.read_exact(&mut type_buf)?;
    let msg_type = type_buf[0];

    match msg_type {
        INPUT_TYPE_CHANGE_EVENT => {
            // Change event: remaining bytes are BSON document
            let bson_len = len - 1; // subtract type byte
            let mut doc = vec![0u8; bson_len];
            reader.read_exact(&mut doc)?;
            Ok(ReadResult::ChangeEvent(doc))
        }
        INPUT_TYPE_RAW_DOCUMENT => {
            // Raw document: [db_len: u8][db][coll_len: u8][coll][bson]
            let payload_len = len - 1; // subtract type byte
            let mut payload = vec![0u8; payload_len];
            reader.read_exact(&mut payload)?;

            let mut offset = 0;

            // Read database
            if offset >= payload.len() {
                return Err(Error::MissingField("db_len in raw document message"));
            }
            let db_len = payload[offset] as usize;
            offset += 1;

            if offset + db_len > payload.len() {
                return Err(Error::MissingField("db in raw document message"));
            }
            let database = String::from_utf8_lossy(&payload[offset..offset + db_len]).to_string();
            offset += db_len;

            // Read collection
            if offset >= payload.len() {
                return Err(Error::MissingField("coll_len in raw document message"));
            }
            let coll_len = payload[offset] as usize;
            offset += 1;

            if offset + coll_len > payload.len() {
                return Err(Error::MissingField("coll in raw document message"));
            }
            let collection =
                String::from_utf8_lossy(&payload[offset..offset + coll_len]).to_string();
            offset += coll_len;

            // Remaining bytes are the BSON document
            let bson_bytes = payload[offset..].to_vec();

            Ok(ReadResult::RawDocument(bson_bytes, database, collection))
        }
        _ => Err(Error::UnknownOperation(format!(
            "unknown input message type: 0x{:02x}",
            msg_type
        ))),
    }
}

/// Write a length-prefixed binary frame: [length: u32 LE][payload: bytes]
/// Note: Does NOT flush - caller must flush explicitly at batch boundaries.
fn write_frame(writer: &mut impl Write, payload: &[u8]) -> io::Result<()> {
    let len = payload.len() as u32;
    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(payload)
}

/// Write a skip message (1 byte: type only)
pub fn write_skip(writer: &mut impl Write) -> io::Result<()> {
    write_frame(writer, &[MSG_TYPE_SKIP])
}

/// Build a document message payload into the provided buffer.
fn build_document_payload(
    payload: &mut Vec<u8>,
    json_bytes: &[u8],
    completed_split: bool,
    database: &str,
    collection: &str,
) -> Result<()> {
    // Validate length constraints (MongoDB limits: 64 for db, 255 for collection)
    if database.len() > 255 {
        return Err(Error::Fragment(format!(
            "database name too long: {} bytes",
            database.len()
        )));
    }
    if collection.len() > 255 {
        return Err(Error::Fragment(format!(
            "collection name too long: {} bytes",
            collection.len()
        )));
    }

    // Calculate total payload size: type(1) + flags(1) + db_len(1) + db + coll_len(1) + coll + json
    let payload_size = 1 + 1 + 1 + database.len() + 1 + collection.len() + json_bytes.len();
    payload.clear();
    payload.reserve(payload_size);

    payload.push(MSG_TYPE_DOCUMENT);

    let flags = if completed_split {
        FLAG_COMPLETED_SPLIT_EVENT
    } else {
        0
    };
    payload.push(flags);

    payload.push(database.len() as u8);
    payload.extend_from_slice(database.as_bytes());

    payload.push(collection.len() as u8);
    payload.extend_from_slice(collection.as_bytes());

    payload.extend_from_slice(json_bytes);

    Ok(())
}

/// Write a document message with binary protocol (from pre-serialized JSON bytes)
pub fn write_document_bytes(
    writer: &mut impl Write,
    json_bytes: &[u8],
    completed_split: bool,
    database: &str,
    collection: &str,
) -> Result<()> {
    PAYLOAD_BUFFER.with(|payload_buf| {
        let mut payload = payload_buf.borrow_mut();
        build_document_payload(&mut payload, json_bytes, completed_split, database, collection)?;
        write_frame(writer, &payload)?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::JSON_BUFFER;

    fn write_document(
        writer: &mut impl Write,
        doc: &serde_json::Value,
        completed_split: bool,
        database: &str,
        collection: &str,
    ) -> Result<()> {
        JSON_BUFFER.with(|json_buf| {
            PAYLOAD_BUFFER.with(|payload_buf| {
                let mut json_bytes = json_buf.borrow_mut();
                let mut payload = payload_buf.borrow_mut();

                // Serialize JSON to reusable buffer
                json_bytes.clear();
                serde_json::to_writer(&mut *json_bytes, doc)?;

                build_document_payload(
                    &mut payload,
                    &json_bytes,
                    completed_split,
                    database,
                    collection,
                )?;
                write_frame(writer, &payload)?;
                Ok(())
            })
        })
    }

    #[test]
    fn test_write_document_with_metadata() {
        let mut buf = Vec::new();
        let doc = serde_json::json!({
            "_id": "test123",
            "_meta": {
                "source": {
                    "db": "mydb",
                    "collection": "mycoll"
                }
            }
        });

        write_document(&mut buf, &doc, false, "mydb", "mycoll").unwrap();

        // Parse frame
        let frame_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let payload = &buf[4..4 + frame_len];

        // Verify format
        assert_eq!(payload[0], MSG_TYPE_DOCUMENT); // type
        assert_eq!(payload[1], 0); // flags
        assert_eq!(payload[2], 4); // db length
        assert_eq!(&payload[3..7], b"mydb"); // db
        assert_eq!(payload[7], 6); // coll length
        assert_eq!(&payload[8..14], b"mycoll"); // coll

        // Remaining should be valid JSON
        let json_start = 14;
        let json_bytes = &payload[json_start..];
        let parsed: serde_json::Value = serde_json::from_slice(json_bytes).unwrap();
        assert_eq!(parsed["_id"], "test123");
    }

    #[test]
    fn test_write_document_with_split_flag() {
        let mut buf = Vec::new();
        let doc = serde_json::json!({"_id": "test"});

        write_document(&mut buf, &doc, true, "db", "coll").unwrap();

        let payload = &buf[4..]; // Skip frame length
        assert_eq!(payload[1], FLAG_COMPLETED_SPLIT_EVENT);
    }

    #[test]
    fn test_write_document_empty_names() {
        let mut buf = Vec::new();
        let doc = serde_json::json!({"_id": "test"});

        write_document(&mut buf, &doc, false, "", "").unwrap();

        let payload = &buf[4..];
        assert_eq!(payload[2], 0); // db length = 0
        assert_eq!(payload[3], 0); // coll length = 0
    }

    #[test]
    fn test_write_document_long_names() {
        let mut buf = Vec::new();
        let doc = serde_json::json!({"_id": "test"});
        let long_db = "a".repeat(255);
        let long_coll = "b".repeat(255);

        write_document(&mut buf, &doc, false, &long_db, &long_coll).unwrap();

        let payload = &buf[4..];
        assert_eq!(payload[2], 255); // db length
        assert_eq!(payload[3 + 255], 255); // coll length
    }

    #[test]
    fn test_write_document_too_long_db() {
        let mut buf = Vec::new();
        let doc = serde_json::json!({"_id": "test"});
        let too_long_db = "a".repeat(256);

        let result = write_document(&mut buf, &doc, false, &too_long_db, "coll");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("database name too long"));
    }

    #[test]
    fn test_write_document_too_long_coll() {
        let mut buf = Vec::new();
        let doc = serde_json::json!({"_id": "test"});
        let too_long_coll = "a".repeat(256);

        let result = write_document(&mut buf, &doc, false, "db", &too_long_coll);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("collection name too long"));
    }

    #[test]
    fn test_write_document_utf8_names() {
        let mut buf = Vec::new();
        let doc = serde_json::json!({"_id": "test"});

        write_document(&mut buf, &doc, false, "数据库", "集合").unwrap();

        let payload = &buf[4..];
        let db_len = payload[2] as usize;
        let db_bytes = &payload[3..3 + db_len];
        assert_eq!(std::str::from_utf8(db_bytes).unwrap(), "数据库");

        let coll_offset = 3 + db_len;
        let coll_len = payload[coll_offset] as usize;
        let coll_bytes = &payload[coll_offset + 1..coll_offset + 1 + coll_len];
        assert_eq!(std::str::from_utf8(coll_bytes).unwrap(), "集合");
    }

    #[test]
    fn test_write_document_realistic() {
        let mut buf = Vec::new();
        let doc = serde_json::json!({
            "_id": "abc123",
            "name": "John Doe",
            "_meta": {
                "op": "c",
                "source": {
                    "db": "production",
                    "collection": "users"
                }
            }
        });

        write_document(&mut buf, &doc, false, "production", "users").unwrap();

        // Parse and verify the payload structure
        let frame_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let payload = &buf[4..4 + frame_len];

        // Verify header
        assert_eq!(payload[0], MSG_TYPE_DOCUMENT);
        assert_eq!(payload[1], 0); // no flags

        // Verify database
        let db_len = payload[2] as usize;
        assert_eq!(db_len, 10); // "production"
        assert_eq!(&payload[3..3 + db_len], b"production");

        // Verify collection
        let coll_offset = 3 + db_len;
        let coll_len = payload[coll_offset] as usize;
        assert_eq!(coll_len, 5); // "users"
        assert_eq!(
            &payload[coll_offset + 1..coll_offset + 1 + coll_len],
            b"users"
        );

        // Verify JSON
        let json_offset = coll_offset + 1 + coll_len;
        let json_bytes = &payload[json_offset..];
        let parsed: serde_json::Value = serde_json::from_slice(json_bytes).unwrap();
        assert_eq!(parsed["_id"], "abc123");
        assert_eq!(parsed["name"], "John Doe");
    }
}
