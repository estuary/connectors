mod event;
mod protocol;
mod serializer;

use bson::raw::RawDocument;
use std::cell::RefCell;
use std::io::{self, BufReader, BufWriter, Write};
use thiserror::Error;

use event::{
    get_split_info, is_document_operation, process_change_event, process_raw_document,
    ChangeEvent, FragmentAccumulator,
};
use protocol::{read_input_message, write_document_bytes, write_skip, ReadResult};

// Thread-local reusable buffers for JSON serialization and payload building
thread_local! {
    pub static JSON_BUFFER: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    pub static PAYLOAD_BUFFER: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// Run a closure with the thread-local JSON buffer.
fn with_json_buffer<F, R>(f: F) -> R
where
    F: FnOnce(&mut Vec<u8>) -> R,
{
    JSON_BUFFER.with(|buf| f(&mut buf.borrow_mut()))
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("BSON error: {0}")]
    Bson(#[from] bson::error::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Serializer error: {0}")]
    Serialize(#[from] serializer::Error),
    #[error("Missing field: {0}")]
    MissingField(&'static str),
    #[error("Expected object, instead got: {0}")]
    ExpectedObject(String),
    #[error("Unknown operation: {0}")]
    UnknownOperation(String),
    #[error("Fragment error: {0}")]
    Fragment(String),
}

pub type Result<T> = std::result::Result<T, Error>;

fn main() -> Result<()> {
    let stdin = io::stdin().lock();
    let mut stdout = BufWriter::new(io::stdout().lock());
    let mut reader = BufReader::new(stdin);

    // State for accumulating split event fragments
    let mut accumulator: Option<FragmentAccumulator> = None;

    loop {
        match read_input_message(&mut reader)? {
            ReadResult::Eof => break,
            ReadResult::Flush => {
                // Flush signal received - flush buffered output to stdout
                stdout.flush()?;
                continue;
            }
            ReadResult::RawDocument(doc_bytes, database, collection) => {
                // Raw document from backfill - sanitize and add _meta with snapshot: true
                let raw_doc = RawDocument::from_bytes(&doc_bytes)?;
                with_json_buffer(|json_bytes| {
                    process_raw_document(raw_doc, &database, &collection, json_bytes)?;
                    write_document_bytes(&mut stdout, json_bytes, false, &database, &collection)
                })?;
            }
            ReadResult::ChangeEvent(doc_bytes) => {
                let raw_doc = RawDocument::from_bytes(&doc_bytes)?;

                // Check if this is a split event fragment
                if let Some((fragment, of)) = get_split_info(raw_doc) {
                    if fragment == 1 {
                        if accumulator.is_some() {
                            return Err(Error::Fragment(
                                "received new split event while still accumulating previous"
                                    .to_string(),
                            ));
                        }
                        accumulator = Some(FragmentAccumulator::new(doc_bytes, of));
                        // Emit skip - we're waiting for more fragments
                        write_skip(&mut stdout)?;
                    } else {
                        let acc = accumulator.as_mut().ok_or_else(|| {
                            Error::Fragment(format!(
                                "received fragment {} without first fragment",
                                fragment
                            ))
                        })?;

                        let complete = acc.add_fragment(doc_bytes, fragment, of)?;

                        if complete {
                            let acc = accumulator.as_ref().unwrap();
                            let event = ChangeEvent::from_accumulator(acc)?;

                            if !is_document_operation(event.operation_type) {
                                accumulator = None;
                                write_skip(&mut stdout)?;
                                continue;
                            }

                            // FullDocument can be "null" for non-deletion events if another
                            // operation has deleted the document. This happens because update
                            // change events do not hold a copy of the full document, but rather
                            // it is at query time that the full document is looked up, and in
                            // these cases, the FullDocument can end up being null (if deleted)
                            // or different from the deltas in the update event. We ignore
                            // events where FullDocument is null. Another change event of type
                            // delete will eventually come and delete the document.
                            if event.operation_type != "delete" && event.full_document.is_none() {
                                accumulator = None;
                                write_skip(&mut stdout)?;
                                continue;
                            }

                            // Build output using the same path as non-split events
                            with_json_buffer(|json_bytes| {
                                process_change_event(&event, json_bytes)?;
                                write_document_bytes(
                                    &mut stdout,
                                    json_bytes,
                                    true,
                                    event.database,
                                    event.collection,
                                )
                            })?;
                            accumulator = None;
                        } else {
                            // Still waiting for more fragments
                            write_skip(&mut stdout)?;
                        }
                    }
                } else {
                    // Non-split event
                    if accumulator.is_some() {
                        return Err(Error::Fragment(
                            "received non-split event while accumulating fragments".to_string(),
                        ));
                    }

                    let op_type = raw_doc
                        .get_str("operationType")
                        .map_err(|_| Error::MissingField("operationType"))?;

                    if !is_document_operation(op_type) {
                        write_skip(&mut stdout)?;
                        continue;
                    }

                    // FullDocument can be "null" for non-deletion events if another
                    // operation has deleted the document. This happens because update
                    // change events do not hold a copy of the full document, but rather
                    // it is at query time that the full document is looked up, and in
                    // these cases, the FullDocument can end up being null (if deleted)
                    // or different from the deltas in the update event. We ignore
                    // events where FullDocument is null. Another change event of type
                    // delete will eventually come and delete the document.
                    if op_type != "delete" && raw_doc.get_document("fullDocument").is_err() {
                        write_skip(&mut stdout)?;
                        continue;
                    }

                    let event = ChangeEvent::from_raw(raw_doc)?;
                    with_json_buffer(|json_bytes| {
                        process_change_event(&event, json_bytes)?;
                        write_document_bytes(
                            &mut stdout,
                            json_bytes,
                            false,
                            event.database,
                            event.collection,
                        )
                    })?;
                }
            }
        }
    }

    Ok(())
}
