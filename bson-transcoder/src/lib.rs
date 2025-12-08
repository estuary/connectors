pub mod serializer;

use bson::raw::RawDocument;
use std::io::Write;

// Re-export id_to_string for benchmarks
pub use serializer::id_to_string;

/// Transcode a raw BSON document directly to sanitized JSON output.
/// This combines BSON parsing, extended JSON conversion, and sanitization
/// in a single pass without intermediate allocation.
///
/// If `is_root` is true, the `_id` field will be stringified.
pub fn bson_to_sanitized_json<W: Write>(
    doc: &RawDocument,
    writer: W,
    is_root: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let deserializer = bson::RawDeserializer::new(doc.as_bytes())?;
    let mut serializer = serializer::SanitizingSerializer::new(writer, is_root);
    serde_transcode::transcode(deserializer, &mut serializer)?;
    Ok(())
}
