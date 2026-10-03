//! Direct protobuf wire → JSON transcoding for registry protobuf payloads.
//!
//! The production path decodes a payload into a `prost_reflect::DynamicMessage`
//! (a tree of allocated `Value`s behind a `BTreeMap` per message) and then
//! walks that tree through `MergeSerializer` into JSON bytes. This module
//! produces the same bytes straight from the wire, without building the tree.
//!
//! # Equivalence target
//!
//! Output is byte-identical to the `DynamicMessage` + `MergeSerializer` path
//! for every message that has no protobuf map fields, and identical after
//! parsing for messages with maps (the `DynamicMessage` path emits map entries
//! in `HashMap` order; this path emits them sorted by key). One more
//! intentional difference: a `FloatValue` or `DoubleValue` wrapper holding
//! negative zero prints `-0.0` here, where the `DynamicMessage` path loses the
//! sign in a re-encode round trip and prints `0.0`. Malformed input fails on
//! both paths or on neither, including where prost-reflect is more lenient
//! than the spec (map entries are read as length-delimited whatever their
//! wire type). The differential tests pin that contract against the
//! production path.
//!
//! # How it works
//!
//! Each message type is compiled once into a [`plan::MessagePlan`]: its
//! fields in ascending field-number order (the order `DynamicMessage`
//! serializes in), with the leaf type, cardinality, presence rule, and oneof
//! membership resolved up front. Transcoding a message is two passes:
//!
//! 1. **Index** ([`index`]): walk the tags once, validating every value the
//!    way the production decoder does, and record for each known field of
//!    each message instance the chain of its occurrences. This pass is the
//!    decoder: it rejects exactly what `DynamicMessage::decode` rejects, and
//!    it never decides what to print.
//! 2. **Format** ([`emit`], [`wkt`]): walk the plan, not the bytes. For each
//!    field that occurred, write its JSON from the index: the last occurrence
//!    of a scalar, every occurrence of a repeated field, map entries sorted by
//!    key, the last-set member of a oneof. This pass is the serializer: it may
//!    skip whatever the document does not print, because nothing it skips was
//!    left unvalidated.
//!
//! Protobuf merges a singular message field that occurs more than once by
//! decoding the concatenation of the occurrences. The format pass does the
//! same: it appends the pieces to the arena and indexes them as one instance.
//!
//! # Fallback contract
//!
//! Semantics the fast path does not implement return
//! [`TranscodeError::Unsupported`], and the caller must fall back to the
//! `DynamicMessage` path for that message: proto2 groups, messages with
//! registered extensions, and well-known types at the top level of a payload.
//!
//! Malformed bytes return [`TranscodeError::Decode`]; the `DynamicMessage`
//! path fails on the same inputs.

mod emit;
mod index;
mod json;
mod plan;
/// Differential tests: the transcoder against the production path
/// (`DynamicMessage` decode + `MergeSerializer`) on the same wire bytes. The
/// production path is the oracle for both successful output and failure.
#[cfg(test)]
mod tests;
mod wire;
mod wkt;

use std::fmt;

use serde::Serialize;
use serde_json::{Map, Value};

pub use plan::{PlanId, Plans};

use emit::{MapItem, Merge};
use index::{Entry, Occ};
use wire::{Depth, Kind};

#[derive(Debug)]
pub enum TranscodeError {
    /// The payload uses protobuf semantics the fast path does not implement.
    /// The caller must transcode this message through `DynamicMessage`.
    Unsupported(&'static str),
    /// The payload is malformed. The `DynamicMessage` path fails on it too.
    Decode(String),
}

impl fmt::Display for TranscodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TranscodeError::Unsupported(what) => write!(f, "unsupported by transcoder: {what}"),
            TranscodeError::Decode(what) => write!(f, "failed to decode protobuf message: {what}"),
        }
    }
}

impl std::error::Error for TranscodeError {}

type Result<T> = std::result::Result<T, TranscodeError>;

fn decode_err(what: impl Into<String>) -> TranscodeError {
    TranscodeError::Decode(what.into())
}

/// Reusable scratch space for transcoding. One per capture task; every
/// buffer is cleared per message and keeps its capacity.
pub struct Transcoder {
    /// The payload, followed by any merged message instances the format pass
    /// builds. Entries address it by offset.
    arena: Vec<u8>,
    /// Every occurrence of a known field, in the order it was indexed.
    entries: Vec<Entry>,
    /// Occurrence chains, one slot per field of each indexed instance.
    occ: Vec<Occ>,
    /// Map entries being sorted for output.
    map_scratch: Vec<MapItem>,
    /// Formatting buffer for well-known types that print as one string.
    text: String,
}

impl Default for Transcoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Transcoder {
    pub fn new() -> Transcoder {
        Transcoder {
            arena: Vec::new(),
            entries: Vec::new(),
            occ: Vec::new(),
            map_scratch: Vec::new(),
            text: String::new(),
        }
    }

    /// Append to `out` the captured document for `body` (the protobuf bytes
    /// after the Confluent framing): the message's fields in field-number
    /// order, then the key fields, then `_meta`, exactly as `MergeSerializer`
    /// emits them. On any error `out` is restored to its prior length.
    pub fn transcode<M: Serialize>(
        &mut self,
        plans: &Plans,
        plan: PlanId,
        body: &[u8],
        key: Option<&Map<String, Value>>,
        meta: &M,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        let start = out.len();
        self.arena.clear();
        self.entries.clear();
        self.occ.clear();
        self.map_scratch.clear();
        let result = self.transcode_cleared(plans, plan, body, key, meta, out);
        if result.is_err() {
            out.truncate(start);
        }
        result
    }

    fn transcode_cleared<M: Serialize>(
        &mut self,
        plans: &Plans,
        plan: PlanId,
        body: &[u8],
        key: Option<&Map<String, Value>>,
        meta: &M,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        if plans.messages[plan as usize].wkt.is_some() {
            return Err(TranscodeError::Unsupported(
                "well-known type as the payload",
            ));
        }
        if u32::try_from(body.len()).is_err() {
            return Err(TranscodeError::Unsupported("payload of 4 GiB or more"));
        }
        self.arena.extend_from_slice(body);
        let base = self.index_instance(plans, plan, 0, body.len(), Depth::TOP, Kind::Message)?;
        let merge = Merge { key, meta };
        self.write_message(plans, plan, base, Some(&merge), out)
    }
}
