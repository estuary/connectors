//! Pass one: validate every byte the DynamicMessage path would decode, and
//! record where each known field's values are, so pass two can format from
//! the index without touching tags again.
//!
//! Laziness is allowed for formatting, never for validation: a value the
//! document ends up not printing (a superseded occurrence, a losing oneof
//! member, a key-overridden field) is still checked here exactly as the
//! production path would decode it, so malformed bytes fail on both paths.

use super::plan::{Card, Leaf, PlanId, Plans, Scalar};
use super::wire::{
    read_fixed32, read_fixed64, read_len_prefixed, read_tag, read_varint, skip_unknown,
    wire_type_name, Depth, Kind, WT_FIXED32, WT_FIXED64, WT_LEN, WT_VARINT,
};
use super::{decode_err, Result, TranscodeError, Transcoder};

pub(super) const NONE: u32 = u32::MAX;

/// One occurrence of a known field. `start..end` is the value's bytes in the
/// arena: the content for length-delimited kinds, the encoded value for
/// varints and fixed-width kinds. Occurrences of the same field in the same
/// instance are chained through `next`, in wire order.
#[derive(Clone, Copy, Debug)]
pub(super) struct Entry {
    pub wt: u8,
    pub start: u32,
    pub end: u32,
    pub next: u32,
    /// For message-typed values and map entries, the base of the nested
    /// instance's occurrence table.
    pub child: u32,
}

/// Per field of a message instance: the chain of its occurrences.
#[derive(Clone, Copy, Debug)]
pub(super) struct Occ {
    pub first: u32,
    pub last: u32,
    pub count: u32,
}

impl Occ {
    pub const EMPTY: Occ = Occ {
        first: NONE,
        last: NONE,
        count: 0,
    };
}

impl Transcoder {
    /// Validate and index the message instance at `arena[start..end]` as
    /// `plan_id`, returning the base of its occurrence table.
    pub(super) fn index_instance(
        &mut self,
        plans: &Plans,
        plan_id: PlanId,
        start: usize,
        end: usize,
        depth: Depth,
        kind: Kind,
    ) -> Result<u32> {
        let plan = &plans.messages[plan_id as usize];
        if plan.has_extensions {
            return Err(TranscodeError::Unsupported("message with extensions"));
        }
        let base = self.occ.len();
        self.occ.resize(base + plan.fields.len(), Occ::EMPTY);

        let mut pos = start;
        while pos < end {
            let (number, wt, after) = read_tag(&self.arena[..end], pos)?;
            let Some(idx) = plan.lookup.get(number) else {
                pos = skip_unknown(&self.arena[..end], after, wt, number, depth, kind)?;
                continue;
            };
            let field = &plan.fields[idx];
            if field.is_group {
                return Err(TranscodeError::Unsupported("proto2 group field"));
            }
            let expected = field.leaf.wire_type();
            let (vstart, vend, child) = match field.card {
                Card::Single { .. } => {
                    if wt != expected {
                        return Err(wire_type_mismatch(wt, expected, &field.name));
                    }
                    self.validate_value(plans, field.leaf, after, end, depth)?
                }
                Card::Repeated => {
                    if wt == WT_LEN && field.leaf.is_packable() {
                        let (s, e) = read_len_prefixed(&self.arena[..end], after)?;
                        validate_packed(&self.arena[..e], field.leaf, s)?;
                        (s, e, NONE)
                    } else if wt == expected {
                        self.validate_value(plans, field.leaf, after, end, depth)?
                    } else {
                        return Err(wire_type_mismatch(wt, expected, &field.name));
                    }
                }
                Card::Map { .. } => {
                    // prost-reflect reads a map entry's length whatever the
                    // wire type says, so production accepts any; mirror it.
                    let Leaf::Message(entry_plan) = field.leaf else {
                        unreachable!("map fields are message-typed")
                    };
                    let (s, e) = read_len_prefixed(&self.arena[..end], after)?;
                    let child = self.index_instance(
                        plans,
                        entry_plan,
                        s,
                        e,
                        depth.enter_map_entry()?,
                        Kind::MapEntry,
                    )?;
                    (s, e, child)
                }
            };
            self.record(base + idx, wt, vstart, vend, child);
            pos = vend;
        }

        Ok(base as u32)
    }

    /// Validate one value of `leaf` whose encoding starts at `pos`. Returns
    /// the value's byte range (content for length-delimited kinds) and the
    /// nested instance base for messages.
    fn validate_value(
        &mut self,
        plans: &Plans,
        leaf: Leaf,
        pos: usize,
        end: usize,
        depth: Depth,
    ) -> Result<(usize, usize, u32)> {
        match leaf {
            Leaf::Scalar(scalar) => {
                let (s, e) = validate_scalar(&self.arena[..end], scalar, pos)?;
                Ok((s, e, NONE))
            }
            Leaf::Enum(_) => {
                let (_, next) = read_varint(&self.arena[..end], pos)?;
                Ok((pos, next, NONE))
            }
            Leaf::Message(nested) => {
                let (s, e) = read_len_prefixed(&self.arena[..end], pos)?;
                let is_wkt = plans.messages[nested as usize].wkt.is_some();
                let child = self.index_instance(
                    plans,
                    nested,
                    s,
                    e,
                    depth.enter_message(is_wkt)?,
                    Kind::Message,
                )?;
                Ok((s, e, child))
            }
        }
    }

    fn record(&mut self, slot: usize, wt: u8, start: usize, end: usize, child: u32) {
        let ei = self.entries.len() as u32;
        self.entries.push(Entry {
            wt,
            start: start as u32,
            end: end as u32,
            next: NONE,
            child,
        });
        let o = &mut self.occ[slot];
        if o.count == 0 {
            o.first = ei;
        } else {
            self.entries[o.last as usize].next = ei;
        }
        o.last = ei;
        o.count += 1;
    }
}

fn wire_type_mismatch(wt: u8, expected: u8, name: &str) -> TranscodeError {
    decode_err(format!(
        "invalid wire type: {} (expected {}) for field {}",
        wire_type_name(wt),
        wire_type_name(expected),
        name
    ))
}

/// Validate a scalar at `pos`, returning its value bytes (content for strings
/// and bytes). Strings must be UTF-8, as prost requires.
fn validate_scalar(buf: &[u8], scalar: Scalar, pos: usize) -> Result<(usize, usize)> {
    match scalar.wire_type() {
        WT_VARINT => {
            let (_, next) = read_varint(buf, pos)?;
            Ok((pos, next))
        }
        WT_FIXED32 => {
            let (_, next) = read_fixed32(buf, pos)?;
            Ok((pos, next))
        }
        WT_FIXED64 => {
            let (_, next) = read_fixed64(buf, pos)?;
            Ok((pos, next))
        }
        _ => {
            let (s, e) = read_len_prefixed(buf, pos)?;
            if scalar == Scalar::String && std::str::from_utf8(&buf[s..e]).is_err() {
                return Err(decode_err(
                    "invalid string value: data is not UTF-8 encoded",
                ));
            }
            Ok((s, e))
        }
    }
}

/// Validate the elements of a packed field whose content runs from `pos` to
/// the end of `buf`. A trailing partial element is a buffer underflow, as in
/// prost's `merge_loop`.
fn validate_packed(buf: &[u8], leaf: Leaf, pos: usize) -> Result<()> {
    let wt = leaf.wire_type();
    let mut p = pos;
    while p < buf.len() {
        p = match wt {
            WT_VARINT => read_varint(buf, p)?.1,
            WT_FIXED32 => read_fixed32(buf, p)?.1,
            WT_FIXED64 => read_fixed64(buf, p)?.1,
            _ => unreachable!("only numeric kinds are packable"),
        };
    }
    Ok(())
}
