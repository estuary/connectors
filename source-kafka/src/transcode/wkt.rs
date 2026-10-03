//! Well-known types serialize as a JSON scalar, a string, or a free-form
//! value rather than as an object of their fields. Each formatter reads its
//! fields from the index like any other message; only the output differs.

use serde_json::ser::{CompactFormatter, Formatter};

use super::emit::{write_scalar, write_scalar_default};
use super::index::{Entry, NONE};
use super::json::{sep, write_json_str};
use super::plan::{Card, Leaf, MessagePlan, Plans, Scalar, Wkt};
use super::wire::{read_fixed32, read_fixed64, Depth, Kind};
use super::{decode_err, Result, Transcoder};

// Range checks prost-reflect applies to the time well-known types before
// formatting them.
pub(super) const MAX_DURATION_SECONDS: u64 = 315_576_000_000;
pub(super) const MAX_DURATION_NANOS: u32 = 999_999_999;
pub(super) const MIN_TIMESTAMP_SECONDS: i64 = -62_135_596_800;
pub(super) const MAX_TIMESTAMP_SECONDS: i64 = 253_402_300_799;

impl Transcoder {
    pub(super) fn write_wkt(
        &mut self,
        plans: &Plans,
        plan: &MessagePlan,
        wkt: Wkt,
        base: u32,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        match wkt {
            Wkt::Timestamp => {
                let seconds = self.last_varint(plan, base, 1).unwrap_or(0) as i64;
                let nanos = self.last_varint(plan, base, 2).unwrap_or(0) as i32;
                if !(MIN_TIMESTAMP_SECONDS..=MAX_TIMESTAMP_SECONDS).contains(&seconds) {
                    return Err(decode_err("timestamp out of range"));
                }
                self.text.clear();
                use std::fmt::Write as _;
                write!(self.text, "{}", prost_types::Timestamp { seconds, nanos })
                    .expect("formatting a timestamp cannot fail");
                write_json_str(out, &self.text);
                Ok(())
            }
            Wkt::Duration => {
                let seconds = self.last_varint(plan, base, 1).unwrap_or(0) as i64;
                let nanos = self.last_varint(plan, base, 2).unwrap_or(0) as i32;
                if seconds.unsigned_abs() > MAX_DURATION_SECONDS
                    || nanos.unsigned_abs() > MAX_DURATION_NANOS
                {
                    return Err(decode_err("duration out of range"));
                }
                self.text.clear();
                use std::fmt::Write as _;
                write!(self.text, "{}", prost_types::Duration { seconds, nanos })
                    .expect("formatting a duration cannot fail");
                write_json_str(out, &self.text);
                Ok(())
            }
            Wkt::FloatValue
            | Wkt::DoubleValue
            | Wkt::Int32Value
            | Wkt::Int64Value
            | Wkt::UInt32Value
            | Wkt::UInt64Value
            | Wkt::BoolValue
            | Wkt::StringValue
            | Wkt::BytesValue => {
                let scalar = match wkt {
                    Wkt::FloatValue => Scalar::Float,
                    Wkt::DoubleValue => Scalar::Double,
                    Wkt::Int32Value => Scalar::Int32,
                    Wkt::Int64Value => Scalar::Int64,
                    Wkt::UInt32Value => Scalar::Uint32,
                    Wkt::UInt64Value => Scalar::Uint64,
                    Wkt::BoolValue => Scalar::Bool,
                    Wkt::StringValue => Scalar::String,
                    _ => Scalar::Bytes,
                };
                match self.last_entry(plan, base, 1) {
                    None => write_scalar_default(out, scalar),
                    Some(e) => match scalar {
                        // Wrappers go straight to serde_json's float
                        // serializer, which writes non-finite values as null.
                        Scalar::Float => {
                            let v = f32::from_bits(read_fixed32(self.bytes(e), 0)?.0);
                            if v.is_finite() {
                                CompactFormatter.write_f32(out, v).expect("vec write");
                            } else {
                                out.extend_from_slice(b"null");
                            }
                        }
                        Scalar::Double => {
                            let v = f64::from_bits(read_fixed64(self.bytes(e), 0)?.0);
                            if v.is_finite() {
                                CompactFormatter.write_f64(out, v).expect("vec write");
                            } else {
                                out.extend_from_slice(b"null");
                            }
                        }
                        _ => {
                            write_scalar(out, scalar, self.bytes(e));
                        }
                    },
                }
                Ok(())
            }
            Wkt::Empty => {
                out.extend_from_slice(b"{}");
                Ok(())
            }
            Wkt::FieldMask => {
                self.text.clear();
                let paths_idx = plan.lookup.get(1).expect("FieldMask.paths");
                let mut ei = self.occ[base as usize + paths_idx].first;
                while ei != NONE {
                    let e = self.entries[ei as usize];
                    // Validated as UTF-8 when indexed.
                    let path = unsafe {
                        std::str::from_utf8_unchecked(&self.arena[e.start as usize..e.end as usize])
                    };
                    // prost-reflect separates on a non-empty result, not on
                    // path count, so empty paths add no comma.
                    if !self.text.is_empty() {
                        self.text.push(',');
                    }
                    let mut is_first_part = true;
                    for part in path.split('.') {
                        if !is_first_part {
                            self.text.push('.');
                        }
                        is_first_part = false;
                        snake_case_to_camel_case(&mut self.text, part).map_err(|()| {
                            decode_err("cannot roundtrip field name through camelcase")
                        })?;
                    }
                    ei = e.next;
                }
                write_json_str(out, &self.text);
                Ok(())
            }
            Wkt::Any => {
                // The production path decodes the payload only when it
                // serializes the Any, as a fresh top-level message of the
                // type the URL names; an absent payload decodes as empty.
                let (us, ue) = self
                    .last_entry(plan, base, 1)
                    .map_or((0, 0), |e| (e.start, e.end));
                let (ps, pe) = self
                    .last_entry(plan, base, 2)
                    .map_or((0, 0), |e| (e.start, e.end));
                // Validated as UTF-8 when indexed.
                let url =
                    unsafe { std::str::from_utf8_unchecked(&self.arena[us as usize..ue as usize]) };
                let message_name = url.rsplit_once('/').map(|(_, name)| name).ok_or_else(|| {
                    decode_err(format!(
                        "unsupported type url '{url}': missing at least one '/'"
                    ))
                })?;
                let target = *plans
                    .by_name
                    .get(message_name)
                    .ok_or_else(|| decode_err(format!("message '{message_name}' not found")))?;
                out.extend_from_slice(b"{\"@type\":");
                write_json_str(out, url);
                let target_plan = &plans.messages[target as usize];
                let payload_depth = Depth {
                    abs: 0,
                    rel: target_plan.wkt.map(|_| 0),
                };
                let payload = self.index_instance(
                    plans,
                    target,
                    ps as usize,
                    pe as usize,
                    payload_depth,
                    Kind::Message,
                )?;
                if target_plan.wkt.is_some() {
                    out.extend_from_slice(b",\"value\":");
                    self.write_message::<()>(plans, target, payload, None, out)?;
                } else {
                    let mut first = false;
                    self.write_fields::<()>(plans, target_plan, payload, None, &mut first, out)?;
                }
                out.push(b'}');
                Ok(())
            }
            Wkt::Struct => {
                // Struct.fields is map<string, Value>. prost keeps it in a
                // BTreeMap, so emitting in key order is byte-identical.
                let fields_idx = plan.lookup.get(1).expect("Struct.fields");
                let field = &plan.fields[fields_idx];
                let (Card::Map { key, value }, Leaf::Message(entry_plan)) =
                    (field.card, field.leaf)
                else {
                    unreachable!("Struct.fields is a map")
                };
                self.write_map(plans, entry_plan, key, value, base, fields_idx, out)
            }
            Wkt::Value => {
                // A oneof over fields 1..=6; the last one written wins.
                let kind_oneof = plan.fields[0].oneof.expect("Value.kind is a oneof");
                let Some(idx) = self.oneof_winner(plan, base, kind_oneof) else {
                    out.extend_from_slice(b"null");
                    return Ok(());
                };
                let field = &plan.fields[idx];
                let o = self.occ[base as usize + idx];
                match field.number {
                    1 => out.extend_from_slice(b"null"),
                    2 => {
                        let e = self.entries[o.last as usize];
                        let v = f64::from_bits(read_fixed64(self.bytes(e), 0)?.0);
                        if !v.is_finite() {
                            return Err(decode_err(
                                "cannot serialize non-finite double in google.protobuf.Value",
                            ));
                        }
                        CompactFormatter.write_f64(out, v).expect("vec write");
                    }
                    3 => {
                        let e = self.entries[o.last as usize];
                        let text = unsafe { std::str::from_utf8_unchecked(self.bytes(e)) };
                        write_json_str(out, text);
                    }
                    4 => {
                        let v = self.entry_varint(o.last) != 0;
                        out.extend_from_slice(if v { b"true" } else { b"false" });
                    }
                    _ => {
                        let Leaf::Message(nested) = field.leaf else {
                            unreachable!("struct_value and list_value are messages")
                        };
                        let child = self.message_instance(plans, nested, plan, base, idx)?;
                        self.write_message::<()>(plans, nested, child, None, out)?;
                    }
                }
                Ok(())
            }
            Wkt::ListValue => {
                let values_idx = plan.lookup.get(1).expect("ListValue.values");
                let Leaf::Message(value_plan) = plan.fields[values_idx].leaf else {
                    unreachable!("ListValue.values holds Values")
                };
                out.push(b'[');
                let mut first = true;
                let mut ei = self.occ[base as usize + values_idx].first;
                while ei != NONE {
                    let e = self.entries[ei as usize];
                    sep(out, &mut first);
                    self.write_message::<()>(plans, value_plan, e.child, None, out)?;
                    ei = e.next;
                }
                out.push(b']');
                Ok(())
            }
        }
    }

    fn last_entry(&self, plan: &MessagePlan, base: u32, number: u32) -> Option<Entry> {
        let idx = plan.lookup.get(number)?;
        let o = self.occ[base as usize + idx];
        (o.count > 0).then(|| self.entries[o.last as usize])
    }

    fn last_varint(&self, plan: &MessagePlan, base: u32, number: u32) -> Option<u64> {
        let idx = plan.lookup.get(number)?;
        let o = self.occ[base as usize + idx];
        (o.count > 0).then(|| self.entry_varint(o.last))
    }
}

/// Same rule prost-reflect applies to FieldMask paths: an error means the name
/// could not round-trip back to snake_case.
fn snake_case_to_camel_case(dst: &mut String, src: &str) -> std::result::Result<(), ()> {
    let mut is_upper_next = false;
    for ch in src.chars() {
        if ch.is_ascii_uppercase() {
            return Err(());
        }
        if is_upper_next {
            let upper = ch.to_ascii_uppercase();
            if upper == ch {
                return Err(());
            }
            dst.push(upper);
            is_upper_next = false;
        } else if ch == '_' {
            is_upper_next = true;
        } else {
            dst.push(ch);
        }
    }
    Ok(())
}
