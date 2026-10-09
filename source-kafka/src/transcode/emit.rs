//! Pass two: format a message from its index. Nothing here reads a tag; every
//! value was located and validated by `index.rs`, so this pass only decides
//! what to print and prints it: fields in plan order, last occurrence wins,
//! repeated fields in wire order, maps sorted by key, the last-set oneof
//! member, defaults omitted for implicit presence.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use serde::Serialize;
use serde_json::ser::{CompactFormatter, Formatter};
use serde_json::{Map, Value};

use super::index::{Entry, NONE};
use super::json::{sep, write_double, write_float, write_json_str};
use super::plan::{Card, EnumPlan, Leaf, MessagePlan, PlanId, Plans, Scalar};
use super::wire::{
    read_fixed32, read_fixed64, read_varint, zigzag32, zigzag64, Depth, Kind, WT_FIXED32,
    WT_FIXED64, WT_LEN, WT_VARINT,
};
use super::{decode_err, Result, Transcoder};

/// What a captured document merges over the payload: the message key's fields
/// override payload fields of the same name, and `_meta` is appended.
pub(super) struct Merge<'a, M> {
    pub(super) key: Option<&'a Map<String, Value>>,
    pub(super) meta: &'a M,
}

/// A map key, canonical for comparison. String keys point at their bytes in
/// the arena and compare by content.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum KeyRepr {
    Bool(bool),
    I64(i64),
    U64(u64),
    Str(u32, u32),
}

/// Where a map entry's value lives.
#[derive(Clone, Copy)]
pub(super) enum ValueRef {
    /// No value field: the leaf's default.
    Default,
    /// A scalar or enum entry.
    Entry(u32),
    /// A message instance (merged if the entry held several).
    Instance(u32),
}

#[derive(Clone, Copy)]
pub(super) struct MapItem {
    pub key: KeyRepr,
    pub value: ValueRef,
    pub order: u32,
}

impl Transcoder {
    /// Write the instance at `base` as a JSON value: an object of its fields,
    /// or a well-known type's special form.
    pub(super) fn write_message<M: Serialize>(
        &mut self,
        plans: &Plans,
        plan_id: PlanId,
        base: u32,
        merge: Option<&Merge<'_, M>>,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        let plan = &plans.messages[plan_id as usize];
        if let Some(wkt) = plan.wkt {
            return self.write_wkt(plans, plan, wkt, base, out);
        }
        out.push(b'{');
        let mut first = true;
        self.write_fields(plans, plan, base, merge, &mut first, out)?;
        if let Some(merge) = merge {
            // The key is merged in last, so a key field named `_meta`
            // overrides ours. Emit ours only if the key didn't.
            let mut key_has_meta = false;
            if let Some(key_fields) = merge.key {
                for (name, value) in key_fields {
                    if name == "_meta" {
                        key_has_meta = true;
                    }
                    sep(out, &mut first);
                    write_json_str(out, name);
                    out.push(b':');
                    serde_json::to_writer(&mut *out, value)
                        .map_err(|e| decode_err(e.to_string()))?;
                }
            }
            if !key_has_meta {
                sep(out, &mut first);
                out.extend_from_slice(b"\"_meta\":");
                serde_json::to_writer(&mut *out, merge.meta)
                    .map_err(|e| decode_err(e.to_string()))?;
            }
        }
        out.push(b'}');
        Ok(())
    }

    /// Write the fields of a non-WKT instance, without the braces, so `Any`
    /// can prepend `@type`.
    pub(super) fn write_fields<M: Serialize>(
        &mut self,
        plans: &Plans,
        plan: &MessagePlan,
        base: u32,
        merge: Option<&Merge<'_, M>>,
        first: &mut bool,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        for (idx, field) in plan.fields.iter().enumerate() {
            let o = self.occ[base as usize + idx];
            if o.count == 0 {
                continue;
            }
            // `_meta` is reserved for the connector's metadata, and a key
            // field of the same name as a payload field overrides it.
            if merge.is_some_and(|m| {
                field.name == "_meta" || m.key.is_some_and(|k| k.contains_key(field.name.as_str()))
            }) {
                continue;
            }
            if let Some(oneof) = field.oneof {
                if self.oneof_winner(plan, base, oneof) != Some(idx) {
                    continue;
                }
            }

            match field.card {
                Card::Single {
                    has_explicit_presence,
                } => match field.leaf {
                    Leaf::Message(nested) => {
                        let child = self.message_instance(plans, nested, plan, base, idx)?;
                        sep(out, first);
                        write_json_str(out, &field.name);
                        out.push(b':');
                        self.write_message::<()>(plans, nested, child, None, out)?;
                    }
                    Leaf::Enum(enum_id) => {
                        let number = self.entry_varint(o.last) as i32;
                        if !has_explicit_presence && number == 0 {
                            continue;
                        }
                        sep(out, first);
                        write_json_str(out, &field.name);
                        out.push(b':');
                        write_enum(out, &plans.enums[enum_id as usize], number);
                    }
                    Leaf::Scalar(scalar) => {
                        let mark = out.len();
                        let was_first = *first;
                        sep(out, first);
                        write_json_str(out, &field.name);
                        out.push(b':');
                        let e = self.entries[o.last as usize];
                        let is_default = write_scalar(out, scalar, self.bytes(e));
                        if is_default && !has_explicit_presence {
                            out.truncate(mark);
                            *first = was_first;
                        }
                    }
                },
                Card::Repeated => {
                    let mark = out.len();
                    let was_first = *first;
                    sep(out, first);
                    write_json_str(out, &field.name);
                    out.extend_from_slice(b":[");
                    let mut n = 0usize;
                    let mut ei = o.first;
                    while ei != NONE {
                        let e = self.entries[ei as usize];
                        if e.wt == WT_LEN && field.leaf.is_packable() {
                            // Packed: elements back to back inside the content.
                            let mut p = e.start as usize;
                            while p < e.end as usize {
                                if n > 0 {
                                    out.push(b',');
                                }
                                p = self.write_packed_element(
                                    plans,
                                    field.leaf,
                                    p,
                                    e.end as usize,
                                    out,
                                )?;
                                n += 1;
                            }
                        } else {
                            if n > 0 {
                                out.push(b',');
                            }
                            self.write_entry_value(plans, field.leaf, e, out)?;
                            n += 1;
                        }
                        ei = e.next;
                    }
                    if n == 0 {
                        // Only an empty packed field gets here; an empty list
                        // is not present.
                        out.truncate(mark);
                        *first = was_first;
                    } else {
                        out.push(b']');
                    }
                }
                Card::Map { key, value } => {
                    let Leaf::Message(entry_plan) = field.leaf else {
                        unreachable!("map fields are message-typed")
                    };
                    sep(out, first);
                    write_json_str(out, &field.name);
                    out.push(b':');
                    self.write_map(plans, entry_plan, key, value, base, idx, out)?;
                }
            }
        }
        Ok(())
    }

    /// The oneof member that was written last, which is the one present.
    pub(super) fn oneof_winner(&self, plan: &MessagePlan, base: u32, oneof: u16) -> Option<usize> {
        let mut winner: Option<(u32, usize)> = None;
        for &member in &plan.oneof_members[oneof as usize] {
            let o = self.occ[base as usize + member as usize];
            if o.count > 0 && winner.is_none_or(|(last, _)| o.last > last) {
                winner = Some((o.last, member as usize));
            }
        }
        winner.map(|(_, idx)| idx)
    }

    /// The instance for a singular message field. One occurrence is its
    /// indexed child; several are merged, which for protobuf means decoding
    /// their concatenation. Inside a oneof only the trailing run after the
    /// last other member counts, since setting another member clears it.
    pub(super) fn message_instance(
        &mut self,
        plans: &Plans,
        nested: PlanId,
        plan: &MessagePlan,
        base: u32,
        idx: usize,
    ) -> Result<u32> {
        let o = self.occ[base as usize + idx];
        if o.count == 1 {
            return Ok(self.entries[o.last as usize].child);
        }
        let mut boundary = NONE;
        if let Some(oneof) = plan.fields[idx].oneof {
            for &member in &plan.oneof_members[oneof as usize] {
                let other = self.occ[base as usize + member as usize];
                if member as usize != idx
                    && other.count > 0
                    && (boundary == NONE || other.last > boundary)
                {
                    boundary = other.last;
                }
            }
        }
        self.merged_instance(plans, nested, o.first, boundary)
    }

    /// Decode the concatenation of the occurrences in the chain from `first`
    /// that come after entry `boundary` (`NONE` for all of them).
    pub(super) fn merged_instance(
        &mut self,
        plans: &Plans,
        nested: PlanId,
        first: u32,
        boundary: u32,
    ) -> Result<u32> {
        let mut pieces = 0;
        let mut only: Option<Entry> = None;
        let mut ei = first;
        while ei != NONE {
            let e = self.entries[ei as usize];
            if boundary == NONE || ei > boundary {
                pieces += 1;
                only = Some(e);
            }
            ei = e.next;
        }
        if pieces == 1 {
            return Ok(only.expect("one piece").child);
        }
        let start = self.arena.len();
        let mut ei = first;
        while ei != NONE {
            let e = self.entries[ei as usize];
            if boundary == NONE || ei > boundary {
                self.arena
                    .extend_from_within(e.start as usize..e.end as usize);
            }
            ei = e.next;
        }
        let end = self.arena.len();
        // The pieces were validated at their real depth; nothing new can
        // fail here, so the depth is only a formality.
        self.index_instance(plans, nested, start, end, Depth::TOP, Kind::Message)
    }

    /// Write one element of a repeated field.
    fn write_entry_value(
        &mut self,
        plans: &Plans,
        leaf: Leaf,
        e: Entry,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        match leaf {
            Leaf::Scalar(scalar) => {
                write_scalar(out, scalar, self.bytes(e));
            }
            Leaf::Enum(enum_id) => {
                let number = read_varint(self.bytes(e), 0)?.0 as i32;
                write_enum(out, &plans.enums[enum_id as usize], number);
            }
            Leaf::Message(nested) => {
                self.write_message::<()>(plans, nested, e.child, None, out)?;
            }
        }
        Ok(())
    }

    /// Write the packed element at `pos`, returning the position after it.
    fn write_packed_element(
        &mut self,
        plans: &Plans,
        leaf: Leaf,
        pos: usize,
        end: usize,
        out: &mut Vec<u8>,
    ) -> Result<usize> {
        match leaf {
            Leaf::Scalar(scalar) => {
                let next = match scalar.wire_type() {
                    WT_VARINT => read_varint(&self.arena[..end], pos)?.1,
                    WT_FIXED32 => read_fixed32(&self.arena[..end], pos)?.1,
                    _ => read_fixed64(&self.arena[..end], pos)?.1,
                };
                write_scalar(out, scalar, &self.arena[pos..next]);
                Ok(next)
            }
            Leaf::Enum(enum_id) => {
                let (raw, next) = read_varint(&self.arena[..end], pos)?;
                write_enum(out, &plans.enums[enum_id as usize], raw as i32);
                Ok(next)
            }
            Leaf::Message(_) => unreachable!("messages are not packable"),
        }
    }

    /// Write a map field: its entries sorted by key, last write winning for
    /// duplicates. The DynamicMessage path emits HashMap order; the runtime
    /// sorts keys anyway, and sorted output is deterministic.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn write_map(
        &mut self,
        plans: &Plans,
        entry_plan: PlanId,
        key: Scalar,
        value_leaf: Leaf,
        base: u32,
        idx: usize,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        let entry = &plans.messages[entry_plan as usize];
        let key_idx = entry.lookup.get(1).expect("map entry key");
        let value_idx = entry.lookup.get(2).expect("map entry value");
        let scratch_base = self.map_scratch.len();

        let mut order = 0u32;
        let mut ei = self.occ[base as usize + idx].first;
        while ei != NONE {
            let e = self.entries[ei as usize];
            let child = e.child as usize;
            let ko = self.occ[child + key_idx];
            let key = if ko.count == 0 {
                match key {
                    Scalar::Bool => KeyRepr::Bool(false),
                    Scalar::String => KeyRepr::Str(0, 0),
                    Scalar::Uint32 | Scalar::Fixed32 | Scalar::Uint64 | Scalar::Fixed64 => {
                        KeyRepr::U64(0)
                    }
                    _ => KeyRepr::I64(0),
                }
            } else {
                let ke = self.entries[ko.last as usize];
                key_repr(key, self.bytes(ke), ke)?
            };
            let vo = self.occ[child + value_idx];
            let value = if vo.count == 0 {
                ValueRef::Default
            } else if let Leaf::Message(_) = value_leaf {
                if vo.count == 1 {
                    ValueRef::Instance(self.entries[vo.last as usize].child)
                } else {
                    let Leaf::Message(nested) = value_leaf else {
                        unreachable!()
                    };
                    ValueRef::Instance(self.merged_instance(plans, nested, vo.first, NONE)?)
                }
            } else {
                ValueRef::Entry(vo.last)
            };
            self.map_scratch.push(MapItem { key, value, order });
            order += 1;
            ei = e.next;
        }

        let arena = &self.arena;
        self.map_scratch[scratch_base..]
            .sort_by(|a, b| cmp_keys(arena, a.key, b.key).then(a.order.cmp(&b.order)));

        out.push(b'{');
        let mut first = true;
        let mut i = scratch_base;
        while i < self.map_scratch.len() {
            let item = self.map_scratch[i];
            let is_superseded = i + 1 < self.map_scratch.len()
                && cmp_keys(&self.arena, self.map_scratch[i + 1].key, item.key)
                    == std::cmp::Ordering::Equal;
            i += 1;
            if is_superseded {
                continue;
            }
            sep(out, &mut first);
            self.write_map_key(out, item.key);
            out.push(b':');
            match item.value {
                ValueRef::Default => self.write_default(plans, value_leaf, out)?,
                ValueRef::Entry(ei) => {
                    let e = self.entries[ei as usize];
                    self.write_entry_value(plans, value_leaf, e, out)?;
                }
                ValueRef::Instance(child) => {
                    let Leaf::Message(nested) = value_leaf else {
                        unreachable!()
                    };
                    self.write_message::<()>(plans, nested, child, None, out)?;
                }
            }
        }
        out.push(b'}');
        self.map_scratch.truncate(scratch_base);
        Ok(())
    }

    /// The JSON for a leaf's default value, for a map entry with no value.
    pub(super) fn write_default(
        &mut self,
        plans: &Plans,
        leaf: Leaf,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        match leaf {
            Leaf::Scalar(scalar) => {
                write_scalar_default(out, scalar);
                Ok(())
            }
            Leaf::Enum(enum_id) => {
                let plan = &plans.enums[enum_id as usize];
                write_enum(out, plan, plan.default_number);
                Ok(())
            }
            Leaf::Message(nested) => {
                let child = self.index_instance(plans, nested, 0, 0, Depth::TOP, Kind::Message)?;
                self.write_message::<()>(plans, nested, child, None, out)
            }
        }
    }

    fn write_map_key(&self, out: &mut Vec<u8>, key: KeyRepr) {
        match key {
            KeyRepr::Bool(v) => out.extend_from_slice(if v { b"\"true\"" } else { b"\"false\"" }),
            KeyRepr::I64(v) => {
                out.push(b'"');
                CompactFormatter.write_i64(out, v).expect("vec write");
                out.push(b'"');
            }
            KeyRepr::U64(v) => {
                out.push(b'"');
                CompactFormatter.write_u64(out, v).expect("vec write");
                out.push(b'"');
            }
            KeyRepr::Str(s, e) => {
                // Validated as UTF-8 when indexed.
                let text =
                    unsafe { std::str::from_utf8_unchecked(&self.arena[s as usize..e as usize]) };
                write_json_str(out, text);
            }
        }
    }

    /// The value bytes of an entry.
    pub(super) fn bytes(&self, e: Entry) -> &[u8] {
        &self.arena[e.start as usize..e.end as usize]
    }

    pub(super) fn entry_varint(&self, ei: u32) -> u64 {
        let e = self.entries[ei as usize];
        read_varint(self.bytes(e), 0)
            .expect("validated when indexed")
            .0
    }
}

fn key_repr(key: Scalar, bytes: &[u8], e: Entry) -> Result<KeyRepr> {
    Ok(match key {
        Scalar::Bool => KeyRepr::Bool(read_varint(bytes, 0)?.0 != 0),
        Scalar::Int32 => KeyRepr::I64(read_varint(bytes, 0)?.0 as i32 as i64),
        Scalar::Int64 => KeyRepr::I64(read_varint(bytes, 0)?.0 as i64),
        Scalar::Sint32 => KeyRepr::I64(zigzag32(read_varint(bytes, 0)?.0) as i64),
        Scalar::Sint64 => KeyRepr::I64(zigzag64(read_varint(bytes, 0)?.0)),
        Scalar::Sfixed32 => KeyRepr::I64(read_fixed32(bytes, 0)?.0 as i32 as i64),
        Scalar::Sfixed64 => KeyRepr::I64(read_fixed64(bytes, 0)?.0 as i64),
        Scalar::Uint32 => KeyRepr::U64(read_varint(bytes, 0)?.0 as u32 as u64),
        Scalar::Uint64 => KeyRepr::U64(read_varint(bytes, 0)?.0),
        Scalar::Fixed32 => KeyRepr::U64(read_fixed32(bytes, 0)?.0 as u64),
        Scalar::Fixed64 => KeyRepr::U64(read_fixed64(bytes, 0)?.0),
        Scalar::String => KeyRepr::Str(e.start, e.end),
        Scalar::Float | Scalar::Double | Scalar::Bytes => {
            return Err(decode_err("invalid map key type"));
        }
    })
}

fn cmp_keys(arena: &[u8], a: KeyRepr, b: KeyRepr) -> std::cmp::Ordering {
    match (a, b) {
        (KeyRepr::Str(as_, ae), KeyRepr::Str(bs, be)) => {
            arena[as_ as usize..ae as usize].cmp(&arena[bs as usize..be as usize])
        }
        _ => a.cmp(&b),
    }
}

/// Write a scalar from its value bytes (content for strings and bytes).
/// Returns whether the value is the type's default, for implicit presence.
pub(super) fn write_scalar(out: &mut Vec<u8>, scalar: Scalar, bytes: &[u8]) -> bool {
    let varint = || read_varint(bytes, 0).expect("validated when indexed").0;
    let fixed32 = || read_fixed32(bytes, 0).expect("validated when indexed").0;
    let fixed64 = || read_fixed64(bytes, 0).expect("validated when indexed").0;
    match scalar {
        Scalar::Bool => {
            let v = varint() != 0;
            out.extend_from_slice(if v { b"true" } else { b"false" });
            !v
        }
        Scalar::Int32 => {
            let v = varint() as i32;
            CompactFormatter.write_i32(out, v).expect("vec write");
            v == 0
        }
        Scalar::Sint32 => {
            let v = zigzag32(varint());
            CompactFormatter.write_i32(out, v).expect("vec write");
            v == 0
        }
        Scalar::Sfixed32 => {
            let v = fixed32() as i32;
            CompactFormatter.write_i32(out, v).expect("vec write");
            v == 0
        }
        Scalar::Uint32 => {
            let v = varint() as u32;
            CompactFormatter.write_u32(out, v).expect("vec write");
            v == 0
        }
        Scalar::Fixed32 => {
            let v = fixed32();
            CompactFormatter.write_u32(out, v).expect("vec write");
            v == 0
        }
        // 64-bit integers are strings in protobuf JSON.
        Scalar::Int64 => {
            let v = varint() as i64;
            out.push(b'"');
            CompactFormatter.write_i64(out, v).expect("vec write");
            out.push(b'"');
            v == 0
        }
        Scalar::Sint64 => {
            let v = zigzag64(varint());
            out.push(b'"');
            CompactFormatter.write_i64(out, v).expect("vec write");
            out.push(b'"');
            v == 0
        }
        Scalar::Sfixed64 => {
            let v = fixed64() as i64;
            out.push(b'"');
            CompactFormatter.write_i64(out, v).expect("vec write");
            out.push(b'"');
            v == 0
        }
        Scalar::Uint64 => {
            let v = varint();
            out.push(b'"');
            CompactFormatter.write_u64(out, v).expect("vec write");
            out.push(b'"');
            v == 0
        }
        Scalar::Fixed64 => {
            let v = fixed64();
            out.push(b'"');
            CompactFormatter.write_u64(out, v).expect("vec write");
            out.push(b'"');
            v == 0
        }
        Scalar::Float => {
            let v = f32::from_bits(fixed32());
            write_float(out, v);
            // -0.0 compares equal to 0.0, so it is a default too, as on the
            // DynamicMessage path.
            v == 0.0
        }
        Scalar::Double => {
            let v = f64::from_bits(fixed64());
            write_double(out, v);
            v == 0.0
        }
        Scalar::String => {
            // Validated as UTF-8 when indexed.
            let text = unsafe { std::str::from_utf8_unchecked(bytes) };
            write_json_str(out, text);
            bytes.is_empty()
        }
        Scalar::Bytes => {
            out.push(b'"');
            let start = out.len();
            let encoded_len = base64::encoded_len(bytes.len(), true).expect("length fits");
            out.resize(start + encoded_len, 0);
            BASE64
                .encode_slice(bytes, &mut out[start..])
                .expect("buffer sized for the encoding");
            out.push(b'"');
            bytes.is_empty()
        }
    }
}

/// The JSON for a scalar's default value.
pub(super) fn write_scalar_default(out: &mut Vec<u8>, scalar: Scalar) {
    let zero = [0u8; 8];
    let bytes: &[u8] = match scalar.wire_type() {
        WT_VARINT => &zero[..1],
        WT_FIXED32 => &zero[..4],
        WT_FIXED64 => &zero[..8],
        _ => &zero[..0],
    };
    write_scalar(out, scalar, bytes);
}

pub(super) fn write_enum(out: &mut Vec<u8>, plan: &EnumPlan, number: i32) {
    if plan.is_null_value {
        out.extend_from_slice(b"null");
        return;
    }
    match plan.values.binary_search_by_key(&number, |(n, _)| *n) {
        Ok(i) => write_json_str(out, &plan.values[i].1),
        Err(_) => CompactFormatter.write_i32(out, number).expect("vec write"),
    }
}
