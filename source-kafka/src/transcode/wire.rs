//! Protobuf wire-format readers: tags, varints, fixed-width values, length
//! prefixes, and skipping over unknown fields, with prost's limits.

use super::{decode_err, Result};

/// Nesting depth at which prost stops decoding.
pub(super) const RECURSION_LIMIT: u32 = 100;

// Wire types, as encoded in the low three bits of a tag.
pub(super) const WT_VARINT: u8 = 0;
pub(super) const WT_FIXED64: u8 = 1;
pub(super) const WT_LEN: u8 = 2;
pub(super) const WT_START_GROUP: u8 = 3;
pub(super) const WT_END_GROUP: u8 = 4;
pub(super) const WT_FIXED32: u8 = 5;

pub(super) fn wire_type_name(wt: u8) -> &'static str {
    match wt {
        WT_VARINT => "Varint",
        WT_FIXED64 => "SixtyFourBit",
        WT_LEN => "LengthDelimited",
        WT_START_GROUP => "StartGroup",
        WT_END_GROUP => "EndGroup",
        WT_FIXED32 => "ThirtyTwoBit",
        _ => "invalid",
    }
}

#[inline]
pub(super) fn zigzag32(v: u64) -> i32 {
    // prost narrows to 32 bits before decoding, which matters for overlong
    // encodings whose high bits are set.
    let v = v as u32;
    ((v >> 1) as i32) ^ (-((v & 1) as i32))
}

#[inline]
pub(super) fn zigzag64(v: u64) -> i64 {
    ((v >> 1) as i64) ^ (-((v & 1) as i64))
}

/// Decode a varint at `pos`, returning the value and the position after it.
/// Accepts the same encodings prost does: at most ten bytes, with the tenth
/// contributing at most one bit.
#[inline]
pub(super) fn read_varint(buf: &[u8], pos: usize) -> Result<(u64, usize)> {
    let Some(&b0) = buf.get(pos) else {
        return Err(decode_err("buffer underflow"));
    };
    if b0 < 0x80 {
        return Ok((b0 as u64, pos + 1));
    }
    let mut value = (b0 & 0x7F) as u64;
    let mut shift = 7u32;
    let mut i = pos + 1;
    loop {
        let Some(&b) = buf.get(i) else {
            return Err(decode_err("buffer underflow"));
        };
        i += 1;
        if shift == 63 {
            if b > 1 {
                return Err(decode_err("invalid varint"));
            }
            value |= (b as u64) << 63;
            return Ok((value, i));
        }
        value |= ((b & 0x7F) as u64) << shift;
        if b < 0x80 {
            return Ok((value, i));
        }
        shift += 7;
    }
}

#[inline]
pub(super) fn read_fixed32(buf: &[u8], pos: usize) -> Result<(u32, usize)> {
    match buf.get(pos..pos + 4) {
        Some(b) => Ok((u32::from_le_bytes([b[0], b[1], b[2], b[3]]), pos + 4)),
        None => Err(decode_err("buffer underflow")),
    }
}

#[inline]
pub(super) fn read_fixed64(buf: &[u8], pos: usize) -> Result<(u64, usize)> {
    match buf.get(pos..pos + 8) {
        Some(b) => Ok((
            u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]),
            pos + 8,
        )),
        None => Err(decode_err("buffer underflow")),
    }
}

/// Read a length prefix at `pos` and return the (start, end) of the bytes it
/// covers.
#[inline]
pub(super) fn read_len_prefixed(buf: &[u8], pos: usize) -> Result<(usize, usize)> {
    let (len, start) = read_varint(buf, pos)?;
    let end = start
        .checked_add(len as usize)
        .filter(|&end| len <= buf.len() as u64 && end <= buf.len())
        .ok_or_else(|| decode_err("buffer underflow"))?;
    Ok((start, end))
}

/// Read a tag at `pos`: (field number, wire type, position after).
#[inline]
pub(super) fn read_tag(buf: &[u8], pos: usize) -> Result<(u32, u8, usize)> {
    let (key, next) = read_varint(buf, pos)?;
    if key > u32::MAX as u64 {
        return Err(decode_err(format!("invalid key value: {key}")));
    }
    let wt = (key & 7) as u8;
    let number = (key >> 3) as u32;
    if number == 0 {
        return Err(decode_err("invalid tag value: 0"));
    }
    if wt == 6 || wt == 7 {
        return Err(decode_err(format!("invalid wire type value: {wt}")));
    }
    Ok((number, wt, next))
}

/// Where a message instance sits in the two decoders the production path
/// runs. `abs` is the nesting depth prost-reflect's dynamic decode sees; `rel`
/// is the depth prost's generated decoder sees inside a well-known type, which
/// the production path re-decodes from that type's root.
#[derive(Clone, Copy, Debug)]
pub(super) struct Depth {
    pub abs: u32,
    pub rel: Option<u32>,
}

/// How unknown fields in an instance are skipped: the dynamic decode uses
/// `UnknownField::decode_value` at message level (scalars unchecked, groups
/// counted) and prost's `skip_field` inside map entries (always checked).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Kind {
    Message,
    MapEntry,
}

impl Depth {
    pub const TOP: Depth = Depth { abs: 0, rel: None };

    /// Entering a nested message: both decoders refuse past their limit, and
    /// a well-known type starts prost's relative count.
    pub fn enter_message(self, child_is_wkt: bool) -> Result<Depth> {
        if self.abs >= RECURSION_LIMIT || self.rel.is_some_and(|r| r >= RECURSION_LIMIT) {
            return Err(decode_err("recursion limit reached"));
        }
        let rel = match self.rel {
            Some(r) => Some(r + 1),
            None if child_is_wkt => Some(0),
            None => None,
        };
        Ok(Depth {
            abs: self.abs + 1,
            rel,
        })
    }

    /// Entering a map entry: the dynamic decode does not count it, prost's
    /// generated map merge does.
    pub fn enter_map_entry(self) -> Result<Depth> {
        if self.rel.is_some_and(|r| r >= RECURSION_LIMIT) {
            return Err(decode_err("recursion limit reached"));
        }
        Ok(Depth {
            abs: self.abs,
            rel: self.rel.map(|r| r + 1),
        })
    }

    fn deeper(self) -> Depth {
        Depth {
            abs: self.abs + 1,
            rel: self.rel.map(|r| r + 1),
        }
    }
}

/// Skip an unknown field with both decoders' limits applied. Groups are
/// skipped recursively, counting a level each.
pub(super) fn skip_unknown(
    buf: &[u8],
    pos: usize,
    wt: u8,
    number: u32,
    depth: Depth,
    kind: Kind,
) -> Result<usize> {
    let is_checked_by_dynamic = kind == Kind::MapEntry || wt == WT_START_GROUP;
    if (is_checked_by_dynamic && depth.abs >= RECURSION_LIMIT)
        || depth.rel.is_some_and(|r| r >= RECURSION_LIMIT)
    {
        return Err(decode_err("recursion limit reached"));
    }
    if wt != WT_START_GROUP {
        return skip_value(buf, pos, wt, number);
    }
    let mut p = pos;
    loop {
        let (inner_number, inner_wt, after) = read_tag(buf, p)?;
        if inner_wt == WT_END_GROUP {
            if inner_number != number {
                return Err(decode_err("unexpected end group tag"));
            }
            return Ok(after);
        }
        p = skip_unknown(buf, after, inner_wt, inner_number, depth.deeper(), kind)?;
    }
}

/// Skip the value of wire type `wt` starting at `pos`, returning the position
/// after it. Structural only: unknown fields go through [`skip_unknown`], which
/// also applies prost's recursion limit.
pub(super) fn skip_value(buf: &[u8], pos: usize, wt: u8, number: u32) -> Result<usize> {
    match wt {
        WT_VARINT => Ok(read_varint(buf, pos)?.1),
        WT_FIXED64 => {
            if pos + 8 > buf.len() {
                return Err(decode_err("buffer underflow"));
            }
            Ok(pos + 8)
        }
        WT_FIXED32 => {
            if pos + 4 > buf.len() {
                return Err(decode_err("buffer underflow"));
            }
            Ok(pos + 4)
        }
        WT_LEN => Ok(read_len_prefixed(buf, pos)?.1),
        WT_START_GROUP => {
            let mut p = pos;
            loop {
                let (inner_number, inner_wt, after) = read_tag(buf, p)?;
                if inner_wt == WT_END_GROUP {
                    if inner_number != number {
                        return Err(decode_err("unexpected end group tag"));
                    }
                    return Ok(after);
                }
                p = skip_value(buf, after, inner_wt, inner_number)?;
            }
        }
        WT_END_GROUP => Err(decode_err("unexpected end group tag")),
        _ => Err(decode_err(format!("invalid wire type value: {wt}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varints_accept_prost_encodings_and_reject_the_rest() {
        assert_eq!(read_varint(&[0x01], 0).unwrap(), (1, 1));
        assert_eq!(read_varint(&[0xAC, 0x02], 0).unwrap(), (300, 2));
        // Redundant continuation bytes are legal.
        assert_eq!(read_varint(&[0x81, 0x80, 0x00], 0).unwrap(), (1, 3));
        // Ten bytes with the tenth contributing one bit is the maximum.
        let max = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01];
        assert_eq!(read_varint(&max, 0).unwrap(), (u64::MAX, 10));
        let tenth_too_big = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x02];
        assert!(read_varint(&tenth_too_big, 0).is_err());
        let eleven = [0xFF; 11];
        assert!(read_varint(&eleven, 0).is_err());
        assert!(read_varint(&[0x80], 0).is_err(), "truncated");
        assert!(read_varint(&[], 0).is_err(), "empty");
    }

    #[test]
    fn tags_reject_zero_and_invalid_wire_types() {
        assert_eq!(read_tag(&[0x08], 0).unwrap(), (1, WT_VARINT, 1));
        assert!(read_tag(&[0x00], 0).is_err(), "field number 0");
        assert!(read_tag(&[0x0E], 0).is_err(), "wire type 6");
        assert!(read_tag(&[0x0F], 0).is_err(), "wire type 7");
        // Field number 2^29 - 1 is the largest tag prost accepts.
        let mut buf = Vec::new();
        encode_varint(((1u64 << 29) - 1) << 3, &mut buf);
        assert_eq!(read_tag(&buf, 0).unwrap().0, (1 << 29) - 1);
        buf.clear();
        encode_varint(1u64 << 32, &mut buf);
        assert!(read_tag(&buf, 0).is_err(), "key wider than 32 bits");
    }

    #[test]
    fn skipping_handles_every_wire_type_and_groups() {
        // varint, fixed64, length-delimited, fixed32
        assert_eq!(skip_value(&[0x96, 0x01], 0, WT_VARINT, 1).unwrap(), 2);
        assert_eq!(skip_value(&[0; 8], 0, WT_FIXED64, 1).unwrap(), 8);
        assert_eq!(skip_value(&[0x02, b'h', b'i'], 0, WT_LEN, 1).unwrap(), 3);
        assert_eq!(skip_value(&[0; 4], 0, WT_FIXED32, 1).unwrap(), 4);
        assert!(
            skip_value(&[0; 7], 0, WT_FIXED64, 1).is_err(),
            "short fixed64"
        );
        assert!(
            skip_value(&[0x05, b'h', b'i'], 0, WT_LEN, 1).is_err(),
            "length overrun"
        );

        // A group: start tag already consumed, contents, matching end tag.
        let group = [0x08, 0x05, 0x12, 0x01, b'x', 0x14]; // field1=5, field2="x", end group for field 2
        assert_eq!(
            skip_value(&group, 0, WT_START_GROUP, 2).unwrap(),
            group.len()
        );
        let mismatched = [0x08, 0x05, 0x1C]; // end group for field 3
        assert!(skip_value(&mismatched, 0, WT_START_GROUP, 2).is_err());
        assert!(
            skip_value(&[], 0, WT_END_GROUP, 2).is_err(),
            "stray end group"
        );
        assert!(
            skip_value(&[0x08, 0x05], 0, WT_START_GROUP, 2).is_err(),
            "unterminated"
        );
    }

    /// Each decoder's limit: the dynamic decode counts groups only, prost's
    /// `skip_field` (map entries, and anything inside a well-known type)
    /// counts every unknown field.
    #[test]
    fn unknown_fields_follow_both_decoders_limits() {
        let nested = |n: usize| [vec![0x0B; n], vec![0x0C; n]].concat();
        let top = Depth::TOP;
        let ok = nested(RECURSION_LIMIT as usize);
        assert_eq!(
            skip_unknown(&ok, 1, WT_START_GROUP, 1, top, Kind::Message).unwrap(),
            ok.len()
        );
        let too_deep = nested(RECURSION_LIMIT as usize + 1);
        assert!(skip_unknown(&too_deep, 1, WT_START_GROUP, 1, top, Kind::Message).is_err());

        let at_limit = Depth {
            abs: RECURSION_LIMIT,
            rel: None,
        };
        // Message-level scalars are never checked by the dynamic decode.
        assert_eq!(
            skip_unknown(&[0x00], 0, WT_VARINT, 1, at_limit, Kind::Message).unwrap(),
            1
        );
        // Map entries and well-known types check before reading anything.
        assert!(skip_unknown(&[0x00], 0, WT_VARINT, 1, at_limit, Kind::MapEntry).is_err());
        let rel_at_limit = Depth {
            abs: 5,
            rel: Some(RECURSION_LIMIT),
        };
        assert!(skip_unknown(&[0x00], 0, WT_VARINT, 1, rel_at_limit, Kind::Message).is_err());
    }

    fn encode_varint(mut v: u64, out: &mut Vec<u8>) {
        while v >= 0x80 {
            out.push((v & 0x7F) as u8 | 0x80);
            v >>= 7;
        }
        out.push(v as u8);
    }
}
