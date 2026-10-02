//! JSON text output identical to serde_json's: number formatting through its
//! `CompactFormatter`, string escaping through the same table.

use serde_json::ser::{CompactFormatter, Formatter};

pub(super) fn write_float(out: &mut Vec<u8>, v: f32) {
    if v.is_finite() {
        CompactFormatter.write_f32(out, v).expect("vec write");
    } else if v == f32::INFINITY {
        out.extend_from_slice(b"\"Infinity\"");
    } else if v == f32::NEG_INFINITY {
        out.extend_from_slice(b"\"-Infinity\"");
    } else {
        out.extend_from_slice(b"\"NaN\"");
    }
}

pub(super) fn write_double(out: &mut Vec<u8>, v: f64) {
    if v.is_finite() {
        CompactFormatter.write_f64(out, v).expect("vec write");
    } else if v == f64::INFINITY {
        out.extend_from_slice(b"\"Infinity\"");
    } else if v == f64::NEG_INFINITY {
        out.extend_from_slice(b"\"-Infinity\"");
    } else {
        out.extend_from_slice(b"\"NaN\"");
    }
}

#[inline]
pub(super) fn sep(out: &mut Vec<u8>, first: &mut bool) {
    if !*first {
        out.push(b',');
    }
    *first = false;
}

// serde_json's escape table: `b'u'` means \u00XX, 0 means copy through.
const UU: u8 = b'u';
const ESCAPE: [u8; 256] = {
    let mut t = [0u8; 256];
    let mut i = 0;
    while i < 0x20 {
        t[i] = UU;
        i += 1;
    }
    t[0x08] = b'b';
    t[0x09] = b't';
    t[0x0A] = b'n';
    t[0x0C] = b'f';
    t[0x0D] = b'r';
    t[b'"' as usize] = b'"';
    t[b'\\' as usize] = b'\\';
    t
};

/// Write `s` as a JSON string with serde_json's exact escaping.
pub(super) fn write_json_str(out: &mut Vec<u8>, s: &str) {
    out.push(b'"');
    let bytes = s.as_bytes();
    let mut run_start = 0usize;
    for (i, &b) in bytes.iter().enumerate() {
        let esc = ESCAPE[b as usize];
        if esc == 0 {
            continue;
        }
        if run_start < i {
            out.extend_from_slice(&bytes[run_start..i]);
        }
        if esc == UU {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            out.extend_from_slice(&[
                b'\\',
                b'u',
                b'0',
                b'0',
                HEX[(b >> 4) as usize],
                HEX[(b & 0xF) as usize],
            ]);
        } else {
            out.extend_from_slice(&[b'\\', esc]);
        }
        run_start = i + 1;
    }
    out.extend_from_slice(&bytes[run_start..]);
    out.push(b'"');
}

#[cfg(test)]
mod tests {
    use super::write_json_str;

    /// Every escape decision serde_json makes, checked against serde_json.
    #[test]
    fn strings_escape_like_serde_json() {
        let mut cases: Vec<String> = [
            "",
            "a",
            "héllo \"world\"\n☃",
            "\u{0}\u{1}\u{1f}\u{7f}",
            "back\\slash/solidus",
            "emoji 🦀 and 中文",
            "\u{80}\u{7ff}\u{800}\u{ffff}\u{10000}\u{10ffff}",
        ]
        .map(str::to_string)
        .to_vec();
        cases.push((0u8..=0x7F).map(|b| b as char).collect());
        for s in cases {
            let mut got = Vec::new();
            write_json_str(&mut got, &s);
            assert_eq!(got, serde_json::to_vec(&s).unwrap(), "{s:?}");
        }
    }
}
