//! Compiled per-message plans: what a descriptor pool says about each field,
//! resolved once so the per-message passes never touch `prost_reflect` types.

use std::collections::{HashMap, HashSet};

use prost_reflect::{Cardinality, DescriptorPool, Kind, MessageDescriptor};

use super::wire::{WT_FIXED32, WT_FIXED64, WT_LEN, WT_VARINT};

/// Index of a [`MessagePlan`] within [`Plans`].
pub type PlanId = u32;

pub(super) type EnumId = u32;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Scalar {
    Bool,
    Int32,
    Sint32,
    Sfixed32,
    Uint32,
    Fixed32,
    Int64,
    Sint64,
    Sfixed64,
    Uint64,
    Fixed64,
    Float,
    Double,
    String,
    Bytes,
}

impl Scalar {
    pub(super) fn wire_type(self) -> u8 {
        match self {
            Scalar::Bool
            | Scalar::Int32
            | Scalar::Sint32
            | Scalar::Uint32
            | Scalar::Int64
            | Scalar::Sint64
            | Scalar::Uint64 => WT_VARINT,
            Scalar::Sfixed32 | Scalar::Fixed32 | Scalar::Float => WT_FIXED32,
            Scalar::Sfixed64 | Scalar::Fixed64 | Scalar::Double => WT_FIXED64,
            Scalar::String | Scalar::Bytes => WT_LEN,
        }
    }

    pub(super) fn is_packable(self) -> bool {
        !matches!(self, Scalar::String | Scalar::Bytes)
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Leaf {
    Scalar(Scalar),
    Enum(EnumId),
    Message(PlanId),
}

impl Leaf {
    pub(super) fn wire_type(self) -> u8 {
        match self {
            Leaf::Scalar(s) => s.wire_type(),
            Leaf::Enum(_) => WT_VARINT,
            Leaf::Message(_) => WT_LEN,
        }
    }

    pub(super) fn is_packable(self) -> bool {
        match self {
            Leaf::Scalar(s) => s.is_packable(),
            Leaf::Enum(_) => true,
            Leaf::Message(_) => false,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Card {
    Single {
        /// proto2 optional/required fields, proto3 `optional` fields, oneof
        /// members, and message fields are emitted even at their default
        /// value. Everything else is omitted when equal to the default.
        has_explicit_presence: bool,
    },
    Repeated,
    Map {
        key: Scalar,
        value: Leaf,
    },
}

pub(super) struct FieldPlan {
    pub(super) number: u32,
    pub(super) name: String,
    pub(super) leaf: Leaf,
    pub(super) card: Card,
    /// Index of the containing oneof within the message, if any. Synthetic
    /// oneofs for proto3 `optional` are included; they have one member, so the
    /// last-set rule is a no-op for them.
    pub(super) oneof: Option<u16>,
    pub(super) is_group: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Wkt {
    Any,
    Timestamp,
    Duration,
    Struct,
    Value,
    ListValue,
    FloatValue,
    DoubleValue,
    Int32Value,
    Int64Value,
    UInt32Value,
    UInt64Value,
    BoolValue,
    StringValue,
    BytesValue,
    FieldMask,
    Empty,
}

impl Wkt {
    pub(super) fn from_full_name(name: &str) -> Option<Wkt> {
        Some(match name {
            "google.protobuf.Any" => Wkt::Any,
            "google.protobuf.Timestamp" => Wkt::Timestamp,
            "google.protobuf.Duration" => Wkt::Duration,
            "google.protobuf.Struct" => Wkt::Struct,
            "google.protobuf.Value" => Wkt::Value,
            "google.protobuf.ListValue" => Wkt::ListValue,
            "google.protobuf.FloatValue" => Wkt::FloatValue,
            "google.protobuf.DoubleValue" => Wkt::DoubleValue,
            "google.protobuf.Int32Value" => Wkt::Int32Value,
            "google.protobuf.Int64Value" => Wkt::Int64Value,
            "google.protobuf.UInt32Value" => Wkt::UInt32Value,
            "google.protobuf.UInt64Value" => Wkt::UInt64Value,
            "google.protobuf.BoolValue" => Wkt::BoolValue,
            "google.protobuf.StringValue" => Wkt::StringValue,
            "google.protobuf.BytesValue" => Wkt::BytesValue,
            "google.protobuf.FieldMask" => Wkt::FieldMask,
            "google.protobuf.Empty" => Wkt::Empty,
            _ => return None,
        })
    }
}

/// Field-number → field-index lookup. Dense when field numbers are small,
/// which they almost always are.
pub(super) enum Lookup {
    Dense(Vec<u16>),
    Sparse(Vec<(u32, u16)>),
}

pub(super) const NO_FIELD: u16 = u16::MAX;
pub(super) const DENSE_LOOKUP_LIMIT: u32 = 1024;

impl Lookup {
    fn build(numbers: &[u32]) -> Lookup {
        let max = numbers.iter().copied().max().unwrap_or(0);
        if max <= DENSE_LOOKUP_LIMIT {
            let mut table = vec![NO_FIELD; max as usize + 1];
            for (idx, &n) in numbers.iter().enumerate() {
                table[n as usize] = idx as u16;
            }
            Lookup::Dense(table)
        } else {
            let mut pairs: Vec<(u32, u16)> = numbers
                .iter()
                .enumerate()
                .map(|(i, &n)| (n, i as u16))
                .collect();
            pairs.sort_unstable();
            Lookup::Sparse(pairs)
        }
    }

    #[inline]
    pub(super) fn get(&self, number: u32) -> Option<usize> {
        match self {
            Lookup::Dense(table) => match table.get(number as usize) {
                Some(&idx) if idx != NO_FIELD => Some(idx as usize),
                _ => None,
            },
            Lookup::Sparse(pairs) => pairs
                .binary_search_by_key(&number, |&(n, _)| n)
                .ok()
                .map(|i| pairs[i].1 as usize),
        }
    }
}

pub(super) struct MessagePlan {
    pub(super) wkt: Option<Wkt>,
    /// Ascending by field number.
    pub(super) fields: Vec<FieldPlan>,
    pub(super) lookup: Lookup,
    /// Field indexes of each oneof's members, by oneof index.
    pub(super) oneof_members: Vec<Vec<u16>>,
    /// Extensions are serialized by the `DynamicMessage` path with their JSON
    /// names; the fast path does not implement them.
    pub(super) has_extensions: bool,
}

pub(super) struct EnumPlan {
    /// Sorted by number.
    pub(super) values: Vec<(i32, String)>,
    /// The first declared value, which is the default for a map entry with no
    /// value field. Zero in proto3; anything in proto2.
    pub(super) default_number: i32,
    pub(super) is_null_value: bool,
}

/// Compiled plans for every message type reachable from a schema's descriptor
/// pool, plus the root message the schema registry subject names.
pub struct Plans {
    pub(super) pool: DescriptorPool,
    pub(super) root_name: String,
    pub(super) root: PlanId,
    pub(super) messages: Vec<MessagePlan>,
    pub(super) enums: Vec<EnumPlan>,
    pub(super) by_name: HashMap<String, PlanId>,
}

impl Plans {
    pub fn for_schema(pool: &DescriptorPool, root_message_name: &str) -> anyhow::Result<Plans> {
        let mut by_name: HashMap<String, PlanId> = HashMap::new();
        let descriptors: Vec<MessageDescriptor> = pool.all_messages().collect();
        for (idx, desc) in descriptors.iter().enumerate() {
            by_name.insert(desc.full_name().to_string(), idx as PlanId);
        }

        let mut enum_ids: HashMap<String, EnumId> = HashMap::new();
        let mut enums = Vec::new();
        for desc in pool.all_enums() {
            enum_ids.insert(desc.full_name().to_string(), enums.len() as EnumId);
            let mut numbers: Vec<i32> = desc.values().map(|v| v.number()).collect();
            numbers.sort_unstable();
            numbers.dedup();
            // Aliases share a number; resolve each number the way the
            // DynamicMessage path does so the chosen name is identical.
            let values: Vec<(i32, String)> = numbers
                .into_iter()
                .map(|n| {
                    let name = desc
                        .get_value(n)
                        .expect("every declared number resolves")
                        .name()
                        .to_string();
                    (n, name)
                })
                .collect();
            enums.push(EnumPlan {
                values,
                default_number: desc.default_value().number(),
                is_null_value: desc.full_name() == "google.protobuf.NullValue",
            });
        }

        let mut extended: HashSet<String> = HashSet::new();
        for ext in pool.all_extensions() {
            extended.insert(ext.containing_message().full_name().to_string());
        }

        let leaf_for = |kind: Kind| -> anyhow::Result<Leaf> {
            Ok(match kind {
                Kind::Double => Leaf::Scalar(Scalar::Double),
                Kind::Float => Leaf::Scalar(Scalar::Float),
                Kind::Int32 => Leaf::Scalar(Scalar::Int32),
                Kind::Int64 => Leaf::Scalar(Scalar::Int64),
                Kind::Uint32 => Leaf::Scalar(Scalar::Uint32),
                Kind::Uint64 => Leaf::Scalar(Scalar::Uint64),
                Kind::Sint32 => Leaf::Scalar(Scalar::Sint32),
                Kind::Sint64 => Leaf::Scalar(Scalar::Sint64),
                Kind::Fixed32 => Leaf::Scalar(Scalar::Fixed32),
                Kind::Fixed64 => Leaf::Scalar(Scalar::Fixed64),
                Kind::Sfixed32 => Leaf::Scalar(Scalar::Sfixed32),
                Kind::Sfixed64 => Leaf::Scalar(Scalar::Sfixed64),
                Kind::Bool => Leaf::Scalar(Scalar::Bool),
                Kind::String => Leaf::Scalar(Scalar::String),
                Kind::Bytes => Leaf::Scalar(Scalar::Bytes),
                Kind::Enum(e) => Leaf::Enum(
                    *enum_ids
                        .get(e.full_name())
                        .ok_or_else(|| anyhow::anyhow!("enum {} not in pool", e.full_name()))?,
                ),
                Kind::Message(m) => Leaf::Message(
                    *by_name
                        .get(m.full_name())
                        .ok_or_else(|| anyhow::anyhow!("message {} not in pool", m.full_name()))?,
                ),
            })
        };

        let mut messages = Vec::with_capacity(descriptors.len());
        for desc in &descriptors {
            let mut fields = Vec::with_capacity(desc.fields().len());
            // `fields()` yields ascending field numbers.
            for field in desc.fields() {
                let leaf = leaf_for(field.kind())?;
                let card = if field.is_map() {
                    let entry = match field.kind() {
                        Kind::Message(m) => m,
                        _ => anyhow::bail!("map field {} is not a message", field.full_name()),
                    };
                    let key = match leaf_for(entry.map_entry_key_field().kind())? {
                        Leaf::Scalar(s) => s,
                        _ => anyhow::bail!("map key of {} is not scalar", field.full_name()),
                    };
                    Card::Map {
                        key,
                        value: leaf_for(entry.map_entry_value_field().kind())?,
                    }
                } else if field.cardinality() == Cardinality::Repeated {
                    Card::Repeated
                } else {
                    Card::Single {
                        has_explicit_presence: field.supports_presence(),
                    }
                };
                let oneof = field.containing_oneof().map(|containing| {
                    desc.oneofs()
                        .position(|o| o == containing)
                        .expect("a field's containing oneof belongs to its message")
                        as u16
                });
                fields.push(FieldPlan {
                    number: field.number(),
                    name: field.name().to_string(),
                    leaf,
                    card,
                    oneof,
                    is_group: field.is_group(),
                });
            }
            let numbers: Vec<u32> = fields.iter().map(|f| f.number).collect();
            let mut oneof_members = vec![Vec::new(); desc.oneofs().len()];
            for (idx, field) in fields.iter().enumerate() {
                if let Some(oneof) = field.oneof {
                    oneof_members[oneof as usize].push(idx as u16);
                }
            }
            messages.push(MessagePlan {
                wkt: Wkt::from_full_name(desc.full_name()),
                lookup: Lookup::build(&numbers),
                fields,
                oneof_members,
                has_extensions: extended.contains(desc.full_name()),
            });
        }

        let root = *by_name.get(root_message_name).ok_or_else(|| {
            anyhow::anyhow!("message '{root_message_name}' not found in descriptor pool")
        })?;

        Ok(Plans {
            pool: pool.clone(),
            root_name: root_message_name.to_string(),
            root,
            messages,
            enums,
            by_name,
        })
    }

    pub fn root(&self) -> PlanId {
        self.root
    }

    pub fn plan_for(&self, desc: &MessageDescriptor) -> Option<PlanId> {
        self.by_name.get(desc.full_name()).copied()
    }

    /// Resolve the plan selected by the Confluent message-index framing that
    /// follows the schema id, returning it with the number of index bytes
    /// consumed. Mirrors `protobuf::parse_message_indexes` +
    /// `protobuf::resolve_message_from_indexes`.
    pub fn plan_for_indexes(&self, index_bytes: &[u8]) -> anyhow::Result<(PlanId, usize)> {
        // The common framings select the root without any allocation: an
        // empty array (one zero byte) or no index bytes at all.
        match index_bytes.first() {
            None => return Ok((self.root, 0)),
            Some(0) => return Ok((self.root, 1)),
            Some(_) => {}
        }
        let (indexes, consumed) = crate::protobuf::parse_message_indexes(index_bytes)?;
        if indexes.len() == 1 && indexes[0] == 0 {
            return Ok((self.root, consumed));
        }
        let desc =
            crate::protobuf::resolve_message_from_indexes(&self.pool, &self.root_name, &indexes)?;
        let plan = self
            .plan_for(&desc)
            .ok_or_else(|| anyhow::anyhow!("message '{}' has no plan", desc.full_name()))?;
        Ok((plan, consumed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_is_dense_for_small_numbers_and_sparse_above_the_limit() {
        let dense = Lookup::build(&[1, 3, 7]);
        assert!(matches!(dense, Lookup::Dense(_)));
        assert_eq!(dense.get(3), Some(1));
        assert_eq!(dense.get(2), None);
        assert_eq!(dense.get(8), None);

        let sparse = Lookup::build(&[1, 2000, 3000]);
        assert!(matches!(sparse, Lookup::Sparse(_)));
        assert_eq!(sparse.get(1), Some(0));
        assert_eq!(sparse.get(2000), Some(1));
        assert_eq!(sparse.get(2500), None);
        assert_eq!(sparse.get(u32::MAX), None);
    }

    #[test]
    fn message_index_framing_resolves_like_production() {
        let pool = crate::document::differential::pool();
        let plans = Plans::for_schema(&pool, "differential.Everything").unwrap();
        // Zigzag-encoded index arrays: empty, [0], [1], [0,0], [1,0], [5].
        for framing in [
            vec![],
            vec![0x00],
            vec![0x02, 0x00],
            vec![0x02, 0x02],
            vec![0x04, 0x00, 0x00],
            vec![0x04, 0x02, 0x00],
            vec![0x02, 0x0A],
        ] {
            let production =
                crate::protobuf::parse_message_indexes(&framing).and_then(|(indexes, used)| {
                    crate::protobuf::resolve_message_from_indexes(
                        &pool,
                        "differential.Everything",
                        &indexes,
                    )
                    .map(|desc| (plans.plan_for(&desc).unwrap(), used))
                });
            let ours = plans.plan_for_indexes(&framing);
            match (production, ours) {
                (Ok(expected), Ok(got)) => assert_eq!(got, expected, "{framing:?}"),
                (Err(_), Err(_)) => {}
                (p, o) => panic!("{framing:?}: production {p:?} vs transcoder {o:?}"),
            }
        }
    }
}
