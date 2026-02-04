use anyhow::{Context, Result};
use doc::{
    shape::{schema::to_schema, ObjProperty},
    Shape,
};
use json::schema::{self as JsonSchema, types};
use prost_reflect::{DescriptorPool, DynamicMessage, Kind, MessageDescriptor};
use schemars::schema::RootSchema;

/// Parse Confluent Schema Registry Protobuf wire format message indexes.
/// Wire format after magic byte (0) + schema ID (4 bytes):
/// - zigzag varint: array length (number of indexes, 0 means use first message)
/// - zigzag varints: message indexes (path through nested messages)
///
/// Returns the parsed indexes and the number of bytes consumed.
pub fn parse_message_indexes(data: &[u8]) -> Result<(Vec<i32>, usize)> {
    if data.is_empty() {
        return Ok((vec![0], 0));
    }

    let (array_len, mut offset) = decode_zigzag_varint(data)
        .context("failed to decode message index array length")?;

    if array_len == 0 {
        // Array length of 0 means use the first (index 0) message
        return Ok((vec![0], offset));
    }

    let mut indexes = Vec::with_capacity(array_len as usize);
    for _ in 0..array_len {
        let (idx, consumed) = decode_zigzag_varint(&data[offset..])
            .context("failed to decode message index")?;
        indexes.push(idx as i32);
        offset += consumed;
    }

    Ok((indexes, offset))
}

/// Decode a zigzag-encoded varint from the given byte slice.
/// Returns the decoded value and the number of bytes consumed.
fn decode_zigzag_varint(data: &[u8]) -> Result<(i64, usize)> {
    let (unsigned, consumed) = decode_varint(data)?;
    // Zigzag decoding: (n >> 1) ^ -(n & 1)
    let signed = ((unsigned >> 1) as i64) ^ -((unsigned & 1) as i64);
    Ok((signed, consumed))
}

/// Decode an unsigned varint from the given byte slice.
/// Returns the decoded value and the number of bytes consumed.
fn decode_varint(data: &[u8]) -> Result<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0;

    for (i, &byte) in data.iter().enumerate() {
        if shift >= 64 {
            anyhow::bail!("varint too long");
        }

        result |= ((byte & 0x7F) as u64) << shift;
        shift += 7;

        if byte & 0x80 == 0 {
            return Ok((result, i + 1));
        }
    }

    anyhow::bail!("unexpected end of data while decoding varint")
}

/// Resolve a message descriptor from the descriptor pool using message indexes.
/// The indexes represent a path through nested messages starting from the root message.
pub fn resolve_message_from_indexes(
    pool: &DescriptorPool,
    root_message_name: &str,
    indexes: &[i32],
) -> Result<MessageDescriptor> {
    // If there's only one index and it's 0, just get the root message
    if indexes.len() == 1 && indexes[0] == 0 {
        return pool
            .get_message_by_name(root_message_name)
            .ok_or_else(|| anyhow::anyhow!("message '{}' not found in descriptor pool", root_message_name));
    }

    // For multiple indexes, we need to traverse nested messages
    // The first index selects which top-level message in the file
    // Subsequent indexes select nested messages

    // First, find the file that contains our root message
    let root_msg = pool
        .get_message_by_name(root_message_name)
        .ok_or_else(|| anyhow::anyhow!("message '{}' not found in descriptor pool", root_message_name))?;

    let file = root_msg.parent_file();

    // Get all top-level messages in the file
    let messages: Vec<_> = file.messages().collect();

    if indexes.is_empty() {
        anyhow::bail!("empty message indexes");
    }

    let first_idx = indexes[0] as usize;
    if first_idx >= messages.len() {
        anyhow::bail!(
            "message index {} out of bounds, file has {} messages",
            first_idx,
            messages.len()
        );
    }

    let mut current_msg = messages[first_idx].clone();

    // Traverse nested messages using remaining indexes
    for &idx in &indexes[1..] {
        let nested: Vec<_> = current_msg.child_messages().collect();
        let idx = idx as usize;
        if idx >= nested.len() {
            anyhow::bail!(
                "nested message index {} out of bounds, message '{}' has {} nested messages",
                idx,
                current_msg.name(),
                nested.len()
            );
        }
        current_msg = nested[idx].clone();
    }

    Ok(current_msg)
}

/// Decode a protobuf message from bytes using the given descriptor.
pub fn decode_protobuf_message(
    descriptor: &MessageDescriptor,
    data: &[u8],
) -> Result<DynamicMessage> {
    DynamicMessage::decode(descriptor.clone(), data)
        .context("failed to decode protobuf message")
}

/// Convert a protobuf MessageDescriptor to a Flow Shape for JSON Schema generation.
pub fn proto_descriptor_to_shape(descriptor: &MessageDescriptor) -> Shape {
    let mut shape = Shape::nothing();
    shape.type_ = types::OBJECT;

    shape.object.properties = descriptor
        .fields()
        .map(|field| {
            let field_shape = proto_field_to_shape(&field);
            ObjProperty {
                name: field.name().into(),
                is_required: false, // Proto3 fields are all optional
                shape: field_shape,
            }
        })
        .collect();

    shape
}

fn proto_field_to_shape(field: &prost_reflect::FieldDescriptor) -> Shape {
    let shape = if field.is_list() {
        // Repeated field -> array
        let mut array_shape = Shape::nothing();
        array_shape.type_ = types::ARRAY;

        let item_shape = proto_kind_to_shape(field.kind());
        array_shape.array.additional_items = Some(Box::new(item_shape));

        array_shape
    } else if field.is_map() {
        // Map field -> object
        let mut map_shape = Shape::nothing();
        map_shape.type_ = types::OBJECT;

        if let Kind::Message(value_msg) = field.kind() {
            // Map value is the second field of the map entry message
            if let Some(value_field) = value_msg.fields().nth(1) {
                let value_shape = proto_kind_to_shape(value_field.kind());
                map_shape.object.additional_properties = Some(Box::new(value_shape));
            }
        }

        map_shape
    } else {
        proto_kind_to_shape(field.kind())
    };

    // Add description from field comments if available
    // (prost-reflect doesn't expose comments easily, so we skip this)

    shape
}

fn proto_kind_to_shape(kind: Kind) -> Shape {
    let mut shape = Shape::nothing();

    match kind {
        Kind::Double | Kind::Float => {
            shape.type_ = types::INT_OR_FRAC;
        }
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 | Kind::Uint32 | Kind::Fixed32 => {
            shape.type_ = types::INTEGER;
        }
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 | Kind::Uint64 | Kind::Fixed64 => {
            // 64-bit integers are serialized as strings in protobuf JSON
            shape.type_ = types::STRING;
            shape.string.format = Some(JsonSchema::formats::Format::Integer);
        }
        Kind::Bool => {
            shape.type_ = types::BOOLEAN;
        }
        Kind::String => {
            shape.type_ = types::STRING;
        }
        Kind::Bytes => {
            shape.type_ = types::STRING;
            shape.string.content_encoding = Some("base64".into());
        }
        Kind::Enum(enum_desc) => {
            shape.type_ = types::STRING;
            shape.enum_ = Some(
                enum_desc
                    .values()
                    .map(|v| v.name().to_string().into())
                    .collect(),
            );
        }
        Kind::Message(msg_desc) => {
            // Handle well-known types
            let full_name = msg_desc.full_name();
            match full_name {
                "google.protobuf.Timestamp" => {
                    shape.type_ = types::STRING;
                    shape.string.format = Some(JsonSchema::formats::Format::DateTime);
                }
                "google.protobuf.Duration" => {
                    shape.type_ = types::STRING;
                    // Duration is serialized as "1.5s" format
                }
                "google.protobuf.BoolValue" => {
                    shape.type_ = types::BOOLEAN;
                }
                "google.protobuf.Int32Value" | "google.protobuf.UInt32Value" => {
                    shape.type_ = types::INTEGER;
                }
                "google.protobuf.Int64Value" | "google.protobuf.UInt64Value" => {
                    shape.type_ = types::STRING;
                    shape.string.format = Some(JsonSchema::formats::Format::Integer);
                }
                "google.protobuf.FloatValue" | "google.protobuf.DoubleValue" => {
                    shape.type_ = types::INT_OR_FRAC;
                }
                "google.protobuf.StringValue" => {
                    shape.type_ = types::STRING;
                }
                "google.protobuf.BytesValue" => {
                    shape.type_ = types::STRING;
                    shape.string.content_encoding = Some("base64".into());
                }
                "google.protobuf.Struct" => {
                    shape.type_ = types::OBJECT;
                }
                "google.protobuf.Value" => {
                    // google.protobuf.Value can be any JSON value
                    shape.type_ = types::ANY;
                }
                "google.protobuf.ListValue" => {
                    shape.type_ = types::ARRAY;
                }
                "google.protobuf.NullValue" => {
                    shape.type_ = types::NULL;
                }
                _ => {
                    // Regular nested message
                    shape = proto_descriptor_to_shape(&msg_desc);
                }
            }
        }
    }

    shape
}

/// Convert a protobuf key schema to a Shape for use in collection key derivation.
pub fn protobuf_key_schema_to_shape(descriptor: &MessageDescriptor) -> Shape {
    let mut shape = Shape::nothing();
    shape.type_ = types::OBJECT;

    shape.object.properties = descriptor
        .fields()
        .filter_map(|field| {
            let field_shape = proto_key_field_to_shape(&field)?;
            Some(ObjProperty {
                name: field.name().into(),
                is_required: true, // Keys should be required
                shape: field_shape,
            })
        })
        .collect();

    shape
}

fn proto_key_field_to_shape(field: &prost_reflect::FieldDescriptor) -> Option<Shape> {
    // Keys cannot be repeated or map fields
    if field.is_list() || field.is_map() {
        return None;
    }

    let mut shape = Shape::nothing();

    match field.kind() {
        Kind::Bool => {
            shape.type_ = types::BOOLEAN;
        }
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 | Kind::Uint32 | Kind::Fixed32
        | Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 | Kind::Uint64 | Kind::Fixed64 => {
            shape.type_ = types::INTEGER;
        }
        Kind::String => {
            shape.type_ = types::STRING;
        }
        Kind::Bytes => {
            shape.type_ = types::STRING;
            shape.string.content_encoding = Some("base64".into());
        }
        Kind::Enum(enum_desc) => {
            shape.type_ = types::STRING;
            shape.enum_ = Some(
                enum_desc
                    .values()
                    .map(|v| v.name().to_string().into())
                    .collect(),
            );
        }
        Kind::Message(msg_desc) => {
            // Nested messages can be keys if all their fields are keyable
            shape = protobuf_key_schema_to_shape(&msg_desc);
            if shape.object.properties.is_empty() {
                return None;
            }
        }
        // Float/Double are not good key types
        Kind::Float | Kind::Double => {
            return None;
        }
    }

    Some(shape)
}

/// Convert a protobuf descriptor to a JSON Schema RootSchema.
pub fn proto_descriptor_to_json_schema(descriptor: &MessageDescriptor) -> RootSchema {
    let shape = proto_descriptor_to_shape(descriptor);
    to_schema(shape)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_varint() {
        // Single byte varint: 1
        assert_eq!(decode_varint(&[0x01]).unwrap(), (1, 1));

        // Single byte varint: 127
        assert_eq!(decode_varint(&[0x7F]).unwrap(), (127, 1));

        // Two byte varint: 128
        assert_eq!(decode_varint(&[0x80, 0x01]).unwrap(), (128, 2));

        // Two byte varint: 300
        assert_eq!(decode_varint(&[0xAC, 0x02]).unwrap(), (300, 2));

        // Larger varint: 16384
        assert_eq!(decode_varint(&[0x80, 0x80, 0x01]).unwrap(), (16384, 3));
    }

    #[test]
    fn test_decode_zigzag_varint() {
        // Zigzag encoding: 0 -> 0
        assert_eq!(decode_zigzag_varint(&[0x00]).unwrap(), (0, 1));

        // Zigzag encoding: 1 -> -1
        assert_eq!(decode_zigzag_varint(&[0x01]).unwrap(), (-1, 1));

        // Zigzag encoding: 2 -> 1
        assert_eq!(decode_zigzag_varint(&[0x02]).unwrap(), (1, 1));

        // Zigzag encoding: 3 -> -2
        assert_eq!(decode_zigzag_varint(&[0x03]).unwrap(), (-2, 1));

        // Zigzag encoding: 4 -> 2
        assert_eq!(decode_zigzag_varint(&[0x04]).unwrap(), (2, 1));
    }

    #[test]
    fn test_parse_message_indexes_empty() {
        // Empty data means use first message (index 0)
        let (indexes, consumed) = parse_message_indexes(&[]).unwrap();
        assert_eq!(indexes, vec![0]);
        assert_eq!(consumed, 0);
    }

    #[test]
    fn test_parse_message_indexes_zero_length() {
        // Array length 0 means use first message
        let (indexes, consumed) = parse_message_indexes(&[0x00]).unwrap();
        assert_eq!(indexes, vec![0]);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn test_parse_message_indexes_single() {
        // Array length 1, index 0 (zigzag encoded as 0)
        let (indexes, consumed) = parse_message_indexes(&[0x02, 0x00]).unwrap();
        assert_eq!(indexes, vec![0]);
        assert_eq!(consumed, 2);

        // Array length 1, index 1 (zigzag encoded as 2)
        let (indexes, consumed) = parse_message_indexes(&[0x02, 0x02]).unwrap();
        assert_eq!(indexes, vec![1]);
        assert_eq!(consumed, 2);
    }

    #[test]
    fn test_parse_message_indexes_multiple() {
        // Array length 2, indexes [0, 1] (zigzag encoded as [0, 2])
        let (indexes, consumed) = parse_message_indexes(&[0x04, 0x00, 0x02]).unwrap();
        assert_eq!(indexes, vec![0, 1]);
        assert_eq!(consumed, 3);
    }
}
