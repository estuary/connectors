use std::ops::RangeInclusive;

use serde::Deserialize;
use serde_with::serde_as;

/// Used to parse the exact format of the Range extension to the Airbyte Spec.
/// This intermediate data structure is then immediately converted into a normal
/// Range.
#[serde_as]
#[derive(Debug, Deserialize, PartialEq)]
pub(super) struct ShardRangeDefinition {
    begin: Hex32,
    end: Hex32,
}

impl From<ShardRangeDefinition> for RangeInclusive<u32> {
    fn from(source: ShardRangeDefinition) -> Self {
        source.begin.into()..=source.end.into()
    }
}

#[serde_as]
#[derive(Debug, Deserialize, PartialEq)]
struct Hex32(#[serde_as(as = "serde_with::hex::Hex")] [u8; 4]);

impl From<Hex32> for u32 {
    fn from(hex: Hex32) -> Self {
        u32::from_be_bytes(hex.0)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::{from_value, json};

    #[test]
    fn shard_range_definition_parsing_test() -> eyre::Result<()> {
        let zero: Hex32 = from_value(json!("00000000"))?;
        let two_five_five: Hex32 = from_value(json!("000000ff"))?;

        assert_eq!(
            ShardRangeDefinition {
                begin: zero,
                end: two_five_five
            },
            from_value(json!({"begin": "00000000", "end": "000000ff"}))?
        );

        Ok(())
    }

    #[test]
    fn hex_32_parsing_test() -> eyre::Result<()> {
        let zero: Hex32 = from_value(json!("00000000"))?;
        let one: Hex32 = from_value(json!("00000001"))?;
        let two_five_five: Hex32 = from_value(json!("000000ff"))?;
        let sixty_four_k: Hex32 = from_value(json!("0000ffff"))?;
        let max: Hex32 = from_value(json!("ffffffff"))?;

        assert_eq!(0, u32::from(zero));
        assert_eq!(1, u32::from(one));
        assert_eq!(255, u32::from(two_five_five));
        assert_eq!((2u32.pow(16) - 1) as u32, u32::from(sixty_four_k));
        assert_eq!(u32::MAX as u32, u32::from(max));

        Ok(())
    }
}
