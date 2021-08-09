#![allow(dead_code)]

use std::ops::RangeInclusive;

use serde::Deserialize;
use serde_with::serde_as;
use serde_with::FromInto;

mod shard_range;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to read the catalog file")]
    File(#[from] std::io::Error),

    #[error("failed to validate connector catalog")]
    Format(#[from] serde_json::Error),
}

#[serde_as]
#[derive(Deserialize)]
pub struct ConfiguredCatalog {
    /// A list of streams to read from.
    pub streams: Vec<ConfiguredStream>,

    /// When tailing, the connector will never exit. When not tailing, the
    /// connector will exit once it has consumed messages emitted before the
    /// connector process was launched.
    ///
    /// "estuary.dev/tail": true,
    #[serde(rename = "estuary.dev/tail", default = "tail_default")]
    pub tail: bool,

    /// This instance is only responsible for a subset of the data.
    ///
    ///  "estuary.dev/range": {
    ///    "begin": "00000000",
    ///    "end": "ffffffff"
    ///  }
    #[serde(rename = "estuary.dev/range", default = "range_default")]
    #[serde_as(as = "FromInto<shard_range::ShardRangeDefinition>")]
    pub range: RangeInclusive<u32>,
}

impl Default for ConfiguredCatalog {
    fn default() -> Self {
        Self {
            streams: vec![],
            tail: tail_default(),
            range: range_default(),
        }
    }
}

#[derive(Deserialize)]
pub struct ConfiguredStream {
    pub stream: Stream,
}

#[derive(Deserialize)]
pub struct Stream {
    pub name: String,
}

fn tail_default() -> bool {
    false
}

fn range_default() -> RangeInclusive<u32> {
    0..=u32::MAX
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::{from_value, json};

    #[test]
    fn stream_parsing_test() {
        let input = json!({
            "streams": [
                {"stream": {"name": "test"} },
            ],
            "estuary.dev/tail": false,
            "estuary.dev/range": {
                "begin": "00010000",
                "end": "ffffffff",
            },
        });

        let parsed: ConfiguredCatalog = from_value(input).expect("to parse configured catalog");

        assert_eq!("test", parsed.streams[0].stream.name);
        assert_eq!(false, parsed.tail);
        assert_eq!(65_536..=u32::MAX, parsed.range);
    }
}
