use highway::{HighwayHash, HighwayHasher};
use proto_flow::flow::RangeSpec;
use serde::{Deserialize, Serialize};

mod shard_range;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to read the catalog file")]
    File(#[from] std::io::Error),

    #[error("failed to validate connector catalog")]
    Format(#[from] serde_json::Error),

    #[error("cannot subscribe to non-existent stream: {0}")]
    MissingStream(String),
}

pub fn responsible_for_shard(range: &RangeSpec, key: impl Into<ShardKey>) -> bool {
    let hash = key.into().hash();
    range.key_begin <= hash && range.key_end >= hash
}

#[derive(Serialize, Deserialize)]
pub struct Resource {
    pub stream: String,
}

#[derive(Default)]
pub struct ShardKey(HighwayHasher);

impl ShardKey {
    pub fn add_int(mut self, n: impl Into<i64>) -> Self {
        self.0.append(&n.into().to_be_bytes());
        self
    }

    pub fn add_str(mut self, s: &str) -> Self {
        self.0.append(s.as_bytes());
        self
    }

    fn hash(self) -> u32 {
        self.0.finalize64() as u32
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn shard_assignment_test() {
        let range = RangeSpec {
            key_begin: 0,
            key_end: 0x7fffffff,
            r_clock_begin: 0,
            r_clock_end: 0xffffffff,
        };

        let shards = (0..10).map(|n| ShardKey::default().add_int(n));
        let shards_covered = 2;

        assert_eq!(
            shards_covered,
            shards
                .map(|n| responsible_for_shard(&range, n))
                .filter(|b| *b)
                .count()
        );
    }
}
