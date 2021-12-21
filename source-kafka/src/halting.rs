use crate::catalog;
use crate::state;

#[derive(Debug)]
pub enum HaltCheck {
    Neverending,
    UntilWatermarks(state::CheckpointSet),
}

impl HaltCheck {
    pub fn new(
        catalog: &catalog::ConfiguredCatalog,
        watermarks: state::CheckpointSet,
    ) -> HaltCheck {
        if catalog.tail {
            HaltCheck::Neverending
        } else {
            HaltCheck::UntilWatermarks(watermarks)
        }
    }

    pub fn should_halt(&self, latest_state: &state::CheckpointSet) -> bool {
        match self {
            HaltCheck::Neverending => false,
            HaltCheck::UntilWatermarks(watermarks) => watermarks
                .iter()
                .all(|w| reached_latest_offset(&w, latest_state)),
        }
    }
}

fn reached_latest_offset(
    watermark: &state::Checkpoint,
    current_levels: &state::CheckpointSet,
) -> bool {
    if let Some(latest_offset) = current_levels.offset_for(&watermark.topic, watermark.partition) {
        latest_offset >= watermark.offset
    } else {
        true
    }
}
