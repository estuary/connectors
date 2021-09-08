use crate::catalog;
use crate::state;

#[derive(Debug)]
pub enum HaltCheck {
    Neverending,
    UntilWatermarks(state::TopicSet),
}

impl HaltCheck {
    pub fn new(catalog: &catalog::ConfiguredCatalog, watermarks: state::TopicSet) -> HaltCheck {
        if catalog.tail {
            HaltCheck::Neverending
        } else {
            HaltCheck::UntilWatermarks(watermarks)
        }
    }

    pub fn should_halt(&self, latest_state: &state::TopicSet) -> bool {
        match self {
            HaltCheck::Neverending => false,
            HaltCheck::UntilWatermarks(watermarks) => watermarks
                .0
                .iter()
                .all(|w| reached_latest_offset(w, latest_state)),
        }
    }
}

fn reached_latest_offset(watermark: &state::Topic, current_levels: &state::TopicSet) -> bool {
    if let Some(latest_offset) = current_levels.offset_for(&watermark.name, watermark.partition) {
        latest_offset >= watermark.offset
    } else {
        true
    }
}
