use crate::hash_slot::hash_slot::HashSlot;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::ops::Range;

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
/// Combine two [`HashSlot`]s into a range.
#[derive(Eq, Hash, PartialEq)]
pub struct HashSlotRange {
    pub(crate) start: HashSlot,
    pub(crate) end: HashSlot,
}

impl HashSlotRange {
    pub fn new(start: HashSlot, end: HashSlot) -> Self {
        HashSlotRange { start, end }
    }

    pub fn contains(&self, slot: &HashSlot) -> bool {
        self.start <= *slot && self.end >= *slot
    }

    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }
}

impl Display for HashSlotRange {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            formatter,
            "{}..{}",
            u16::from(self.start),
            u16::from(self.end)
        )
    }
}

impl From<Range<u16>> for HashSlotRange {
    fn from(value: Range<u16>) -> Self {
        HashSlotRange {
            start: value.start.into(),
            end: value.end.into(),
        }
    }
}

impl From<HashSlotRange> for Range<u16> {
    fn from(value: HashSlotRange) -> Self {
        value.start.into()..value.end.into()
    }
}
