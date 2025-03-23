use crate::hash_slot::hash_slot::HashSlot;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::ops::Range;

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
/// Combine two [`HashSlot`]s into a range.
/// This is an INCLUSIVE range, `end` is part of the range.
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
        self.start > self.end
    }

    pub fn len(&self) -> u16 {
        u16::from(self.end) + 1 - u16::from(self.start)
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

// TODO: This might be unintuitive:
// ```
//  let r = 0..1;
//  assert!(!r.contains(1))
// ```
// BUT
// ```
// let hsr = HashSlotRange::from(0..1);
// assert!(hsr.contains(1.into()));
// ```
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

impl From<&HashSlotRange> for (u16, u16) {
    fn from(value: &HashSlotRange) -> Self {
        (value.start.0, value.end.0)
    }
}

#[cfg(test)]
mod test {
    use crate::hash_slot::hash_slot::HashSlot;
    use crate::hash_slot::hash_slot_range::HashSlotRange;

    #[test]
    fn test_inclusive_range() {
        let start = HashSlot(0);
        let end = HashSlot(1);
        let hsr = HashSlotRange::new(start, end);

        assert!(!hsr.is_empty());
        assert_eq!(hsr.len(), 2);
        assert!(hsr.contains(&start));
        assert!(hsr.contains(&end));
    }

    #[test]
    fn test_single_element() {
        let start = HashSlot(0);
        let end = HashSlot(0);
        let hsr = HashSlotRange::new(start, end);

        assert!(!hsr.is_empty());
        assert_eq!(hsr.len(), 1);
        assert!(hsr.contains(&start));
        assert!(hsr.contains(&end));
    }

    #[test]
    fn test_start_gt_end() {
        let start = HashSlot(1);
        let end = HashSlot(0);
        let hsr = HashSlotRange::new(start, end);

        assert!(hsr.is_empty());
        assert_eq!(hsr.len(), 0);
        assert!(!hsr.contains(&start));
        assert!(!hsr.contains(&end));
    }

    #[test]
    fn test_from_range() {
        let start = 0;
        let end = 1;
        let hsr = HashSlotRange::from(start..end);

        assert!(!hsr.is_empty());
        assert_eq!(hsr.len(), 2);
        assert!(hsr.contains(&start.into()));
        assert!(hsr.contains(&end.into()));
    }
}
