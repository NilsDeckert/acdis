/// Definitions for the type that is used to determine responsibility for keys.
pub mod hash_slot;

/// Definition for a Range type that combines two [`hash_slot::HashSlot`]s.
pub mod hash_slot_range;

/// The redis hash slots only use 14 of 16 bits of the crc16 output.
const MOD: u16 = 1 << 14; // 16384
