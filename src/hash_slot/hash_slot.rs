use crate::hash_slot::MOD;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::ops::{Add, AddAssign, Div, Sub, SubAssign};

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
pub struct HashSlot(pub u16);

impl HashSlot {
    pub fn new(key: &str) -> HashSlot {
        key.into()
    }
}

impl Display for HashSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Sub for HashSlot {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        HashSlot((self.0 - rhs.0) % MOD)
    }
}

impl Sub<u16> for HashSlot {
    type Output = Self;

    fn sub(self, rhs: u16) -> Self::Output {
        HashSlot(self.0 - rhs)
    }
}

impl SubAssign<u16> for HashSlot {
    fn sub_assign(&mut self, rhs: u16) {
        self.0 -= rhs;
    }
}

impl Add for HashSlot {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        HashSlot((self.0 + rhs.0) % MOD)
    }
}

impl Add<u16> for HashSlot {
    type Output = Self;
    fn add(self, rhs: u16) -> Self::Output {
        HashSlot(self.0 + rhs)
    }
}

impl AddAssign<i32> for HashSlot {
    fn add_assign(&mut self, rhs: i32) {
        self.0 += rhs as u16;
        self.0 %= MOD
    }
}

impl Div<u16> for HashSlot {
    type Output = Self;

    fn div(self, rhs: u16) -> Self::Output {
        HashSlot((self.0 / rhs) % MOD)
    }
}

impl From<&str> for HashSlot {
    /// Hash the given `str` and return a HashSlot.
    /// If the value contains `{` and `}`, only the string in between will be hashed.
    /// See <a href="https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#hash-tags">Redis-Cluster specification: Hashtags</a>
    fn from(val: &str) -> Self {
        if let Some(s) = val.find("{") {
            if let Some(e) = val.find("}") {
                if (s + 1) < e
                /* Ensure Hashtag is not empty */
                {
                    return HashSlot(crc16(val[s + 1..e].into()) % MOD);
                }
            }
        }
        HashSlot(crc16(val) % MOD)
    }
}

impl From<u16> for HashSlot {
    fn from(val: u16) -> Self {
        HashSlot(val % MOD)
    }
}

impl From<HashSlot> for u16 {
    fn from(val: HashSlot) -> Self {
        val.0
    }
}

/// Shorthand to calculate crc16, because the full version is cumbersome.
fn crc16(key: &str) -> u16 {
    crc16::State::<crc16::XMODEM>::calculate(key.as_bytes())
}
