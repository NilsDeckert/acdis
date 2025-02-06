use std::fmt::{Display, Formatter};
use redis_protocol::resp3::types::OwnedFrame;
use redis_protocol_bridge::util::convert::AsFrame;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum MapEntry {
    STRING(String),
    USIZE(usize),
}

impl From<String> for MapEntry {
    fn from(string: String) -> Self {
        Self::STRING(string)
    }
}

impl From<usize> for MapEntry {
    fn from(size: usize) -> Self {
        Self::USIZE(size)
    }
}

impl MapEntry {
    /// Helper function to avoid duplication in From<MapEntry> and From<&MapEntry>
    fn to_owned_frame(&self) -> OwnedFrame {
        match self {
            MapEntry::STRING(s) => s.as_frame(),
            MapEntry::USIZE(u) => u.as_frame(),
        }
    }
}

impl From<MapEntry> for OwnedFrame {
    fn from(item: MapEntry) -> Self {
        item.to_owned_frame()
    }
}

impl From<&MapEntry> for OwnedFrame {
    fn from(item: &MapEntry) -> Self {
        item.to_owned_frame()
    }
}

impl Clone for MapEntry {
    fn clone(&self) -> Self {
        match self {
            MapEntry::STRING(a) => MapEntry::STRING(a.clone()),
            MapEntry::USIZE(a) => MapEntry::USIZE(a.clone())
        }
    }
}

impl Display for MapEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MapEntry::STRING(a) => write!(f, "{}", a),
            MapEntry::USIZE(a) => write!(f, "{}", a)
        }
    }
}