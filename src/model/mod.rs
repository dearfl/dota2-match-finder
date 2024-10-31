use std::fmt::Display;

use clickhouse::Row;
use serde::{Deserialize, Serialize};

pub mod full;
pub mod partial;

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub enum Side {
    Radiant,
    Dire,
}

impl From<u8> for Side {
    fn from(value: u8) -> Self {
        if value & 0x80u8 != 0 {
            Self::Dire
        } else {
            Self::Radiant
        }
    }
}

impl Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Side::Radiant => "radiant",
            Side::Dire => "dire",
        };
        f.write_str(name)
    }
}

#[derive(Row, Serialize, Deserialize)]
pub struct MatchId {
    pub match_id: u64,
}

impl From<u64> for MatchId {
    fn from(value: u64) -> Self {
        Self { match_id: value }
    }
}
