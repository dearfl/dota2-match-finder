use std::fmt::Display;

use clickhouse::Row;
use primitive_types::U256;
use serde::{Deserialize, Serialize};

pub mod full;
pub mod partial;

mod u256 {
    use primitive_types::U256;
    use serde::{
        de::{Deserialize, Deserializer},
        ser::{Serialize, Serializer},
    };

    pub fn serialize<S: Serializer>(u: &U256, serializer: S) -> Result<S::Ok, S::Error> {
        let buf = u.to_little_endian();
        buf.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<U256, D::Error>
    where
        D: Deserializer<'de>,
    {
        let u: [u8; 32] = Deserialize::deserialize(deserializer)?;
        Ok(U256::from_little_endian(&u))
    }
}

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

#[derive(Row, Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MatchMask {
    pub match_id: u64,
    #[serde(with = "u256")]
    pub radiant: U256,
    #[serde(with = "u256")]
    pub dire: U256,
}

impl From<&full::Match> for MatchMask {
    fn from(value: &full::Match) -> Self {
        let match_id = value.match_id;
        let mut radiant = U256::zero();
        let mut dire = U256::zero();
        for player in &value.players {
            let side: Side = player.player_slot.into();
            let hero_mask = U256::one() << player.hero_id;
            match side {
                Side::Radiant => radiant |= hero_mask,
                Side::Dire => dire |= hero_mask,
            }
        }
        Self {
            match_id,
            radiant,
            dire,
        }
    }
}

#[derive(Row, Debug, Clone, Serialize, Deserialize)]
pub struct MatchDraft {
    pub match_id: u64,
    pub radiant: Vec<u8>,
    pub dire: Vec<u8>,
}

impl From<&MatchMask> for MatchDraft {
    fn from(value: &MatchMask) -> Self {
        let to_array =
            |val: U256| -> Vec<u8> { (0..=255u8).filter(|&idx| val.bit(idx.into())).collect() };

        let match_id = value.match_id;
        let radiant = to_array(value.radiant);
        let dire = to_array(value.dire);

        Self {
            match_id,
            radiant,
            dire,
        }
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

mod tests {

    #[test]
    fn test() {
        use super::full::MatchHistoryResponse;
        use super::{MatchDraft, MatchMask};

        let parse = |file: &str| -> Vec<MatchDraft> {
            let content = std::fs::read_to_string(file).expect("Failed to read file");
            let resp = serde_json::from_str::<MatchHistoryResponse>(&content)
                .expect("Failed to parse json response");
            resp.result
                .matches
                .iter()
                .map(|mat| {
                    let mask: MatchMask = mat.into();
                    (&mask).into()
                })
                .collect()
        };
        parse("./tests/1730303804-error.json");
        parse("./tests/6742154809-error.json");
    }
}
