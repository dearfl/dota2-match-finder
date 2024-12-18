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

#[derive(Row, Debug, Clone, Serialize, Deserialize)]
pub struct MatchDraft {
    pub match_id: u64,
    pub radiant: [u8; 5],
    pub dire: [u8; 5],
}

impl From<&full::Match> for MatchDraft {
    fn from(value: &full::Match) -> Self {
        let match_id = value.match_id;
        let mut radiant = [0; 5];
        let mut dire = [0; 5];
        let mut ridx = 0;
        let mut didx = 0;
        value.players.iter().for_each(|player| {
            let side: Side = player.player_slot.into();
            // ideally there should be exactly 5 randiant and 5 dire
            // however we live in a bizarre world
            match side {
                Side::Radiant if ridx < 5 => {
                    radiant[ridx] = player.hero_id;
                    ridx += 1;
                }
                Side::Dire if didx < 5 => {
                    dire[didx] = player.hero_id;
                    didx += 1;
                }
                _ => log::warn!("problematic match {}", match_id),
            }
        });
        Self {
            match_id,
            radiant,
            dire,
        }
    }
}

impl From<full::Match> for MatchDraft {
    fn from(value: full::Match) -> Self {
        Self::from(&value)
    }
}

mod tests {
    use super::full::MatchHistoryResponse;
    use super::MatchDraft;
    #[allow(dead_code)]
    fn parse_file(file: &str) -> Vec<MatchDraft> {
        let content = std::fs::read_to_string(file).expect("Failed to read file");
        let resp = serde_json::from_str::<MatchHistoryResponse>(&content)
            .expect("Failed to parse json response");
        resp.result.matches.iter().map(Into::into).collect()
    }

    #[test]
    fn test_1730303804() {
        parse_file("./tests/1730303804-error.json");
    }

    #[test]
    fn test_6742154809() {
        parse_file("./tests/6742154809-error.json");
    }

    #[test]
    fn test_6796079312() {
        parse_file("./tests/6796079312-error.json");
    }
}
