use clickhouse::Row;
use kez::dota2::get_match_history_by_seq_num::Match;
use serde::{Deserialize, Serialize};

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

impl From<&Match> for MatchDraft {
    fn from(value: &Match) -> Self {
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

impl From<Match> for MatchDraft {
    fn from(value: Match) -> Self {
        Self::from(&value)
    }
}
