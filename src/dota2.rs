use clickhouse::Row;
use kez::dota2::{Match, Side};
use serde::{Deserialize, Serialize};

#[derive(Row, Debug, Clone, Serialize, Deserialize)]
pub struct MatchDraft {
    pub match_id: u64,
    pub radiant: [u8; 5],
    pub dire: [u8; 5],
}

impl From<&Match> for MatchDraft {
    fn from(value: &Match) -> Self {
        let match_id = u64::from(value.match_id);
        let mut radiant = [0; 5];
        let mut dire = [0; 5];
        let mut ridx = 0;
        let mut didx = 0;
        value.players.iter().for_each(|player| {
            let (side, _) = player.slot;
            // ideally there should be exactly 5 randiant and 5 dire
            // however we live in a bizarre world
            match side {
                Side::Radiant if ridx < 5 => {
                    radiant[ridx] = u8::from(player.hero.0);
                    ridx += 1;
                }
                Side::Dire if didx < 5 => {
                    dire[didx] = u8::from(player.hero.0);
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
