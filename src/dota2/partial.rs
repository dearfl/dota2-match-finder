// this is the response definition of get_match_history
// which is currently unused

use clickhouse::Row;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct Player {
    #[serde(default)]
    pub account_id: u32,
    pub player_slot: u8,
    pub hero_id: u8,
}

#[derive(Row, Deserialize, Serialize, Clone, Debug, Default)]
pub struct Match {
    pub players: Vec<Player>,
    pub start_time: u64,
    pub match_id: u64,
    pub match_seq_num: u64,
    pub lobby_type: u8,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct MatchHistory {
    pub status: u8,
    pub matches: Vec<Match>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct MatchHistoryResponse {
    pub result: MatchHistory,
}
