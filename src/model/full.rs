use clickhouse::Row;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Copy, Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct AbilityUpgrade {
    pub ability: u16,
    pub time: u16,
    pub level: u16,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct Unit {
    pub unitname: String,
    pub item_0: u16,
    pub item_1: u16,
    pub item_2: u16,
    pub item_3: u16,
    pub item_4: u16,
    pub item_5: u16,
    pub backpack_0: u16,
    pub backpack_1: u16,
    pub backpack_2: u16,
    pub item_neutral: u16,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct Player {
    #[serde(default)]
    pub account_id: u32,
    pub player_slot: u8,
    pub team_number: u8,
    pub team_slot: u8,
    pub hero_id: u8,
    pub hero_variant: u8,
    pub item_0: u16,
    pub item_1: u16,
    pub item_2: u16,
    pub item_3: u16,
    pub item_4: u16,
    pub item_5: u16,
    pub backpack_0: u16,
    pub backpack_1: u16,
    pub backpack_2: u16,
    pub item_neutral: u16,
    pub kills: u8,
    pub deaths: u8,
    pub assists: u8,
    #[serde(default)]
    pub leaver_status: u8,
    pub last_hits: u16,
    pub denies: u16,
    pub gold_per_min: u16,
    pub xp_per_min: u16,
    pub level: u8,
    pub net_worth: u32,
    #[serde(default)]
    pub aghanims_scepter: u8,
    #[serde(default)]
    pub aghanims_shard: u8,
    #[serde(default)]
    pub moonshard: u8,
    #[serde(default)]
    pub hero_damage: u32,
    #[serde(default)]
    pub tower_damage: u32,
    #[serde(default)]
    pub hero_healing: u32,
    #[serde(default)]
    pub gold: u32,
    #[serde(default)]
    pub gold_spent: u32,
    #[serde(default)]
    pub scaled_hero_damage: u32,
    #[serde(default)]
    pub scaled_tower_damage: u32,
    #[serde(default)]
    pub scaled_hero_healing: u32,
    #[serde(default)]
    pub ability_upgrades: Vec<AbilityUpgrade>,
    #[serde(default)]
    pub additional_units: Vec<Unit>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct HeroSelection {
    pub is_pick: bool,
    pub hero_id: u8,
    pub team: u8,
    pub order: u8,
}

#[derive(Row, Deserialize, Serialize, Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct Match {
    pub players: Vec<Player>,
    pub radiant_win: bool,
    pub duration: u16,
    pub pre_game_duration: u16,
    pub start_time: u64,
    pub match_id: u64,
    pub match_seq_num: u64,
    pub tower_status_radiant: u32,
    pub tower_status_dire: u32,
    pub barracks_status_radiant: u32,
    pub barracks_status_dire: u32,
    pub cluster: u32,
    pub first_blood_time: u16,
    pub lobby_type: u8,
    pub human_players: u8,
    pub leagueid: u32,
    pub game_mode: u8,
    pub flags: u8,
    pub engine: u8,
    pub radiant_score: u16,
    pub dire_score: u16,
    #[serde(default)]
    pub radiant_team_id: u64,
    #[serde(default)]
    pub radiant_name: String,
    #[serde(default)]
    pub radiant_logo: u64,
    #[serde(default)]
    pub radiant_team_complete: u64,
    #[serde(default)]
    pub dire_team_id: u64,
    #[serde(default)]
    pub dire_name: String,
    #[serde(default)]
    pub dire_logo: u64,
    #[serde(default)]
    pub dire_team_complete: u64,
    #[serde(default)]
    pub radiant_captain: u64,
    #[serde(default)]
    pub dire_captain: u64,
    #[serde(default)]
    pub picks_bans: Vec<HeroSelection>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct MatchHistory {
    pub status: u8,
    pub matches: Vec<Match>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct MatchHistoryResponse {
    pub result: MatchHistory,
}

mod tests {

    #[test]
    fn test_1730303804() {
        use super::MatchHistoryResponse;
        let content =
            std::fs::read_to_string("./tests/1730303804-error.json").expect("Failed to read json");
        serde_json::from_str::<MatchHistoryResponse>(&content)
            .expect("Failed to parse json response");
    }
}
