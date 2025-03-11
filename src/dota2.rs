use clickhouse::Row;
use serde::{Deserialize, Serialize};

pub type RawMatch = kez::dota2::get_match_history_by_seq_num::Match;

#[derive(Row, Deserialize, Serialize, Clone, Debug)]
pub struct AbilityUpgrade {
    pub ability: u16,
    pub time: u16,
    pub level: u16,
}

impl From<kez::dota2::get_match_history_by_seq_num::AbilityUpgrade> for AbilityUpgrade {
    fn from(value: kez::dota2::get_match_history_by_seq_num::AbilityUpgrade) -> Self {
        Self {
            ability: value.ability,
            time: value.time,
            level: value.level,
        }
    }
}

#[derive(Row, Deserialize, Serialize, Clone, Debug)]
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
    pub item_neutral2: u16,
}

impl From<kez::dota2::get_match_history_by_seq_num::Unit> for Unit {
    fn from(value: kez::dota2::get_match_history_by_seq_num::Unit) -> Self {
        Self {
            unitname: value.unitname,
            item_0: value.item_0,
            item_1: value.item_1,
            item_2: value.item_2,
            item_3: value.item_3,
            item_4: value.item_4,
            item_5: value.item_5,
            backpack_0: value.backpack_0,
            backpack_1: value.backpack_1,
            backpack_2: value.backpack_2,
            item_neutral: value.item_neutral,
            item_neutral2: value.item_neutral2,
        }
    }
}

#[derive(Row, Deserialize, Serialize, Clone, Debug)]
pub struct Player {
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
    pub item_neutral2: u16,
    pub kills: u8,
    pub deaths: u8,
    pub assists: u8,
    pub leaver_status: u8,
    pub last_hits: u16,
    pub denies: u16,
    pub gold_per_min: u16,
    pub xp_per_min: u16,
    pub level: u8,
    pub net_worth: u32,
    pub aghanims_scepter: u8,
    pub aghanims_shard: u8,
    pub moonshard: u8,
    pub hero_damage: u32,
    pub tower_damage: u32,
    pub hero_healing: u32,
    pub gold: u32,
    pub gold_spent: u32,
    pub scaled_hero_damage: u32,
    pub scaled_tower_damage: u32,
    pub scaled_hero_healing: u32,
    pub ability_upgrades: Vec<AbilityUpgrade>,
    pub additional_units: Vec<Unit>,
}

impl From<kez::dota2::get_match_history_by_seq_num::Player> for Player {
    fn from(value: kez::dota2::get_match_history_by_seq_num::Player) -> Self {
        Self {
            account_id: value.account_id,
            player_slot: value.player_slot,
            team_number: value.team_number,
            team_slot: value.team_slot,
            hero_id: value.hero_id,
            hero_variant: value.hero_variant,
            item_0: value.item_0,
            item_1: value.item_1,
            item_2: value.item_2,
            item_3: value.item_3,
            item_4: value.item_4,
            item_5: value.item_5,
            backpack_0: value.backpack_0,
            backpack_1: value.backpack_1,
            backpack_2: value.backpack_2,
            item_neutral: value.item_neutral,
            item_neutral2: value.item_neutral2,
            kills: value.kills,
            deaths: value.deaths,
            assists: value.assists,
            leaver_status: value.leaver_status,
            last_hits: value.last_hits,
            denies: value.denies,
            gold_per_min: value.gold_per_min,
            xp_per_min: value.xp_per_min,
            level: value.level,
            net_worth: value.net_worth,
            aghanims_scepter: value.aghanims_scepter,
            aghanims_shard: value.aghanims_shard,
            moonshard: value.moonshard,
            hero_damage: value.hero_damage,
            tower_damage: value.tower_damage,
            hero_healing: value.hero_healing,
            gold: value.gold,
            gold_spent: value.gold_spent,
            scaled_hero_damage: value.scaled_hero_damage,
            scaled_tower_damage: value.scaled_tower_damage,
            scaled_hero_healing: value.scaled_hero_healing,
            ability_upgrades: value.ability_upgrades.into_iter().map(Into::into).collect(),
            additional_units: value.additional_units.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Row, Deserialize, Serialize, Clone, Debug)]
pub struct Draft {
    pub is_pick: bool,
    pub hero_id: u8,
    pub team: u8,
    pub order: u8,
}

impl From<kez::dota2::get_match_history_by_seq_num::Draft> for Draft {
    fn from(value: kez::dota2::get_match_history_by_seq_num::Draft) -> Self {
        Self {
            is_pick: value.is_pick,
            hero_id: value.hero_id,
            team: value.team,
            order: value.order,
        }
    }
}

#[derive(Row, Deserialize, Serialize, Clone, Debug)]
pub struct Match {
    pub players: Vec<Player>,
    pub radiant_win: bool,
    pub duration: u16,
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
    pub tournament_id: u64,
    pub tournament_round: u64,
    pub picks_bans: Vec<Draft>,
}

impl From<RawMatch> for Match {
    fn from(value: RawMatch) -> Self {
        Self {
            players: value.players.into_iter().map(Into::into).collect(),
            radiant_win: value.radiant_win,
            duration: value.duration,
            start_time: value.start_time,
            match_id: value.match_id,
            match_seq_num: value.match_seq_num,
            tower_status_radiant: value.tower_status_radiant,
            tower_status_dire: value.tower_status_dire,
            barracks_status_radiant: value.barracks_status_radiant,
            barracks_status_dire: value.barracks_status_dire,
            cluster: value.cluster,
            first_blood_time: value.first_blood_time,
            lobby_type: value.lobby_type,
            human_players: value.human_players,
            leagueid: value.leagueid,
            game_mode: value.game_mode,
            flags: value.flags,
            engine: value.engine,
            radiant_score: value.radiant_score,
            dire_score: value.dire_score,
            tournament_id: value.tournament_id,
            tournament_round: value.tournament_round,
            picks_bans: value.picks_bans.into_iter().map(Into::into).collect(),
        }
    }
}
