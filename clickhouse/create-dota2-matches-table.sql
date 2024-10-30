CREATE TABLE IF NOT EXISTS dota2.matches (
    match_id UInt64,
    match_seq_num UInt64,
    game_mode UInt8,
    lobby_type UInt8,
    radiant_win Bool,
    duration UInt16,
    pre_game_duration UInt16,
    start_time UInt64,
    tower_status_radiant UInt32,
    tower_status_dire UInt32,
    barracks_status_radiant UInt32,
    barracks_status_dire UInt32,
    cluster UInt32,
    first_blood_time UInt16,
    human_players UInt8,
    leagueid UInt32,
    flags UInt8,
    engine UInt8,
    radiant_score UInt16,
    dire_score UInt16,
    radiant_team_id UInt64,
    radiant_name LowCardinality(String),
    radiant_logo UInt64,
    radiant_team_complete UInt64,
    dire_team_id UInt64,
    dire_name LowCardinality(String),
    dire_logo UInt64,
    dire_team_complete UInt64,
    radiant_captain UInt64,
    dire_captain UInt64,
    players Array(Tuple(
        account_id UInt32,
        player_slot UInt8,
        team_number UInt8,
        team_slot UInt8,
        hero_id UInt8,
        hero_variant UInt8,
        item_0 UInt16,
        item_1 UInt16,
        item_2 UInt16,
        item_3 UInt16,
        item_4 UInt16,
        item_5 UInt16,
        backpack_0 UInt16,
        backpack_1 UInt16,
        backpack_2 UInt16,
        item_neutral UInt16,
        kills UInt8,
        deaths UInt8,
        assists UInt8,
        leaver_status UInt8,
        last_hits UInt16,
        denies UInt16,
        gold_per_min UInt16,
        xp_per_min UInt16,
        level UInt8,
        net_worth UInt32,
        aghanims_scepter UInt8,
        aghanims_shard UInt8,
        moonshard UInt8,
        hero_damage UInt32,
        tower_damage UInt32,
        hero_healing UInt32,
        gold UInt32,
        gold_spent UInt32,
        scaled_hero_damage UInt32,
        scaled_tower_damage UInt32,
        scaled_hero_healing UInt32,       
        ability_upgrades Array(Tuple(
            ability UInt16,
            time UInt16,
            level UInt16,
        )),
        additional_units Array(Tuple(
            unitname LowCardinality(String),
            item_0 UInt16,
            item_1 UInt16,
            item_2 UInt16,
            item_3 UInt16,
            item_4 UInt16,
            item_5 UInt16,
            backpack_0 UInt16,
            backpack_1 UInt16,
            backpack_2 UInt16,
            item_neutral UInt16,
        )),
    )),
    picks_bans Array(Tuple(
        is_pick Bool,
        hero_id UInt8,
        team UInt8,
        order UInt8,
    )),
)
ENGINE = MergeTree
PRIMARY KEY (match_id, match_seq_num)
ORDER BY (match_id, match_seq_num);
