use clickhouse::{Client, error::Error};

use crate::dota2::Match;

pub struct Database {
    database: String,
    table: String,
    client: Client,
}

const TABLE_MATCHES: &str = r#"
(
    match_id UInt64,
    match_seq_num UInt64,
    game_mode UInt8,
    lobby_type UInt8,
    radiant_win Bool,
    duration UInt16,
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
    tournament_id UInt64,
    tournament_round UInt64,
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
        item_neutral2 UInt16,
        kills UInt8,
        deaths UInt8,
        assists UInt8,
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
        leaver_status UInt8,
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
"#;

impl Database {
    pub async fn new(
        server: &str,
        database: &str,
        user: Option<&str>,
        password: Option<&str>,
    ) -> Result<Self, Error> {
        let database = database.to_string();
        let client = Client::default().with_url(server);

        let client = match user {
            Some(user) => client.with_user(user),
            _ => client,
        };

        let client = match password {
            Some(password) => client.with_password(password),
            _ => client,
        };

        // create database if not exists
        let query = format!("CREATE DATABASE IF NOT EXISTS {};", database);
        client.query(&query).execute().await?;

        let client = client.with_database(&database);
        let table = "matches".to_string();

        let query = format!(
            "CREATE TABLE IF NOT EXISTS {}.{} {}",
            &database, &table, TABLE_MATCHES
        );
        client.query(&query).execute().await?;

        Ok(Self {
            database,
            client,
            table,
        })
    }

    pub async fn save(&self, matches: &[Match]) -> Result<(), Error> {
        let mut insert = self.client.insert(&self.table)?;
        for mat in matches {
            insert.write(mat).await?;
        }
        insert.end().await?;
        Ok(())
    }

    pub async fn latest_match_seq_num(&self) -> u64 {
        let query = format!(
            "SELECT match_seq_num FROM {}.{} ORDER BY match_seq_num DESC LIMIT 1",
            self.database, self.table,
        );
        self.client
            .query(&query)
            .fetch_one()
            .await
            .map(|idx: u64| idx + 1)
            .unwrap_or_default()
    }
}
