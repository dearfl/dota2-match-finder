use clickhouse::{error::Error, Client};
use primitive_types::U256;

use crate::dota2::{MatchDraft, MatchMask};

pub struct Database {
    database: String,
    client: Client,
}

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

        for hero in 0u8..=255 {
            let table = format!("index_mask_{}", hero);
            let query = format!(
                "CREATE TABLE IF NOT EXISTS {}.{} (
                        match_id UInt64,
                        radiant UInt256,
                        dire UInt256,
                    )
                    ENGINE = MergeTree()
                    ORDER BY match_id
                    PARTITION BY intDiv(match_id, 100000000)
                    PRIMARY KEY match_id;",
                &database, table
            );
            client.query(&query).execute().await?;
        }

        Ok(Self { database, client })
    }

    pub async fn query_matches(
        &self,
        team1: &[u8],
        team2: &[u8],
        limit: usize,
        offset: usize,
    ) -> Result<Vec<MatchDraft>, Error> {
        async fn count(client: &Client, database: &str, team: &[u8]) -> Option<u8> {
            let mut ret = Vec::with_capacity(5);
            for &hero in team {
                let table = format!("index_mask_{}", hero);
                let query = format!("SELECT count() FROM {}.{}", database, table);
                let count = client
                    .query(&query)
                    .fetch_one::<u64>()
                    .await
                    .ok()
                    .unwrap_or_default();
                ret.push((count, hero));
            }
            ret.sort_unstable();
            ret.first().map(|&(_, hero)| hero)
        }

        let to_mask = |heroes: &[u8]| {
            let mut mask = U256::zero();
            for &hero in heroes {
                mask |= U256::one() << hero;
            }
            mask
        };

        let hero1 = count(&self.client, &self.database, team1).await;
        let hero2 = count(&self.client, &self.database, team2).await;
        let mask1 = to_mask(team1);
        let mask2 = to_mask(team2);

        let filters = match (hero1, hero2) {
            (None, None) => None,
            (None, Some(h2)) => Some(((h2, mask2), None)),
            (Some(h1), None) => Some(((h1, mask1), None)),
            (Some(h1), Some(h2)) => Some(((h1, mask1), Some((h2, mask2)))),
        };

        let to_cond =
            |side: &str, mask: U256| format!("(bitOr({}, toUInt256('{}')) = {})", side, mask, side);

        let result = match filters {
            None => vec![],
            Some(((hero, mask), None)) => {
                let table = format!("index_mask_{}", hero);
                let cond1 = to_cond("radiant", mask);
                let cond2 = to_cond("dire", mask);
                let query = format!(
                    "SELECT ?fields FROM {}.{}
                     WHERE ({} OR {})
                     ORDER BY match_id DESC
                     LIMIT {} OFFSET {}",
                    self.database, table, cond1, cond2, limit, offset
                );
                self.client
                    .query(&query)
                    .fetch_all::<MatchMask>()
                    .await?
                    .iter()
                    .map(MatchDraft::from)
                    .collect()
            }
            Some(((hero, mask1), Some((_, mask2)))) => {
                let table = format!("index_mask_{}", hero);
                let cond1 = format!(
                    "({} AND {})",
                    to_cond("radiant", mask1),
                    to_cond("dire", mask2)
                );
                let cond2 = format!(
                    "({} AND {})",
                    to_cond("radiant", mask2),
                    to_cond("dire", mask1)
                );
                let query = format!(
                    "SELECT ?fields FROM {}.{}
                     WHERE ({} OR {})
                     ORDER BY match_id DESC
                     LIMIT {} OFFSET {}",
                    self.database, table, cond1, cond2, limit, offset
                );
                self.client
                    .query(&query)
                    .fetch_all::<MatchMask>()
                    .await?
                    .iter()
                    .map(MatchDraft::from)
                    .collect()
            }
        };
        Ok(result)
    }

    pub async fn save_indexed_masks(&self, hero: u8, masks: &[MatchMask]) -> Result<(), Error> {
        let table = format!("index_mask_{}", hero);
        let mut insert = self.client.insert(&table)?;
        for mat in masks {
            insert.write(mat).await?;
        }
        insert.end().await?;
        Ok(())
    }
}
