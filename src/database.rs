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

    async fn least_count_hero(&self, team: &[u8]) -> Option<(u64, u8)> {
        let mut ret = Vec::with_capacity(5);
        for &hero in team {
            let table = format!("index_mask_{}", hero);
            let query = format!("SELECT count() FROM {}.{}", &self.database, table);
            let count = self
                .client
                .query(&query)
                .fetch_one::<u64>()
                .await
                .ok()
                .unwrap_or_default();
            ret.push((count, hero));
        }
        ret.sort_unstable();
        ret.first().copied()
    }

    pub async fn query_matches(
        &self,
        team1: &[u8],
        team2: &[u8],
        limit: usize,
        offset: usize,
    ) -> Result<Vec<MatchDraft>, Error> {
        let to_mask = |heroes: &[u8]| {
            let mut mask = U256::zero();
            for &hero in heroes {
                mask |= U256::one() << hero;
            }
            mask
        };

        let hero1 = self.least_count_hero(team1).await;
        let hero2 = self.least_count_hero(team2).await;
        let mask1 = to_mask(team1);
        let mask2 = to_mask(team2);

        let (hero, mask1, mask2) = match (hero1, hero2) {
            (None, None) => return Ok(vec![]), // both side are empty, return empty result
            (None, Some((_, h2))) => (h2, mask2, None),
            (Some((_, h1)), None) => (h1, mask1, None),
            (Some((cnt1, h1)), Some((cnt2, h2))) => {
                if cnt1 < cnt2 {
                    (h1, mask1, Some(mask2))
                } else {
                    (h2, mask2, Some(mask1))
                }
            }
        };

        let side_check =
            |side: &str, mask: U256| format!("(bitOr({}, toUInt256('{}')) = {})", side, mask, side);

        let table = format!("index_mask_{}", hero);
        let (cond1, cond2) = match (mask1, mask2) {
            (mask, None) => (side_check("radiant", mask), side_check("dire", mask)),
            (mask1, Some(mask2)) => {
                let cond1 = format!(
                    "({} AND {})",
                    side_check("radiant", mask1),
                    side_check("dire", mask2)
                );
                let cond2 = format!(
                    "({} AND {})",
                    side_check("radiant", mask2),
                    side_check("dire", mask1)
                );
                (cond1, cond2)
            }
        };
        let query = format!(
            "SELECT ?fields FROM {}.{} WHERE ({} OR {}) ORDER BY match_id DESC LIMIT {} OFFSET {}",
            self.database, table, cond1, cond2, limit, offset
        );
        self.query_match_draft(&query).await
    }

    pub async fn query_match_draft(&self, query: &str) -> Result<Vec<MatchDraft>, Error> {
        let mut cursor = self.client.query(query).fetch::<MatchMask>()?;
        let mut result = Vec::with_capacity(100);
        while let Some(mask) = cursor.next().await? {
            result.push((&mask).into());
        }
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
