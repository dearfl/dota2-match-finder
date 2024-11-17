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

        let query = format!(
            "CREATE TABLE IF NOT EXISTS {}.mask (
                match_id UInt64,
                radiant UInt256,
                dire UInt256,
            )
            ENGINE = MergeTree()
            ORDER BY match_id
            PARTITION BY intDiv(match_id, 100000000)
            PRIMARY KEY match_id;",
            &database
        );
        client.query(&query).execute().await?;

        Ok(Self { database, client })
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

        let mask1 = to_mask(team1);
        let mask2 = to_mask(team2);

        let side_check =
            |side: &str, mask: U256| format!("(bitOr({}, toUInt256('{}')) = {})", side, mask, side);

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
        let query = format!(
            "SELECT ?fields FROM {}.mask WHERE ({} OR {}) ORDER BY match_id DESC LIMIT {} OFFSET {}",
            self.database, cond1, cond2, limit, offset
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

    pub async fn save_match_masks(&self, masks: &[MatchMask]) -> Result<(), Error> {
        let mut insert = self.client.insert("mask")?;
        for mat in masks {
            insert.write(mat).await?;
        }
        insert.end().await?;
        Ok(())
    }
}
