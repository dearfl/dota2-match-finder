use std::collections::HashMap;

use crate::model::{full::Match, MatchId, Side};

pub struct Database {
    database: String,
    client: clickhouse::Client,
}

impl Database {
    pub fn new(
        server: Option<&str>,
        database: Option<&str>,
        user: Option<&str>,
        password: Option<&str>,
    ) -> Self {
        let server = server.unwrap_or("http://127.0.0.1:8123");
        let database = database.unwrap_or("dota2").to_string();

        let client = clickhouse::Client::default()
            .with_url(server)
            .with_database(&database);

        let client = match user {
            Some(user) => client.with_user(user),
            _ => client,
        };

        let client = match password {
            Some(password) => client.with_password(password),
            _ => client,
        };

        Self { database, client }
    }

    pub async fn save(
        &self,
        matches: &HashMap<(Side, u8), Vec<MatchId>>,
    ) -> Result<(), clickhouse::error::Error> {
        for ((side, hero), matches) in matches {
            let table = format!("match_{}_{}", side, hero);
            let query = format!("CREATE TABLE IF NOT EXISTS {}.{} (match_id UInt64) ENGINE = MergeTree() ORDER BY match_id PARTITION BY intDiv(match_id, 100000000) PRIMARY KEY match_id;", &self.database, table);
            self.client.query(&query).execute().await?;
            let mut insert = self.client.insert(&table)?;
            for mat in matches {
                insert.write(mat).await?;
            }
            insert.end().await?;
        }
        Ok(())
    }

    pub async fn save_matches(
        &self,
        table: &str,
        matches: &Vec<Match>,
    ) -> Result<(), clickhouse::error::Error> {
        let mut insert = self.client.insert(table)?;
        for mat in matches {
            insert.write(mat).await?;
        }
        insert.end().await
    }
}
