use clickhouse::error::Error;

use crate::dota2::{MatchMask, MatchRange};

pub struct Database {
    database: String,
    client: clickhouse::Client,
}

impl Database {
    pub async fn new(
        server: Option<&str>,
        database: Option<&str>,
        user: Option<&str>,
        password: Option<&str>,
    ) -> Result<Self, Error> {
        let server = server.unwrap_or("http://127.0.0.1:8123");
        let client = clickhouse::Client::default().with_url(server);

        let client = match user {
            Some(user) => client.with_user(user),
            _ => client,
        };

        let client = match password {
            Some(password) => client.with_password(password),
            _ => client,
        };

        // create database if not exists
        let database = database.unwrap_or("dota2").to_string();
        let query = format!("CREATE DATABASE IF NOT EXISTS {};", database);
        client.query(&query).execute().await?;

        let client = client.with_database(&database);

        Ok(Self { database, client })
    }

    pub async fn save_range(&self, range: MatchRange) -> Result<(), Error> {
        let table = "collected";
        let query = format!(
            "CREATE TABLE IF NOT EXISTS {}.{} (
                start UInt64,
                end UInt64,
            )
            ENGINE = MergeTree()
            ORDER BY (start, end)
            PRIMARY KEY start;",
            &self.database, table
        );
        self.client.query(&query).execute().await?;
        let mut insert = self.client.insert(table)?;
        insert.write(&range).await?;
        insert.end().await?;
        Ok(())
    }

    pub async fn save_indexed_masks(&self, hero: u8, masks: &[MatchMask]) -> Result<(), Error> {
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
            &self.database, table
        );
        self.client.query(&query).execute().await?;
        let mut insert = self.client.insert(&table)?;
        for mat in masks {
            insert.write(mat).await?;
        }
        insert.end().await?;
        Ok(())
    }
}
