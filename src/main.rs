mod client;
mod model;
mod rate;

use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, NaiveDate, Utc};
use clap::Parser;
use client::{Client, ClientError};
use rate::RateControl;

#[derive(Parser)]
pub struct Args {
    #[arg(long)]
    start_idx: u64,
    #[arg(long)]
    proxy: Option<String>,
    #[arg(long)]
    clickhouse_server: Option<String>,
    #[arg(long)]
    clickhouse_database: Option<String>,
    #[arg(long)]
    clickhouse_user: Option<String>,
    #[arg(long)]
    clickhouse_password: Option<String>,
    #[arg(long, default_value_t = 32)]
    min_interval: u64,
    #[arg(long, default_value_t = 1000)]
    max_interval: u64,
    #[arg(long, default_value_t = 10000)]
    insert_batch_size: usize,
    keys: Vec<String>,
}

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub enum Side {
    Radiant,
    Dire,
}

impl From<u8> for Side {
    fn from(value: u8) -> Self {
        if value & 0x80u8 != 0 {
            Self::Dire
        } else {
            Self::Radiant
        }
    }
}

fn new_database_connection(args: &Args) -> clickhouse::Client {
    let clickhouse_server = args
        .clickhouse_server
        .as_ref()
        .map_or("http://127.0.0.1:8123", String::as_str);
    let clickhouse_database = args
        .clickhouse_database
        .as_ref()
        .map_or("dota2", String::as_str);

    let database = clickhouse::Client::default()
        .with_url(clickhouse_server)
        .with_database(clickhouse_database);

    let database = match args.clickhouse_user.as_ref() {
        Some(user) => database.with_user(user),
        _ => database,
    };

    match args.clickhouse_password.as_ref() {
        Some(password) => database.with_password(password),
        _ => database,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();
    let clients = args
        .keys
        .iter()
        .map(|key| Client::new(key, args.proxy.as_deref()))
        .collect::<Result<Vec<_>, _>>()?;

    let mut index = args.start_idx;

    let mut rate = RateControl::new(args.min_interval, args.max_interval);

    let _database = new_database_connection(&args);
    let mut _buffer = HashMap::<NaiveDate, HashMap<Side, HashMap<u8, Vec<u64>>>>::new();

    for clt in clients.iter().cycle() {
        rate.wait().await;
        match clt.get_match_history(index, 100).await {
            Ok(matches) => {
                if matches.matches.len() >= 100 {
                    rate.speed_up();
                } else {
                    rate.slow_down();
                }
                index = matches.matches.iter().fold(index, |init, mat| {
                    let date = DateTime::from_timestamp(mat.start_time as i64, 0)
                        .map_or(NaiveDate::default(), |arg0: DateTime<Utc>| {
                            DateTime::date_naive(&arg0)
                        });
                    mat.players.iter().for_each(|player| {
                        let side: Side = player.player_slot.into();
                        _buffer
                            .entry(date)
                            .or_default()
                            .entry(side)
                            .or_default()
                            .entry(player.hero_id)
                            .or_default()
                            .push(mat.match_id);
                    });
                    std::cmp::max(init, mat.match_id + 1)
                });
            }
            Err(ClientError::DecodeError(err, content)) => {
                // maybe valve have changed the json response format
                // this is when things really goes wrong, we need to fix it manually
                log::error!("decode error: {}", err);
                let filename = format!("{}-error.json", index);
                std::fs::write(filename, content)?;
                // we have to quit or else we'll end in a dead loop
                return Err(err.into());
            }
            Err(ClientError::ConnectionError(err)) => {
                log::warn!("connection error: {}", err);
                rate.slow_down();
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(ClientError::TooManyRequests) => {
                // we're requesting API too frequently, so slowing it down a little.
                log::warn!("too many requests");
                rate.slow_down();
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            Err(ClientError::OtherResponse(status)) => {
                log::error!("other response: {}", status);
                rate.slow_down();
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            Err(ClientError::ProxyError(_)) | Err(ClientError::ConstructError(_)) => unreachable!(),
        }
    }

    Ok(())
}
