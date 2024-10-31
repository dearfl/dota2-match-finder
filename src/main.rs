mod client;
mod database;
mod model;
mod rate;

use std::{collections::HashMap, time::Duration};

use clap::Parser;

use client::{Client, ClientError};
use database::Database;
use model::{MatchId, Side};
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();
    let clients = args
        .keys
        .iter()
        .map(|key| Client::new(key, args.proxy.as_deref()))
        .collect::<Result<Vec<_>, _>>()?;

    let mut match_seq_num = args.start_idx;

    let mut rate = RateControl::new(args.min_interval, args.max_interval);

    let database = Database::new(
        args.clickhouse_server.as_deref(),
        args.clickhouse_database.as_deref(),
        args.clickhouse_user.as_deref(),
        args.clickhouse_password.as_deref(),
    );

    let mut buffer = HashMap::<(Side, u8), Vec<MatchId>>::new();

    for (idx, clt) in (0..100).cycle().zip(clients.iter().cycle()) {
        log::debug!("rate control!");
        rate.wait().await;
        // get_match_history is not reliable so we switch back to get_match_history_by_seq_num
        match clt.get_match_history_full(match_seq_num, 100).await {
            Ok(matches) => {
                match_seq_num = matches.matches.iter().fold(match_seq_num, |init, mat| {
                    mat.players.iter().for_each(|player| {
                        let side: Side = player.player_slot.into();
                        buffer
                            .entry((side, player.hero_id))
                            .or_default()
                            .push(mat.match_id.into());
                    });
                    std::cmp::max(init, mat.match_seq_num + 1)
                });
                rate.speed_up();
            }
            Err(ClientError::DecodeError(err, content)) => {
                // maybe valve have changed the json response format
                // this is when things really goes wrong, we need to fix it manually
                log::error!("decode error: {}", err);
                let filename = format!("{}-error.json", match_seq_num);
                // intentionally ignore result of this write so we wont skip database write
                let _ = std::fs::write(filename, content);
                // we have to quit or else we'll end in a dead loop
                // return Err(err.into());
                break;
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

        if idx == 99 {
            log::info!("write database!");
            database.save(&buffer).await?;
            buffer.clear();
        }
    }

    log::info!("write database!");
    database.save(&buffer).await?;
    buffer.clear();

    Ok(())
}
