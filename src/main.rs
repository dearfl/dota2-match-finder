mod client;
mod model;

use clap::Parser;
use client::{Client, ClientError};

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
    clickhouse_table: Option<String>,
    keys: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();
    let clients = args
        .keys
        .into_iter()
        .map(|key| Client::new(key, args.proxy.as_deref()))
        .collect::<Result<Vec<_>, _>>()?;

    let mut index = args.start_idx;
    let mut interval = std::time::Duration::from_millis(100);
    const MIN_INTERVAL: std::time::Duration = std::time::Duration::from_millis(32);
    const MAX_INTERVAL: std::time::Duration = std::time::Duration::from_millis(1000);

    let clickhouse_server = args
        .clickhouse_server
        .as_ref()
        .map_or("http://127.0.0.1:8123", String::as_str);
    let clickhouse_database = args
        .clickhouse_database
        .as_ref()
        .map_or("dota2", String::as_str);
    let clickhouse_table = args
        .clickhouse_table
        .as_ref()
        .map_or("matches", String::as_str);

    let database = clickhouse::Client::default()
        .with_url(clickhouse_server)
        .with_database(clickhouse_database);

    let mut buffer = vec![];

    for clt in clients.iter().cycle() {
        // traffic control?
        tokio::time::sleep(interval).await;
        log::debug!("request interval: {}ms", interval.as_millis());
        match clt.get_matches(index, 100).await {
            Ok(matches) => {
                log::debug!("success");
                interval = std::cmp::max(interval * 4 / 5, MIN_INTERVAL);
                index = matches.matches.iter().fold(index, |init, mat| {
                    std::cmp::max(init, mat.match_seq_num + 1)
                });
                buffer.extend(matches.matches);
            }
            Err(ClientError::DecodeError(err, content)) => {
                // maybe valve have changed the json response format
                // this is when things really goes wrong, we need to fix it manually
                log::error!("decode error: {}", err);
                // TODO: use timestamp based filename
                std::fs::write("error.json", content)?;
                return Err(err.into());
            }
            Err(ClientError::TooManyRequests) => {
                // we're requesting API too frequently, so slowing it down a little.
                log::warn!("too many requests");
                interval = std::cmp::min(interval * 5, MAX_INTERVAL);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
            Err(ClientError::ConnectionError(err)) => {
                log::warn!("connection error: {}", err);
                interval = std::cmp::min(interval * 2, MAX_INTERVAL);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            Err(ClientError::ProxyError(_)) | Err(ClientError::ConstructError(_)) => unreachable!(),
        }

        // manual batching
        log::debug!("buffer len: {}", buffer.len());
        if buffer.len() >= 10000 {
            let mut insert = database.insert(clickhouse_table)?;
            for mat in &buffer {
                insert.write(mat).await?;
            }
            insert.end().await?;
            buffer.clear();
        }
    }

    Ok(())
}
