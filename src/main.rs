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
        .into_iter()
        .map(|key| Client::new(key, args.proxy.as_deref()))
        .collect::<Result<Vec<_>, _>>()?;

    let mut index = args.start_idx;
    let mut interval = std::time::Duration::from_millis(100);
    let min_interval: std::time::Duration = std::time::Duration::from_millis(args.min_interval);
    let max_interval: std::time::Duration = std::time::Duration::from_millis(args.max_interval);

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

    let database = match args.clickhouse_user.as_ref() {
        Some(user) => database.with_user(user),
        _ => database,
    };

    let database = match args.clickhouse_password.as_ref() {
        Some(password) => database.with_password(password),
        _ => database,
    };

    let mut buffer = Vec::with_capacity(args.insert_batch_size + 100);

    for clt in clients.iter().cycle() {
        // traffic control?
        tokio::time::sleep(interval).await;
        log::debug!("request interval: {}ms", interval.as_millis());
        match clt.get_matches(index, 100).await {
            Ok(matches) => {
                log::debug!("success");
                interval = std::cmp::max(interval * 9 / 10, min_interval);
                index = matches.matches.iter().fold(index, |init, mat| {
                    std::cmp::max(init, mat.match_seq_num + 1)
                });
                buffer.extend(matches.matches);
            }
            Err(ClientError::DecodeError(err, content)) => {
                // maybe valve have changed the json response format
                // this is when things really goes wrong, we need to fix it manually
                log::error!("decode error: {}", err);
                let filename = format!("{}-error.json", index);
                std::fs::write(filename, content)?;
                // TODO: quit or continue?
                return Err(err.into());
            }
            Err(ClientError::ConnectionError(err)) => {
                log::warn!("connection error: {}", err);
                interval = std::cmp::min(interval * 2, max_interval);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            Err(ClientError::TooManyRequests) => {
                // we're requesting API too frequently, so slowing it down a little.
                log::warn!("too many requests");
                interval = std::cmp::min(interval * 5, max_interval);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
            Err(ClientError::OtherResponse(status)) => {
                log::error!("other response: {}", status);
                interval = std::cmp::min(interval * 5, max_interval);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
            Err(ClientError::ProxyError(_)) | Err(ClientError::ConstructError(_)) => unreachable!(),
        }

        // manual batching
        log::debug!("buffer len: {}", buffer.len());
        if buffer.len() >= args.insert_batch_size {
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
