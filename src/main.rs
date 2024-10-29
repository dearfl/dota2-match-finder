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

    for clt in clients.iter().cycle() {
        // traffic control?
        tokio::time::sleep(interval).await;
        log::info!("requesting");
        match clt.get_matches(index, 100).await {
            Ok(matches) => {
                log::info!("success");
                interval = std::cmp::max(interval * 4 / 5, MIN_INTERVAL);
                index = matches
                    .matches
                    .iter()
                    .fold(index, |init, md| std::cmp::max(init, md.match_seq_num) + 1);
            }
            Err(ClientError::DecodeError(err, content)) => {
                // maybe valve have changed the json response format
                // this is when things really goes wrong, we need to fix it manually
                log::error!("decode error: {}", err);
                std::fs::write("error.json", content)?;
                return Err(err.into());
            }
            Err(ClientError::TooManyRequests) => {
                // we're requesting API too frequently, so slowing it down a little.
                log::warn!("too many requests");
                interval = std::cmp::min(interval * 5, MAX_INTERVAL);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
            Err(ClientError::ConnectionError(err)) => {
                log::warn!("connection error: {}", err);
                interval = std::cmp::min(interval * 2, MAX_INTERVAL);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            Err(ClientError::ProxyError(_)) | Err(ClientError::ConstructError(_)) => unreachable!(),
        }
    }

    Ok(())
}
