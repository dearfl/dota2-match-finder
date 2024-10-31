mod args;
mod client;
mod collector;
mod database;
mod model;

use clap::Parser;

use args::Args;
use client::Client;
use collector::Collector;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();
    let mut collector = Collector::new(&args)?;
    let clients = args
        .keys
        .iter()
        .map(|key| Client::new(key, args.proxy.as_deref()))
        .collect::<Result<Vec<_>, _>>()?;

    for (idx, clt) in (0..100).cycle().zip(clients.iter().cycle()) {
        collector.rate_control().await;
        collector.request(clt).await?;

        if idx == 99 {
            collector.save().await?;
        }
    }

    collector.save().await?;

    Ok(())
}
