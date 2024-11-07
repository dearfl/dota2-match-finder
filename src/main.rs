mod args;
mod client;
mod collector;
mod database;
mod dota2;

use clap::Parser;
use itertools::Itertools;

use args::Args;
use client::Client;
use collector::Collector;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();
    let mut collector = Collector::new(&args).await?;
    let clients = args
        .keys
        .iter()
        .map(|key| Client::new(key, args.proxy.as_deref()))
        .collect::<Result<Vec<_>, _>>()?;

    for clts in clients
        .iter()
        .cycle()
        .chunks(args.insert_batch_size)
        .into_iter()
    {
        for clt in clts {
            collector.rate_control().await;
            collector.request(clt).await?;
        }
        collector.save().await?;
    }

    collector.save().await?;

    Ok(())
}
