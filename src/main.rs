mod args;
mod client;
mod collector;
mod database;
mod dota2;

use clap::Parser;

use args::Args;
use collector::Collector;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();
    let mut collector = Collector::new(args).await?;
    collector.run().await?;
    Ok(())
}
