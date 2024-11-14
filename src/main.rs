mod args;
mod client;
mod collector;
mod database;
mod dota2;
mod store;

use std::time::Duration;

use clap::Parser;

use args::Args;
use collector::Collector;
use database::Database;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();

    let database = Database::new(
        args.clickhouse_server.as_deref(),
        args.clickhouse_database.as_deref(),
        args.clickhouse_user.as_deref(),
        args.clickhouse_password.as_deref(),
    )
    .await?;

    let collector = Collector::new(&database, &args.key, args.proxy.as_deref())?;
    let task_collect = collector.collect(args.batch_size, Duration::from_millis(args.interval));

    // ideally this select should never end
    tokio::select! {
        val = task_collect => {
            return val;
        },
    };
}
