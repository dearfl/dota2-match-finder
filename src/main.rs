mod args;
mod client;
mod collector;
mod database;
mod dota2;
mod rate;

use clap::Parser;

use args::Args;
use collector::Collector;
use database::Database;
use rate::RateControl;

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

    let rate = RateControl::new(args.min_interval_onward, args.min_interval_onward);
    let collector = Collector::new(&database, &args.keys, args.proxy.as_deref())?;
    let collect_onward_matches = collector.collect(0..u64::MAX, args.batch_size_onward, rate);

    // this join will never end
    let _ = tokio::join!(collect_onward_matches,);
    Ok(())
}
