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

async fn collect_past_matches(
    collector: &Collector<'_>,
    mut end: u64,
    batch: usize,
    min_interval_ms: u64,
    max_interval_ms: u64,
) -> anyhow::Result<()> {
    const N: u64 = 1000000;
    while end > 0 {
        let rate = RateControl::new(min_interval_ms, max_interval_ms);
        let start = (end - 1) / N * N;
        collector.collect(start..end, batch, rate).await?;
        end = start;
    }
    Ok(())
}

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

    let col_onward = Collector::new(&database, &args.keys[0..1], args.proxy.as_deref())?;
    let start = col_onward.get_a_recent_match_seq_num().await?;

    let rate_onward = RateControl::new(args.min_interval_onward, args.max_interval_onward);
    let task_collect_onward_matches =
        col_onward.collect(start..u64::MAX, args.batch_size_onward, rate_onward);

    let col_past = Collector::new(&database, &args.keys[1..], args.proxy.as_deref())?;
    let task_collect_past_matches = collect_past_matches(
        &col_past,
        start,
        args.batch_size_past,
        args.min_interval_past,
        args.max_interval_past,
    );

    // this join will never end
    let _ = tokio::join!(task_collect_onward_matches, task_collect_past_matches);
    Ok(())
}
