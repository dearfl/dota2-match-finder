mod args;
mod client;
mod collector;
mod database;
mod dota2;
mod scheduler;
mod service;

use std::{sync::Arc, time::Duration};

use axum::{routing::post, Router};
use clap::Parser;

use args::Args;
use database::Database;
use scheduler::Scheduler;
use service::{find_matches, AppState};

async fn serve(database: Arc<Database>, address: String) -> anyhow::Result<()> {
    let state = AppState::new(database);
    let app = Router::new().route(
        "/",
        post({
            let state = Arc::new(state);
            move |body| find_matches(body, state)
        }),
    );
    let listener = tokio::net::TcpListener::bind(address).await?;

    axum::serve(listener, app).await?;
    Ok(())
}

async fn collect(database: Arc<Database>, args: Args) -> anyhow::Result<()> {
    let interval = Duration::from_millis(args.interval);
    let mut sche = Scheduler::new(
        &args.key,
        args.proxy.as_deref(),
        database,
        &args.collected,
        args.batch,
        interval,
    )
    .await?;

    sche.run().await
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();

    let database = Database::new(
        &args.clickhouse_server,
        &args.clickhouse_database,
        args.clickhouse_user.as_deref(),
        args.clickhouse_password.as_deref(),
    )
    .await?;

    let database = Arc::new(database);

    let address = format!("{}:{}", args.addr, args.port);
    let serve = tokio::spawn(serve(database.clone(), address));
    let collect = tokio::spawn(collect(database.clone(), args));

    // ideally this select should never end
    tokio::select! {
        val = collect => {
            val
        },
        val = serve => {
            val
        }
    }?
}
