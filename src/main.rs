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

    let state = AppState::new(database.clone());

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

    let app = Router::new().route(
        "/",
        post({
            let state = Arc::new(state);
            move |body| find_matches(body, state)
        }),
    );
    let address = format!("{}:{}", args.addr, args.port);
    let listener = tokio::net::TcpListener::bind(&address).await?;

    // ideally this select should never end
    tokio::select! {
        val = sche.run() => {
            val?;
            return Ok(());
        },
        val = axum::serve(listener, app) => {
            val?;
            return Ok(());
        }
    };
}
