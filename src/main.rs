mod args;
mod client;
mod collector;
mod database;
mod dota2;
mod store;

use std::{sync::Arc, time::Duration};

use axum::{routing::post, Json, Router};
use clap::Parser;

use args::Args;
use collector::Collector;
use database::Database;
use dota2::MatchDraft;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct QueryParameter {
    pub team1: Vec<u8>,
    pub team2: Vec<u8>,
    #[serde(default = "default_count")]
    pub count: usize,
    #[serde(default)]
    pub offset: usize,
}

pub fn default_count() -> usize {
    10
}

pub struct AppState {
    database: Arc<Database>,
}

async fn find_matches(
    Json(para): Json<QueryParameter>,
    state: Arc<AppState>,
) -> Json<Vec<MatchDraft>> {
    let result = state
        .database
        .query_matches(&para.team1, &para.team2, para.count.min(100), para.offset)
        .await
        .ok()
        .unwrap_or_default();
    Json(result)
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

    let db_axum = Arc::new(database);
    let db_collector = db_axum.clone();

    let state = AppState { database: db_axum };

    let collector = Collector::new(db_collector.as_ref(), &args.key, args.proxy.as_deref())?;
    let task_collect = collector.collect(args.batch_size, Duration::from_millis(args.interval));

    let app = Router::new().route(
        "/",
        post({
            let state = Arc::new(state);
            move |body| find_matches(body, state)
        }),
    );
    let address = args.address.unwrap_or("localhost".to_string());
    let address = format!("{}:{}", address, args.port);
    let listener = tokio::net::TcpListener::bind(&address).await?;

    // ideally this select should never end
    tokio::select! {
        val = task_collect => {
            val?;
            return Ok(());
        },
        val = axum::serve(listener, app) => {
            val?;
            return Ok(());
        }
    };
}
