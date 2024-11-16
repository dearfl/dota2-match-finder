use std::sync::Arc;

use axum::Json;
use serde::{Deserialize, Serialize};

use crate::{database::Database, dota2::MatchDraft};

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

impl AppState {
    pub fn new(database: Arc<Database>) -> Self {
        Self { database }
    }
}

pub async fn find_matches(
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
