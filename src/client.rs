use std::time::Duration;

use reqwest::{Proxy, StatusCode};
use thiserror::Error;

use crate::model::{full, partial};

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("Failed to create proxy from provided proxy scheme: {0}.")]
    ProxyError(String),
    #[error("Failed to create reqwest client: {0}.")]
    ConstructError(reqwest::Error),
    #[error("Failed to retrive result from web API: {0}")]
    ConnectionError(#[from] reqwest::Error),
    #[error("Failed to decode web API response: {0}")]
    DecodeError(serde_json::Error, String),
    #[error("Too Many Requests")]
    TooManyRequests,
    #[error("Other Response: {0}")]
    OtherResponse(StatusCode),
}

pub struct Client {
    client: reqwest::Client,
    key: String,
}

#[allow(dead_code)]
impl Client {
    const URL_GET_MATCH_HISTORY_BY_SEQUENCE_NUM: &str =
        "https://api.steampowered.com/IDOTA2Match_570/GetMatchHistoryBySequenceNum/v1";

    const URL_GET_MATCH_HISTORY: &str =
        "https://api.steampowered.com/IDOTA2Match_570/GetMatchHistory/v1";

    pub fn new(key: &str, proxy: Option<&str>) -> Result<Self, ClientError> {
        let builder = reqwest::Client::builder().connect_timeout(Duration::from_secs(10));
        let builder = match proxy {
            Some(proxy) => {
                let proxy =
                    Proxy::all(proxy).map_err(|_| ClientError::ProxyError(proxy.to_string()))?;
                builder.proxy(proxy)
            }
            None => builder,
        };
        let client = builder.build().map_err(ClientError::ConstructError)?;
        let key = key.to_string();
        Ok(Self { client, key })
    }

    pub async fn get_match_history(
        &self,
        start_id: u64,
        count: u8,
    ) -> Result<partial::MatchHistory, ClientError> {
        let req = self
            .client
            .get(Self::URL_GET_MATCH_HISTORY)
            .query(&[("key", &self.key)])
            .query(&[("min_players", 10)])
            .query(&[("start_at_match_id", start_id)])
            .query(&[("matches_requested", count)]);
        let resp = req.send().await?;
        match resp.status() {
            StatusCode::OK => {
                let content = resp.text().await?;
                serde_json::from_str(&content)
                    .map(|result: partial::MatchHistoryResponse| result.result)
                    .map_err(|err| ClientError::DecodeError(err, content))
            }
            StatusCode::TOO_MANY_REQUESTS => Err(ClientError::TooManyRequests),
            other => Err(ClientError::OtherResponse(other)),
        }
    }

    pub async fn get_match_history_full(
        &self,
        start_id: u64,
        count: u8,
    ) -> Result<full::MatchHistory, ClientError> {
        let req = self
            .client
            .get(Self::URL_GET_MATCH_HISTORY_BY_SEQUENCE_NUM)
            .query(&[("key", &self.key)])
            .query(&[("start_at_match_seq_num", start_id)])
            .query(&[("matches_requested", count)]);
        let resp = req.send().await?;
        match resp.status() {
            StatusCode::OK => {
                let content = resp.text().await?;
                serde_json::from_str(&content)
                    .map(|result: full::MatchHistoryResponse| result.result)
                    .map_err(|err| ClientError::DecodeError(err, content))
            }
            StatusCode::TOO_MANY_REQUESTS => Err(ClientError::TooManyRequests),
            other => Err(ClientError::OtherResponse(other)),
        }
    }
}
