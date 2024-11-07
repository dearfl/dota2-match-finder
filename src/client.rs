use std::time::Duration;

use reqwest::{Proxy, StatusCode};
use thiserror::Error;

use crate::dota2::{full, partial};

// we use separate error types for construction and request

#[derive(Error, Debug)]
pub enum ConstructionError {
    #[error("ProxyError: {0} from scheme: {1}.")]
    ProxyError(reqwest::Error, String),
    #[error("BuildError: {0}.")]
    BuildError(#[from] reqwest::Error),
}

#[derive(Error, Debug)]
pub enum RequestError {
    #[error("Failed to retrive result from web API: {0}")]
    ConnectionError(#[from] reqwest::Error),
    #[error("Failed to decode web API response: {0}")]
    DecodeError(serde_json::Error, String),
    #[error("Too Many Requests")]
    TooManyRequests,
    #[error("Other Response: {0}")]
    OtherResponse(reqwest::StatusCode),
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

    pub fn new(key: &str, proxy: Option<&str>) -> Result<Self, ConstructionError> {
        let builder = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .connect_timeout(Duration::from_secs(60));
        let builder = match proxy {
            Some(proxy) => {
                let proxy = Proxy::all(proxy)
                    .map_err(|err| ConstructionError::ProxyError(err, proxy.to_string()))?;
                builder.proxy(proxy)
            }
            None => builder,
        };
        let client = builder.build()?;
        let key = key.to_string();
        Ok(Self { client, key })
    }

    pub async fn get_match_history(
        &self,
        start_id: u64,
        count: u8,
    ) -> Result<partial::MatchHistory, RequestError> {
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
                    .map_err(|err| RequestError::DecodeError(err, content))
            }
            StatusCode::TOO_MANY_REQUESTS => Err(RequestError::TooManyRequests),
            other => Err(RequestError::OtherResponse(other)),
        }
    }

    pub async fn get_match_history_full(
        &self,
        start_id: u64,
        count: u8,
    ) -> Result<full::MatchHistory, RequestError> {
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
                    .map_err(|err| RequestError::DecodeError(err, content))
            }
            StatusCode::TOO_MANY_REQUESTS => Err(RequestError::TooManyRequests),
            other => Err(RequestError::OtherResponse(other)),
        }
    }
}
