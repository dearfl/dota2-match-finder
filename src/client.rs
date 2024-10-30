use reqwest::{Proxy, StatusCode};
use thiserror::Error;

use crate::model::{MatchHistory, MatchHistoryResponse};

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

impl Client {
    const URL_GET_MATCH_HISTORY_BY_SEQUENCE_NUM: &str =
        "https://api.steampowered.com/IDOTA2Match_570/GetMatchHistoryBySequenceNum/v1";

    pub fn new(key: String, proxy: Option<&str>) -> Result<Self, ClientError> {
        let builder = reqwest::Client::builder();
        let builder = match proxy {
            Some(proxy) => {
                let proxy =
                    Proxy::all(proxy).map_err(|_| ClientError::ProxyError(proxy.to_string()))?;
                builder.proxy(proxy)
            }
            None => builder,
        };
        let client = builder.build().map_err(ClientError::ConstructError)?;
        Ok(Self { client, key })
    }

    pub async fn get_matches(
        &self,
        start_idx: u64,
        count: u8,
    ) -> Result<MatchHistory, ClientError> {
        let req = self
            .client
            .get(Self::URL_GET_MATCH_HISTORY_BY_SEQUENCE_NUM)
            .query(&[("key", &self.key)])
            .query(&[("start_at_match_seq_num", start_idx)])
            .query(&[("matches_requested", count)]);
        let resp = req.send().await?;
        match resp.status() {
            StatusCode::TOO_MANY_REQUESTS => Err(ClientError::TooManyRequests),
            StatusCode::OK => {
                let content = resp.text().await?;
                serde_json::from_str(&content)
                    .map(|result: MatchHistoryResponse| result.result)
                    .map_err(|err| ClientError::DecodeError(err, content))
            }
            other => Err(ClientError::OtherResponse(other)),
        }
    }
}
