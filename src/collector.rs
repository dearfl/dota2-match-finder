use std::ops::Range;

use backon::{ExponentialBuilder, Retryable};

use crate::{
    client::{Client, RequestError},
    dota2::{full::Match, MatchDraft},
};

#[derive(Debug, Clone)]
pub enum CollectResult {
    Normal,
    Yield,
    Decel,
    Save(Range<u64>, Vec<MatchDraft>),
    Completed(Range<u64>, Vec<MatchDraft>),
}

pub struct Collector {
    // currently collecting range
    cur: Range<u64>,
    // currently cached range
    cached: Range<u64>,
    // use Vec<Vec> instead of HashMap<NonZeroU8, Vec> for better performance
    cache: Vec<MatchDraft>,
    batch: usize,
}

impl Collector {
    pub fn new(range: Range<u64>, batch: usize) -> Self {
        let Range { start, end } = range;
        let cur = range.clone();
        let cached = range.start..range.start;
        let cache = Vec::with_capacity(batch + 100);
        log::info!("Start collecting matches in [{}, {})", start, end);
        Self {
            cur,
            cache,
            batch,
            cached,
        }
    }

    fn process(&mut self, matches: &[Match]) -> CollectResult {
        let start = self.cur.start;
        self.cache.extend(
            matches
                .iter()
                .filter(|&mat| self.cur.contains(&mat.match_seq_num))
                .map(Into::into),
        );

        // in case the result is empty, we start the next iteration from start+1
        let end = matches.iter().fold(start + 1, |init, mat| {
            std::cmp::max(init, mat.match_seq_num + 1)
        });
        let count = matches.len();
        log::debug!("Collected {} matches in [{}, {})", count, start, end);

        self.cur.start = end;
        self.cached.end = end;

        if matches.len() < 100 {
            return CollectResult::Yield;
        }

        if self.cur.is_empty() {
            let range = self.cur.start..self.cur.start;
            let range = std::mem::replace(&mut self.cached, range);
            let masks = vec![];
            let masks = std::mem::replace(&mut self.cache, masks);
            return CollectResult::Completed(range, masks);
        }

        if self.cache.len() >= self.batch {
            let range = self.cur.start..self.cur.start;
            let range = std::mem::replace(&mut self.cached, range);
            let masks = Vec::with_capacity(self.batch + 100);
            let masks = std::mem::replace(&mut self.cache, masks);
            return CollectResult::Save(range, masks);
        }

        CollectResult::Normal
    }

    pub async fn step(&mut self, client: &Client) -> anyhow::Result<CollectResult> {
        let start = self.cur.start;
        let result = { || async { client.get_match_history_full(start, 100).await } }
            .retry(ExponentialBuilder::default())
            .when(|err| matches!(err, RequestError::ConnectionError(_)))
            .notify(|_, dur| {
                log::warn!("Retring connection error after {}ms", dur.as_millis());
            })
            .await;

        match result {
            Ok(history) => Ok(self.process(&history.matches)),
            Err(RequestError::DecodeError(err, content)) => {
                log::error!("DecodeError: {}", err);
                log::info!("Saving response to {}-error.json", start);
                let filename = format!("{}-error.json", start);
                std::fs::write(filename, content)?;
                Err(err.into())
            }
            Err(RequestError::ConnectionError(error)) => {
                log::warn!("ConnectionError({}): {}", start, error.without_url());
                Ok(CollectResult::Normal)
            }
            Err(error) => {
                log::warn!("RequestError({}): {}", start, error);
                Ok(CollectResult::Decel)
            }
        }
    }
}
