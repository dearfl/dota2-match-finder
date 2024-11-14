use std::{ops::Range, time::Duration};

use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::{
    client::{Client, RequestError},
    database::Database,
    store::Store,
};

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct CollectorStatus {
    pub collected: Vec<(u64, u64)>,
}

impl CollectorStatus {
    pub fn prev(idx: u64) -> Range<u64> {
        const N: u64 = 100000;
        let start = (idx - 1) / N * N;
        start..idx
    }

    pub async fn onward_range(&mut self, client: &Client) -> anyhow::Result<Range<u64>> {
        let last = self.collected.last();
        match last {
            None => {
                let start = client.get_a_recent_match_seq_num().await?;
                self.collected.push((start, start));
                Ok(start..u64::MAX)
            }
            Some((_, end)) => Ok(*end..u64::MAX),
        }
    }

    pub async fn past_range(&mut self, client: &Client) -> anyhow::Result<Range<u64>> {
        let mut iter = self.collected.iter().rev();
        let last = iter.next();
        let sec = iter.next();
        match (sec, last) {
            (None, None) => {
                let start = client.get_a_recent_match_seq_num().await?;
                self.collected.push((start, start));
                Ok(Self::prev(start))
            }
            (None, Some(&(start, _))) => Ok(Self::prev(start)),
            (Some(&(_, start)), Some(&(end, _))) => Ok(start..end),
            (Some(_), None) => unreachable!(),
        }
    }

    pub fn finish(&mut self, range: Range<u64>) {
        self.collected.push((range.start, range.end));
        self.collected.sort_unstable();
        self.collected = self.collected.iter().fold(
            Vec::with_capacity(self.collected.len()),
            |mut init, &(start, end)| {
                match init.last_mut() {
                    Some((_, e)) if start <= *e => {
                        *e = std::cmp::max(*e, end);
                    }
                    _ => {
                        init.push((start, end));
                    }
                };
                init
            },
        );
    }
}

pub struct Collector<'db> {
    client: Client,
    database: &'db Database,
}

impl<'db> Collector<'db> {
    pub fn new(database: &'db Database, key: &str, proxy: Option<&str>) -> anyhow::Result<Self> {
        let client = Client::new(key, proxy)?;
        Ok(Self { client, database })
    }

    pub async fn collect(&self, batch: usize, interval: Duration) -> anyhow::Result<()> {
        let path = "./collected.json";
        let mut collected = std::fs::read_to_string(path)
            .ok()
            .and_then(|content| serde_json::from_str::<CollectorStatus>(&content).ok())
            .unwrap_or_default();

        let range_onward = collected.onward_range(&self.client).await?;
        let range_past = collected.past_range(&self.client).await?;

        // using larger batch size when collecting past matches
        let batch_past = batch * 10;

        let mut stores = [
            (6, Store::new(self.database, range_onward, batch)), // onward => 6
            (3, Store::new(self.database, range_past, batch_past)), // past => 3
        ];

        let mut base = Instant::now();

        // the outer loop never ends
        loop {
            // switch between onward and past
            for (count, store) in stores.iter_mut() {
                for _ in 0..*count {
                    tokio::time::sleep_until(base + interval).await;
                    base = Instant::now();
                    let start = store.current_range().start;
                    match self.client.get_match_history_full(start, 100).await {
                        Ok(history) => {
                            if store.push(&history.matches, &mut collected, path).await? {
                                let range = store.range();
                                collected.finish(range);
                                let new_range = collected.past_range(&self.client).await?;
                                *store = Store::new(self.database, new_range, batch_past);
                            }
                            if history.matches.len() < 100 {
                                break;
                            }
                        }
                        Err(RequestError::DecodeError(err, content)) => {
                            log::error!("DecodeError: {}", err);
                            log::info!("Saving response to {}-error.json", start);
                            let filename = format!("{}-error.json", start);
                            std::fs::write(filename, content)?;
                            return Err(err.into());
                        }
                        Err(RequestError::ConnectionError(error)) => {
                            log::warn!("ConnectionError: {}", error)
                        }
                        Err(error) => {
                            log::warn!("RequestError: {}", error);
                            base += Duration::from_secs(1);
                        }
                    }
                }
            }
        }
    }
}
