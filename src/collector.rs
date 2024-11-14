use std::time::Duration;

use tokio::time::Instant;

use crate::{
    client::{Client, RequestError},
    database::Database,
    store::Store,
};

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
        let prev = |idx: u64| {
            const N: u64 = 1000000;
            (idx - 1) / N * N..idx
        };

        let start = self.client.get_a_recent_match_seq_num().await?;
        let (range_onward, range_past) = (start..u64::MAX, prev(start));

        // using larger batch size when collecting past matches
        let batch_past = batch * 10;

        let mut stores = [
            Store::new(self.database, range_onward, batch),
            Store::new(self.database, range_past, batch_past),
        ];

        let mut base = Instant::now();

        // the outer loop never ends
        loop {
            // switch between onward and past
            for (idx, store) in stores.iter_mut().enumerate() {
                // 0 => 6, onward collecting at most 6 batch
                // 1 => 3, past always collecting 3 batch
                let count = 6 - 3 * idx;
                for _ in 0..count {
                    tokio::time::sleep_until(base + interval).await;
                    base = Instant::now();
                    let start = store.current_range().start;
                    match self.client.get_match_history_full(start, 100).await {
                        Ok(history) => {
                            let cnt = history.matches.len();
                            let end = history.matches.iter().fold(start, |init, mat| {
                                std::cmp::max(init, mat.match_seq_num + 1)
                            });
                            log::info!("Collected {} matches in [{}, {})", cnt, start, end);
                            if store.push(&history.matches).await? {
                                let start = store.range.start;
                                *store = Store::new(self.database, prev(start), batch_past);
                            }
                            if history.matches.len() < 100 {
                                break;
                            }
                        }
                        Err(RequestError::DecodeError(err, content)) => {
                            // maybe valve have changed the json response format
                            // this is when things really goes wrong, we need to fix it manually
                            log::error!("DecodeError: {}", err);
                            log::info!("Saving response to {}-error.json", start);
                            let filename = format!("{}-error.json", start);
                            std::fs::write(filename, content)?;
                            // we have to quit or else we'll end in a dead loop
                            // we could in theory accept unknown fields so we don't have to quit here
                            // but we don't want to
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
