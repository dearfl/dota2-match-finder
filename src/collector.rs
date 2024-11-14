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
            const N: u64 = 100000;
            let start = (idx - 1) / N * N;
            start..idx
        };

        let start = self.client.get_a_recent_match_seq_num().await?;
        let (range_onward, range_past) = (start..u64::MAX, prev(start));

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
                            if store.push(&history.matches).await? {
                                let start = store.start();
                                *store = Store::new(self.database, prev(start), batch_past);
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
