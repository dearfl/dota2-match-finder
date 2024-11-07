use std::{collections::HashMap, time::Duration};

use tokio::time::Instant;

use crate::{
    args::Args,
    client::{Client, RequestError},
    database::Database,
    dota2::{MatchMask, Side},
};

pub struct RateControl {
    interval: Duration,
    min_interval: Duration,
    max_interval: Duration,
    last_timestamp: Instant,
}

impl RateControl {
    pub fn new(min: u64, max: u64) -> Self {
        let interval = Duration::from_millis(5000);
        let min_interval = Duration::from_millis(min);
        let max_interval = Duration::from_millis(max);
        let last_timestamp = Instant::now();
        Self {
            interval,
            min_interval,
            max_interval,
            last_timestamp,
        }
    }

    pub async fn wait(&mut self) {
        log::debug!("rate control: waiting {}ms!", self.interval.as_millis());
        tokio::time::sleep_until(self.last_timestamp + self.interval).await;
        self.last_timestamp = Instant::now();
    }

    pub fn speed_up(&mut self) {
        self.interval = std::cmp::max(self.interval * 9 / 10, self.min_interval);
    }

    pub fn slow_down(&mut self) {
        self.interval = std::cmp::min(self.interval * 2, self.max_interval);
    }
}

pub struct Collector {
    match_seq_num: u64,
    rate: RateControl,
    database: Database,
    indices: HashMap<(Side, u8), Vec<MatchMask>>,
}

impl Collector {
    pub async fn new(args: &Args) -> anyhow::Result<Self> {
        let match_seq_num = args.start_idx;
        let rate = RateControl::new(args.min_interval, args.max_interval);
        let indices = HashMap::with_capacity(256 * 2);
        let database = Database::new(
            args.clickhouse_server.as_deref(),
            args.clickhouse_database.as_deref(),
            args.clickhouse_user.as_deref(),
            args.clickhouse_password.as_deref(),
        )
        .await?;

        Ok(Self {
            match_seq_num,
            rate,
            database,
            indices,
        })
    }

    pub async fn request(&mut self, client: &Client) -> anyhow::Result<()> {
        // get_match_history is not reliable so we switch back to get_match_history_by_seq_num
        match client.get_match_history_full(self.match_seq_num, 100).await {
            Ok(matches) => {
                let left = self.match_seq_num;
                self.match_seq_num =
                    matches
                        .matches
                        .iter()
                        .fold(self.match_seq_num, |init, mat| {
                            let mask = mat.into();
                            mat.players
                                .iter()
                                .filter_map(|p| match p.hero_id {
                                    0 => None, // 0 means unknown hero
                                    h => Some((p.player_slot.into(), h)),
                                })
                                .for_each(|key| self.indices.entry(key).or_default().push(mask));
                            std::cmp::max(init, mat.match_seq_num + 1)
                        });
                if matches.matches.len() < 100 {
                    self.rate.slow_down();
                }
                self.rate.speed_up();
                log::debug!(
                    "retrived {} matches from range [{}, {}).",
                    matches.matches.len(),
                    left,
                    self.match_seq_num
                );
            }
            Err(RequestError::DecodeError(err, content)) => {
                // maybe valve have changed the json response format
                // this is when things really goes wrong, we need to fix it manually
                log::error!("decode error: {}", err);
                let filename = format!("{}-error.json", self.match_seq_num);
                std::fs::write(filename, content)?;
                // we have to quit or else we'll end in a dead loop
                return Err(err.into());
            }
            Err(RequestError::ConnectionError(err)) => {
                log::warn!("connection error: {}", err);
                self.rate.slow_down();
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(RequestError::TooManyRequests) => {
                // we're requesting API too frequently, so slowing it down a little.
                log::warn!("too many requests");
                self.rate.slow_down();
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            Err(RequestError::OtherResponse(status)) => {
                log::error!("other response: {}", status);
                self.rate.slow_down();
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
        Ok(())
    }

    pub async fn save(&mut self) -> anyhow::Result<()> {
        log::debug!("saving indices to database!");
        for (key, masks) in self.indices.iter_mut() {
            self.database.save_indexed_masks(*key, masks).await?;
            masks.clear();
        }
        Ok(())
    }

    pub async fn rate_control(&mut self) {
        self.rate.wait().await;
    }
}
