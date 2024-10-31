use std::{collections::HashMap, time::Duration};

use crate::{
    args::Args,
    client::{Client, ClientError},
    database::Database,
    model::{MatchId, Side},
};

pub struct RateControl {
    interval: Duration,
    min_interval: Duration,
    max_interval: Duration,
}

impl RateControl {
    pub fn new(min: u64, max: u64) -> Self {
        let interval = Duration::from_millis(100);
        let min_interval = Duration::from_millis(min);
        let max_interval = Duration::from_millis(max);
        Self {
            interval,
            min_interval,
            max_interval,
        }
    }

    pub async fn wait(&self) {
        tokio::time::sleep(self.interval).await;
    }

    pub fn speed_up(&mut self) {
        self.interval = std::cmp::min(self.interval * 2, self.max_interval);
    }

    pub fn slow_down(&mut self) {
        self.interval = std::cmp::max(self.interval * 9 / 10, self.min_interval);
    }
}

pub struct Collector {
    match_seq_num: u64,
    rate: RateControl,
    database: Database,
    buffer: HashMap<(Side, u8), Vec<MatchId>>,
}

impl Collector {
    pub fn new(args: &Args) -> anyhow::Result<Self> {
        let match_seq_num = args.start_idx;
        let rate = RateControl::new(args.min_interval, args.max_interval);
        let buffer = HashMap::<(Side, u8), Vec<MatchId>>::new();
        let database = Database::new(
            args.clickhouse_server.as_deref(),
            args.clickhouse_database.as_deref(),
            args.clickhouse_user.as_deref(),
            args.clickhouse_password.as_deref(),
        );

        Ok(Self {
            match_seq_num,
            rate,
            database,
            buffer,
        })
    }

    pub async fn request(&mut self, client: &Client) -> anyhow::Result<()> {
        // get_match_history is not reliable so we switch back to get_match_history_by_seq_num
        match client.get_match_history_full(self.match_seq_num, 100).await {
            Ok(matches) => {
                self.match_seq_num =
                    matches
                        .matches
                        .iter()
                        .fold(self.match_seq_num, |init, mat| {
                            mat.players.iter().for_each(|player| {
                                let side: Side = player.player_slot.into();
                                self.buffer
                                    .entry((side, player.hero_id))
                                    .or_default()
                                    .push(mat.match_id.into());
                            });
                            std::cmp::max(init, mat.match_seq_num + 1)
                        });
                self.rate.speed_up();
            }
            Err(ClientError::DecodeError(err, content)) => {
                // maybe valve have changed the json response format
                // this is when things really goes wrong, we need to fix it manually
                log::error!("decode error: {}", err);
                let filename = format!("{}-error.json", self.match_seq_num);
                std::fs::write(filename, content)?;
                // we have to quit or else we'll end in a dead loop
                return Err(err.into());
            }
            Err(ClientError::ConnectionError(err)) => {
                log::warn!("connection error: {}", err);
                self.rate.slow_down();
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(ClientError::TooManyRequests) => {
                // we're requesting API too frequently, so slowing it down a little.
                log::warn!("too many requests");
                self.rate.slow_down();
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            Err(ClientError::OtherResponse(status)) => {
                log::error!("other response: {}", status);
                self.rate.slow_down();
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            Err(ClientError::ProxyError(_)) | Err(ClientError::ConstructError(_)) => unreachable!(),
        }
        Ok(())
    }

    pub async fn save(&mut self) -> anyhow::Result<()> {
        log::info!("write database!");
        self.database.save(&self.buffer).await?;
        self.buffer.clear();
        Ok(())
    }

    pub async fn rate_control(&self) {
        log::debug!("rate control!");
        self.rate.wait().await;
    }
}
