use std::time::Duration;

use clap::Parser;
use kez::Client;
use reqwest::StatusCode;
use tokio::{sync::mpsc, time::Instant};

use args::Args;
use database::Database;
use dota2::{Match, RawMatch};

mod args;
mod database;
mod dota2;

struct Collect {
    client: kez::Client,
    tx: mpsc::Sender<Vec<RawMatch>>,
    interval: u64,
}

impl Collect {
    async fn collect(self, mut index: u64) -> anyhow::Result<()> {
        let mut interval = Duration::from_millis(self.interval);
        let mut base = Instant::now();
        loop {
            // since we don't care about exact meaning of fields right now, we just use
            // Client::get_match_history_by_seq_num
            match self
                .client
                .get_match_history_by_seq_num((index.into(), 100))
                .await
            {
                Ok(result) => {
                    // figuring out the new index for query, since there match_seq_num gaps between
                    // adjacent matches, and we can't confirm they are sorted, we just iterate every
                    // matches and get the maxium match_seq_num, and plus 1 for next query
                    index = result.matches.iter().fold(index + 1, |init, mat| {
                        std::cmp::max(init, mat.match_seq_num + 1)
                    });
                    self.tx.send(result.matches).await?;
                    interval = std::cmp::max(interval * 9 / 10, Duration::from_secs(1));
                }
                Err(kez::Error::DecodeError(err, content)) => {
                    // this means the format have been updated, we probably want to exit and update
                    // our program/database.
                    log::error!("Decode Error: {}", err);
                    log::error!("Raw Content: {}", content);
                    return Err(err.into());
                }
                Err(kez::Error::OtherResponse(StatusCode::FORBIDDEN)) => {
                    anyhow::bail!("Invalid API_KEY");
                }
                Err(kez::Error::OtherResponse(StatusCode::INTERNAL_SERVER_ERROR)) => {
                    log::warn!("Internal server error!");
                    interval = std::cmp::min(interval * 5, Duration::from_secs(60));
                }
                Err(err) => {
                    // network errors, we could probably try again.
                    log::warn!("{}", err);
                    interval = std::cmp::min(interval * 2, Duration::from_secs(10));
                }
            };

            // rate limit
            tokio::time::sleep_until(base + interval).await;
            base = Instant::now();
        }
    }
}

struct Save {
    db: Database,
    rx: mpsc::Receiver<Vec<RawMatch>>,
    batch: usize,
}

impl Save {
    async fn save(mut self) -> anyhow::Result<()> {
        let mut buffer: Vec<Match> = Vec::with_capacity(self.batch + 100);
        while let Some(matches) = self.rx.recv().await {
            buffer.extend(matches.into_iter().map(Into::into));
            // save in bulk
            if buffer.len() >= self.batch {
                self.db.save(&buffer).await?;
                buffer.clear();
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();
    let (tx, rx) = mpsc::channel(64);

    let database = Database::new(
        &args.clickhouse_server,
        &args.clickhouse_database,
        args.clickhouse_user.as_deref(),
        args.clickhouse_password.as_deref(),
    )
    .await?;
    // since we strictly collect from lower to higher, we can just resume from the
    // biggest match_seq_num + 1
    let previous_index = database.latest_match_seq_num().await;

    // we want timeout else sometimes it just stuck
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .connect_timeout(Duration::from_secs(30))
        .build()?;
    let client = Client::with_client(client, args.key);

    let collect = Collect {
        client,
        tx,
        interval: args.interval,
    };
    let save = Save {
        db: database,
        rx,
        batch: args.batch,
    };

    let collect = tokio::spawn(collect.collect(previous_index));
    let save = tokio::spawn(save.save());

    tokio::select! {
        c = collect => {
            c?
        }
        s = save => {
            s?
        }
    }?;

    Ok(())
}
