use std::time::Duration;

use clap::Parser;
use kez::Client;
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
        let interval = Duration::from_millis(self.interval);
        let mut base = Instant::now();
        loop {
            match self
                .client
                .get_match_history_by_seq_num((index.into(), 100))
                .await
            {
                Ok(result) => {
                    index = result.matches.iter().fold(index + 1, |init, mat| {
                        std::cmp::max(init, mat.match_seq_num + 1)
                    });
                    self.tx.send(result.matches).await?;
                }
                Err(kez::Error::DecodeError(err, content)) => {
                    log::error!("Decode Error: {}", err);
                    log::error!("Raw Content: {}", content);
                    return Err(err.into());
                }
                Err(err) => {
                    log::warn!("{}", err);
                }
            };

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
    let previous_index = database.latest_match_seq_num().await;

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
