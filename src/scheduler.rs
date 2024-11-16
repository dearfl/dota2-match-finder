use std::{collections::VecDeque, ops::Range, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::{
    client::Client,
    collector::{CollectResult, Collector},
    database::Database,
    dota2::MatchMask,
};

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct CollectorState {
    pub collected: Vec<(u64, u64)>,
}

impl CollectorState {
    pub fn prev_range(idx: u64) -> Option<Range<u64>> {
        const N: u64 = 100000;
        match idx {
            0 => None,
            idx => {
                let start = (idx - 1) / N * N;
                Some(start..idx)
            }
        }
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

    pub async fn past_range(&mut self, client: &Client) -> anyhow::Result<Option<Range<u64>>> {
        let mut iter = self.collected.iter().rev();
        let last = iter.next();
        let sec = iter.next();
        match (sec, last) {
            (None, None) => {
                let start = client.get_a_recent_match_seq_num().await?;
                self.collected.push((start, start));
                Ok(Self::prev_range(start))
            }
            (None, Some(&(start, _))) => Ok(Self::prev_range(start)),
            (Some(&(_, start)), Some(&(end, _))) => Ok(Some(start..end)),
            (Some(_), None) => unreachable!(),
        }
    }

    pub fn complete(&mut self, range: Range<u64>) {
        self.collected.push((range.start, range.end));
        self.collected.sort_unstable();
        self.collected = self.collected.iter().fold(
            Vec::with_capacity(self.collected.len()),
            |mut init, &(start, end)| {
                match init.last_mut() {
                    // merge two overlapping ranges
                    Some((_, e)) if start <= *e => *e = std::cmp::max(*e, end),
                    _ => init.push((start, end)),
                };
                init
            },
        );
    }
}

pub struct Scheduler {
    client: Client,
    database: Arc<Database>,
    batch: usize,
    interval: Duration,
    state_path: String,
    state: CollectorState,
    queue: VecDeque<(usize, Collector)>,
}

impl Scheduler {
    pub async fn new(
        key: &str,
        proxy: Option<&str>,
        database: Arc<Database>,
        state_path: &str,
        batch: usize,
        interval: Duration,
    ) -> anyhow::Result<Self> {
        let client = Client::new(key, proxy)?;

        let state_path = state_path.to_string();
        let mut state = std::fs::read_to_string(&state_path)
            .ok()
            .and_then(|content| serde_json::from_str::<CollectorState>(&content).ok())
            .unwrap_or_default();

        let range_onward = state.onward_range(&client).await?;

        let queue = VecDeque::from([
            (16, Collector::new(range_onward, batch)), // onward => 16
        ]);

        Ok(Self {
            client,
            database,
            batch,
            interval,
            queue,
            state_path,
            state,
        })
    }

    pub async fn new_past_collector(&mut self) -> anyhow::Result<Option<Collector>> {
        let col = self
            .state
            .past_range(&self.client)
            .await?
            .map(|range| Collector::new(range, self.batch * 10));
        Ok(col)
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        // add a collector for past matches
        let past_col = self.new_past_collector().await?;
        if let Some(col) = past_col {
            self.queue.push_back((4, col)); // by default, past collector only runs 4 times in one iteration
        }

        let mut base = Instant::now();

        // ideally the outer loop should never ends
        loop {
            let Some((count, mut col)) = self.queue.pop_front() else {
                break;
            };

            let mut index = 0;
            // we need this loop return a collector back to us, so we use loop instead of for
            let task = loop {
                if index >= count {
                    // collector end a loop with count times
                    break Some((count, col));
                }

                // request rate control
                tokio::time::sleep_until(base + self.interval).await;
                base = Instant::now();

                match col.step(&self.client).await? {
                    CollectResult::Normal => {
                        // in normal case, we don't need to do anything
                    }
                    CollectResult::Yield => {
                        // yield, give back the original collector
                        // maybe we could do some scheduler strategy here?
                        break Some((count, col));
                    }
                    CollectResult::Decel => {
                        base += Duration::from_secs(2);
                    }
                    CollectResult::Save(range, masks) => {
                        // received some data to save
                        self.save(range, masks).await?;
                    }
                    CollectResult::Completed(range, masks) => {
                        self.save(range, masks).await?;
                        // completed current range, try to schedule a new range
                        // None means we have finished collecting all history matches
                        break Some(count).zip(self.new_past_collector().await?);
                    }
                }
                index += 1;
            };

            if let Some(task) = task {
                // add task back to task queue to keep this loop running
                self.queue.push_back(task);
            }
        }

        Ok(())
    }

    async fn save(&mut self, range: Range<u64>, masks: Vec<Vec<MatchMask>>) -> anyhow::Result<()> {
        log::info!("Saving matches in [{}, {})!", range.start, range.end);
        for (hero, masks) in masks.into_iter().enumerate() {
            if masks.is_empty() {
                continue;
            }
            // could we make some retry attempts?
            self.database.save_indexed_masks(hero as u8, &masks).await?;
            // wait a bit here to make clickhouse happy?
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        self.state.complete(range);
        self.save_state()?;
        Ok(())
    }

    fn save_state(&self) -> anyhow::Result<()> {
        let content = serde_json::to_string_pretty(&self.state)?;
        std::fs::write(&self.state_path, content)?;
        Ok(())
    }
}
