use std::{collections::VecDeque, ops::Range, sync::Arc, time::Duration};

use backon::ExponentialBuilder;
use backon::Retryable;
use kez::Client;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::{
    collector::{CollectResult, Collector},
    database::Database,
    dota2::MatchDraft,
};

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct CollectorState {
    collected: Vec<(u64, u64)>,
}

pub async fn get_a_recent_match_seq_num(client: &Client) -> kez::Result<u64> {
    let filter = kez::dota2::get_match_history::MatchHistoryParameter::default();
    client.get_match_history(filter).await.map(|history| {
        history
            .matches
            .iter()
            .fold(0, |init, mat| std::cmp::max(init, mat.match_seq_num))
    })
}

impl CollectorState {
    pub async fn new(path: &str, client: &Client) -> anyhow::Result<Self> {
        let mut state = std::fs::read_to_string(path)
            .ok()
            .and_then(|content| serde_json::from_str::<CollectorState>(&content).ok())
            .unwrap_or_default();
        if state.collected.is_empty() {
            let start = { || async { get_a_recent_match_seq_num(client).await } }
                .retry(ExponentialBuilder::default())
                .notify(|_, dur| {
                    log::warn!("Retrying match seq num after {}ms.", dur.as_millis());
                })
                .await?;
            state.collected.push((start, start));
        }
        Ok(state)
    }

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

    pub fn onward_range(&self) -> Range<u64> {
        let end = self.collected.last().unwrap().1;
        end..u64::MAX
    }

    pub fn past_range(&self) -> Option<Range<u64>> {
        let mut iter = self.collected.iter().rev();
        let last = iter.next();
        let sec = iter.next();
        match (sec, last) {
            (None, Some(&(start, _))) => Self::prev_range(start),
            (Some(&(_, start)), Some(&(end, _))) => Some(start..end),
            _ => unreachable!(),
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
        database: Arc<Database>,
        state_path: &str,
        batch: usize,
        interval: Duration,
    ) -> anyhow::Result<Self> {
        let client = Client::new(key)?;

        let state_path = state_path.to_string();
        let state = CollectorState::new(&state_path, &client).await?;

        let range_onward = state.onward_range();

        let queue = VecDeque::from([
            (256, Collector::new(range_onward, batch)), // onward => 256
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

    pub fn new_past_collector(&self) -> Option<Collector> {
        self.state
            .past_range()
            .map(|range| Collector::new(range, self.batch * 10))
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        // maybe we should use something like actor model
        // add a collector for past matches if possible
        if let Some(col) = self.new_past_collector() {
            self.queue.push_back((3, col)); // by default, past collector runs 3 times in one iteration
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
                        break Some(count).zip(self.new_past_collector());
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

    async fn save(&mut self, range: Range<u64>, masks: Vec<MatchDraft>) -> anyhow::Result<()> {
        log::info!("Saving matches in [{}, {})!", range.start, range.end);
        { || async { self.database.save_match_masks(&masks).await } }
            .retry(ExponentialBuilder::default())
            .notify(|err, dur| {
                log::warn!("Retrying {} after {}ms.", err, dur.as_millis());
            })
            .await?;
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
