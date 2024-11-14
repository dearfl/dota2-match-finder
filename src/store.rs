use std::{num::NonZeroU8, ops::Range, time::Duration};

use crate::{
    collector::CollectorStatus,
    database::Database,
    dota2::{full::Match, MatchMask},
};

pub struct Store<'db> {
    index: u64,
    range: Range<u64>,
    // use Vec<Vec> instead of HashMap<NonZeroU8, Vec> for better performance
    masks: Vec<Vec<MatchMask>>,
    count: usize,
    batch: usize,
    database: &'db Database,
}

impl<'db> Store<'db> {
    pub fn new(database: &'db Database, range: Range<u64>, batch: usize) -> Self {
        let Range { start, end } = range;
        log::info!("Start collecting matches from [{}, {})", start, end);
        Self {
            index: range.start,
            range,
            masks: (0..256).map(|_| Vec::with_capacity(batch)).collect(),
            count: 0,
            batch,
            database,
        }
    }

    pub fn range(&self) -> Range<u64> {
        self.range.start..self.range.end
    }

    pub fn current_range(&self) -> Range<u64> {
        self.index..self.range.end
    }

    // TODO: maybe decoupling(collector logic) a bit?
    pub async fn push(
        &mut self,
        matches: &[Match],
        status: &mut CollectorStatus,
        path: &str,
    ) -> anyhow::Result<bool> {
        matches
            .iter()
            .filter(|&mat| self.range.contains(&mat.match_seq_num)) // filter out OutOfRange matches
            .for_each(|mat| {
                let mask = mat.into();
                mat.players
                    .iter()
                    .filter_map(|p| NonZeroU8::new(p.hero_id)) // filter out unknown hero
                    .for_each(|idx| self.masks[idx.get() as usize].push(mask));
            });

        let start = self.index;
        self.index = matches.iter().fold(self.index, |init, mat| {
            std::cmp::max(init, mat.match_seq_num + 1)
        });
        let count = matches.len();
        log::debug!("Collected {} matches in [{}, {})", count, start, self.index);

        self.count += count;
        // save when reaching <batch> matches or completed
        if self.count >= self.batch || self.is_completed() {
            self.save().await?;
            status.finish(self.range.start..self.index);
            let content = serde_json::to_string_pretty(status)?;
            std::fs::write(path, content)?;
        }
        Ok(self.is_completed())
    }

    async fn save(&mut self) -> anyhow::Result<()> {
        // the number here may not be accurate
        log::info!("Saving {} matches in [..., {})!", self.count, self.index);
        for (hero, masks) in self.masks.iter_mut().enumerate() {
            if masks.is_empty() {
                continue;
            }
            self.database.save_indexed_masks(hero as u8, masks).await?;
            masks.clear();
            // wait a bit here to make clickhouse happy?
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        self.count = 0;
        Ok(())
    }

    fn is_completed(&self) -> bool {
        self.current_range().is_empty()
    }
}
