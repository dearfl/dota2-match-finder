use std::{num::NonZeroU8, ops::Range, time::Duration};

use itertools::Itertools;

use crate::{
    client::{Client, RequestError},
    database::Database,
    dota2::{full::MatchHistory, MatchMask},
    rate::RateControl,
};

pub struct Collector<'db> {
    clients: Vec<Client>,
    database: &'db Database,
}

impl<'db> Collector<'db> {
    pub fn new(
        database: &'db Database,
        keys: &[String],
        proxy: Option<&str>,
    ) -> anyhow::Result<Self> {
        let clients = keys
            .iter()
            .map(|key| Client::new(key, proxy))
            .collect::<Result<_, _>>()?;
        Ok(Self { clients, database })
    }

    pub async fn request(
        client: &Client,
        range: Range<u64>,
        rate: &mut RateControl,
        indexed_masks: &mut [Vec<MatchMask>],
    ) -> anyhow::Result<Range<u64>> {
        // get_match_history is very limited so we switch back to get_match_history_by_seq_num
        let left = range.start;
        match client.get_match_history_full(left, 100).await {
            Ok(MatchHistory { status: _, matches }) => {
                matches
                    .iter()
                    .filter(|&mat| range.contains(&mat.match_seq_num)) // filter out OutOfRange matches
                    .for_each(|mat| {
                        let mask = mat.into();
                        mat.players
                            .iter()
                            .filter_map(|p| NonZeroU8::new(p.hero_id)) // filter out unknown hero
                            .for_each(|idx| indexed_masks[idx.get() as usize].push(mask));
                    });

                // rate control stuff
                rate.accelerate();
                if matches.len() < 100 {
                    // this means we're reaching the newest matches, so slowing down a bit
                    rate.decelerate();
                }

                // match_seq_num range of current batch: [left, right)
                let right = matches
                    .iter()
                    .fold(left, |init, mat| std::cmp::max(init, mat.match_seq_num + 1));
                let count = matches.len();
                log::info!("retrived {} matches from [{}, {}).", count, left, right);
                // return the new range
                Ok(right..range.end)
            }
            Err(RequestError::DecodeError(err, content)) => {
                // maybe valve have changed the json response format
                // this is when things really goes wrong, we need to fix it manually
                log::error!("DecodeError: {}", err);
                log::info!("Saving response to {}-error.json", left);
                let filename = format!("{}-error.json", left);
                std::fs::write(filename, content)?;
                // we have to quit or else we'll end in a dead loop
                // we could in theory accept unknown fields so we don't have to quit here
                // but we don't want to
                Err(err.into())
            }
            Err(error) => {
                // similar connection errors returns the unchanged range
                log::warn!("RequestError: {}", error);
                rate.decelerate();
                tokio::time::sleep(Duration::from_secs(5)).await;
                Ok(range)
            }
        }
    }

    pub async fn collect(
        &self,
        mut range: Range<u64>,
        batch: usize,
        mut rate: RateControl,
    ) -> anyhow::Result<()> {
        // use Vec<Vec> instead of HashMap<NonZeroU8, Vec> for better performance
        let mut indexed_masks: Vec<Vec<MatchMask>> =
            (0..256).map(|_| Vec::with_capacity(batch * 100)).collect();

        for clients in self.clients.iter().cycle().chunks(batch).into_iter() {
            for client in clients {
                rate.wait().await;
                match Self::request(client, range, &mut rate, &mut indexed_masks).await {
                    Err(err) => {
                        // request will only fail when decode error happened
                        // in case this happens, we still want to save requested matches
                        self.save(&indexed_masks).await?;
                        return Err(err);
                    }
                    Ok(new_range) if new_range.is_empty() => {
                        // we have finished collect this range
                        return Ok(());
                    }
                    Ok(new_range) => range = new_range,
                }
            }
            // save to clickhouse every <batch> requests
            self.save(&indexed_masks).await?;
            // clear saved inner vec so we don't have to dealloc and realloc
            indexed_masks.iter_mut().for_each(|masks| masks.clear());
        }

        Ok(())
    }

    pub async fn save(&self, indexed_masks: &[Vec<MatchMask>]) -> anyhow::Result<()> {
        // saving the full result uses way too much storage space which we can't afford!
        log::info!("saving indices to database!");
        for (hero, masks) in indexed_masks.iter().enumerate() {
            if masks.is_empty() {
                continue;
            }
            self.database.save_indexed_masks(hero as u8, masks).await?;
        }
        Ok(())
    }
}
