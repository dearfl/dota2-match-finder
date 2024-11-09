use std::{num::NonZeroU8, ops::Range, time::Duration};

use itertools::Itertools;

use crate::{
    client::{Client, RequestError},
    database::Database,
    dota2::{full::Match, MatchMask},
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

    fn gen_mask(matches: &[Match], index: u64, indexed_masks: &mut [Vec<MatchMask>]) -> u64 {
        // collect a single batch
        // do we want to do anything else?
        matches.iter().fold(index, |init, mat| {
            // update masks
            let mask = mat.into();
            mat.players
                .iter()
                .filter_map(|p| NonZeroU8::new(p.hero_id))
                .for_each(|idx| indexed_masks[idx.get() as usize].push(mask));
            // calculate the new match_seq_num
            std::cmp::max(init, mat.match_seq_num + 1)
        })
    }

    pub async fn request(
        client: &Client,
        index: u64,
        rate: &mut RateControl,
        indexed_masks: &mut [Vec<MatchMask>],
    ) -> anyhow::Result<u64> {
        // get_match_history is very limited so we switch back to get_match_history_by_seq_num
        match client.get_match_history_full(index, 100).await {
            Ok(matches) => {
                // match_seq_num range of current batch: [left, right)
                let (left, right) = (
                    index,
                    Self::gen_mask(&matches.matches, index, indexed_masks),
                );
                let count = matches.matches.len();
                log::info!("retrived {} matches from [{}, {}).", count, left, right);

                rate.accelerate();
                if matches.matches.len() < 100 {
                    // this means we're reaching the newest matches, so slowing down a bit
                    rate.decelerate();
                }
                Ok(right)
            }
            Err(RequestError::DecodeError(err, content)) => {
                // maybe valve have changed the json response format
                // this is when things really goes wrong, we need to fix it manually
                log::error!("DecodeError: {}", err);
                log::info!("Saving response to {}-error.json", index);
                let filename = format!("{}-error.json", index);
                std::fs::write(filename, content)?;
                // we have to quit or else we'll end in a dead loop
                // we could in theory accept unknown fields so we don't have to quit here
                // but we don't want to
                Err(err.into())
            }
            Err(error) => {
                // similar connection errors returns the unchanged index
                log::warn!("RequestError: {}", error);
                rate.decelerate();
                tokio::time::sleep(Duration::from_secs(5)).await;
                Ok(index)
            }
        }
    }

    pub async fn collect(
        &self,
        range: Range<u64>,
        batch: usize,
        mut rate: RateControl,
    ) -> anyhow::Result<()> {
        // use Vec<Vec> instead of HashMap<NonZeroU8, Vec> for better performance
        let mut indexed_masks: Vec<Vec<MatchMask>> =
            (0..256).map(|_| Vec::with_capacity(batch * 100)).collect();

        let mut index = range.start;
        for clients in self.clients.iter().cycle().chunks(batch).into_iter() {
            for client in clients {
                rate.wait().await;
                match Self::request(client, index, &mut rate, &mut indexed_masks).await {
                    Ok(idx) => {
                        // update match_seq_num in success
                        index = idx;
                        if index >= range.end {
                            return Ok(());
                        }
                    }
                    Err(err) => {
                        // request will only fail when decode error happened
                        // in case this happens, we still want to save requested matches
                        self.save(&indexed_masks).await?;
                        // clear inner vec so we don't have to dealloc and realloc
                        indexed_masks.iter_mut().for_each(|masks| masks.clear());
                        return Err(err);
                    }
                }
            }
            // save to clickhouse every <batch> requests
            self.save(&indexed_masks).await?;
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
