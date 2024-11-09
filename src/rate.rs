use std::time::Duration;

use tokio::time::Instant;

pub struct RateControl {
    // maybe we could do pwm control?
    // this is way too simple
    interval: Duration,
    min_interval: Duration,
    max_interval: Duration,
    last_timestamp: Instant,
}

impl RateControl {
    pub fn new(min_interval_ms: u64, max_interval_ms: u64) -> Self {
        let min_interval = Duration::from_millis(min_interval_ms);
        let max_interval = Duration::from_millis(max_interval_ms);
        let interval = min_interval * 2;
        let last_timestamp = Instant::now();
        Self {
            interval,
            min_interval,
            max_interval,
            last_timestamp,
        }
    }

    pub async fn wait(&mut self) {
        log::trace!("rate control: waiting {}ms!", self.interval.as_millis());
        tokio::time::sleep_until(self.last_timestamp + self.interval).await;
        self.last_timestamp = Instant::now();
    }

    pub fn accelerate(&mut self) {
        self.interval = std::cmp::max(self.interval * 9 / 10, self.min_interval);
    }

    pub fn decelerate(&mut self) {
        self.interval = std::cmp::min(self.interval * 4 / 3, self.max_interval);
    }
}
