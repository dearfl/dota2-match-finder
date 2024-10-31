use std::time::Duration;

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
