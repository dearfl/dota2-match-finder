use clap::Parser;

#[derive(Parser)]
pub struct Args {
    #[arg(long)]
    pub start_idx: u64,
    #[arg(long)]
    pub proxy: Option<String>,
    #[arg(long)]
    pub clickhouse_server: Option<String>,
    #[arg(long)]
    pub clickhouse_database: Option<String>,
    #[arg(long)]
    pub clickhouse_user: Option<String>,
    #[arg(long)]
    pub clickhouse_password: Option<String>,
    #[arg(long, default_value_t = 32)]
    pub min_interval: u64,
    #[arg(long, default_value_t = 1000)]
    pub max_interval: u64,
    #[arg(long, default_value_t = 10000)]
    pub insert_batch_size: usize,
    pub keys: Vec<String>,
}
