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
    #[arg(long, default_value_t = 4000)]
    pub min_interval: u64,
    #[arg(long, default_value_t = 30000)]
    pub max_interval: u64,
    #[arg(long, default_value_t = 10000)]
    pub insert_batch_size: usize,
    pub keys: Vec<String>,
}
