use clap::Parser;

#[derive(Parser)]
pub struct Args {
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

    #[arg(long, default_value_t = 1000)]
    pub min_interval_past: u64,
    #[arg(long, default_value_t = 10000)]
    pub max_interval_past: u64,
    #[arg(long, default_value_t = 100)]
    pub batch_size_past: usize,

    #[arg(long, default_value_t = 5000)]
    pub min_interval_onward: u64,
    #[arg(long, default_value_t = 60000)]
    pub max_interval_onward: u64,
    #[arg(long, default_value_t = 10)]
    pub batch_size_onward: usize,

    pub keys: Vec<String>,
}
