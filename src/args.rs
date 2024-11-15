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

    #[arg(long, default_value_t = 6400)]
    pub interval: u64,
    #[arg(long, default_value_t = 1000)]
    pub batch_size: usize,

    #[arg(long)]
    pub address: Option<String>,
    #[arg(long, default_value_t = 8888)]
    pub port: u16,

    pub key: String,
}
