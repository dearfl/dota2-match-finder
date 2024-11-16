use clap::Parser;

#[derive(Parser)]
pub struct Args {
    #[arg(long)]
    pub proxy: Option<String>,

    #[arg(long, default_value = "http://localhost:8123")]
    pub clickhouse_server: String,
    #[arg(long)]
    #[arg(long, default_value = "dota2")]
    pub clickhouse_database: String,
    #[arg(long)]
    pub clickhouse_user: Option<String>,
    #[arg(long)]
    pub clickhouse_password: Option<String>,

    #[arg(long, default_value_t = 6400)]
    pub interval: u64,
    #[arg(long, default_value_t = 1000)]
    pub batch: usize,
    #[arg(long, default_value = "./collected.json")]
    pub collected: String,

    #[arg(long, default_value = "localhost")]
    pub addr: String,
    #[arg(long, default_value_t = 8888)]
    pub port: u16,

    pub key: String,
}
