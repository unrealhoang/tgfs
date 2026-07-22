use anyhow::Result;
use clap::Parser;

use tgfs::cli::Cli;
use tgfs::commands;

#[tokio::main]
async fn main() -> Result<()> {
    let filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::WARN.into())
        .from_env_lossy();
    tracing_subscriber::fmt().with_env_filter(filter).init();

    commands::run(Cli::parse()).await
}
