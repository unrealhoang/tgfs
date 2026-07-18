mod config;
mod index;
mod sync;
mod tg;

use std::io::Write as _;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};

use config::Config;
use index::Index;
use tg::Tg;

/// tgfs — use Telegram as a file storage / backup backend.
///
/// See docs/SPEC.md for the design.
#[derive(Parser)]
#[command(name = "tgfs", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Log in to Telegram and create/select the storage channel
    Init,
    /// Sync a local folder to Telegram (one-way backup)
    Sync {
        /// Folder to back up
        path: PathBuf,
    },
    /// List files stored in the remote index
    Ls {
        /// Remote path prefix to list
        prefix: Option<String>,
        /// Include tombstoned (deleted) files
        #[arg(long)]
        all: bool,
    },
    /// Download a file (or folder) from the remote index
    Get {
        /// Remote path to download
        remote: String,
        /// Local destination directory (defaults to current directory)
        dest: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    match cli.command {
        Command::Init => init().await,
        Command::Sync { path } => {
            let config = Config::load()?;
            let tg = connect_authorized(&config).await?;
            let mut index = Index::open(&config::index_path()?)?;
            sync::sync(&tg, &mut index, &config, &path).await
        }
        Command::Ls { prefix, all } => {
            let index = Index::open(&config::index_path()?)?;
            for f in index.list_files(prefix.as_deref(), all)? {
                let marker = if f.deleted { " (deleted)" } else { "" };
                println!("{:>12}  {}{marker}", human_size(f.size), f.path);
            }
            Ok(())
        }
        Command::Get { remote, dest } => {
            let config = Config::load()?;
            let tg = connect_authorized(&config).await?;
            let index = Index::open(&config::index_path()?)?;
            sync::get(&tg, &index, &config, &remote, dest).await
        }
    }
}

async fn init() -> Result<()> {
    // Reuse api credentials from an existing config when re-running init.
    let (api_id, api_hash) = match Config::load() {
        Ok(c) => (c.api_id, c.api_hash),
        Err(_) => {
            println!("Get an api_id/api_hash at https://my.telegram.org/apps");
            let api_id: i32 = prompt("api_id: ")?
                .trim()
                .parse()
                .context("api_id must be a number")?;
            let api_hash = prompt("api_hash: ")?.trim().to_string();
            (api_id, api_hash)
        }
    };
    let tg = Tg::connect(api_id).await?;
    tg.login_interactive(&api_hash).await?;
    let (channel_id, channel_access_hash) = tg.ensure_channel().await?;
    let config = Config {
        api_id,
        api_hash,
        channel_id,
        channel_access_hash,
        chunk_size: config::DEFAULT_CHUNK_SIZE,
    };
    config.save()?;
    println!("config written to {}", config::config_path()?.display());
    println!("ready — run `tgfs sync <folder>` to back up a folder");
    Ok(())
}

async fn connect_authorized(config: &Config) -> Result<Tg> {
    let tg = Tg::connect(config.api_id).await?;
    if !tg.client.is_authorized().await? {
        bail!("not logged in — run `tgfs init` first");
    }
    Ok(tg)
}

fn human_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn prompt(msg: &str) -> Result<String> {
    print!("{msg}");
    std::io::stdout().flush()?;
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    Ok(line)
}
