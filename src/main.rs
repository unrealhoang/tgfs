mod config;
mod index;
mod sync;
mod tg;

use std::io::Write as _;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};

use config::{GlobalConfig, Repo, RepoConfig};
use index::Index;
use tg::Tg;

/// tgfs — use Telegram as a file storage / backup backend.
///
/// Folder-based, like git: run `tgfs init` inside the folder you want to
/// back up; every other command works from anywhere inside that folder.
/// See docs/SPEC.md for the design.
#[derive(Parser)]
#[command(name = "tgfs", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Authenticate the Telegram account (once per machine)
    Login,
    /// Turn the current folder into a tgfs repo (creates .tgfs/ + channel)
    Init,
    /// Show local changes and the local vs remote index version
    Status,
    /// Push new/changed files to Telegram and pin a new index snapshot
    Sync {
        /// Push even if the remote index is newer (overwrites remote state)
        #[arg(long)]
        force: bool,
    },
    /// Import a newer remote index snapshot into the local index
    Pull {
        /// Pull even if the local index is newer (rolls back local state)
        #[arg(long)]
        force: bool,
    },
    /// List files stored in the index
    Ls {
        /// Path prefix to list
        prefix: Option<String>,
        /// Include tombstoned (deleted) files
        #[arg(long)]
        all: bool,
    },
    /// Download a file (or folder) from the channel into the working tree
    Get {
        /// Repo-relative path to restore
        path: String,
        /// Alternative destination directory (defaults to the repo root)
        dest: Option<PathBuf>,
    },
    /// List index snapshots known to this repo
    Log,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    match cli.command {
        Command::Login => login().await,
        Command::Init => init().await,
        Command::Status => {
            let (repo, index, tg) = open_repo().await?;
            sync::status(&tg, &index, &repo).await
        }
        Command::Sync { force } => {
            let (repo, mut index, tg) = open_repo().await?;
            sync::sync(&tg, &mut index, &repo, force).await
        }
        Command::Pull { force } => {
            let (repo, mut index, tg) = open_repo().await?;
            sync::pull(&tg, &mut index, &repo, force).await
        }
        Command::Ls { prefix, all } => {
            let repo = Repo::discover(&std::env::current_dir()?)?;
            let index = Index::open(&repo.index_path())?;
            for f in index.list_files(prefix.as_deref(), all)? {
                let marker = if f.deleted { " (deleted)" } else { "" };
                println!("{:>12}  {}{marker}", human_size(f.size), f.path);
            }
            Ok(())
        }
        Command::Get { path, dest } => {
            let (repo, index, tg) = open_repo().await?;
            sync::get(&tg, &index, &repo, &path, dest).await
        }
        Command::Log => {
            let repo = Repo::discover(&std::env::current_dir()?)?;
            let index = Index::open(&repo.index_path())?;
            println!("local index version: v{}", index.version()?);
            for (version, created_at, msg_id) in index.list_snapshots()? {
                println!("v{version}  created={created_at}  message={msg_id}");
            }
            Ok(())
        }
    }
}

async fn login() -> Result<()> {
    let (api_id, api_hash) = match GlobalConfig::load() {
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
    GlobalConfig { api_id, api_hash }.save()?;
    println!("logged in — run `tgfs init` inside a folder to start backing it up");
    Ok(())
}

async fn init() -> Result<()> {
    let cwd = std::env::current_dir()?;
    if let Ok(repo) = Repo::discover(&cwd) {
        bail!(
            "already inside a tgfs repo rooted at {}",
            repo.root.display()
        );
    }
    let global = GlobalConfig::load()?;
    let tg = connect_authorized(&global).await?;

    let title = Repo::channel_title(&cwd)?;
    let (channel_id, channel_access_hash, existed) = tg.ensure_channel(&title).await?;
    let repo = Repo::create(
        &cwd,
        RepoConfig {
            channel_id,
            channel_access_hash,
            chunk_size: config::DEFAULT_CHUNK_SIZE,
        },
    )?;
    let mut index = Index::open(&repo.index_path())?;

    // Adopting an existing channel (e.g. on a new machine): pull its
    // pinned index so `tgfs get` can restore immediately.
    if existed {
        sync::pull(&tg, &mut index, &repo, false).await?;
    }
    println!(
        "initialized tgfs repo at {} — `tgfs status` to compare, `tgfs sync` to push",
        repo.root.display()
    );
    Ok(())
}

type OpenedRepo = (Repo, Index, Tg);

async fn open_repo() -> Result<OpenedRepo> {
    let repo = Repo::discover(&std::env::current_dir()?)?;
    let index = Index::open(&repo.index_path())?;
    let global = GlobalConfig::load()?;
    let tg = connect_authorized(&global).await?;
    Ok((repo, index, tg))
}

async fn connect_authorized(global: &GlobalConfig) -> Result<Tg> {
    let tg = Tg::connect(global.api_id).await?;
    if !tg.client.is_authorized().await? {
        bail!("not logged in — run `tgfs login` first");
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
