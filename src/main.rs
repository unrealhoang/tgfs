mod config;
mod crypto;
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
    Init {
        /// Generate a client-side encryption key for this repo
        #[arg(long)]
        encrypt: bool,
    },
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
    /// List this account's tgfs channels (repos you can clone)
    Channels,
    /// Pull an existing tgfs channel into a new folder
    Clone {
        /// Channel to clone: folder name or full channel title (tgfs-<name>)
        name: String,
        /// Destination folder (defaults to the name without the tgfs- prefix)
        dir: Option<PathBuf>,
        /// Encryption key of the repo (required if it was created with --encrypt)
        #[arg(long)]
        key: Option<String>,
    },
    /// Print this repo's encryption key (keep it safe; needed to clone)
    Key,
    /// Share this repo with another Telegram user
    Share {
        /// User to invite, e.g. @alice (omit when using --link)
        user: Option<String>,
        /// Grant write access (post + pin admin rights, needed for sync)
        #[arg(long)]
        write: bool,
        /// Create a read-only invite link instead of inviting a user
        #[arg(long)]
        link: bool,
    },
    /// List who has access to this repo's channel
    Members,
    /// Remove a user's access to this repo
    Unshare {
        /// User to remove, e.g. @alice
        user: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    match cli.command {
        Command::Login => login().await,
        Command::Init { encrypt } => init(encrypt).await,
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
        Command::Channels => channels().await,
        Command::Clone { name, dir, key } => clone(&name, dir, key).await,
        Command::Share { user, write, link } => share(user, write, link).await,
        Command::Members => {
            let (repo, _index, tg) = open_repo().await?;
            let peer = tg.peer(&repo.config)?;
            for (name, username, role) in tg.members(peer).await? {
                let username = username.map(|u| format!(" (@{u})")).unwrap_or_default();
                println!("{role:<8} {name}{username}");
            }
            Ok(())
        }
        Command::Unshare { user } => {
            let (repo, _index, tg) = open_repo().await?;
            let peer = tg.peer(&repo.config)?;
            let target = tg.resolve_user(&user).await?;
            tg.remove_user(peer, target).await?;
            println!("removed {user} from this repo");
            Ok(())
        }
        Command::Key => {
            let repo = Repo::discover(&std::env::current_dir()?)?;
            match &repo.config.encryption_key {
                Some(key) => {
                    println!("{key}");
                    Ok(())
                }
                None => bail!("this repo is not encrypted"),
            }
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

async fn init(encrypt: bool) -> Result<()> {
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

    // Adopting an existing channel (e.g. on a new machine): fetch its pinned
    // index so `tgfs get` can restore immediately — prompting for the
    // encryption key if the snapshot turns out to be sealed.
    let snapshot = if existed {
        let peer = tg.peer_from(channel_id, channel_access_hash)?;
        fetch_remote_snapshot(&tg, peer, None).await?
    } else {
        None
    };
    let existing_key = snapshot.as_ref().and_then(|f| f.key.clone());
    let encryption_key = match existing_key {
        Some(key) => {
            if encrypt {
                println!("channel is already encrypted — keeping its existing key");
            }
            Some(key)
        }
        None if encrypt => {
            let key = crypto::Crypto::key_to_string(&crypto::Crypto::generate_key());
            println!("generated encryption key: {key}");
            println!("KEEP IT SAFE: without it, encrypted backups cannot be restored");
            Some(key)
        }
        None => None,
    };

    let repo = Repo::create(
        &cwd,
        RepoConfig {
            channel_id,
            channel_access_hash,
            chunk_size: config::DEFAULT_CHUNK_SIZE,
            encryption_key,
        },
    )?;
    let mut index = Index::open(&repo.index_path())?;
    if let Some(f) = snapshot {
        sync::import_snapshot(&mut index, &f.plain, f.info)?;
    }
    println!(
        "initialized tgfs repo at {} — `tgfs status` to compare, `tgfs sync` to push",
        repo.root.display()
    );
    Ok(())
}

/// List all tgfs-* channels of the account, with their pinned index state.
async fn channels() -> Result<()> {
    let global = GlobalConfig::load()?;
    let tg = connect_authorized(&global).await?;
    let found = tg.list_channels("tgfs-").await?;
    if found.is_empty() {
        println!("no tgfs channels on this account — `tgfs init` in a folder creates one");
        return Ok(());
    }
    for channel in found {
        let name = channel.title.strip_prefix("tgfs-").unwrap_or(&channel.title);
        let peer = tg.peer_from(channel.id, channel.access_hash)?;
        let remote = match tg.remote_index_info(peer).await {
            Ok(Some(info)) => format!("index v{}", info.version),
            Ok(None) => "no index snapshot".to_string(),
            Err(e) => format!("index unreadable: {e}"),
        };
        println!("{name:<30}  {}  ({remote})", channel.title);
    }
    println!("clone one with `tgfs clone <name>`");
    Ok(())
}

/// Adopt an existing channel into a fresh folder and pull its index.
async fn clone(name: &str, dir: Option<PathBuf>, key: Option<String>) -> Result<()> {
    let title = if name.starts_with("tgfs-") {
        name.to_string()
    } else {
        format!("tgfs-{name}")
    };
    let folder_name = title.strip_prefix("tgfs-").expect("prefixed above");
    let dest = dir.unwrap_or_else(|| PathBuf::from(folder_name));
    if dest.join(config::REPO_DIR).exists() {
        bail!("{} is already a tgfs repo", dest.display());
    }

    let global = GlobalConfig::load()?;
    let tg = connect_authorized(&global).await?;
    let channel = tg
        .find_channel(&title)
        .await?
        .with_context(|| format!("no channel titled {title:?} — see `tgfs channels`"))?;

    // Validate a provided key before doing anything with it.
    if let Some(k) = &key {
        crypto::Crypto::key_from_string(k)?;
    }
    // Fetch (and, for an encrypted repo, prompt for the key and decrypt)
    // BEFORE creating the folder, so a wrong key leaves nothing behind.
    let peer = tg.peer_from(channel.id, channel.access_hash)?;
    let fetched = fetch_remote_snapshot(&tg, peer, key).await?;

    std::fs::create_dir_all(&dest)?;
    let (encryption_key, snapshot) = match fetched {
        Some(f) => (f.key, Some((f.info, f.plain))),
        None => {
            println!("channel has no index snapshot yet — cloning it empty");
            (None, None)
        }
    };
    let repo = Repo::create(
        &dest,
        RepoConfig {
            channel_id: channel.id,
            channel_access_hash: channel.access_hash,
            chunk_size: config::DEFAULT_CHUNK_SIZE,
            encryption_key,
        },
    )?;
    let mut index = Index::open(&repo.index_path())?;
    if let Some((info, plain)) = snapshot {
        sync::import_snapshot(&mut index, &plain, info)?;
    }
    println!(
        "cloned {title:?} into {} — `tgfs ls` to browse, `tgfs get <path>` to restore files",
        repo.root.display()
    );
    Ok(())
}

/// Share the repo: invite a user (optionally as writer) or export a link.
async fn share(user: Option<String>, write: bool, link: bool) -> Result<()> {
    let (repo, _index, tg) = open_repo().await?;
    let peer = tg.peer(&repo.config)?;
    let encrypted = repo.config.encryption_key.is_some();

    if link {
        if write {
            bail!("invite links are read-only; grant write with `tgfs share <@user> --write`");
        }
        let url = tg.export_invite_link(peer).await?;
        println!("{url}");
        if encrypted {
            println!("note: this repo is encrypted — also send the key (`tgfs key`) securely");
        }
        return Ok(());
    }

    let Some(user) = user else {
        bail!("specify a user to invite (e.g. `tgfs share @alice`) or use --link");
    };
    let target = tg.resolve_user(&user).await?;
    tg.invite(peer, target).await?;
    if write {
        tg.set_writer(peer, target, true).await?;
    }
    let role = if write { "writer" } else { "reader" };
    println!("invited {user} as {role} — they can now `tgfs clone` this repo");
    if encrypted {
        println!("note: this repo is encrypted — also send the key (`tgfs key`) securely");
    }
    Ok(())
}

/// The pinned remote snapshot, decrypted and ready to import, plus the
/// encryption key that was needed to read it (if any).
struct FetchedSnapshot {
    info: tg::RemoteIndexInfo,
    /// zstd-compressed snapshot JSON (already decrypted).
    plain: Vec<u8>,
    /// Key that successfully opened it: from `--key` or an interactive prompt.
    key: Option<String>,
}

/// Download the channel's pinned snapshot. If it is encrypted, obtain the
/// key — from `key_flag` if given, otherwise by prompting (3 attempts) —
/// and validate it against the snapshot before returning it.
async fn fetch_remote_snapshot(
    tg: &Tg,
    peer: grammers_client::session::types::PeerRef,
    key_flag: Option<String>,
) -> Result<Option<FetchedSnapshot>> {
    let Some(info) = tg.remote_index_info(peer).await? else {
        return Ok(None);
    };
    let mut raw = Vec::new();
    tg.download_document(peer, info.msg_id, &mut raw).await?;

    if !crypto::Crypto::is_sealed_blob(&raw) {
        if key_flag.is_some() {
            println!("note: --key given but this repo's snapshots are not encrypted");
        }
        return Ok(Some(FetchedSnapshot {
            info,
            plain: raw,
            key: key_flag,
        }));
    }

    let interactive = key_flag.is_none();
    let mut attempt = 0;
    let mut key_str = key_flag;
    loop {
        attempt += 1;
        let candidate = match key_str.take() {
            Some(k) => k,
            None => {
                println!("this repo is encrypted; its key is needed to read the index");
                rpassword::prompt_password("Encryption key (base64): ")?
                    .trim()
                    .to_string()
            }
        };
        let opened = crypto::Crypto::key_from_string(&candidate)
            .map(|k| crypto::Crypto::new(&k))
            .and_then(|c| c.open_blob(&raw));
        match opened {
            Ok(plain) => {
                return Ok(Some(FetchedSnapshot {
                    info,
                    plain,
                    key: Some(candidate),
                }));
            }
            Err(e) if interactive && attempt < 3 => eprintln!("{e}; try again"),
            Err(e) => return Err(e.context("cannot decrypt the remote index snapshot")),
        }
    }
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
