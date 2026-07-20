mod cli;
mod config;
mod context;
mod crypto;
mod diff;
mod file;
mod index;
mod key;
mod restore;
mod session;
mod snapshot;
mod sync;
mod telegram_transfer;
mod tg;

use std::io::Write as _;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Parser;

use cli::{Cli, Command};
use config::{GlobalConfig, Repo, RepoConfig};
use index::Index;
use key::{configure_repo_encryption, generate_keyfile};
use tg::Tg;

#[tokio::main]
async fn main() -> Result<()> {
    let filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::WARN.into())
        .from_env_lossy();
    tracing_subscriber::fmt().with_env_filter(filter).init();
    let cli = Cli::parse();
    match cli.command {
        Command::Login => login().await,
        Command::Genkey { path } => generate_keyfile(&path),
        Command::Init { encrypt, key } => init(encrypt, key.load()?).await,
        Command::Status { verbose } => {
            let context = open_repo().await?;
            sync::status(&context, verbose).await
        }
        Command::Push {
            force,
            verbose,
            key,
        } => {
            let key = key.load()?;
            let mut context = open_repo().await?;
            if configure_repo_encryption(&mut context.repo, key.as_ref())? {
                context.repo.save_config()?;
            }
            sync::push(&mut context, force, verbose, key.as_ref()).await
        }
        Command::Pull { force, key } => {
            let key = key.load()?;
            let mut context = open_repo().await?;
            let config_changed = configure_repo_encryption(&mut context.repo, key.as_ref())?;
            let remote_encrypted = sync::pull(&mut context, force, key.as_ref()).await?;
            if remote_encrypted && !context.repo.config.encrypted {
                context.repo.config.encrypted = true;
            }
            if config_changed || remote_encrypted {
                context.repo.save_config()?;
            }
            Ok(())
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
        Command::Get { path, dest, key } => {
            let key = key.load()?;
            let mut context = open_repo().await?;
            let config_changed = configure_repo_encryption(&mut context.repo, key.as_ref())?;
            restore::get(&context, &path, dest, key.as_ref()).await?;
            if config_changed {
                context.repo.save_config()?;
            }
            Ok(())
        }
        Command::Channels => channels().await,
        Command::Clone { name, dir, key } => clone(&name, dir, key.load()?).await,
        Command::Share { user, write, link } => share(user, write, link).await,
        Command::Members => {
            let context = open_repo().await?;
            let peer = context.tg.peer(&context.repo.config)?;
            for (name, username, role) in context.tg.members(peer).await? {
                let username = username.map(|u| format!(" (@{u})")).unwrap_or_default();
                println!("{role:<8} {name}{username}");
            }
            Ok(())
        }
        Command::Unshare { user } => {
            let context = open_repo().await?;
            let peer = context.tg.peer(&context.repo.config)?;
            let target = context.tg.resolve_user(&user).await?;
            context.tg.remove_user(peer, target).await?;
            println!("removed {user} from this repo");
            Ok(())
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

async fn init(encrypt: bool, supplied_key: Option<[u8; 32]>) -> Result<()> {
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
    // index so `tgfs get` can restore immediately. Encrypted snapshots require
    // an explicit --key or --keyfile.
    let snapshot = if existed {
        let peer = tg.peer_from(channel_id, channel_access_hash)?;
        let snapshot = snapshot::fetch(&tg, peer, supplied_key.as_ref()).await?;
        if snapshot.is_none() {
            println!("channel has no index snapshot yet — initializing it empty");
        }
        snapshot
    } else {
        None
    };
    let remote_encrypted = snapshot.as_ref().is_some_and(|snapshot| snapshot.encrypted);
    let generated_key = if encrypt && supplied_key.is_none() && !remote_encrypted {
        let key = key::generate();
        println!("generated encryption key: {}", key::encode(&key));
        println!("KEEP IT SAFE: pass it with --key or --keyfile for encrypted operations");
        Some(key)
    } else {
        None
    };
    let effective_key = supplied_key.as_ref().or(generated_key.as_ref());

    let repo = Repo::create(
        &cwd,
        RepoConfig {
            channel_id,
            channel_access_hash,
            chunk_size: config::DEFAULT_CHUNK_SIZE,
            pack: config::PackConfig::default(),
            encrypted: encrypt || remote_encrypted,
            key_verifier: effective_key.map(key::verifier),
            exclude: config::default_excludes(),
        },
    )?;
    let mut index = Index::open(&repo.index_path())?;
    if let Some(f) = snapshot {
        snapshot::import(&mut index, &f.plain, f.info)?;
    }
    println!(
        "initialized tgfs repo at {} — `tgfs status` to compare, `tgfs push` to upload",
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
        let name = channel
            .title
            .strip_prefix("tgfs-")
            .unwrap_or(&channel.title);
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
async fn clone(name: &str, dir: Option<PathBuf>, key: Option<[u8; 32]>) -> Result<()> {
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

    // Fetch and decrypt BEFORE creating the folder, so a missing or wrong key
    // leaves nothing behind.
    let peer = tg.peer_from(channel.id, channel.access_hash)?;
    let fetched = snapshot::fetch(&tg, peer, key.as_ref()).await?;

    std::fs::create_dir_all(&dest)?;
    let (encrypted, snapshot) = match fetched {
        Some(f) => (f.encrypted || key.is_some(), Some((f.info, f.plain))),
        None => {
            println!("channel has no index snapshot yet — cloning it empty");
            (key.is_some(), None)
        }
    };
    let repo = Repo::create(
        &dest,
        RepoConfig {
            channel_id: channel.id,
            channel_access_hash: channel.access_hash,
            chunk_size: config::DEFAULT_CHUNK_SIZE,
            pack: config::PackConfig::default(),
            encrypted,
            key_verifier: key.as_ref().map(key::verifier),
            exclude: config::default_excludes(),
        },
    )?;
    let mut index = Index::open(&repo.index_path())?;
    if let Some((info, plain)) = snapshot {
        snapshot::import(&mut index, &plain, info)?;
    }
    println!(
        "cloned {title:?} into {} — `tgfs ls` to browse, `tgfs get <path>` to restore files",
        repo.root.display()
    );
    Ok(())
}

/// Share the repo: invite a user (optionally as writer) or export a link.
async fn share(user: Option<String>, write: bool, link: bool) -> Result<()> {
    let context = open_repo().await?;
    let peer = context.tg.peer(&context.repo.config)?;
    let encrypted = context.repo.config.encrypted;

    if link {
        if write {
            bail!("invite links are read-only; grant write with `tgfs share <@user> --write`");
        }
        let url = context.tg.export_invite_link(peer).await?;
        println!("{url}");
        if encrypted {
            println!("note: this repo is encrypted — also provide its key securely");
        }
        return Ok(());
    }

    let Some(user) = user else {
        bail!("specify a user to invite (e.g. `tgfs share @alice`) or use --link");
    };
    let target = context.tg.resolve_user(&user).await?;
    context.tg.invite(peer, target).await?;
    if write {
        context.tg.set_writer(peer, target, true).await?;
    }
    let role = if write { "writer" } else { "reader" };
    println!("invited {user} as {role} — they can now `tgfs clone` this repo");
    if encrypted {
        println!("note: this repo is encrypted — also provide its key securely");
    }
    Ok(())
}

async fn open_repo() -> Result<context::RepoContext> {
    let repo = Repo::discover(&std::env::current_dir()?)?;
    let index = Index::open(&repo.index_path())?;
    let global = GlobalConfig::load()?;
    let tg = connect_authorized(&global).await?;
    Ok(context::RepoContext::new(tg, index, repo))
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

#[cfg(test)]
mod cli_tests {
    use super::*;

    fn encoded_key(byte: u8) -> String {
        key::encode(&[byte; 32])
    }

    fn repo_with_verifier(encrypted: bool, verifier: Option<String>) -> Repo {
        Repo {
            root: PathBuf::new(),
            config: RepoConfig {
                channel_id: 1,
                channel_access_hash: 2,
                chunk_size: config::DEFAULT_CHUNK_SIZE,
                pack: config::PackConfig::default(),
                encrypted,
                key_verifier: verifier,
                exclude: config::default_excludes(),
            },
        }
    }

    #[test]
    fn cli_uses_push_and_enforces_key_sources() {
        let key = encoded_key(7);
        assert!(Cli::try_parse_from(["tgfs", "genkey", "key.txt"]).is_ok());
        assert!(Cli::try_parse_from(["tgfs", "push"]).is_ok());
        assert!(Cli::try_parse_from(["tgfs", "push", "-v"]).is_ok());
        assert!(Cli::try_parse_from(["tgfs", "status", "--verbose"]).is_ok());
        assert!(Cli::try_parse_from(["tgfs", "sync"]).is_err());
        assert!(Cli::try_parse_from(["tgfs", "key"]).is_err());
        assert!(
            Cli::try_parse_from(["tgfs", "init", "--key", &key]).is_err(),
            "init keys require --encrypt"
        );
        assert!(Cli::try_parse_from(["tgfs", "init", "--encrypt", "--key", &key]).is_ok());
        assert!(
            Cli::try_parse_from(["tgfs", "push", "--key", &key, "--keyfile", "key.txt",]).is_err(),
            "--key and --keyfile are mutually exclusive"
        );
    }

    #[test]
    fn encryption_key_options_are_bound_to_environment_variables() {
        use std::ffi::OsStr;

        use clap::CommandFactory;

        let command = Cli::command();
        for subcommand_name in ["init", "push", "pull", "get", "clone"] {
            let subcommand = command
                .find_subcommand(subcommand_name)
                .expect("key-accepting subcommand exists");
            let key = subcommand
                .get_arguments()
                .find(|argument| argument.get_id() == "key")
                .expect("--key argument exists");
            let keyfile = subcommand
                .get_arguments()
                .find(|argument| argument.get_id() == "keyfile")
                .expect("--keyfile argument exists");

            assert_eq!(key.get_env(), Some(OsStr::new("TGFS_KEY")));
            assert_eq!(keyfile.get_env(), Some(OsStr::new("TGFS_KEYFILE")));
        }
    }

    #[test]
    fn keyfile_is_trimmed_and_validated() {
        let path = std::env::temp_dir().join(format!("tgfs-key-test-{}", std::process::id()));
        let key = encoded_key(9);
        std::fs::write(&path, format!("{key}\n")).unwrap();
        let args = key::KeyArgs {
            key: None,
            keyfile: Some(path.clone()),
        };
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(args.load().is_err());
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        assert_eq!(args.load().unwrap(), Some([9u8; 32]));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn genkey_creates_a_private_file_without_overwriting() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("tgfs-genkey-test-{}-{unique}", std::process::id()));
        generate_keyfile(&path).unwrap();
        let contents = std::fs::read_to_string(&path).unwrap();
        assert!(contents.ends_with('\n'));
        assert!(key::decode(&contents).is_ok());
        assert!(
            key::KeyArgs {
                key: None,
                keyfile: Some(path.clone()),
            }
            .load()
            .unwrap()
            .is_some()
        );
        assert!(generate_keyfile(&path).is_err());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn verifier_rejects_a_different_key() {
        let first = [1u8; 32];
        let second = [2u8; 32];
        let mut repo = repo_with_verifier(false, None);
        assert!(configure_repo_encryption(&mut repo, Some(&first)).unwrap());
        assert!(repo.config.encrypted);
        assert_eq!(repo.config.key_verifier, Some(key::verifier(&first)));
        let serialized = toml::to_string(&repo.config).unwrap();
        assert!(serialized.contains("encrypted = true"));
        assert!(serialized.contains("key_verifier"));
        assert!(!serialized.contains(&key::encode(&first)));
        assert!(!configure_repo_encryption(&mut repo, Some(&first)).unwrap());
        assert!(configure_repo_encryption(&mut repo, Some(&second)).is_err());
        assert!(configure_repo_encryption(&mut repo, None).is_err());
    }
}
