//! Command-line schema and argument validation.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::key::KeyArgs;

/// tgfs — use Telegram as a file storage / backup backend.
#[derive(Parser)]
#[command(name = "tgfs", version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Authenticate the Telegram account (once per machine)
    Login,
    /// Generate a new encryption key file
    Genkey {
        /// Destination key file (must not already exist)
        path: PathBuf,
    },
    /// Turn the current folder into a tgfs repo (creates .tgfs/ + channel)
    #[command(group(
        clap::ArgGroup::new("init_key")
            .args(["key", "keyfile"])
            .requires("encrypt")
    ))]
    Init {
        /// Enable client-side encryption for this repo
        #[arg(long)]
        encrypt: bool,
        #[command(flatten)]
        key: KeyArgs,
    },
    /// Show local changes and the local vs remote index version
    Status {
        /// List every changed path instead of capping each category
        #[arg(short, long)]
        verbose: bool,
    },
    /// Push new/changed files to Telegram and pin a new index snapshot
    Push {
        /// Push even if the remote index is newer (overwrites remote state)
        #[arg(long)]
        force: bool,
        /// Print one completion line per uploaded file
        #[arg(short, long)]
        verbose: bool,
        #[command(flatten)]
        key: KeyArgs,
    },
    /// Import a newer remote index snapshot into the local index
    Pull {
        /// Pull even if the local index is newer (rolls back local state)
        #[arg(long)]
        force: bool,
        #[command(flatten)]
        key: KeyArgs,
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
        #[command(flatten)]
        key: KeyArgs,
    },
    /// List index snapshots known to this repo
    Log,
    /// List this account's tgfs channels (repos you can clone)
    Channels,
    /// Pull an existing tgfs channel into a new folder
    Clone {
        /// Channel to clone: folder name or full channel title (`tgfs-<name>`)
        name: String,
        /// Destination folder (defaults to the name without the tgfs- prefix)
        dir: Option<PathBuf>,
        #[command(flatten)]
        key: KeyArgs,
    },
    /// Share this repo with another Telegram user
    Share {
        /// User to invite, e.g. @alice (omit when using --link)
        user: Option<String>,
        /// Grant write access (post + pin admin rights, needed for push)
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
