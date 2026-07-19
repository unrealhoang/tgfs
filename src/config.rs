use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

pub const REPO_DIR: &str = ".tgfs";
/// 1 GiB — well under the 2 GiB per-file cap, large enough to keep
/// message count low.
pub const DEFAULT_CHUNK_SIZE: u64 = 1024 * 1024 * 1024;
/// Files smaller than this are candidates for packing (see PACKING.md).
/// 8 MiB clears most "many small files" workloads while keeping the
/// per-member ranged-download slack negligible.
pub const DEFAULT_PACK_THRESHOLD: u64 = 8 * 1024 * 1024;
/// A pack is flushed once its stored size reaches this. 256 MiB keeps a
/// failed pack's re-upload exposure bounded and stays far from the cap
/// even after encryption overhead.
pub const DEFAULT_PACK_TARGET_SIZE: u64 = 256 * 1024 * 1024;
/// Telegram's per-document size cap (non-premium). A pack's *stored*
/// (possibly sealed) size must fit under this.
pub const MAX_DOCUMENT_SIZE: u64 = 2 * 1024 * 1024 * 1024;

/// Per-machine account credentials (`~/.config/tgfs/config.toml`).
#[derive(Debug, Serialize, Deserialize)]
pub struct GlobalConfig {
    pub api_id: i32,
    pub api_hash: String,
}

/// Per-repo settings (`<repo>/.tgfs/config.toml`).
#[derive(Debug, Serialize, Deserialize)]
pub struct RepoConfig {
    /// Bare channel id of this repo's storage channel.
    pub channel_id: i64,
    /// Telegram access hash for the storage channel.
    pub channel_access_hash: i64,
    #[serde(default = "default_chunk_size")]
    pub chunk_size: u64,
    /// Whether chunks and index snapshots are encrypted. The key itself is
    /// never persisted and must be supplied to commands that need it.
    #[serde(default)]
    pub encrypted: bool,
    /// Domain-separated verifier of the encryption key. This is safe to
    /// persist and lets commands reject an incorrect supplied key locally.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_verifier: Option<String>,
    /// Small-file packing settings (see PACKING.md). Absent in configs that
    /// predate the feature, which default to packing disabled.
    #[serde(default)]
    pub pack: PackConfig,
}

/// Controls packing many small files into one uploaded document. Members
/// stay individually content-addressed and encrypted; only their storage
/// location (one shared message, distinct offsets) changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackConfig {
    /// Off by default so upgrading a binary never changes an existing repo's
    /// on-remote layout; `tgfs init` writes `true` for new repos.
    #[serde(default)]
    pub enabled: bool,
    /// Files strictly smaller than this are packed; larger ones take the
    /// normal one-message-per-chunk path.
    #[serde(default = "default_pack_threshold")]
    pub threshold: u64,
    /// A pack is flushed once its accumulated stored size reaches this.
    #[serde(default = "default_pack_target_size")]
    pub target_size: u64,
}

impl Default for PackConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            threshold: default_pack_threshold(),
            target_size: default_pack_target_size(),
        }
    }
}

impl PackConfig {
    /// Reject nonsensical settings before they can corrupt a push.
    pub fn validate(&self, chunk_size: u64) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        if self.threshold == 0 {
            bail!("pack.threshold must be greater than zero");
        }
        if self.threshold > chunk_size {
            bail!(
                "pack.threshold ({}) must not exceed chunk_size ({chunk_size}) — \
                 a packed file is single-chunk by definition",
                self.threshold
            );
        }
        if self.target_size < self.threshold {
            bail!(
                "pack.target_size ({}) must be at least pack.threshold ({})",
                self.target_size,
                self.threshold
            );
        }
        // The sealed pack must still fit under Telegram's per-document cap.
        if crate::crypto::Crypto::sealed_len(self.target_size) > MAX_DOCUMENT_SIZE {
            bail!(
                "pack.target_size ({}) is too large: its sealed size exceeds the \
                 {MAX_DOCUMENT_SIZE}-byte per-document cap",
                self.target_size
            );
        }
        Ok(())
    }
}

/// A discovered tgfs repo: the folder being backed up plus its `.tgfs/`.
pub struct Repo {
    pub root: PathBuf,
    pub config: RepoConfig,
}

fn default_chunk_size() -> u64 {
    DEFAULT_CHUNK_SIZE
}

fn default_pack_threshold() -> u64 {
    DEFAULT_PACK_THRESHOLD
}

fn default_pack_target_size() -> u64 {
    DEFAULT_PACK_TARGET_SIZE
}

pub fn global_config_path() -> Result<PathBuf> {
    Ok(dirs::config_dir()
        .context("cannot determine config directory")?
        .join("tgfs/config.toml"))
}

pub fn data_dir() -> Result<PathBuf> {
    let dir = dirs::data_dir()
        .context("cannot determine data directory")?
        .join("tgfs");
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

pub fn session_path() -> Result<PathBuf> {
    Ok(data_dir()?.join("session.db"))
}

fn write_private(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, contents)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

/// Create a new secret file without ever replacing an existing path.
pub fn write_new_private(path: &Path, contents: &str) -> Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .with_context(|| format!("cannot create key file {}", path.display()))?;
    file.write_all(contents.as_bytes())?;
    file.sync_all()?;
    Ok(())
}

impl GlobalConfig {
    pub fn load() -> Result<Self> {
        let path = global_config_path()?;
        let raw = std::fs::read_to_string(&path).with_context(|| {
            format!("cannot read {} — run `tgfs login` first", path.display())
        })?;
        toml::from_str(&raw).with_context(|| format!("invalid config at {}", path.display()))
    }

    pub fn save(&self) -> Result<()> {
        write_private(&global_config_path()?, &toml::to_string_pretty(self)?)
    }
}

impl Repo {
    /// Walk up from `start` looking for a `.tgfs` directory, like git does.
    pub fn discover(start: &Path) -> Result<Self> {
        let start = start
            .canonicalize()
            .with_context(|| format!("cannot access {}", start.display()))?;
        for dir in start.ancestors() {
            let marker = dir.join(REPO_DIR);
            if marker.is_dir() {
                let config_path = marker.join("config.toml");
                let raw = std::fs::read_to_string(&config_path)
                    .with_context(|| format!("cannot read {}", config_path.display()))?;
                let config: RepoConfig = toml::from_str(&raw)
                    .with_context(|| format!("invalid config at {}", config_path.display()))?;
                config
                    .pack
                    .validate(config.chunk_size)
                    .with_context(|| format!("invalid config at {}", config_path.display()))?;
                return Ok(Self {
                    root: dir.to_path_buf(),
                    config,
                });
            }
        }
        bail!(
            "not inside a tgfs repo (no {REPO_DIR} directory found from {} upward) — \
             run `tgfs init` in the folder you want to back up",
            start.display()
        )
    }

    /// Create `.tgfs/` in `root` and persist `config`.
    pub fn create(root: &Path, config: RepoConfig) -> Result<Self> {
        config.pack.validate(config.chunk_size)?;
        let root = root.canonicalize()?;
        let marker = root.join(REPO_DIR);
        std::fs::create_dir_all(&marker)?;
        let repo = Self { root, config };
        repo.save_config()?;
        Ok(repo)
    }

    pub fn save_config(&self) -> Result<()> {
        write_private(
            &self.root.join(REPO_DIR).join("config.toml"),
            &toml::to_string_pretty(&self.config)?,
        )
    }

    pub fn index_path(&self) -> PathBuf {
        self.root.join(REPO_DIR).join("index.db")
    }

    /// Channel title for this repo, derived from the folder name.
    pub fn channel_title(root: &Path) -> Result<String> {
        let name = root
            .file_name()
            .context("cannot init the filesystem root")?
            .to_string_lossy();
        Ok(format!("tgfs-{name}"))
    }
}
