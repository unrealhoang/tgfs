//! Repository discovery and configuration: per-machine account credentials,
//! per-repo settings, packing constants, and locating a repo's `.tgfs/` state.

use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::crypto::Crypto;

pub const REPO_DIR: &str = ".tgfs";
/// 1 GiB — well under the 2 GiB per-file cap, large enough to keep
/// message count low.
pub const DEFAULT_CHUNK_SIZE: u64 = 1024 * 1024 * 1024;
pub const MAX_DOCUMENT_SIZE: u64 = 2 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PackConfig {
    /// Files smaller than this are packed. Zero disables packing.
    pub threshold: u64,
    /// Approximate stored size at which a pack is flushed.
    pub target_size: u64,
}

impl Default for PackConfig {
    fn default() -> Self {
        Self {
            threshold: 8 << 20,
            target_size: 256 << 20,
        }
    }
}

/// Per-machine account credentials (`~/.config/tgfs/config.toml`).
#[derive(Debug, Serialize, Deserialize)]
pub struct GlobalConfig {
    pub api_id: i32,
    pub api_hash: String,
}

/// Per-repo settings (`<repo>/.tgfs/config.toml`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoConfig {
    /// Bare channel id of this repo's storage channel.
    pub channel_id: i64,
    /// Telegram access hash for the storage channel.
    pub channel_access_hash: i64,
    #[serde(default = "default_chunk_size")]
    pub chunk_size: u64,
    #[serde(default)]
    pub pack: PackConfig,
    /// Whether chunks and index snapshots are encrypted. The key itself is
    /// never persisted and must be supplied to commands that need it.
    #[serde(default)]
    pub encrypted: bool,
    /// Domain-separated verifier of the encryption key. This is safe to
    /// persist and lets commands reject an incorrect supplied key locally.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_verifier: Option<String>,
    /// Gitignore-style paths to omit from working-tree scans.
    #[serde(default)]
    pub exclude: Vec<String>,
}

/// A discovered tgfs repo: the folder being backed up plus its `.tgfs/`.
#[derive(Clone)]
pub struct Repo {
    pub root: PathBuf,
    pub config: RepoConfig,
}

fn default_chunk_size() -> u64 {
    DEFAULT_CHUNK_SIZE
}

pub fn default_excludes() -> Vec<String> {
    vec![".git/".into(), ".DS_Store".into(), "Thumbs.db".into()]
}

impl RepoConfig {
    fn validate(&self) -> Result<()> {
        if self.pack.threshold > self.chunk_size {
            bail!(
                "invalid pack config: threshold ({}) exceeds chunk_size ({})",
                self.pack.threshold,
                self.chunk_size
            );
        }
        if self.pack.threshold > self.pack.target_size {
            bail!(
                "invalid pack config: threshold ({}) exceeds target_size ({})",
                self.pack.threshold,
                self.pack.target_size
            );
        }
        if self.pack.target_size > MAX_DOCUMENT_SIZE {
            bail!(
                "invalid pack config: target_size ({}) exceeds Telegram's 2 GiB document cap",
                self.pack.target_size
            );
        }
        let sealed_target_size = Crypto::sealed_len(self.pack.target_size);
        if sealed_target_size > MAX_DOCUMENT_SIZE {
            bail!(
                "invalid pack config: encrypted target_size ({sealed_target_size}) exceeds Telegram's 2 GiB document cap"
            );
        }
        Ok(())
    }
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
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("cannot read {} — run `tgfs login` first", path.display()))?;
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
                    .validate()
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
        config.validate()?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn default_repo_config() -> RepoConfig {
        RepoConfig {
            channel_id: 1,
            channel_access_hash: 2,
            chunk_size: DEFAULT_CHUNK_SIZE,
            pack: PackConfig::default(),
            encrypted: false,
            key_verifier: None,
            exclude: default_excludes(),
        }
    }

    #[test]
    fn pack_defaults_deserialize() {
        let config: RepoConfig =
            toml::from_str("channel_id = 1\nchannel_access_hash = 2\nchunk_size = 1073741824\n")
                .unwrap();
        assert_eq!(config.pack.threshold, 8 << 20);
        assert_eq!(config.pack.target_size, 256 << 20);
        config.validate().unwrap();
        let encoded = toml::to_string_pretty(&config).unwrap();
        let decoded: RepoConfig = toml::from_str(&encoded).unwrap();
        assert_eq!(decoded.pack.threshold, config.pack.threshold);
        assert!(!decoded.encrypted);
        assert!(decoded.exclude.is_empty());
    }

    #[test]
    fn pack_config_is_validated() {
        let mut config = default_repo_config();
        config.pack.threshold = config.chunk_size + 1;
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("chunk_size")
        );

        config = default_repo_config();
        config.pack.target_size = config.pack.threshold - 1;
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("target_size")
        );

        config = default_repo_config();
        config.pack.target_size = MAX_DOCUMENT_SIZE;
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("encrypted")
        );

        config = default_repo_config();
        config.pack.threshold = 0;
        config.pack.target_size = 0;
        config.validate().unwrap();
    }
}
