use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

pub const DEFAULT_CHANNEL_TITLE: &str = "tgfs-storage";
/// 1 GiB — well under the 2 GiB per-file cap, large enough to keep
/// message count low.
pub const DEFAULT_CHUNK_SIZE: u64 = 1024 * 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub api_id: i32,
    pub api_hash: String,
    /// Bare channel id of the storage channel.
    pub channel_id: i64,
    /// Telegram access hash for the storage channel.
    pub channel_access_hash: i64,
    #[serde(default = "default_chunk_size")]
    pub chunk_size: u64,
}

fn default_chunk_size() -> u64 {
    DEFAULT_CHUNK_SIZE
}

pub fn config_path() -> Result<PathBuf> {
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

pub fn index_path() -> Result<PathBuf> {
    Ok(data_dir()?.join("index.db"))
}

impl Config {
    pub fn load() -> Result<Self> {
        let path = config_path()?;
        let raw = std::fs::read_to_string(&path).with_context(|| {
            format!(
                "cannot read {} — run `tgfs init` first",
                path.display()
            )
        })?;
        toml::from_str(&raw).with_context(|| format!("invalid config at {}", path.display()))
    }

    pub fn save(&self) -> Result<()> {
        let path = config_path()?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, toml::to_string_pretty(self)?)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    }
}
