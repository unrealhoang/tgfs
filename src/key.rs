//! Repository encryption-key input, validation, and key-file management.

use std::io::Read as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::Args;

use crate::config::{self, Repo};

pub fn generate() -> [u8; 32] {
    rand::random()
}

pub fn encode(key: &[u8; 32]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(key)
}

pub fn decode(value: &str) -> Result<[u8; 32]> {
    use base64::Engine as _;
    let raw = base64::engine::general_purpose::STANDARD
        .decode(value.trim())
        .context("encryption key is not valid base64")?;
    raw.try_into()
        .map_err(|_| anyhow::anyhow!("encryption key must be exactly 32 bytes"))
}

/// Public verifier used to reject an incorrect key without persisting it.
pub fn verifier(key: &[u8; 32]) -> String {
    let verifier = blake3::derive_key("tgfs 2026 key verifier v1", key);
    encode(&verifier)
}

pub fn matches_verifier(key: &[u8; 32], expected: &str) -> Result<bool> {
    let expected = decode(expected).context("invalid key_verifier in .tgfs/config.toml")?;
    let actual = blake3::derive_key("tgfs 2026 key verifier v1", key);
    Ok(actual == expected)
}

/// A repository encryption key supplied directly or read from a file.
#[derive(Args)]
pub struct KeyArgs {
    /// Base64-encoded 32-byte repository encryption key
    #[arg(
        long,
        env = "TGFS_KEY",
        hide_env_values = true,
        value_name = "KEY",
        conflicts_with = "keyfile"
    )]
    pub(crate) key: Option<String>,
    /// File containing the base64-encoded repository encryption key
    #[arg(
        long,
        env = "TGFS_KEYFILE",
        value_name = "PATH",
        conflicts_with = "key"
    )]
    pub(crate) keyfile: Option<PathBuf>,
}

impl KeyArgs {
    pub fn load(&self) -> Result<Option<[u8; 32]>> {
        let (key, source) = match (&self.key, &self.keyfile) {
            (Some(key), None) => (Some(key.clone()), "--key".to_string()),
            (None, Some(path)) => (
                Some(read_keyfile(path)?),
                format!("key file {}", path.display()),
            ),
            (None, None) => (None, String::new()),
            (Some(_), Some(_)) => unreachable!("clap rejects conflicting key sources"),
        };
        key.map(|key| {
            decode(key.trim()).with_context(|| format!("invalid encryption key from {source}"))
        })
        .transpose()
    }
}

fn read_keyfile(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path)
        .with_context(|| format!("cannot open key file {}", path.display()))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("cannot inspect key file {}", path.display()))?;
    if !metadata.is_file() {
        bail!("key file {} is not a regular file", path.display());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!(
                "key file {} is accessible by group or others; run `chmod 600 {}`",
                path.display(),
                path.display()
            );
        }
    }
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .with_context(|| format!("cannot read key file {}", path.display()))?;
    Ok(contents)
}

/// Validate a supplied key and enable encryption when needed.
pub fn configure_repo_encryption(repo: &mut Repo, key: Option<&[u8; 32]>) -> Result<bool> {
    let Some(key) = key else {
        if repo.config.encrypted {
            bail!("this repo is encrypted; supply --key or --keyfile");
        }
        return Ok(false);
    };

    let mut changed = false;
    if !repo.config.encrypted {
        repo.config.encrypted = true;
        changed = true;
    }
    match repo.config.key_verifier.as_deref() {
        Some(verifier) if !matches_verifier(key, verifier)? => {
            bail!("supplied encryption key does not match this repository")
        }
        Some(_) => {}
        None => {
            repo.config.key_verifier = Some(verifier(key));
            changed = true;
        }
    }
    Ok(changed)
}

pub fn generate_keyfile(path: &Path) -> Result<()> {
    let key = generate();
    let contents = format!("{}\n", encode(&key));
    config::write_new_private(path, &contents)?;
    println!("generated encryption key file {}", path.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_roundtrip() {
        let key = generate();
        let encoded = encode(&key);
        assert_eq!(decode(&encoded).unwrap(), key);
        assert!(decode("not base64!!").is_err());
        assert!(decode("c2hvcnQ=").is_err());
    }

    #[test]
    fn verifier_matches_only_its_key() {
        let key = [3u8; 32];
        let key_verifier = verifier(&key);
        assert!(matches_verifier(&key, &key_verifier).unwrap());
        assert!(!matches_verifier(&[4u8; 32], &key_verifier).unwrap());
        assert_ne!(key_verifier, encode(&key));
    }
}
