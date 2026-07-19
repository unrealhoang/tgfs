//! Synchronization coordinator for local changes and remote index versions.

use std::sync::Arc;

use anyhow::{Result, bail};

use crate::context::RepoContext;
use crate::crypto::Crypto;
use crate::diff;
use crate::file;
use crate::snapshot::{self, RemoteIndexInfo};

/// How the local index relates to the remote pinned snapshot.
pub enum VersionState {
    /// No pinned tgfs-index in the channel yet.
    NoRemote,
    UpToDate(u64),
    /// Remote is newer: (local, remote).
    Behind(u64, u64),
    /// Local is newer: (local, remote).
    Ahead(u64, u64),
}

impl VersionState {
    pub fn of(local: u64, remote: Option<RemoteIndexInfo>) -> Self {
        match remote {
            None => VersionState::NoRemote,
            Some(remote) if remote.version == local => VersionState::UpToDate(local),
            Some(remote) if remote.version > local => VersionState::Behind(local, remote.version),
            Some(remote) => VersionState::Ahead(local, remote.version),
        }
    }

    pub fn describe(&self) -> String {
        match self {
            VersionState::NoRemote => "no remote index snapshot yet".to_string(),
            VersionState::UpToDate(version) => {
                format!("index up to date with remote (v{version})")
            }
            VersionState::Behind(local, remote) => {
                format!("index BEHIND remote (local v{local}, remote v{remote}) — run `tgfs pull`")
            }
            VersionState::Ahead(local, remote) => format!(
                "index AHEAD of remote (local v{local}, remote v{remote}) — \
                 a previous push may not have finished; `tgfs push` will re-publish"
            ),
        }
    }
}

/// Print working-tree changes and the local/remote version comparison.
pub async fn status(context: &RepoContext) -> Result<()> {
    let peer = context.tg.peer(&context.repo.config)?;
    let state = VersionState::of(
        context.index.version()?,
        context.tg.remote_index_info(peer).await?,
    );
    println!("{}", state.describe());

    let changes = diff::scan_changes(&context.repo, &context.index)?;
    if changes.is_clean() {
        println!("working tree clean ({} files indexed)", changes.unchanged);
        return Ok(());
    }
    for path in &changes.new {
        println!("  new:      {path}");
    }
    for path in &changes.modified {
        println!("  modified: {path}");
    }
    for path in &changes.deleted {
        println!("  deleted:  {path}");
    }
    println!(
        "{} new, {} modified, {} deleted, {} unchanged — run `tgfs push` to upload",
        changes.new.len(),
        changes.modified.len(),
        changes.deleted.len(),
        changes.unchanged
    );
    Ok(())
}

/// Push the repo into its channel, guarded by the version check.
pub async fn push(context: &mut RepoContext, force: bool, key: Option<&[u8; 32]>) -> Result<()> {
    let peer = context.tg.peer(&context.repo.config)?;
    let crypto = key.map(Crypto::new).map(Arc::new);
    if context.repo.config.encrypted && crypto.is_none() {
        bail!("this repo is encrypted; supply --key or --keyfile");
    }

    let local_version = context.index.version()?;
    let remote = context.tg.remote_index_info(peer).await?;
    let state = VersionState::of(local_version, remote);
    match state {
        VersionState::Behind(..) if !force => {
            bail!(
                "{} (or use --force to overwrite the remote index)",
                state.describe()
            )
        }
        _ => println!("{}", state.describe()),
    }
    let next_version = local_version.max(remote.map(|info| info.version).unwrap_or(0)) + 1;

    let mut alive = Vec::new();
    let mut uploaded_files = 0usize;
    let mut uploaded_bytes = 0u64;
    let mut skipped = 0usize;

    for local_file in diff::walk_repo(&context.repo)? {
        alive.push(local_file.rel_path.clone());
        if let Some(existing) = context.index.get_file(&local_file.rel_path)?
            && !existing.deleted
            && existing.size == local_file.size
            && existing.mtime == local_file.mtime
        {
            skipped += 1;
            continue;
        }

        let uploaded = file::upload(
            &context.tg,
            &context.index,
            peer,
            &local_file,
            context.repo.config.chunk_size,
            crypto.as_ref(),
        )
        .await?;
        uploaded_bytes += uploaded.uploaded_bytes;
        context.index.upsert_file(&uploaded.entry)?;
        uploaded_files += 1;
        println!("✓ {}", local_file.rel_path);
    }

    let tombstoned = context.index.tombstone_missing("", &alive)?;
    println!(
        "push done: {uploaded_files} uploaded ({uploaded_bytes} bytes), \
         {skipped} unchanged, {tombstoned} tombstoned"
    );

    context.index.set_version(next_version)?;
    snapshot::publish(context, crypto.as_deref()).await
}

/// Download the pinned remote snapshot and replace the local index with it.
pub async fn pull(context: &mut RepoContext, force: bool, key: Option<&[u8; 32]>) -> Result<bool> {
    let peer = context.tg.peer(&context.repo.config)?;
    let local_version = context.index.version()?;
    let Some(remote) = context.tg.remote_index_info(peer).await? else {
        println!("no remote index snapshot to pull");
        return Ok(false);
    };
    match VersionState::of(local_version, Some(remote)) {
        VersionState::UpToDate(version) => {
            println!("already up to date (v{version})");
            return Ok(context.repo.config.encrypted);
        }
        VersionState::Ahead(local, remote) if !force => {
            bail!(
                "local index (v{local}) is ahead of remote (v{remote}) — \
                 pulling would discard local index state; use --force to roll back"
            )
        }
        _ => {}
    }

    let fetched = snapshot::download(&context.tg, peer, remote, key).await?;
    snapshot::import(&mut context.index, &fetched.plain, fetched.info)?;
    Ok(fetched.encrypted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_state() {
        let remote = |version| Some(RemoteIndexInfo { version, msg_id: 1 });
        assert!(matches!(VersionState::of(3, None), VersionState::NoRemote));
        assert!(matches!(
            VersionState::of(3, remote(3)),
            VersionState::UpToDate(3)
        ));
        assert!(matches!(
            VersionState::of(2, remote(5)),
            VersionState::Behind(2, 5)
        ));
        assert!(matches!(
            VersionState::of(5, remote(2)),
            VersionState::Ahead(5, 2)
        ));
    }
}
