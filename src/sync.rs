//! Sync engine: incremental one-way backup of the repo folder into its
//! storage channel, restore (`get`), status, and versioned remote index
//! snapshots (see SPEC "Index versioning").

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};

use crate::config::Repo;
use crate::crypto::Crypto;
use crate::diff;
use crate::file;
use crate::index::{Index, Snapshot};
use crate::tg::{RemoteIndexInfo, Tg, index_caption};

/// Shared dependencies for sync operations.
pub struct SyncEngine<'a> {
    tg: &'a Tg,
    index: &'a mut Index,
    repo: &'a Repo,
}

impl<'a> SyncEngine<'a> {
    pub fn new(tg: &'a Tg, index: &'a mut Index, repo: &'a Repo) -> Self {
        Self { tg, index, repo }
    }
}

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
            Some(r) if r.version == local => VersionState::UpToDate(local),
            Some(r) if r.version > local => VersionState::Behind(local, r.version),
            Some(r) => VersionState::Ahead(local, r.version),
        }
    }

    pub fn describe(&self) -> String {
        match self {
            VersionState::NoRemote => "no remote index snapshot yet".to_string(),
            VersionState::UpToDate(v) => format!("index up to date with remote (v{v})"),
            VersionState::Behind(l, r) => {
                format!("index BEHIND remote (local v{l}, remote v{r}) — run `tgfs pull`")
            }
            VersionState::Ahead(l, r) => format!(
                "index AHEAD of remote (local v{l}, remote v{r}) — \
                 a previous push may not have finished; `tgfs push` will re-publish"
            ),
        }
    }
}

impl SyncEngine<'_> {
    /// Print working-tree changes and the local/remote version comparison.
    pub async fn status(&self) -> Result<()> {
        let peer = self.tg.peer(&self.repo.config)?;
        let state = VersionState::of(
            self.index.version()?,
            self.tg.remote_index_info(peer).await?,
        );
        println!("{}", state.describe());

        let changes = diff::scan_changes(self.repo, self.index)?;
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
    pub async fn push(&mut self, force: bool, key: Option<&[u8; 32]>) -> Result<()> {
        let peer = self.tg.peer(&self.repo.config)?;
        let crypto = key.map(Crypto::new).map(Arc::new);
        if self.repo.config.encrypted && crypto.is_none() {
            bail!("this repo is encrypted; supply --key or --keyfile");
        }

        let local_version = self.index.version()?;
        let remote = self.tg.remote_index_info(peer).await?;
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
        // Always publish a version newer than anything seen, so a forced push
        // over a diverged remote still moves the version forward.
        let next_version = local_version.max(remote.map(|r| r.version).unwrap_or(0)) + 1;

        let mut alive = Vec::new();
        let mut uploaded_files = 0usize;
        let mut uploaded_bytes = 0u64;
        let mut skipped = 0usize;

        for file in diff::walk_repo(self.repo)? {
            alive.push(file.rel_path.clone());

            // Fast path: unchanged size+mtime means we trust the index.
            if let Some(existing) = self.index.get_file(&file.rel_path)?
                && !existing.deleted
                && existing.size == file.size
                && existing.mtime == file.mtime
            {
                skipped += 1;
                continue;
            }

            let uploaded = file::upload(
                self.tg,
                self.index,
                peer,
                &file,
                self.repo.config.chunk_size,
                crypto.as_ref(),
            )
            .await?;
            uploaded_bytes += uploaded.uploaded_bytes;
            self.index.upsert_file(&uploaded.entry)?;
            uploaded_files += 1;
            println!("✓ {}", file.rel_path);
        }

        let tombstoned = self.index.tombstone_missing("", &alive)?;

        println!(
            "push done: {uploaded_files} uploaded ({uploaded_bytes} bytes), \
         {skipped} unchanged, {tombstoned} tombstoned"
        );

        self.index.set_version(next_version)?;
        self.snapshot(crypto.as_deref()).await?;
        Ok(())
    }

    /// Serialize the index, compress it (and seal it when the repo is
    /// encrypted), upload it to the channel and pin it.
    async fn snapshot(&self, crypto: Option<&Crypto>) -> Result<()> {
        let peer = self.tg.peer(&self.repo.config)?;
        let dump = self.index.export()?;
        let json = serde_json::to_vec(&dump)?;
        let mut compressed = zstd::encode_all(json.as_slice(), 9)?;
        let mut name = format!("tgfs-index-v{}.json.zst", dump.version);
        if let Some(crypto) = crypto {
            let context = *blake3::hash(&compressed).as_bytes();
            compressed = crypto.seal_blob(&context, &compressed)?;
            name.push_str(".enc");
        }
        let caption = index_caption(
            dump.version,
            dump.files.len(),
            dump.chunks.len(),
            dump.created_at,
        );
        let mut cursor = std::io::Cursor::new(&compressed);
        let msg_id = self
            .tg
            .upload_document(peer, &mut cursor, compressed.len(), name, &caption)
            .await?;
        self.tg.pin(peer, msg_id).await?;
        self.index
            .record_snapshot(dump.version, dump.created_at, msg_id)?;
        println!(
            "index snapshot v{} pinned (message {msg_id}, {} bytes)",
            dump.version,
            compressed.len()
        );
        Ok(())
    }

    /// Download the pinned remote snapshot and replace the local index with it.
    pub async fn pull(&mut self, force: bool, key: Option<&[u8; 32]>) -> Result<bool> {
        let peer = self.tg.peer(&self.repo.config)?;
        let local_version = self.index.version()?;
        let Some(remote) = self.tg.remote_index_info(peer).await? else {
            println!("no remote index snapshot to pull");
            return Ok(false);
        };
        match VersionState::of(local_version, Some(remote)) {
            VersionState::UpToDate(v) => {
                println!("already up to date (v{v})");
                return Ok(self.repo.config.encrypted);
            }
            VersionState::Ahead(l, r) if !force => {
                bail!(
                    "local index (v{l}) is ahead of remote (v{r}) — \
                 pulling would discard local index state; use --force to roll back"
                )
            }
            _ => {}
        }

        let mut raw = Vec::new();
        self.tg
            .download_document(peer, remote.msg_id, &mut raw)
            .await?;
        let encrypted = Crypto::is_sealed_blob(&raw);
        if encrypted {
            let key = key.context("the remote snapshot is encrypted; supply --key or --keyfile")?;
            raw = Crypto::new(key).open_blob(&raw)?;
        }
        import_snapshot(self.index, &raw, remote)?;
        Ok(encrypted)
    }
}

/// Decode a downloaded (already decrypted) snapshot, verify it matches the
/// pinned caption's version, and replace the local index with it.
pub fn import_snapshot(index: &mut Index, plain: &[u8], remote: RemoteIndexInfo) -> Result<()> {
    let json = zstd::decode_all(plain).context("snapshot is not valid zstd")?;
    let snapshot: Snapshot =
        serde_json::from_slice(&json).context("snapshot is not a valid tgfs index")?;
    if snapshot.version != remote.version {
        bail!(
            "pinned caption says v{} but snapshot contains v{} — refusing to import",
            remote.version,
            snapshot.version
        );
    }
    index.import(&snapshot)?;
    index.record_snapshot(snapshot.version, snapshot.created_at, remote.msg_id)?;
    println!(
        "pulled index v{} ({} files, {} chunks) — \
         local files are untouched; use `tgfs get` to restore",
        snapshot.version,
        snapshot.files.len(),
        snapshot.chunks.len()
    );
    Ok(())
}

impl SyncEngine<'_> {
    /// Restore `remote` (an exact file path, or a prefix for a folder) under
    /// `dest` (defaults to the repo root, i.e. restore in place).
    pub async fn get(
        &self,
        remote: &str,
        dest: Option<PathBuf>,
        key: Option<&[u8; 32]>,
    ) -> Result<()> {
        let dest = dest.unwrap_or_else(|| self.repo.root.clone());
        let targets = if let Some(file) = self.index.get_file(remote)?.filter(|f| !f.deleted) {
            vec![file]
        } else {
            let prefix = format!("{}/", remote.trim_end_matches('/'));
            let listed = self.index.list_files(Some(&prefix), false)?;
            if listed.is_empty() {
                bail!("no file or folder named {remote:?} in the index — try `tgfs ls`");
            }
            listed
                .into_iter()
                .map(|f| {
                    self.index
                        .get_file(&f.path)
                        .map(|e| e.expect("just listed"))
                })
                .collect::<Result<Vec<_>>>()?
        };
        let peer = self.tg.peer(&self.repo.config)?;
        let crypto = key.map(Crypto::new);

        for file in targets {
            let out_path =
                file::download(self.tg, self.index, peer, &file, &dest, crypto.as_ref()).await?;
            println!("✓ {} → {}", file.path, out_path.display());
        }
        Ok(())
    }
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
