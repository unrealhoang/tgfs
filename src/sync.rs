//! Synchronization coordinator for local changes and remote index versions.

use std::collections::HashSet;
use std::io::{IsTerminal as _, Write as _};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};
use grammers_client::session::types::PeerRef;

use crate::context::RepoContext;
use crate::crypto::Crypto;
use crate::diff;
use crate::file;
use crate::index::FileEntry;
use crate::snapshot::{self, RemoteIndexInfo};

#[derive(Default)]
struct PendingPack {
    members: Vec<file::PackMember>,
    files: Vec<FileEntry>,
    hashes: HashSet<String>,
    stored_size: u64,
}

impl PendingPack {
    fn contains(&self, hash: &str) -> bool {
        self.hashes.contains(hash)
    }

    fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    fn would_exceed(&self, stored_len: u64, target_size: u64) -> bool {
        !self.is_empty() && self.stored_size + stored_len > target_size
    }

    fn add_member(&mut self, member: file::PackMember) {
        self.stored_size += member.stored_len;
        self.hashes.insert(member.hash.clone());
        self.members.push(member);
    }
}

async fn flush_pack(
    context: &mut RepoContext,
    peer: PeerRef,
    pending: &mut PendingPack,
    crypto: Option<&Arc<Crypto>>,
    verbose: bool,
) -> Result<(usize, u64)> {
    if pending.is_empty() {
        return Ok((0, 0));
    }
    let pack = std::mem::take(pending);
    let uploaded_bytes =
        file::upload_pack(&context.tg, &context.index, peer, pack.members, crypto).await?;
    let uploaded_files = pack.files.len();
    for entry in pack.files {
        context.index.upsert_file(&entry)?;
        if verbose {
            println!("✓ {}", entry.path);
        }
    }
    Ok((uploaded_files, uploaded_bytes))
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

const STATUS_PATH_LIMIT: usize = 20;

struct StatusReporter {
    verbose: bool,
    progress: bool,
    changes: diff::Changes,
    new: Vec<String>,
    modified: Vec<String>,
    deleted: Vec<String>,
    stats: diff::ScanStats,
}

impl StatusReporter {
    fn new(verbose: bool) -> Self {
        Self {
            verbose,
            progress: std::io::stderr().is_terminal(),
            changes: diff::Changes::default(),
            new: Vec::new(),
            modified: Vec::new(),
            deleted: Vec::new(),
            stats: diff::ScanStats::default(),
        }
    }

    fn path(&mut self, kind: &str, path: String) {
        if self.verbose {
            println!("  {kind:<9}{path}");
            return;
        }
        let paths = match kind {
            "new:" => &mut self.new,
            "modified:" => &mut self.modified,
            "deleted:" => &mut self.deleted,
            _ => unreachable!("known status category"),
        };
        if paths.len() < STATUS_PATH_LIMIT {
            paths.push(path);
        }
    }

    fn event(&mut self, event: diff::ScanEvent) {
        match event {
            diff::ScanEvent::New(file) => {
                self.changes.new += 1;
                self.path("new:", file.rel_path);
            }
            diff::ScanEvent::Modified(file) => {
                self.changes.modified += 1;
                self.path("modified:", file.rel_path);
            }
            diff::ScanEvent::Unchanged(file) => {
                drop(file);
                self.changes.unchanged += 1;
            }
            diff::ScanEvent::Deleted(path) => {
                self.changes.deleted += 1;
                self.path("deleted:", path);
            }
            diff::ScanEvent::Progress(stats) => {
                self.stats = stats;
                if !self.progress {
                    return;
                }
                let changed = self.changes.new + self.changes.modified + self.changes.deleted;
                eprint!(
                    "\r\x1b[2Kscanning… {} files, {} dirs ({}), {} changed",
                    stats.scanned,
                    stats.dirs,
                    file::human_size(stats.bytes),
                    changed
                );
                let _ = std::io::stderr().flush();
            }
        }
    }

    fn finish(self) -> diff::Changes {
        clear_progress(self.progress);
        if !self.verbose {
            print_category("new:", &self.new, self.changes.new);
            print_category("modified:", &self.modified, self.changes.modified);
            print_category("deleted:", &self.deleted, self.changes.deleted);
        }
        if self.changes.is_clean() {
            println!(
                "0 new, 0 modified, 0 deleted, {} unchanged, {} dirs — working tree clean",
                self.changes.unchanged, self.stats.dirs
            );
        } else {
            println!(
                "{} new, {} modified, {} deleted, {} unchanged, {} dirs — run `tgfs push` to upload",
                self.changes.new,
                self.changes.modified,
                self.changes.deleted,
                self.changes.unchanged,
                self.stats.dirs
            );
        }
        self.changes
    }
}

fn print_category(label: &str, paths: &[String], total: u64) {
    for path in paths {
        println!("  {label:<9}{path}");
    }
    let omitted = total.saturating_sub(paths.len() as u64);
    if omitted > 0 {
        println!("  {label:<9}… and {omitted} more (use -v to list all)");
    }
}

fn clear_progress(enabled: bool) {
    if enabled {
        eprint!("\r\x1b[2K");
        let _ = std::io::stderr().flush();
    }
}

fn status_scan(
    repo: crate::config::Repo,
    index_path: PathBuf,
    verbose: bool,
) -> Result<diff::Changes> {
    let index = crate::index::Index::open(&index_path)?;
    let mut reporter = StatusReporter::new(verbose);
    diff::scan(&repo, &index, |event| {
        reporter.event(event);
        Ok(())
    })?;
    Ok(reporter.finish())
}

/// Print working-tree changes and the local/remote version comparison.
pub async fn status(context: &RepoContext, verbose: bool) -> Result<()> {
    let peer = context.tg.peer(&context.repo.config)?;
    let local_version = context.index.version()?;
    let repo = context.repo.clone();
    let index_path = context.repo.index_path();
    let scan_task = tokio::task::spawn_blocking(move || status_scan(repo, index_path, verbose));
    let (scan_result, remote) = tokio::join!(scan_task, context.tg.remote_index_info(peer));
    scan_result.context("working-tree scan task failed")??;
    let state = VersionState::of(local_version, remote?);
    println!("{}", state.describe());
    Ok(())
}

/// Push the repo into its channel, guarded by the version check.
pub async fn push(
    context: &mut RepoContext,
    force: bool,
    verbose: bool,
    key: Option<&[u8; 32]>,
) -> Result<()> {
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

    let mut uploaded_files = 0usize;
    let mut uploaded_bytes = 0u64;
    let mut skipped = 0usize;
    let mut tombstoned = 0usize;
    let mut changed = 0u64;
    let mut pending_pack = PendingPack::default();
    let show_progress = std::io::stderr().is_terminal();
    let mut last_progress = None::<Instant>;

    for event in diff::Scanner::new_for_push(&context.repo, &context.index)? {
        let local_file = match event? {
            diff::ScanEvent::Unchanged(file) => {
                drop(file);
                skipped += 1;
                continue;
            }
            diff::ScanEvent::Deleted(path) => {
                tombstoned += usize::from(context.index.tombstone_file(&path)?);
                continue;
            }
            diff::ScanEvent::Progress(stats) => {
                if show_progress {
                    eprint!(
                        "\r\x1b[2Kpushing… {} files, {} dirs scanned ({}), {changed} changed, {uploaded_files} uploaded",
                        stats.scanned,
                        stats.dirs,
                        crate::file::human_size(stats.bytes)
                    );
                    let _ = std::io::stderr().flush();
                    last_progress = Some(Instant::now());
                }
                continue;
            }
            diff::ScanEvent::New(file) | diff::ScanEvent::Modified(file) => file,
        };
        changed += 1;
        if show_progress
            && last_progress.is_none_or(|last| last.elapsed() >= Duration::from_millis(500))
        {
            eprint!(
                "\r\x1b[2Kuploading… {} ({changed} changed, {uploaded_files} uploaded)",
                local_file.rel_path
            );
            let _ = std::io::stderr().flush();
            last_progress = Some(Instant::now());
        }

        if context.repo.config.pack.threshold > 0
            && local_file.size < context.repo.config.pack.threshold
        {
            let entry = file::prepare(&local_file, context.repo.config.chunk_size)?;
            let Some(chunk_hash) = entry.chunks.first() else {
                context.index.upsert_file(&entry)?;
                uploaded_files += 1;
                if verbose {
                    println!("✓ {}", local_file.rel_path);
                }
                continue;
            };
            if context.index.chunk(chunk_hash)?.is_some() {
                context.index.upsert_file(&entry)?;
                uploaded_files += 1;
                if verbose {
                    println!("✓ {}", local_file.rel_path);
                }
                continue;
            }

            if !pending_pack.contains(chunk_hash) {
                let stored_len = if crypto.is_some() {
                    Crypto::sealed_len(local_file.size)
                } else {
                    local_file.size
                };
                if pending_pack.would_exceed(stored_len, context.repo.config.pack.target_size) {
                    let (files, bytes) =
                        flush_pack(context, peer, &mut pending_pack, crypto.as_ref(), verbose)
                            .await?;
                    uploaded_files += files;
                    uploaded_bytes += bytes;
                }
                let member = file::PackMember::new(
                    chunk_hash.clone(),
                    local_file.abs_path.clone(),
                    local_file.size,
                    pending_pack.stored_size,
                    crypto.is_some(),
                );
                pending_pack.add_member(member);
            }
            pending_pack.files.push(entry);
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
        if verbose {
            println!("✓ {}", local_file.rel_path);
        }
    }

    let (files, bytes) =
        flush_pack(context, peer, &mut pending_pack, crypto.as_ref(), verbose).await?;
    uploaded_files += files;
    uploaded_bytes += bytes;

    clear_progress(show_progress);
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
