//! Streaming working-tree discovery and comparison with the local index.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant, UNIX_EPOCH};

use anyhow::{Context, Result};
use ignore::overrides::OverrideBuilder;

use crate::config::{REPO_DIR, Repo};
use crate::index::Index;

/// A file in the working tree, with its repo-relative path and metadata.
pub(crate) struct LocalFile {
    pub(crate) rel_path: String,
    pub(crate) abs_path: PathBuf,
    pub(crate) size: u64,
    pub(crate) mtime: i64,
}

/// Aggregate differences. Paths are delivered through [`ScanEvent`] instead
/// of being retained for the lifetime of a large scan.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Changes {
    pub(crate) new: u64,
    pub(crate) modified: u64,
    pub(crate) deleted: u64,
    pub(crate) unchanged: u64,
}

impl Changes {
    pub(crate) fn is_clean(&self) -> bool {
        self.new == 0 && self.modified == 0 && self.deleted == 0
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ScanStats {
    pub(crate) scanned: u64,
    pub(crate) bytes: u64,
}

pub(crate) enum ScanEvent {
    New(LocalFile),
    Modified(LocalFile),
    Unchanged(LocalFile),
    Deleted(String),
    Progress(ScanStats),
}

/// Lazy filesystem iterator. The `ignore` walker applies exclusions before it
/// descends into a directory and sorts only the directory currently visited.
pub(crate) struct LocalFiles {
    root: PathBuf,
    walk: ignore::Walk,
}

impl Iterator for LocalFiles {
    type Item = Result<LocalFile>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let entry = match self.walk.next()? {
                Ok(entry) => entry,
                Err(error) => return Some(Err(error.into())),
            };
            if !entry.file_type().is_some_and(|kind| kind.is_file()) {
                continue;
            }
            let path = entry.into_path();
            let file = (|| {
                let rel = path.strip_prefix(&self.root).with_context(|| {
                    format!("{} is outside repo {}", path.display(), self.root.display())
                })?;
                let meta = path
                    .metadata()
                    .with_context(|| format!("cannot stat {}", path.display()))?;
                let mtime = meta
                    .modified()?
                    .duration_since(UNIX_EPOCH)
                    .map(|duration| duration.as_secs() as i64)
                    .unwrap_or(0);
                Ok(LocalFile {
                    rel_path: rel.to_string_lossy().replace('\\', "/"),
                    abs_path: path,
                    size: meta.len(),
                    mtime,
                })
            })();
            return Some(file);
        }
    }
}

pub(crate) fn walk_repo(repo: &Repo) -> Result<LocalFiles> {
    let mut overrides = OverrideBuilder::new(&repo.root);
    for pattern in &repo.config.exclude {
        // Override globs use `!` for exclusions. Keeping these as overrides
        // means .gitignore and hidden-file filtering remain disabled.
        overrides
            .add(&format!("!{pattern}"))
            .with_context(|| format!("invalid exclude pattern {pattern:?}"))?;
    }

    let mut builder = ignore::WalkBuilder::new(&repo.root);
    builder
        .standard_filters(false)
        .add_custom_ignore_filename(".tgfsignore")
        .overrides(overrides.build()?)
        .filter_entry(|entry| entry.file_name() != REPO_DIR)
        .sort_by_file_name(|left, right| left.cmp(right));
    Ok(LocalFiles {
        root: repo.root.clone(),
        walk: builder.build(),
    })
}

/// An async-friendly form of the scanner: it owns the preloaded index state,
/// so callers may await uploads between events without borrowing the index.
pub(crate) struct Scanner {
    files: LocalFiles,
    previous: HashMap<String, (u64, i64)>,
    deleted: Option<std::vec::IntoIter<String>>,
    stats: ScanStats,
    last_progress: Instant,
    pending_progress: bool,
}

impl Scanner {
    pub(crate) fn new(repo: &Repo, index: &Index) -> Result<Self> {
        Ok(Self {
            files: walk_repo(repo)?,
            previous: index.stat_map()?,
            deleted: None,
            stats: ScanStats::default(),
            last_progress: Instant::now(),
            pending_progress: false,
        })
    }
}

impl Iterator for Scanner {
    type Item = Result<ScanEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pending_progress {
            self.pending_progress = false;
            self.last_progress = Instant::now();
            return Some(Ok(ScanEvent::Progress(self.stats)));
        }

        if let Some(deleted) = &mut self.deleted {
            return deleted.next().map(|path| Ok(ScanEvent::Deleted(path)));
        }

        match self.files.next() {
            Some(Err(error)) => Some(Err(error)),
            Some(Ok(file)) => {
                self.stats.scanned += 1;
                self.stats.bytes += file.size;
                if self.stats.scanned.is_multiple_of(256)
                    && self.last_progress.elapsed() >= Duration::from_millis(500)
                {
                    self.pending_progress = true;
                }
                let prior = self.previous.remove(&file.rel_path);
                let event = match prior {
                    Some((size, mtime)) if size == file.size && mtime == file.mtime => {
                        ScanEvent::Unchanged(file)
                    }
                    Some(_) => ScanEvent::Modified(file),
                    None => ScanEvent::New(file),
                };
                Some(Ok(event))
            }
            None => {
                let mut deleted: Vec<_> = self.previous.drain().map(|(path, _)| path).collect();
                deleted.sort();
                self.deleted = Some(deleted.into_iter());
                self.next()
            }
        }
    }
}

/// Scan the working tree and synchronously deliver each event to `on_event`.
pub(crate) fn scan(
    repo: &Repo,
    index: &Index,
    mut on_event: impl FnMut(ScanEvent) -> Result<()>,
) -> Result<Changes> {
    let mut changes = Changes::default();
    for event in Scanner::new(repo, index)? {
        let event = event?;
        match &event {
            ScanEvent::New(_) => changes.new += 1,
            ScanEvent::Modified(_) => changes.modified += 1,
            ScanEvent::Unchanged(_) => changes.unchanged += 1,
            ScanEvent::Deleted(_) => changes.deleted += 1,
            ScanEvent::Progress(_) => {}
        }
        on_event(event)?;
    }
    Ok(changes)
}

/// Counter-only convenience wrapper used by callers that do not need paths.
#[allow(dead_code)]
pub(crate) fn scan_changes(repo: &Repo, index: &Index) -> Result<Changes> {
    scan(repo, index, |_| Ok(()))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;
    use crate::config::{PackConfig, RepoConfig, default_excludes};
    use crate::index::FileEntry;

    fn temp_repo(exclude: Vec<String>) -> (Repo, Index, PathBuf) {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let root = std::env::temp_dir().join(format!(
            "tgfs-diff-test-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(root.join(REPO_DIR)).unwrap();
        let repo = Repo {
            root: root.clone(),
            config: RepoConfig {
                channel_id: 1,
                channel_access_hash: 2,
                chunk_size: 1024,
                pack: PackConfig::default(),
                encrypted: false,
                key_verifier: None,
                exclude,
            },
        };
        let index = Index::open(&repo.index_path()).unwrap();
        (repo, index, root)
    }

    fn indexed(path: &str, size: u64, mtime: i64) -> FileEntry {
        FileEntry {
            path: path.into(),
            size,
            mtime,
            hash: "hash".into(),
            deleted: false,
            chunks: vec![],
        }
    }

    #[test]
    fn scan_classifies_files_and_emits_deletions_last() {
        let (repo, mut index, root) = temp_repo(vec![]);
        fs::write(root.join("same"), b"abc").unwrap();
        fs::write(root.join("changed"), b"longer").unwrap();
        fs::write(root.join("new"), b"new").unwrap();
        let same = walk_repo(&repo)
            .unwrap()
            .find_map(|file| {
                let file = file.unwrap();
                (file.rel_path == "same").then_some(file)
            })
            .unwrap();
        index
            .upsert_file(&indexed("same", same.size, same.mtime))
            .unwrap();
        index.upsert_file(&indexed("changed", 1, 0)).unwrap();
        index.upsert_file(&indexed("deleted", 1, 0)).unwrap();

        let mut kinds = Vec::new();
        let changes = scan(&repo, &index, |event| {
            match event {
                ScanEvent::New(file) => kinds.push(format!("new:{}", file.rel_path)),
                ScanEvent::Modified(file) => kinds.push(format!("modified:{}", file.rel_path)),
                ScanEvent::Unchanged(file) => kinds.push(format!("same:{}", file.rel_path)),
                ScanEvent::Deleted(path) => kinds.push(format!("deleted:{path}")),
                ScanEvent::Progress(_) => {}
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(
            changes,
            Changes {
                new: 1,
                modified: 1,
                deleted: 1,
                unchanged: 1
            }
        );
        assert_eq!(kinds.last().unwrap(), "deleted:deleted");

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn config_and_tgfsignore_exclusions_become_deletions() {
        let (repo, mut index, root) = temp_repo(default_excludes());
        fs::create_dir(root.join(".git")).unwrap();
        fs::write(root.join(".git/objects"), b"ignored").unwrap();
        fs::write(root.join("keep.txt"), b"keep").unwrap();
        fs::write(root.join("skip.tmp"), b"skip").unwrap();
        fs::write(root.join(".tgfsignore"), b"*.tmp\n").unwrap();
        fs::write(root.join(".gitignore"), b"git-ignored\n").unwrap();
        fs::write(root.join("git-ignored"), b"still backed up").unwrap();
        fs::create_dir(root.join("nested")).unwrap();
        fs::write(root.join("nested/.tgfsignore"), b"*.cache\n").unwrap();
        fs::write(root.join("nested/thumb.cache"), b"ignored").unwrap();
        index.upsert_file(&indexed("skip.tmp", 4, 0)).unwrap();

        let mut paths = Vec::new();
        let changes = scan(&repo, &index, |event| {
            match event {
                ScanEvent::New(file) => paths.push(file.rel_path),
                ScanEvent::Deleted(path) => paths.push(format!("deleted:{path}")),
                _ => {}
            }
            Ok(())
        })
        .unwrap();
        assert!(!paths.iter().any(|path| path.starts_with(".git/")));
        assert!(!paths.iter().any(|path| path == "skip.tmp"));
        assert!(!paths.iter().any(|path| path == "nested/thumb.cache"));
        assert!(paths.iter().any(|path| path == "git-ignored"));
        assert!(paths.iter().any(|path| path == "deleted:skip.tmp"));
        assert_eq!(changes.deleted, 1);

        fs::remove_dir_all(root).unwrap();
    }
}
