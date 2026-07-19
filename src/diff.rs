//! Working-tree discovery and comparison with the local index.

use std::path::PathBuf;

use anyhow::Result;

use crate::config::{REPO_DIR, Repo};
use crate::index::Index;

/// A file in the working tree, with its repo-relative path and metadata.
pub(crate) struct LocalFile {
    pub(crate) rel_path: String,
    pub(crate) abs_path: PathBuf,
    pub(crate) size: u64,
    pub(crate) mtime: i64,
}

/// Differences between the working tree and the local index.
#[derive(Default)]
pub(crate) struct Changes {
    pub(crate) new: Vec<String>,
    pub(crate) modified: Vec<String>,
    pub(crate) deleted: Vec<String>,
    pub(crate) unchanged: usize,
}

impl Changes {
    pub(crate) fn is_clean(&self) -> bool {
        self.new.is_empty() && self.modified.is_empty() && self.deleted.is_empty()
    }
}

pub(crate) fn walk_repo(repo: &Repo) -> Result<Vec<LocalFile>> {
    let mut files = Vec::new();
    let walker = walkdir::WalkDir::new(&repo.root)
        .sort_by_file_name()
        .into_iter()
        .filter_entry(|e| e.file_name() != REPO_DIR);
    for entry in walker {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        let rel = entry.path().strip_prefix(&repo.root)?;
        let meta = entry.metadata()?;
        files.push(LocalFile {
            rel_path: rel.to_string_lossy().replace('\\', "/"),
            abs_path: entry.path().to_path_buf(),
            size: meta.len(),
            mtime: meta
                .modified()?
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
        });
    }
    Ok(files)
}

/// Diff the working tree against the local index (size+mtime fast path).
pub(crate) fn scan_changes(repo: &Repo, index: &Index) -> Result<Changes> {
    let mut changes = Changes::default();
    let mut alive = std::collections::HashSet::new();
    for file in walk_repo(repo)? {
        alive.insert(file.rel_path.clone());
        match index.get_file(&file.rel_path)? {
            Some(e) if !e.deleted && e.size == file.size && e.mtime == file.mtime => {
                changes.unchanged += 1;
            }
            Some(e) if !e.deleted => changes.modified.push(file.rel_path),
            _ => changes.new.push(file.rel_path),
        }
    }
    for entry in index.list_files(None, false)? {
        if !alive.contains(&entry.path) {
            changes.deleted.push(entry.path);
        }
    }
    Ok(changes)
}
