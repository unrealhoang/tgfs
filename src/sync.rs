//! Sync engine: incremental one-way backup of the repo folder into its
//! storage channel, restore (`get`), status, and versioned remote index
//! snapshots (see SPEC "Index versioning").

use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use std::sync::Arc;

use crate::config::{REPO_DIR, Repo};
use crate::crypto::{Crypto, DecryptingWriter};
use crate::index::{ChunkEntry, FileEntry, Index, Snapshot};
use crate::tg::{PartSource, RemoteIndexInfo, Tg, index_caption, total_parts};

const HASH_BUF_SIZE: usize = 1024 * 1024;

/// Byte range of a plaintext chunk within its file, read with pread so
/// multiple upload workers can share it.
struct PlainChunkSource {
    file: std::fs::File,
    base: u64,
    len: u64,
}

impl PartSource for PlainChunkSource {
    fn len(&self) -> u64 {
        self.len
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        use std::os::unix::fs::FileExt;
        self.file
            .read_exact_at(buf, self.base + offset)
            .context("chunk read failed (file changed during sync?)")
    }
}

/// Like [`PlainChunkSource`] but exposing the *sealed* bytes: reads cover
/// whole segments, which are re-encrypted on demand (deterministic nonces
/// make this stable across retries and resumes).
struct EncryptedChunkSource {
    file: std::fs::File,
    base: u64,
    plain_len: u64,
    crypto: Arc<Crypto>,
    /// Chunk plaintext hash (hex) — the nonce context.
    context: String,
}

impl PartSource for EncryptedChunkSource {
    fn len(&self) -> u64 {
        Crypto::sealed_len(self.plain_len)
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        use std::os::unix::fs::FileExt;
        let sealed_seg = Crypto::sealed_segment_len(crate::crypto::SEGMENT_SIZE);
        let mut written = 0usize;
        while written < buf.len() {
            let pos = offset + written as u64;
            let seg = pos / sealed_seg;
            let in_seg = (pos % sealed_seg) as usize;
            let plain_off = seg * crate::crypto::SEGMENT_SIZE;
            let plain_take =
                crate::crypto::SEGMENT_SIZE.min(self.plain_len - plain_off) as usize;
            let mut plain = vec![0u8; plain_take];
            self.file
                .read_exact_at(&mut plain, self.base + plain_off)
                .context("chunk read failed (file changed during sync?)")?;
            let sealed = self.crypto.seal_segment(self.context.as_bytes(), seg, &plain)?;
            let take = (sealed.len() - in_seg).min(buf.len() - written);
            buf[written..written + take].copy_from_slice(&sealed[in_seg..in_seg + take]);
            written += take;
        }
        Ok(())
    }
}

/// Hash a file, returning the whole-file hash and the hash of every
/// `chunk_size`-sized chunk (streaming; nothing is held in memory).
fn hash_file(path: &Path, chunk_size: u64) -> Result<(String, Vec<String>)> {
    let mut file = std::fs::File::open(path)?;
    let mut file_hasher = blake3::Hasher::new();
    let mut chunk_hashes = Vec::new();
    let mut buf = vec![0u8; HASH_BUF_SIZE];
    loop {
        let mut chunk_hasher = blake3::Hasher::new();
        let mut in_chunk = 0u64;
        while in_chunk < chunk_size {
            let want = buf.len().min((chunk_size - in_chunk) as usize);
            let n = file.read(&mut buf[..want])?;
            if n == 0 {
                break;
            }
            chunk_hasher.update(&buf[..n]);
            file_hasher.update(&buf[..n]);
            in_chunk += n as u64;
        }
        if in_chunk == 0 {
            break;
        }
        chunk_hashes.push(chunk_hasher.finalize().to_hex().to_string());
        if in_chunk < chunk_size {
            break;
        }
    }
    Ok((file_hasher.finalize().to_hex().to_string(), chunk_hashes))
}

/// A file in the working tree, with its repo-relative path.
struct LocalFile {
    rel_path: String,
    abs_path: PathBuf,
    size: u64,
    mtime: i64,
}

/// Differences between the working tree and the local index.
#[derive(Default)]
pub struct Changes {
    pub new: Vec<String>,
    pub modified: Vec<String>,
    pub deleted: Vec<String>,
    pub unchanged: usize,
}

impl Changes {
    pub fn is_clean(&self) -> bool {
        self.new.is_empty() && self.modified.is_empty() && self.deleted.is_empty()
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
            VersionState::Behind(l, r) => format!(
                "index BEHIND remote (local v{l}, remote v{r}) — run `tgfs pull`"
            ),
            VersionState::Ahead(l, r) => format!(
                "index AHEAD of remote (local v{l}, remote v{r}) — \
                 a previous sync may not have finished; `tgfs sync` will re-publish"
            ),
        }
    }
}

fn walk_repo(repo: &Repo) -> Result<Vec<LocalFile>> {
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
pub fn scan_changes(repo: &Repo, index: &Index) -> Result<Changes> {
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

/// Print working-tree changes and the local/remote version comparison.
pub async fn status(tg: &Tg, index: &Index, repo: &Repo) -> Result<()> {
    let peer = tg.peer(&repo.config)?;
    let state = VersionState::of(index.version()?, tg.remote_index_info(peer).await?);
    println!("{}", state.describe());

    let changes = scan_changes(repo, index)?;
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
        "{} new, {} modified, {} deleted, {} unchanged — run `tgfs sync` to push",
        changes.new.len(),
        changes.modified.len(),
        changes.deleted.len(),
        changes.unchanged
    );
    Ok(())
}

/// One-way sync of the repo into its channel, guarded by the version check.
pub async fn sync(tg: &Tg, index: &mut Index, repo: &Repo, force: bool) -> Result<()> {
    let peer = tg.peer(&repo.config)?;
    let crypto = repo.crypto()?.map(Arc::new);

    let local_version = index.version()?;
    let remote = tg.remote_index_info(peer).await?;
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

    for file in walk_repo(repo)? {
        alive.push(file.rel_path.clone());

        // Fast path: unchanged size+mtime means we trust the index.
        if let Some(existing) = index.get_file(&file.rel_path)?
            && !existing.deleted
            && existing.size == file.size
            && existing.mtime == file.mtime
        {
            skipped += 1;
            continue;
        }

        let (file_hash, chunk_hashes) = hash_file(&file.abs_path, repo.config.chunk_size)?;
        let total = chunk_hashes.len();
        for (seq, chunk_hash) in chunk_hashes.iter().enumerate() {
            if index.chunk(chunk_hash)?.is_some() {
                continue; // dedup: chunk already in the channel
            }
            let offset = seq as u64 * repo.config.chunk_size;
            let chunk_len = (file.size - offset).min(repo.config.chunk_size);
            let opened = std::fs::File::open(&file.abs_path)?;
            let source: Arc<dyn PartSource> = match &crypto {
                Some(c) => Arc::new(EncryptedChunkSource {
                    file: opened,
                    base: offset,
                    plain_len: chunk_len,
                    crypto: Arc::clone(c),
                    context: chunk_hash.clone(),
                }),
                None => Arc::new(PlainChunkSource {
                    file: opened,
                    base: offset,
                    len: chunk_len,
                }),
            };
            let parts = total_parts(source.len());
            // Resume a matching interrupted upload of this very chunk.
            let resume = index
                .journal_get(chunk_hash)?
                .filter(|&(_, journal_total, _)| journal_total == parts)
                .map(|(file_id, _, done)| (file_id, done));
            let name = format!("{}.bin", &chunk_hash[..16]);
            let caption = format!("{} [{}/{total}]", file.rel_path, seq + 1);
            println!("  ↑ {caption} ({chunk_len} bytes)");
            let msg_id = tg
                .upload_source(peer, source, name, &caption, resume, |file_id, done| {
                    if done == 0 {
                        index.journal_start(chunk_hash, file_id, parts)
                    } else {
                        index.journal_progress(chunk_hash, done)
                    }
                })
                .await?;
            index.journal_clear(chunk_hash)?;
            index.insert_chunk(&ChunkEntry {
                hash: chunk_hash.clone(),
                size: chunk_len,
                msg_id,
                encrypted: crypto.is_some(),
            })?;
            uploaded_bytes += chunk_len;
        }
        index.upsert_file(&FileEntry {
            path: file.rel_path.clone(),
            size: file.size,
            mtime: file.mtime,
            hash: file_hash,
            deleted: false,
            chunks: chunk_hashes,
        })?;
        uploaded_files += 1;
        println!("✓ {}", file.rel_path);
    }

    let tombstoned = index.tombstone_missing("", &alive)?;

    println!(
        "sync done: {uploaded_files} uploaded ({uploaded_bytes} bytes), \
         {skipped} unchanged, {tombstoned} tombstoned"
    );

    index.set_version(next_version)?;
    snapshot(tg, index, repo).await?;
    Ok(())
}

/// Serialize the index, compress it (and seal it when the repo is
/// encrypted), upload it to the channel and pin it.
pub async fn snapshot(tg: &Tg, index: &Index, repo: &Repo) -> Result<()> {
    let peer = tg.peer(&repo.config)?;
    let dump = index.export()?;
    let json = serde_json::to_vec(&dump)?;
    let mut compressed = zstd::encode_all(json.as_slice(), 9)?;
    let mut name = format!("tgfs-index-v{}.json.zst", dump.version);
    if let Some(crypto) = repo.crypto()? {
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
    let msg_id = tg
        .upload_document(peer, &mut cursor, compressed.len(), name, &caption)
        .await?;
    tg.pin(peer, msg_id).await?;
    index.record_snapshot(dump.version, dump.created_at, msg_id)?;
    println!(
        "index snapshot v{} pinned (message {msg_id}, {} bytes)",
        dump.version,
        compressed.len()
    );
    Ok(())
}

/// Download the pinned remote snapshot and replace the local index with it.
pub async fn pull(tg: &Tg, index: &mut Index, repo: &Repo, force: bool) -> Result<()> {
    let peer = tg.peer(&repo.config)?;
    let local_version = index.version()?;
    let Some(remote) = tg.remote_index_info(peer).await? else {
        println!("no remote index snapshot to pull");
        return Ok(());
    };
    match VersionState::of(local_version, Some(remote)) {
        VersionState::UpToDate(v) => {
            println!("already up to date (v{v})");
            return Ok(());
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
    tg.download_document(peer, remote.msg_id, &mut raw).await?;
    if Crypto::is_sealed_blob(&raw) {
        let crypto = repo.crypto()?.context(
            "the remote index snapshot is encrypted but this repo has no \
             encryption_key — add the repo's key to .tgfs/config.toml \
             (see `tgfs key` on a machine that has it)",
        )?;
        raw = crypto.open_blob(&raw)?;
    }
    import_snapshot(index, &raw, remote)?;
    Ok(())
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

/// Restore `remote` (an exact file path, or a prefix for a folder) under
/// `dest` (defaults to the repo root, i.e. restore in place).
pub async fn get(
    tg: &Tg,
    index: &Index,
    repo: &Repo,
    remote: &str,
    dest: Option<PathBuf>,
) -> Result<()> {
    let dest = dest.unwrap_or_else(|| repo.root.clone());
    let targets = if let Some(file) = index.get_file(remote)?.filter(|f| !f.deleted) {
        vec![file]
    } else {
        let prefix = format!("{}/", remote.trim_end_matches('/'));
        let listed = index.list_files(Some(&prefix), false)?;
        if listed.is_empty() {
            bail!("no file or folder named {remote:?} in the index — try `tgfs ls`");
        }
        listed
            .into_iter()
            .map(|f| index.get_file(&f.path).map(|e| e.expect("just listed")))
            .collect::<Result<Vec<_>>>()?
    };
    let peer = tg.peer(&repo.config)?;
    let crypto = repo.crypto()?;

    for file in targets {
        let out_path = dest.join(&file.path);
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let part_path = out_path.with_extension("tgfs-part");
        let mut out = std::fs::File::create(&part_path)?;
        let mut hasher = blake3::Hasher::new();
        for chunk_hash in &file.chunks {
            let chunk = index
                .chunk(chunk_hash)?
                .with_context(|| format!("chunk {chunk_hash} missing from index"))?;
            let mut writer = HashingWriter {
                inner: &mut out,
                hasher: &mut hasher,
            };
            let (n, expected) = if chunk.encrypted {
                let crypto = crypto.as_ref().context(
                    "chunk is encrypted but this repo has no encryption_key in \
                     .tgfs/config.toml",
                )?;
                let mut decryptor =
                    DecryptingWriter::new(&mut writer, crypto, chunk_hash.as_bytes(), chunk.size);
                let n = tg
                    .download_document(peer, chunk.msg_id, &mut decryptor)
                    .await?;
                (n, Crypto::sealed_len(chunk.size))
            } else {
                let n = tg.download_document(peer, chunk.msg_id, &mut writer).await?;
                (n, chunk.size)
            };
            if n != expected {
                bail!("chunk {chunk_hash}: downloaded {n} bytes, expected {expected}");
            }
        }
        let actual = hasher.finalize().to_hex().to_string();
        if actual != file.hash {
            bail!(
                "hash mismatch for {}: expected {}, got {actual}",
                file.path,
                file.hash
            );
        }
        std::fs::rename(&part_path, &out_path)?;
        println!("✓ {} → {}", file.path, out_path.display());
    }
    Ok(())
}

struct HashingWriter<'a, W: std::io::Write> {
    inner: &'a mut W,
    hasher: &'a mut blake3::Hasher,
}

impl<W: std::io::Write> std::io::Write for HashingWriter<'_, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_file_chunking() {
        let path = std::env::temp_dir().join(format!("tgfs-hash-test-{}", std::process::id()));
        // 10 bytes with chunk size 4 → chunks of 4, 4, 2.
        std::fs::write(&path, b"0123456789").unwrap();
        let (file_hash, chunks) = hash_file(&path, 4).unwrap();
        assert_eq!(chunks.len(), 3);
        assert_eq!(file_hash, blake3::hash(b"0123456789").to_hex().to_string());
        assert_eq!(chunks[0], blake3::hash(b"0123").to_hex().to_string());
        assert_eq!(chunks[2], blake3::hash(b"89").to_hex().to_string());

        // Exact multiple of the chunk size → no empty trailing chunk.
        std::fs::write(&path, b"01234567").unwrap();
        let (_, chunks) = hash_file(&path, 4).unwrap();
        assert_eq!(chunks.len(), 2);

        // Empty file → zero chunks, hash of empty input.
        std::fs::write(&path, b"").unwrap();
        let (file_hash, chunks) = hash_file(&path, 4).unwrap();
        assert!(chunks.is_empty());
        assert_eq!(file_hash, blake3::hash(b"").to_hex().to_string());

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn encrypted_source_roundtrips_through_decrypting_writer() {
        use crate::crypto::SEGMENT_SIZE;
        let path = std::env::temp_dir().join(format!("tgfs-enc-src-{}", std::process::id()));
        // Chunk starting at a non-zero base, spanning >2 segments.
        let base = 1000u64;
        let plain_len = 2 * SEGMENT_SIZE + 4321;
        let mut content = vec![0u8; (base + plain_len) as usize];
        for (i, b) in content.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        std::fs::write(&path, &content).unwrap();

        let crypto = Arc::new(Crypto::new(&[9u8; 32]));
        let source = EncryptedChunkSource {
            file: std::fs::File::open(&path).unwrap(),
            base,
            plain_len,
            crypto: Arc::clone(&crypto),
            context: "deadbeef".into(),
        };
        assert_eq!(source.len(), Crypto::sealed_len(plain_len));

        // Read the sealed stream in odd-sized pieces, as the uploader would
        // in 512 KiB parts (and simulate out-of-order part reads).
        let mut sealed = vec![0u8; source.len() as usize];
        let piece = 512 * 1024 + 7;
        let mut offsets: Vec<usize> = (0..sealed.len()).step_by(piece).collect();
        offsets.reverse();
        for off in offsets {
            let end = (off + piece).min(sealed.len());
            source.read_at(off as u64, &mut sealed[off..end]).unwrap();
        }

        let mut out = Vec::new();
        {
            use std::io::Write as _;
            let mut w = DecryptingWriter::new(&mut out, &crypto, b"deadbeef", plain_len);
            w.write_all(&sealed).unwrap();
        }
        assert_eq!(out, &content[base as usize..]);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn version_state() {
        let remote = |version| Some(RemoteIndexInfo { version, msg_id: 1 });
        assert!(matches!(
            VersionState::of(3, None),
            VersionState::NoRemote
        ));
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
