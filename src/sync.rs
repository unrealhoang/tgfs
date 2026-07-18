//! Sync engine: incremental one-way backup of a folder into the storage
//! channel, plus restore (`get`) and remote index snapshots.

use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::config::Config;
use crate::index::{ChunkEntry, FileEntry, Index, now_unix};
use crate::tg::Tg;

const HASH_BUF_SIZE: usize = 1024 * 1024;

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

/// One-way sync of `folder` into the channel. The remote path of each file
/// is `<folder-name>/<relative-path>`.
pub async fn sync(tg: &Tg, index: &mut Index, config: &Config, folder: &Path) -> Result<()> {
    let folder = folder
        .canonicalize()
        .with_context(|| format!("cannot access {}", folder.display()))?;
    if !folder.is_dir() {
        bail!("{} is not a directory", folder.display());
    }
    let root_name = folder
        .file_name()
        .context("cannot sync filesystem root")?
        .to_string_lossy()
        .to_string();
    let peer = tg.peer(config)?;

    let mut alive = Vec::new();
    let mut uploaded_files = 0usize;
    let mut uploaded_bytes = 0u64;
    let mut skipped = 0usize;

    for entry in walkdir::WalkDir::new(&folder).sort_by_file_name() {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        let rel = entry.path().strip_prefix(&folder)?;
        let remote_path = format!("{root_name}/{}", rel.to_string_lossy().replace('\\', "/"));
        let meta = entry.metadata()?;
        let size = meta.len();
        let mtime = meta
            .modified()?
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        alive.push(remote_path.clone());

        // Fast path: unchanged size+mtime means we trust the index.
        if let Some(existing) = index.get_file(&remote_path)?
            && !existing.deleted
            && existing.size == size
            && existing.mtime == mtime
        {
            skipped += 1;
            continue;
        }

        let (file_hash, chunk_hashes) = hash_file(entry.path(), config.chunk_size)?;
        let total = chunk_hashes.len();
        for (seq, chunk_hash) in chunk_hashes.iter().enumerate() {
            if index.chunk(chunk_hash)?.is_some() {
                continue; // dedup: chunk already in the channel
            }
            let offset = seq as u64 * config.chunk_size;
            let chunk_len = (size - offset).min(config.chunk_size);
            let mut file = tokio::fs::File::open(entry.path()).await?;
            file.seek(std::io::SeekFrom::Start(offset)).await?;
            let mut stream = file.take(chunk_len);
            let name = format!("{}.bin", &chunk_hash[..16]);
            let caption = format!("{remote_path} [{}/{total}]", seq + 1);
            println!("  ↑ {caption} ({chunk_len} bytes)");
            let msg_id = tg
                .upload_document(peer, &mut stream, chunk_len as usize, name, &caption)
                .await?;
            index.insert_chunk(&ChunkEntry {
                hash: chunk_hash.clone(),
                size: chunk_len,
                msg_id,
            })?;
            uploaded_bytes += chunk_len;
        }
        index.upsert_file(&FileEntry {
            path: remote_path.clone(),
            size,
            mtime,
            hash: file_hash,
            deleted: false,
            chunks: chunk_hashes,
        })?;
        uploaded_files += 1;
        println!("✓ {remote_path}");
    }

    let prefix = format!("{root_name}/");
    let tombstoned = index.tombstone_missing(&prefix, &alive)?;

    println!(
        "sync done: {uploaded_files} uploaded ({uploaded_bytes} bytes), \
         {skipped} unchanged, {tombstoned} tombstoned"
    );

    snapshot(tg, index, config).await?;
    Ok(())
}

/// Serialize the index, compress it, upload it to the channel and pin it.
pub async fn snapshot(tg: &Tg, index: &Index, config: &Config) -> Result<()> {
    let peer = tg.peer(config)?;
    let dump = index.export()?;
    let json = serde_json::to_vec(&dump)?;
    let compressed = zstd::encode_all(json.as_slice(), 9)?;
    let created_at = now_unix();
    let name = format!("tgfs-index-{created_at}.json.zst");
    let mut cursor = std::io::Cursor::new(&compressed);
    let msg_id = tg
        .upload_document(
            peer,
            &mut cursor,
            compressed.len(),
            name,
            &format!("tgfs-index snapshot ({} files)", dump.files.len()),
        )
        .await?;
    tg.pin(peer, msg_id).await?;
    index.record_snapshot(created_at, msg_id)?;
    println!("index snapshot pinned (message {msg_id}, {} bytes)", compressed.len());
    Ok(())
}

/// Restore `remote` (an exact file path, or a prefix for a folder) under
/// `dest`.
pub async fn get(
    tg: &Tg,
    index: &Index,
    config: &Config,
    remote: &str,
    dest: Option<PathBuf>,
) -> Result<()> {
    let dest = dest.unwrap_or_else(|| PathBuf::from("."));
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
    let peer = tg.peer(config)?;

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
            let n = tg.download_document(peer, chunk.msg_id, &mut writer).await?;
            if n != chunk.size {
                bail!(
                    "chunk {chunk_hash}: downloaded {n} bytes, expected {}",
                    chunk.size
                );
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
}
