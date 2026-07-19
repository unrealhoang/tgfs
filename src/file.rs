//! File engine: content hashing, chunk upload, and verified restoration.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use grammers_client::session::types::PeerRef;

use crate::crypto::{Crypto, DecryptingWriter};
use crate::diff::LocalFile;
use crate::index::{ChunkEntry, FileEntry, Index};
use crate::telegram_transfer::{PartSource, total_parts};
use crate::tg::Tg;

const HASH_BUF_SIZE: usize = 1024 * 1024;

/// Result of uploading all missing chunks for one local file.
pub(crate) struct UploadedFile {
    pub(crate) entry: FileEntry,
    pub(crate) uploaded_bytes: u64,
}

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
            .context("chunk read failed (file changed during push?)")
    }
}

/// Like [`PlainChunkSource`] but exposing the sealed bytes. Reads cover whole
/// segments, which are re-encrypted on demand; deterministic nonces keep the
/// output stable across retries and resumes.
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
            let plain_take = crate::crypto::SEGMENT_SIZE.min(self.plain_len - plain_off) as usize;
            let mut plain = vec![0u8; plain_take];
            self.file
                .read_exact_at(&mut plain, self.base + plain_off)
                .context("chunk read failed (file changed during push?)")?;
            let sealed = self
                .crypto
                .seal_segment(self.context.as_bytes(), seg, &plain)?;
            let take = (sealed.len() - in_seg).min(buf.len() - written);
            buf[written..written + take].copy_from_slice(&sealed[in_seg..in_seg + take]);
            written += take;
        }
        Ok(())
    }
}

/// Upload the chunks missing from `index` and return the new file entry.
pub(crate) async fn upload(
    tg: &Tg,
    index: &Index,
    peer: PeerRef,
    file: &LocalFile,
    chunk_size: u64,
    crypto: Option<&Arc<Crypto>>,
) -> Result<UploadedFile> {
    let (file_hash, chunk_hashes) = hash_file(&file.abs_path, chunk_size)?;
    let total = chunk_hashes.len();
    let mut uploaded_bytes = 0;

    for (seq, chunk_hash) in chunk_hashes.iter().enumerate() {
        if index.chunk(chunk_hash)?.is_some() {
            continue;
        }
        let offset = seq as u64 * chunk_size;
        let chunk_len = (file.size - offset).min(chunk_size);
        let opened = std::fs::File::open(&file.abs_path)?;
        let source: Arc<dyn PartSource> = match crypto {
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

    Ok(UploadedFile {
        entry: FileEntry {
            path: file.rel_path.clone(),
            size: file.size,
            mtime: file.mtime,
            hash: file_hash,
            deleted: false,
            chunks: chunk_hashes,
        },
        uploaded_bytes,
    })
}

/// Download and verify one indexed file under `dest`.
pub(crate) async fn download(
    tg: &Tg,
    index: &Index,
    peer: PeerRef,
    file: &FileEntry,
    dest: &Path,
    crypto: Option<&Crypto>,
) -> Result<PathBuf> {
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
            let crypto = crypto.context("chunk is encrypted; supply --key or --keyfile")?;
            let mut decryptor =
                DecryptingWriter::new(&mut writer, crypto, chunk_hash.as_bytes(), chunk.size);
            let n = tg
                .download_document(peer, chunk.msg_id, &mut decryptor)
                .await?;
            (n, Crypto::sealed_len(chunk.size))
        } else {
            let n = tg
                .download_document(peer, chunk.msg_id, &mut writer)
                .await?;
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
    Ok(out_path)
}

/// Hash a file, returning the whole-file hash and each `chunk_size` hash.
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
        std::fs::write(&path, b"0123456789").unwrap();
        let (file_hash, chunks) = hash_file(&path, 4).unwrap();
        assert_eq!(chunks.len(), 3);
        assert_eq!(file_hash, blake3::hash(b"0123456789").to_hex().to_string());
        assert_eq!(chunks[0], blake3::hash(b"0123").to_hex().to_string());
        assert_eq!(chunks[2], blake3::hash(b"89").to_hex().to_string());

        std::fs::write(&path, b"01234567").unwrap();
        let (_, chunks) = hash_file(&path, 4).unwrap();
        assert_eq!(chunks.len(), 2);

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
}
