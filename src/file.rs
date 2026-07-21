//! File engine: content hashing, chunk upload, and verified restoration.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use grammers_client::media::Document;
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

/// One distinct chunk queued into a pack. Files are opened only when the
/// uploader asks for bytes, so a pack never holds one descriptor per member.
#[derive(Clone)]
pub(crate) struct PackMember {
    pub(crate) hash: String,
    pub(crate) abs_path: PathBuf,
    pub(crate) size: u64,
    pub(crate) offset: u64,
    pub(crate) stored_len: u64,
}

impl PackMember {
    pub(crate) fn new(
        hash: String,
        abs_path: PathBuf,
        size: u64,
        offset: u64,
        encrypted: bool,
    ) -> Self {
        Self {
            hash,
            abs_path,
            size,
            offset,
            stored_len: if encrypted {
                Crypto::sealed_len(size)
            } else {
                size
            },
        }
    }
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

/// Concatenated stored representation of independently addressable chunks.
/// Reads may span member boundaries and are safe to issue concurrently.
struct PackSource {
    members: Vec<PackMember>,
    len: u64,
    crypto: Option<Arc<Crypto>>,
}

impl PackSource {
    fn new(members: Vec<PackMember>, crypto: Option<Arc<Crypto>>) -> Result<Self> {
        if members.first().is_some_and(|member| member.offset != 0) {
            bail!("first pack member does not start at offset zero");
        }
        let len = match members.last() {
            Some(member) => member
                .offset
                .checked_add(member.stored_len)
                .context("pack length overflow")?,
            None => 0,
        };
        for pair in members.windows(2) {
            if pair[1].offset != pair[0].offset + pair[0].stored_len {
                bail!("pack members are not contiguous");
            }
        }
        Ok(Self {
            members,
            len,
            crypto,
        })
    }
}

impl PartSource for PackSource {
    fn len(&self) -> u64 {
        self.len
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let end = offset
            .checked_add(buf.len() as u64)
            .context("pack read offset overflow")?;
        if end > self.len {
            bail!("pack read beyond end: {offset}..{end} of {}", self.len);
        }

        let mut written = 0usize;
        while written < buf.len() {
            let pos = offset + written as u64;
            let member_index = self
                .members
                .partition_point(|member| member.offset <= pos)
                .saturating_sub(1);
            let member = &self.members[member_index];
            let member_offset = pos - member.offset;
            let take =
                (member.stored_len - member_offset).min((buf.len() - written) as u64) as usize;
            let opened = std::fs::File::open(&member.abs_path).with_context(|| {
                format!("cannot open pack member {}", member.abs_path.display())
            })?;
            match &self.crypto {
                Some(crypto) => EncryptedChunkSource {
                    file: opened,
                    base: 0,
                    plain_len: member.size,
                    crypto: Arc::clone(crypto),
                    context: member.hash.clone(),
                }
                .read_at(member_offset, &mut buf[written..written + take])?,
                None => PlainChunkSource {
                    file: opened,
                    base: 0,
                    len: member.size,
                }
                .read_at(member_offset, &mut buf[written..written + take])?,
            }
            written += take;
        }
        Ok(())
    }
}

/// Hash a local file and build the index entry that will reference its chunk.
pub(crate) fn prepare(file: &LocalFile, chunk_size: u64) -> Result<FileEntry> {
    let (hash, chunks) = hash_file(&file.abs_path, chunk_size)?;
    Ok(FileEntry {
        path: file.rel_path.clone(),
        size: file.size,
        mtime: file.mtime,
        hash,
        deleted: false,
        chunks,
    })
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
            offset: 0,
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

/// Upload one completed pack, atomically publish all of its member locations,
/// and return the number of plaintext bytes newly stored.
pub(crate) async fn upload_pack(
    tg: &Tg,
    index: &Index,
    peer: PeerRef,
    members: Vec<PackMember>,
    crypto: Option<&Arc<Crypto>>,
) -> Result<u64> {
    if members.is_empty() {
        bail!("refusing to upload an empty pack");
    }
    let pack_hash = pack_hash(&members);
    let pack_source = PackSource::new(members.clone(), crypto.cloned())?;
    let stored_len = pack_source.len();
    let parts = total_parts(stored_len);
    let resume = index
        .journal_get(&pack_hash)?
        .filter(|&(_, journal_total, _)| journal_total == parts)
        .map(|(file_id, _, done)| (file_id, done));
    let name = format!("pack-{}.bin", &pack_hash[..16]);
    let caption = format!("pack files={} bytes={stored_len}", members.len());
    println!("  ↑ {caption}");
    let source: Arc<dyn PartSource> = Arc::new(pack_source);
    let msg_id = tg
        .upload_source(peer, source, name, &caption, resume, |file_id, done| {
            if done == 0 {
                index.journal_start(&pack_hash, file_id, parts)
            } else {
                index.journal_progress(&pack_hash, done)
            }
        })
        .await?;
    let chunks = members
        .iter()
        .map(|member| ChunkEntry {
            hash: member.hash.clone(),
            size: member.size,
            msg_id,
            offset: member.offset,
            encrypted: crypto.is_some(),
        })
        .collect::<Vec<_>>();
    index.insert_chunks(&chunks)?;
    index.journal_clear(&pack_hash)?;
    Ok(members.iter().map(|member| member.size).sum())
}

fn pack_hash(members: &[PackMember]) -> String {
    let mut hasher = blake3::Hasher::new();
    for member in members {
        hasher.update(member.hash.as_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

/// Download and verify one indexed file under `dest`.
pub(crate) async fn download(
    tg: &Tg,
    index: &Index,
    peer: PeerRef,
    file: &FileEntry,
    dest: &Path,
    crypto: Option<&Crypto>,
    documents: &mut std::collections::HashMap<i32, Document>,
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
        let document = if let Some(document) = documents.get(&chunk.msg_id) {
            document.clone()
        } else {
            let document = tg.document(peer, chunk.msg_id).await?;
            documents.insert(chunk.msg_id, document.clone());
            document
        };
        let expected = if chunk.encrypted {
            Crypto::sealed_len(chunk.size)
        } else {
            chunk.size
        };
        let mut writer = HashingWriter {
            inner: &mut out,
            hasher: &mut hasher,
        };
        let n = if chunk.encrypted {
            let crypto = crypto.context("chunk is encrypted; supply --key or --keyfile")?;
            let mut decryptor =
                DecryptingWriter::new(&mut writer, crypto, chunk_hash.as_bytes(), chunk.size);
            tg.download_range(&document, chunk.offset, expected, &mut decryptor)
                .await?
        } else {
            tg.download_range(&document, chunk.offset, expected, &mut writer)
                .await?
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

pub(crate) fn human_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
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

    fn temp_file(label: &str, contents: &[u8]) -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let path = std::env::temp_dir().join(format!(
            "tgfs-{label}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::write(&path, contents).unwrap();
        path
    }

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
        let base = 1000u64;
        let plain_len = 2 * SEGMENT_SIZE + 4321;
        let mut content = vec![0u8; (base + plain_len) as usize];
        for (i, b) in content.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        let path = temp_file("enc-src", &content);

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

    #[test]
    fn plain_pack_source_reads_across_member_boundaries() {
        let first = temp_file("pack-a", b"abc");
        let second = temp_file("pack-b", b"defgh");
        let members = vec![
            PackMember::new("aa".into(), first.clone(), 3, 0, false),
            PackMember::new("bb".into(), second.clone(), 5, 3, false),
        ];
        let source = PackSource::new(members.clone(), None).unwrap();
        assert_eq!(source.len(), 8);

        let mut all = vec![0; 8];
        source.read_at(0, &mut all).unwrap();
        assert_eq!(all, b"abcdefgh");
        let mut crossing = vec![0; 4];
        source.read_at(2, &mut crossing).unwrap();
        assert_eq!(crossing, b"cdef");
        assert!(source.read_at(7, &mut [0; 2]).is_err());
        assert_ne!(
            pack_hash(&members),
            pack_hash(&[members[1].clone(), members[0].clone()])
        );

        let _ = std::fs::remove_file(first);
        let _ = std::fs::remove_file(second);
    }

    #[test]
    fn encrypted_pack_members_match_standalone_ciphertext() {
        let first_bytes = vec![7u8; crate::crypto::SEGMENT_SIZE as usize + 13];
        let second_bytes = vec![9u8; 12345];
        let first = temp_file("pack-enc-a", &first_bytes);
        let second = temp_file("pack-enc-b", &second_bytes);
        let crypto = Arc::new(Crypto::new(&[4u8; 32]));
        let first_len = Crypto::sealed_len(first_bytes.len() as u64);
        let members = vec![
            PackMember::new(
                "aa11".into(),
                first.clone(),
                first_bytes.len() as u64,
                0,
                true,
            ),
            PackMember::new(
                "bb22".into(),
                second.clone(),
                second_bytes.len() as u64,
                first_len,
                true,
            ),
        ];
        let pack = PackSource::new(members.clone(), Some(Arc::clone(&crypto))).unwrap();
        let mut packed = vec![0; pack.len() as usize];
        pack.read_at(0, &mut packed).unwrap();

        let mut expected = Vec::new();
        for member in &members {
            let source = EncryptedChunkSource {
                file: std::fs::File::open(&member.abs_path).unwrap(),
                base: 0,
                plain_len: member.size,
                crypto: Arc::clone(&crypto),
                context: member.hash.clone(),
            };
            let start = expected.len();
            expected.resize(start + source.len() as usize, 0);
            source.read_at(0, &mut expected[start..]).unwrap();
        }
        assert_eq!(packed, expected);

        let _ = std::fs::remove_file(first);
        let _ = std::fs::remove_file(second);
    }

    #[test]
    fn human_size_formats() {
        assert_eq!(human_size(0), "0 B");
        assert_eq!(human_size(512), "512 B");
        assert_eq!(human_size(1024), "1.0 KiB");
        assert_eq!(human_size(1536), "1.5 KiB");
        assert_eq!(human_size(1048576), "1.0 MiB");
        assert_eq!(human_size(1073741824), "1.0 GiB");
        assert_eq!(human_size(1099511627776), "1.0 TiB");
    }
}
