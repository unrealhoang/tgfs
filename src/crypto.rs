//! Client-side encryption (M4). Telegram cloud chats are not E2E-encrypted,
//! so chunks and index snapshots can be sealed with a per-repo key before
//! upload.
//!
//! Scheme: XChaCha20-Poly1305 over independent 1 MiB plaintext segments.
//! Nonces are derived deterministically with keyed BLAKE3 from the chunk's
//! plaintext hash and the segment number, so:
//! - re-encrypting the same chunk yields identical ciphertext (upload resume
//!   and dedup keep working),
//! - segments are independently random-access decryptable,
//! - a nonce only repeats for an identical (key, chunk, segment) triple,
//!   which produces an identical ciphertext — no keystream reuse leak.
//!
//! What this protects: chunk and snapshot *contents* against anyone who can
//! read the channel. What it does not hide: chunk sizes, counts, and
//! equality (inherent to content-addressed dedup). Integrity of each file is
//! additionally verified by its whole-file BLAKE3 on restore.

use anyhow::{Context, Result, bail};
use chacha20poly1305::aead::Aead;
use chacha20poly1305::{KeyInit, XChaCha20Poly1305, XNonce};

/// Plaintext bytes per sealed segment.
pub const SEGMENT_SIZE: u64 = 1024 * 1024;
/// Poly1305 tag appended to every segment.
pub const TAG_SIZE: u64 = 16;
/// Magic prefix for self-describing encrypted blobs (index snapshots).
pub const SNAPSHOT_MAGIC: &[u8; 8] = b"tgfsenc1";

pub struct Crypto {
    cipher: XChaCha20Poly1305,
    nonce_key: [u8; 32],
}

impl Crypto {
    /// Build from the repo's 32-byte master key. Cipher and nonce keys are
    /// domain-separated derivations, so the master key is never used raw.
    pub fn new(master_key: &[u8; 32]) -> Self {
        let enc_key = blake3::derive_key("tgfs 2026 encryption key v1", master_key);
        let nonce_key = blake3::derive_key("tgfs 2026 nonce key v1", master_key);
        Self {
            cipher: XChaCha20Poly1305::new((&enc_key).into()),
            nonce_key,
        }
    }

    /// Ciphertext length for a `plain_len`-byte chunk.
    pub fn sealed_len(plain_len: u64) -> u64 {
        plain_len + TAG_SIZE * plain_len.div_ceil(SEGMENT_SIZE)
    }

    /// Sealed length of one segment holding `plain_len` plaintext bytes.
    pub fn sealed_segment_len(plain_len: u64) -> u64 {
        plain_len + TAG_SIZE
    }

    fn nonce(&self, context: &[u8], segment: u64) -> XNonce {
        let mut hasher = blake3::Hasher::new_keyed(&self.nonce_key);
        hasher.update(context);
        hasher.update(&segment.to_le_bytes());
        let digest = hasher.finalize();
        let mut nonce = [0u8; 24];
        nonce.copy_from_slice(&digest.as_bytes()[..24]);
        nonce.into()
    }

    /// Seal one segment. `context` is the chunk's plaintext hash (or another
    /// unique-per-blob string); `segment` is its index within the blob.
    pub fn seal_segment(&self, context: &[u8], segment: u64, plaintext: &[u8]) -> Result<Vec<u8>> {
        self.cipher
            .encrypt(&self.nonce(context, segment), plaintext)
            .map_err(|_| anyhow::anyhow!("encryption failed"))
    }

    /// Open one sealed segment.
    pub fn open_segment(&self, context: &[u8], segment: u64, sealed: &[u8]) -> Result<Vec<u8>> {
        self.cipher
            .decrypt(&self.nonce(context, segment), sealed)
            .map_err(|_| {
                anyhow::anyhow!(
                    "decryption failed (wrong key, or the data was corrupted/tampered with)"
                )
            })
    }

    /// Seal a whole in-memory blob into a self-describing container
    /// (used for index snapshots): magic || context || sealed segments.
    pub fn seal_blob(&self, context: &[u8; 32], plain: &[u8]) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(
            SNAPSHOT_MAGIC.len() + context.len() + Self::sealed_len(plain.len() as u64) as usize,
        );
        out.extend_from_slice(SNAPSHOT_MAGIC);
        out.extend_from_slice(context);
        for (i, segment) in plain.chunks(SEGMENT_SIZE as usize).enumerate() {
            out.extend_from_slice(&self.seal_segment(context, i as u64, segment)?);
        }
        Ok(out)
    }

    /// Detect whether a downloaded blob is a sealed container.
    pub fn is_sealed_blob(raw: &[u8]) -> bool {
        raw.starts_with(SNAPSHOT_MAGIC)
    }

    /// Open a container produced by [`Self::seal_blob`].
    pub fn open_blob(&self, raw: &[u8]) -> Result<Vec<u8>> {
        let body = raw
            .strip_prefix(SNAPSHOT_MAGIC.as_slice())
            .context("not an encrypted tgfs blob")?;
        if body.len() < 32 {
            bail!("encrypted blob is truncated");
        }
        let (context, mut sealed) = body.split_at(32);
        let mut out = Vec::with_capacity(sealed.len());
        let sealed_seg = Self::sealed_segment_len(SEGMENT_SIZE) as usize;
        let mut segment = 0u64;
        while !sealed.is_empty() {
            let take = sealed.len().min(sealed_seg);
            out.extend_from_slice(&self.open_segment(context, segment, &sealed[..take])?);
            sealed = &sealed[take..];
            segment += 1;
        }
        Ok(out)
    }
}

/// Wraps a writer, decrypting a stream of sealed segments as produced by
/// chunk encryption. Buffers at most one segment.
pub struct DecryptingWriter<'a, W: std::io::Write> {
    inner: &'a mut W,
    crypto: &'a Crypto,
    /// Chunk plaintext hash (nonce context).
    context: Vec<u8>,
    /// Total ciphertext length expected, to size the final segment.
    sealed_len: u64,
    received: u64,
    segment: u64,
    buf: Vec<u8>,
}

impl<'a, W: std::io::Write> DecryptingWriter<'a, W> {
    pub fn new(inner: &'a mut W, crypto: &'a Crypto, context: &[u8], plain_len: u64) -> Self {
        Self {
            inner,
            crypto,
            context: context.to_vec(),
            sealed_len: Crypto::sealed_len(plain_len),
            received: 0,
            segment: 0,
            buf: Vec::new(),
        }
    }

    fn flush_segment(&mut self) -> std::io::Result<()> {
        let plain = self
            .crypto
            .open_segment(&self.context, self.segment, &self.buf)
            .map_err(std::io::Error::other)?;
        self.inner.write_all(&plain)?;
        self.segment += 1;
        self.buf.clear();
        Ok(())
    }
}

impl<W: std::io::Write> std::io::Write for DecryptingWriter<'_, W> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        let sealed_seg = Crypto::sealed_segment_len(SEGMENT_SIZE) as usize;
        let mut rest = data;
        while !rest.is_empty() {
            let want = sealed_seg - self.buf.len();
            let take = rest.len().min(want);
            self.buf.extend_from_slice(&rest[..take]);
            self.received += take as u64;
            rest = &rest[take..];
            // A segment is complete when full, or when it is the last one.
            if self.buf.len() == sealed_seg || self.received == self.sealed_len {
                self.flush_segment()?;
            }
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn crypto() -> Crypto {
        Crypto::new(&[7u8; 32])
    }

    #[test]
    fn segment_roundtrip_and_determinism() {
        let c = crypto();
        let sealed = c.seal_segment(b"ctx", 0, b"hello").unwrap();
        assert_eq!(sealed.len(), 5 + TAG_SIZE as usize);
        assert_eq!(c.open_segment(b"ctx", 0, &sealed).unwrap(), b"hello");
        // Deterministic: same inputs → same ciphertext (resume safety).
        assert_eq!(sealed, c.seal_segment(b"ctx", 0, b"hello").unwrap());
        // Different segment index or context → different ciphertext.
        assert_ne!(sealed, c.seal_segment(b"ctx", 1, b"hello").unwrap());
        assert_ne!(sealed, c.seal_segment(b"other", 0, b"hello").unwrap());
        // Tampering is detected.
        let mut bad = sealed.clone();
        bad[0] ^= 1;
        assert!(c.open_segment(b"ctx", 0, &bad).is_err());
        // Wrong key fails.
        assert!(
            Crypto::new(&[8u8; 32])
                .open_segment(b"ctx", 0, &sealed)
                .is_err()
        );
    }

    #[test]
    fn sealed_len_math() {
        assert_eq!(Crypto::sealed_len(0), 0);
        assert_eq!(Crypto::sealed_len(1), 1 + 16);
        assert_eq!(Crypto::sealed_len(SEGMENT_SIZE), SEGMENT_SIZE + 16);
        assert_eq!(Crypto::sealed_len(SEGMENT_SIZE + 1), SEGMENT_SIZE + 1 + 32);
    }

    #[test]
    fn blob_roundtrip() {
        let c = crypto();
        let plain: Vec<u8> = (0..(2 * SEGMENT_SIZE + 100) as usize)
            .map(|i| (i % 251) as u8)
            .collect();
        let sealed = c.seal_blob(&[3u8; 32], &plain).unwrap();
        assert!(Crypto::is_sealed_blob(&sealed));
        assert!(!Crypto::is_sealed_blob(&plain));
        assert_eq!(c.open_blob(&sealed).unwrap(), plain);
    }

    #[test]
    fn decrypting_writer_reassembles_arbitrary_chunking() {
        let c = crypto();
        let plain: Vec<u8> = (0..(SEGMENT_SIZE + 12345) as usize)
            .map(|i| (i % 251) as u8)
            .collect();
        let mut sealed = Vec::new();
        for (i, seg) in plain.chunks(SEGMENT_SIZE as usize).enumerate() {
            sealed.extend_from_slice(&c.seal_segment(b"ctx", i as u64, seg).unwrap());
        }
        // Feed the sealed stream in awkward write sizes.
        let mut out = Vec::new();
        {
            use std::io::Write as _;
            let mut w = DecryptingWriter::new(&mut out, &c, b"ctx", plain.len() as u64);
            for piece in sealed.chunks(100_000) {
                w.write_all(piece).unwrap();
            }
        }
        assert_eq!(out, plain);
    }
}
