# Chunk compression — design

Status: **proposed**. Extends [SPEC.md](SPEC.md) §2 and
[0001-packing.md](0001-packing.md).

tgfs is unreleased, so this design carries **no migration or backward
compatibility**: the schema and snapshot shape below are simply what the
code will do. An index created before this change is recreated by
deleting `.tgfs/index.db` and running `tgfs pull`.

## 1. Problem

tgfs uploads chunk bytes verbatim (sealed, but not compressed). Backup
workloads are full of compressible data — source trees, logs, mail,
SQL dumps, JSON — where zstd routinely shaves 50–80%. Today that cost
is paid three times: upload time, download time, and channel storage.
Meanwhile the *index snapshots* are already zstd-compressed; the data
path should get the same treatment.

The constraints that make this more than "pipe through zstd":

1. **Stored length becomes unpredictable.** Today the byte length of a
   stored chunk is derivable from its plaintext `size` (`size`, or
   `Crypto::sealed_len(size)` when encrypted). Packing (0001) leans on
   this to compute member offsets and ranged-download lengths without
   any extra bookkeeping. A compressed length is only known after
   compressing, so the index must start recording it.
2. **Uploads need random access.** `upload_source` reads parts in
   parallel through `PartSource::read_at(offset, buf)`. A compression
   *stream* cannot serve `read_at` at arbitrary offsets without
   compressing from the start each time.
3. **Determinism and nonces.** The encryption scheme derives nonces
   from `(chunk plaintext hash, segment number)` and is safe because
   an identical context always seals identical plaintext. zstd output
   is deterministic for a fixed (input, level, library version) but is
   **not guaranteed stable across zstd versions**. If the same chunk
   were recompressed differently after an upgrade and re-uploaded
   (e.g. after a crash orphaned the first message), the same nonce
   would seal *different* plaintext — a real XChaCha20-Poly1305 nonce
   reuse, leaking the XOR of the two compressed streams. The nonce
   context must therefore be bound to the bytes actually sealed, not
   to the pre-compression hash.

## 2. Design overview

Compression is a per-chunk transformation inserted between chunking
and encryption:

> plaintext chunk → **zstd** → (optional) seal → upload

What deliberately does **not** change:

- Chunks stay content-addressed by the BLAKE3 of their **uncompressed
  plaintext**. Dedup, the file→chunk mapping, whole-file hashes, and
  the diff fast path are untouched. Compression, like packing, is a
  storage-representation detail described entirely by the index.
- Packing composes cleanly: a pack concatenates the *stored* form of
  its members, and member offsets/lengths are computed after
  compression. Compressed members make packs denser (more files per
  message), which compounds the flood-wait win from 0001.
- Encryption stays segment-wise over the stored stream, same cipher,
  same derivations — only the nonce context changes for compressed
  chunks (§5.3).

The decision is per chunk and recorded per chunk: a chunk is stored
compressed only when compression actually pays (§5.2). Incompressible
chunks (media, archives, already-encrypted files) are stored verbatim,
exactly as today. Mixing is free — every `chunks` row says what its
bytes are.

## 3. Repo config

`.tgfs/config.toml` grows an optional `[compression]` table.
Compression is on by default; `level = 0` disables it.

```toml
[compression]
level = 3    # zstd level, 0 disables; valid range 0..=22
```

```rust
#[derive(Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct CompressionConfig {
    /// zstd compression level. 0 disables compression.
    pub level: i32,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self { level: 3 }
    }
}

// In RepoConfig:
#[serde(default)]
pub compression: CompressionConfig,
```

Validation at load time: `0 <= level <= 22`.

The level is a *push-time* knob. Changing it never invalidates
anything: chunks are dedup-keyed by plaintext hash, so an
already-stored chunk is never re-stored, and `get` reads the per-chunk
record, not the config. Different pushes (or different machines) may
freely use different levels against the same repo.

Defaults rationale: zstd level 3 compresses at roughly 500 MB/s per
core — far faster than any realistic uplink, so trying it on every
chunk costs effectively nothing; levels beyond ~7 buy little ratio for
sharply more CPU. No extension-based skip list: the keep-if-smaller
rule (§5.2) already neutralizes incompressible inputs, and zstd gives
up on such data quickly.

## 4. Index & snapshot changes

`chunks` gains three columns, added directly to the `CREATE TABLE`
(no migration):

```sql
CREATE TABLE chunks (
    hash        TEXT PRIMARY KEY,
    size        INTEGER NOT NULL,           -- uncompressed plaintext size
    msg_id      INTEGER NOT NULL,
    offset      INTEGER NOT NULL DEFAULT 0,
    encrypted   INTEGER NOT NULL DEFAULT 0,
    comp        INTEGER NOT NULL DEFAULT 0, -- NEW: 0 = none, 1 = zstd
    comp_size   INTEGER NOT NULL DEFAULT 0, -- NEW: compressed length (comp != 0)
    stored_hash TEXT NOT NULL DEFAULT ''    -- NEW: BLAKE3 of compressed bytes (comp != 0)
);
```

`ChunkEntry` gains the matching `#[serde(default)]` fields. Two
derived quantities replace today's ad-hoc math, as methods on
`ChunkEntry`:

- **pre-seal length** — `comp_size` if `comp != 0`, else `size`;
- **stored length** (bytes occupying the document) —
  `Crypto::sealed_len(pre_seal_len)` if `encrypted`, else the pre-seal
  length. Every current call site of the `size`/`sealed_len(size)`
  pattern (`get`'s expected length, pack offset accounting) switches
  to these methods.

`stored_hash` is the hex BLAKE3 of the compressed byte stream and
exists for one reason: it is the **nonce context** for sealing
compressed chunks (§5.3). For `comp = 0` rows it stays empty and the
context remains the plaintext `hash`, which keeps uncompressed chunks
bit-identical to what tgfs produces today.

`comp` is an enum, not a bool, so a future algorithm bump is a new
value, not a schema change.

The snapshot `format` stays `1`, matching 0001's stance: the new
fields are simply part of what format 1 means from now on, and absent
fields deserialize to "uncompressed" via serde defaults.

`upload_journal` gains one column:

```sql
ALTER: upload_journal (
    chunk_hash  TEXT PRIMARY KEY,
    file_id     INTEGER NOT NULL,
    total_parts INTEGER NOT NULL,
    done_parts  INTEGER NOT NULL DEFAULT 0,
    stored_hash TEXT NOT NULL DEFAULT ''   -- NEW: hash of the exact bytes being uploaded
);
```

`stored_hash` here records what the in-flight upload's bytes hash to
(for a pack: the BLAKE3 of the concatenated member stored streams),
so resume can prove it is continuing the *same bytes* (§7) instead of
inferring it from part counts.

## 5. Push pipeline

### 5.1 Spill files

Compression happens once per uploaded chunk, streamed into a **spill
file** `.tgfs/tmp/<chunk-hash>.zst`, and uploads read from the spill:

1. Hash pass (existing `hash_file`) → chunk hashes; dedup check
   against the index exactly as today. Chunks already stored are never
   compressed at all.
2. For each chunk to upload: stream the plaintext range through a zstd
   encoder into the spill file, hashing the compressed output on the
   way (this produces `comp_size` and `stored_hash` in the same pass).
3. Decide keep/discard (§5.2). Discard ⇒ delete the spill and take
   today's path verbatim against the original file.
4. Keep ⇒ upload from the spill via the existing sources:
   `PlainChunkSource` over the spill file, or `EncryptedChunkSource`
   over the spill file with `plain_len = comp_size` and
   `context = stored_hash`. No new `PartSource` impl is needed — the
   spill file *is* the random-access answer to constraint (2): pread
   serves parallel part reads, and re-sealing segments on demand works
   unchanged because the sealed input is now an ordinary file.
5. On success, insert the `chunks` row (`comp = 1`, `comp_size`,
   `stored_hash`) and delete the spill.

Small files headed for a pack follow the same steps at queue time: the
`PackMember` records the spill path and stored length, `PendingPack`'s
size accounting and member offsets use the compressed stored length,
and `PackSource` delegates member reads to the spill-backed sources
above. Members that don't compress keep pointing at the original file,
as today — a pack may freely mix compressed and verbatim members.

Disk cost is bounded and transient: one spill at a time for the
standalone path (≤ one chunk's compressed size, itself ≤ `chunk_size`),
and at most `pack.target_size` of spills for the pending pack. Spills
are deleted as soon as their chunk row lands; push startup sweeps
`.tgfs/tmp` of any file not referenced by a live `upload_journal`
entry.

Why not compress on the fly inside `read_at`? Because serving a read
at stored offset `o` would require compressing the chunk from byte 0
every time, turning the 4-way parallel part upload into an O(n²) CPU
burn. The spill trades bounded temp disk for doing the work exactly
once, and doubles as the determinism anchor for resume (§7).

### 5.2 Keep-if-smaller rule

A chunk is stored compressed iff

```
comp_size * 100 <= size * 98        // saves at least 2%
```

otherwise the spill is discarded and the chunk is stored verbatim
(`comp = 0`), byte-identical to today's output. The 2% guard avoids
churning already-compressed media into marginally-smaller blobs that
cost a decompression pass on every restore for no real gain. The rule
is intentionally a constant, not config: there is no workload where
tuning it matters more than picking `level`.

### 5.3 Nonce context for compressed chunks

Sealing a compressed chunk uses `context = stored_hash` (the hash of
the compressed bytes) instead of the plaintext hash. This restores the
invariant the scheme's safety rests on — *a given (key, context,
segment) triple only ever seals one plaintext* — without trusting
zstd to be deterministic across library versions:

- Same compressed bytes ⇒ same context ⇒ identical ciphertext.
  Dedup of the sealed representation and part-level resume keep
  working exactly as before.
- Different compressed bytes for the same chunk (a recompression
  after a zstd upgrade, re-uploaded because a crash orphaned the
  first message) ⇒ different `stored_hash` ⇒ different nonces. The
  nonce-reuse hazard from constraint (3) is structurally impossible,
  not merely unlikely.

`get` knows the context because the index stores it. Uncompressed
chunks keep `context = hash`, so encrypted repos created before this
change need no re-upload (incidental, per the no-compat stance — but
it also means one code path: "context = stored_hash if set, else
hash").

## 6. Download path (`get`)

One insertion in the existing pipeline. Per chunk, `get` currently
does: ranged download → (`DecryptingWriter` if encrypted) →
`HashingWriter`. It becomes:

```
download_range(expected = chunk.stored_len())
  → DecryptingWriter(context = stored_hash or hash, plain_len = pre_seal_len)   // if encrypted
  → ZstdWriter (zstd::stream::write::Decoder)                                    // if comp != 0
  → HashingWriter → file
```

`zstd::stream::write::Decoder` is a `std::io::Write` adapter, so it
slots into the writer chain the same way `DecryptingWriter` does and
buffers only its internal window. Verification gains one check and
keeps the rest: downloaded byte count must equal the stored length,
the decompressed byte count must equal `chunk.size` (a counting
shim on the writer chain), and the whole-file BLAKE3 check remains the
end-to-end integrity authority. For encrypted chunks, AEAD tags have
already authenticated the compressed stream before the decoder ever
sees it, so a malicious channel cannot feed the decompressor attacker
chosen bytes; for plaintext repos the zstd frame checksum plus the
file hash cover corruption.

Ranged reads over packs are unaffected: offsets and lengths in the
index are already expressed in stored space, and members were packed
in stored space.

## 7. Resume & failure model

- **Interrupted upload, spill present**: the journal entry's
  `total_parts` *and* `stored_hash` must match the current candidate
  bytes. On resume, tgfs re-hashes the spill file(s) — local BLAKE3 at
  memory bandwidth, negligible — and resumes from `done_parts` only on
  an exact match. The spill file pins the exact bytes across the
  crash, so this is airtight even if the zstd library changed in
  between.
- **Interrupted upload, spill missing** (user cleaned `.tgfs/tmp`,
  different machine): recompression might produce different bytes, so
  the journal entry is invalid by construction — the recomputed
  `stored_hash` won't match — and the upload simply restarts. As with
  packing, correctness never depends on resume; resume is purely an
  optimization.
- **Crash after upload, before index insert**: the message is orphaned
  in the channel and the next push re-uploads — same exposure as
  today. If the re-upload compresses differently, §5.3 guarantees the
  orphan and the replacement never share a nonce.
- **Pack resume**: unchanged shape — journal keyed by the pack hash
  (still BLAKE3 of member *plaintext* hashes, so the pack's identity
  is content-defined), with the new `stored_hash` column validating
  that the rebuilt pack's bytes are the same bytes.

## 8. What compression does and does not hide

The existing honesty list ("sizes, chunk counts and chunk equality
remain visible") gains one entry: **stored sizes now reveal
compressibility**. An observer of an encrypted channel can see that a
chunk shrank from its (invisible) plaintext size only via the index —
which is sealed — but can see absolute stored sizes and may infer
data types from ratios across a repo's chunks (e.g. "these documents
are text-like"). This is inherent to compress-then-encrypt and shared
by every backup tool that does both (borg, restic); the alternative —
encrypt-then-compress — is useless, since ciphertext doesn't
compress. Users for whom this is a concern set `level = 0`.

## 9. Limits & future work

- **Intra-chunk random access is lost for compressed chunks.** The
  optional FUSE mount (SPEC §6) maps reads onto ranged downloads;
  a compressed chunk can only be decoded from its start. Acceptable
  for now (the mount is M5, read paths there can decompress-and-cache
  whole chunks), and fixable later with the zstd *seekable format*
  (independent frames + a seek table) as a new `comp` value — the
  enum column exists precisely so this is an addition, not a redesign.
- **Repacking** (0001 §8) composes: a future `tgfs prune` that
  rewrites packs would recompress members with the then-current level.
- **Dictionary compression** for packs of many tiny similar files
  (zstd `--train`-style shared dictionaries per pack) could improve
  small-file ratios substantially, at the cost of storing the
  dictionary in the pack; out of scope until measured.

## 10. Milestone

- **M7 — compression**: `[compression]` config + validation, the
  `comp`/`comp_size`/`stored_hash` columns and `ChunkEntry` length
  methods, spill-file compression in push (standalone + pack paths),
  `stored_hash` nonce context, the `write::Decoder` stage plus
  decompressed-length check in get, and journal `stored_hash`
  validation for resume.
