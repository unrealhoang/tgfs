# Packing small files — design

Status: **design** (not yet implemented). Extends [SPEC.md](SPEC.md) §2.

## 1. Problem

tgfs currently uploads **one Telegram message per chunk**, and every file
contributes at least one chunk. For big files this is exactly right — the
chunk size (default 1 GiB) keeps message count low. For repos made of many
small files (photo libraries, node_modules-shaped trees, maildirs) it is
pathological:

- Telegram rate-limits message sends per channel (`FLOOD_WAIT`, on the
  order of ~20 messages/minute sustained). 10 000 small files ≈ 10 000
  messages ≈ **8+ hours** of flood-wait sleeping for what may be a few
  hundred MB of data.
- Each message has fixed protocol overhead (`send_uploaded` round trip),
  so throughput collapses even before flood waits kick in.
- The channel history becomes noise: thousands of 4 KiB documents.

The fix is the same one git uses: **pack many small objects into one
stored blob** and remember each object's offset.

## 2. Design overview

Packing happens at the **message** level, not the content level:

> A *pack* is a single uploaded Telegram document that carries the
> concatenated bytes of many chunks. Each member chunk keeps its own
> content hash; the index maps every chunk to `(msg_id, offset)` instead
> of just `msg_id`.

What deliberately does **not** change:

- Chunks stay content-addressed by the BLAKE3 of their **plaintext**
  bytes. A small file is still exactly one chunk with its own hash.
- Dedup stays chunk-level: a duplicate small file hits the existing
  `chunks` row and uploads nothing, regardless of which pack (or
  standalone message) the original lives in.
- Encryption stays per-chunk: each member is sealed independently with
  the existing segment scheme (nonce context = the chunk's plaintext
  hash), and the *sealed* members are concatenated. Ciphertext remains
  deterministic, so dedup and resume keep working.
- Files at or above the threshold take today's path unchanged: their
  chunks are uploaded as standalone documents at `offset = 0`.

A pack is therefore invisible to the content model — it is purely a
storage-location optimization, described entirely by the index.

## 3. Repo config

`.tgfs/config.toml` grows an optional `[pack]` table:

```toml
channel_id = ...
channel_access_hash = ...
chunk_size = 1073741824

[pack]
enabled = true            # default: false for pre-existing repos (see §8)
threshold = 8388608       # 8 MiB — files smaller than this are packed
target_size = 268435456   # 256 MiB — flush a pack when it reaches this
```

```rust
#[derive(Debug, Serialize, Deserialize)]
pub struct PackConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_pack_threshold")]   // 8 MiB
    pub threshold: u64,
    #[serde(default = "default_pack_target_size")] // 256 MiB
    pub target_size: u64,
}

// In RepoConfig:
#[serde(default)]
pub pack: PackConfig,
```

Validation at load/init time:

- `threshold <= chunk_size` (a packed file is by definition
  single-chunk; files ≥ `threshold` go through normal chunking).
- `threshold <= target_size <= 2 GiB` (the per-document cap; sealed
  size overhead must also fit, so effectively
  `sealed_len(target_size) <= cap`).
- `enabled = true` is written explicitly by `tgfs init` for new repos;
  `tgfs init --no-pack` opts out. Existing configs without a `[pack]`
  table default to `enabled = false` so their snapshots stay readable
  by older clients until the owner opts in (§8).

Defaults rationale: 8 MiB clears the vast majority of "many small
files" workloads while keeping the per-file ranged-download waste (§6)
negligible; 256 MiB ≈ one message per ~30–60 s of upload on a typical
uplink, keeps a failed pack's re-upload exposure bounded, and stays far
from the 2 GiB cap even after encryption overhead.

## 4. Index & snapshot changes

`chunks` gains a byte offset **within the uploaded document, in stored
(possibly sealed) space**:

```sql
CREATE TABLE chunks (
    hash      TEXT PRIMARY KEY,
    size      INTEGER NOT NULL,          -- plaintext size, as today
    msg_id    INTEGER NOT NULL,
    offset    INTEGER NOT NULL DEFAULT 0, -- NEW: start within the document
    encrypted INTEGER NOT NULL DEFAULT 0
);
```

Migration: `ALTER TABLE chunks ADD COLUMN offset INTEGER NOT NULL
DEFAULT 0` on open (existing standalone chunks are all at offset 0, so
the default is already correct).

`ChunkEntry` gains `#[serde(default)] pub offset: u64`. The stored
length needs no column — it is `size` for plaintext chunks and
`Crypto::sealed_len(size)` for encrypted ones, both already derivable.

Snapshot `format` bumps `1 → 2` when any chunk has a nonzero offset
(i.e. the first packed push). Readers must **refuse formats they don't
know** — a format-1-only client silently ignoring `offset` would
download the whole pack and fail the length/hash check with a confusing
error; an explicit "snapshot format 2 requires a newer tgfs" is the
correct failure. Writers keep emitting `format: 1` while the repo
contains no packed chunks, so merely upgrading the binary breaks
nothing for other machines.

New journal table for pack-upload resume (§7):

```sql
CREATE TABLE pack_journal (
    pack_hash  TEXT NOT NULL,   -- hash naming the in-flight pack
    seq        INTEGER NOT NULL,
    chunk_hash TEXT NOT NULL,   -- member, in concatenation order
    PRIMARY KEY (pack_hash, seq)
);
```

## 5. Push pipeline

During `push`, files are split into two streams:

- `size >= pack.threshold` (or packing disabled): today's path,
  unchanged.
- `size < pack.threshold`: the file's single chunk is handed to a
  **pack builder** instead of being uploaded immediately. Chunks
  already in the index (dedup) or empty are skipped exactly as today.

The pack builder accumulates members `(chunk_hash, abs_path, size)` and
flushes a pack when the **stored** size would exceed `target_size`, and
once more at the end of the walk for the remainder. A flush:

1. Fixes the member order (walk order — deterministic because the walk
   is sorted) and computes each member's stored offset:
   `offset[i+1] = offset[i] + stored_len(size[i])`.
2. Journals the member list into `pack_journal` under a *pack hash* —
   BLAKE3 of the concatenated member chunk hashes. This names the pack
   deterministically without reading file bytes twice.
3. Uploads via the existing resumable uploader with a new
   `PackSource: PartSource` that maps a read at stored offset `o` to
   the member covering `o` and delegates to the existing
   `PlainChunkSource` / `EncryptedChunkSource` logic for that member
   (each member is sealed with its own hash as nonce context, exactly
   as standalone chunks are — so a member's bytes are identical whether
   it is packed or not, keeping resume and re-flush deterministic).
   Document name: `pack-<pack-hash-prefix>.bin`; caption:
   `pack files=<n> bytes=<stored>` (informational only — the index is
   authoritative, per the "no metadata in captions" rule).
4. On success, inserts one `chunks` row per member — same `msg_id`,
   ascending `offset` — clears both journals, and only then upserts the
   corresponding `files` rows.

A pack with a single member is still uploaded as a pack (offset 0);
it is bit-identical to a standalone chunk upload, so no special case.

Files whose chunk is pending in an unflushed pack must not be
`upsert_file`d yet — a crash between "file recorded" and "pack
uploaded" would corrupt the index. Ordering: upload pack → insert
chunks → upsert files, mirroring today's per-chunk ordering.

## 6. Download path (`get`)

`tg.rs` gains a ranged download:

```rust
async fn download_range(&self, peer, msg_id, offset: u64, len: u64, out) -> Result<u64>
```

implemented with raw `upload.getFile`: Telegram requires `offset` to be
4 KiB-aligned (with the `precise` flag) and `limit` a multiple of 4 KiB
(≤ 1 MiB), so the implementation rounds the requested offset down to
4 KiB, requests aligned windows, and discards the sub-block slack
before writing to `out`. Worst-case waste is < 4 KiB + one trailing
partial block per member — irrelevant at these sizes.

`get` then changes in one place: instead of downloading `chunk.msg_id`
whole, it downloads `stored_len(chunk.size)` bytes at `chunk.offset`.
For `offset == 0` standalone chunks it may keep using the whole-message
path (equivalent, and avoids raw-API alignment handling in the common
case). Decryption, the length check, and the whole-file hash check are
untouched — `DecryptingWriter` already consumes exactly the sealed
member stream.

Restoring a whole packed *directory* naturally issues one ranged read
per file against the same message; an easy later optimization is
sorting targets by `(msg_id, offset)` and coalescing adjacent members
into one ranged read, but it is not required for correctness.

## 7. Resume & failure model

- **Interrupted pack upload**: `upload_journal` already records
  `(hash → telegram file_id, total_parts, done_parts)`; packs use it
  keyed by the pack hash. On the next push, the builder re-collects
  small-file chunks; before flushing, it checks `pack_journal` for an
  in-flight pack whose member set is still fully pending (every member
  absent from `chunks`, every member file unchanged — same chunk hash).
  If so it rebuilds *that exact* member order and resumes the part
  upload; deterministic per-member ciphertext guarantees identical
  bytes. If any member changed or was already uploaded elsewhere, both
  journal entries for the pack are dropped and packing starts fresh —
  correctness never depends on resume.
- **Crash after upload, before index insert**: the pack message is
  orphaned in the channel; the next push re-uploads. Same exposure as
  today's per-chunk path, just bigger; bounded by `target_size`, and
  `tgfs prune` (future) can drop unreferenced pack messages.
- **`FLOOD_WAIT`**: unchanged — handled inside the uploader. Packing
  reduces the number of `send_uploaded` calls by orders of magnitude,
  which is the point.

## 8. Compatibility & sharing

- **Old client, new snapshot**: refused by the format-2 check with an
  upgrade message (§4). This is why `enabled` defaults to `false` for
  configs that predate the feature: turning it on is an explicit,
  per-repo decision by the owner, who knows which machines/readers pull
  from the channel. New `tgfs init` repos enable it from day one.
- **New client, old snapshot / old repo**: fully compatible —
  `offset` defaults to 0 everywhere.
- **Mixed history**: enabling packing mid-life is safe. Existing chunks
  stay where they are; only new uploads pack. Disabling it later is
  equally safe — packed chunks remain readable forever via their
  `(msg_id, offset)`.
- **Readers (shared repos)**: membership semantics are unchanged; a
  reader needs a tgfs version that understands format 2 once the writer
  starts packing.

## 9. Garbage & future work

Messages are immutable, so deleting or modifying one member cannot
shrink its pack — the dead bytes stay in the channel until every
member is dead and `tgfs prune` deletes the whole message. Two future
extensions, both out of scope here:

- **Repack during prune**: rewrite packs whose live ratio drops below a
  watermark (download live members, re-pack, delete the old message) —
  the offline analogue of `git gc`.
- **Read coalescing** for whole-tree restores (§6).

## 10. Milestone

- **M6 — packing**: `[pack]` config + validation, `offset` column &
  snapshot format 2 (with reader-side format guard shipped first),
  `PackSource` + pack builder in push, `download_range` in get,
  `pack_journal` resume. Format guard and `offset`-aware reading can
  ship one release ahead of the writer, easing fleet upgrades.
