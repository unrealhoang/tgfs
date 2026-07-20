# Packing small files — design

Status: **implemented**. Extends [SPEC.md](SPEC.md) §2.

tgfs is unreleased, so this design carries **no migration or backward
compatibility**: the schema and snapshot shape below are simply what the
code does. An index created before this change is recreated by deleting
`.tgfs/index.db` and running `tgfs pull`.

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

`.tgfs/config.toml` grows an optional `[pack]` table. Packing is on by
default; there is no `enabled` flag — `threshold = 0` disables it.

```toml
channel_id = ...
channel_access_hash = ...
chunk_size = 1073741824

[pack]
threshold = 8388608       # 8 MiB — files smaller than this are packed; 0 disables
target_size = 268435456   # 256 MiB — flush a pack when it reaches this
```

```rust
#[derive(Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct PackConfig {
    pub threshold: u64,   // 0 = packing disabled
    pub target_size: u64,
}

impl Default for PackConfig {
    fn default() -> Self {
        Self { threshold: 8 << 20, target_size: 256 << 20 }
    }
}

// In RepoConfig:
#[serde(default)]
pub pack: PackConfig,
```

Validation at load time:

- `threshold <= chunk_size` (a packed file is by definition
  single-chunk; files ≥ `threshold` go through normal chunking).
- `threshold <= target_size` and `Crypto::sealed_len(target_size)`
  within the 2 GiB per-document cap.

Defaults rationale: 8 MiB clears the vast majority of "many small
files" workloads while keeping the per-file ranged-download waste (§6)
negligible; 256 MiB ≈ one message per ~30–60 s of upload on a typical
uplink, keeps a failed pack's re-upload exposure bounded, and stays far
from the 2 GiB cap even after encryption overhead.

## 4. Index & snapshot changes

`chunks` gains a byte offset **within the uploaded document, in stored
(possibly sealed) space** — added directly to the `CREATE TABLE`, no
migration:

```sql
CREATE TABLE chunks (
    hash      TEXT PRIMARY KEY,
    size      INTEGER NOT NULL,          -- plaintext size, as today
    msg_id    INTEGER NOT NULL,
    offset    INTEGER NOT NULL DEFAULT 0, -- NEW: start within the document
    encrypted INTEGER NOT NULL DEFAULT 0
);
```

`ChunkEntry` gains `#[serde(default)] pub offset: u64`; `insert_chunk`,
`import`, and `export` carry it through. The stored length needs no
column — it is `size` for plaintext chunks and `Crypto::sealed_len(size)`
for encrypted ones, both already derivable.

The snapshot `format` stays `1`; the `offset` field is just part of what
format 1 means from now on (any snapshot written before this change
deserializes fine anyway via the serde default, but that is incidental,
not a supported path).

Pack-upload resume reuses the existing `upload_journal` table (which
already maps a hash to `(telegram file_id, total_parts, done_parts)`),
keyed by the *pack hash* instead of a chunk hash. No new table is
needed: the member order is reconstructed deterministically from the
sorted walk (§7), so there is nothing extra to persist.

## 5. Push pipeline

During `push`, files are split into two streams:

- `size >= pack.threshold` (or packing disabled): today's path,
  unchanged.
- `size < pack.threshold`: the file's single chunk is handed to a
  **pack builder** instead of being uploaded immediately. Chunks
  already in the index (dedup) or empty are skipped exactly as today.
  Two files with identical content queued in the same batch are stored
  as **one** member (same-push dedup): both files are still recorded,
  but only the first occurrence contributes bytes and counts toward the
  flush threshold — matching the standalone path, which dedups because
  it inserts each chunk into the index before the next file is scanned.

The pack builder accumulates members `(chunk_hash, abs_path, size)` and
flushes a pack when the **stored** size would exceed `target_size`, and
once more at the end of the walk for the remainder — only if it holds at
least one member (`upload_source` refuses empty uploads). Members hold
only the file *path*, not an open descriptor — each is opened lazily on
read — so a pack of thousands of tiny files doesn't hold thousands of
descriptors at once (which would exhaust the FD limit on exactly the
workload packing targets). A flush:

1. Fixes the member order (first-occurrence walk order — deterministic
   because `walk_repo` sorts by file name) and computes each distinct
   member's stored offset: `offset[i+1] = offset[i] + stored_len(size[i])`.
2. Names the pack by a *pack hash* — BLAKE3 of the concatenated member
   chunk hashes — and records the in-flight upload under `upload_journal`
   keyed by that hash. This names the pack deterministically without
   reading file bytes twice.
3. Uploads via the existing `upload_source` with a new
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
   ascending `offset` — clears the journal entry, and only then upserts
   the corresponding `files` rows.

A pack with a single member is still uploaded as a pack (offset 0);
it is bit-identical to a standalone chunk upload, so no special case.

Files whose chunk is pending in an unflushed pack must not be
`upsert_file`d yet — a crash between "file recorded" and "pack
uploaded" would corrupt the index. Ordering: upload pack → insert
chunks → upsert files, mirroring today's per-chunk ordering.

## 6. Download path (`get`)

`tg.rs` gains a ranged download built on grammers' `Client::iter_download`
/ `DownloadIter` — no raw `upload.getFile`:

```rust
/// Write `len` stored bytes starting at `offset` of the document in
/// `msg_id` into `out`.
async fn download_range<W: std::io::Write>(
    &self,
    document: &Document,
    offset: u64,
    len: u64,
    out: &mut W,
) -> Result<u64>
```

Implementation — deliberately simple; over-fetch is fine at these
sizes. Requests stay at grammers' default chunk size (`MAX_CHUNK_SIZE` =
512 KiB), so every request offset is a multiple of the limit and all of
`upload.getFile`'s alignment and 1 MiB-block rules hold automatically:

1. `iter_download(&document).skip_chunks((offset / 512K) as i32)` starts
   at the greatest 512 KiB boundary at or below `offset`.
2. Discard the `offset % 512K` leading slack of the first chunk, write
   until `len` bytes are covered, drop the iterator.

Worst-case over-fetch is < 512 KiB of leading slack plus one trailing
partial chunk per member — bounded and irrelevant next to the message
round trips saved. `DownloadIter` itself follows `FILE_MIGRATE` and
exports authorization into other DCs (`copy_auth_to_dc`), so cross-DC
handling needs zero code here.

`get` then changes in one place: instead of streaming `chunk.msg_id`
whole, it resolves each message's `Document` once (a small
`HashMap<i32, Document>` cache across the whole `get`, so N files sharing
one packed message cost one `get_messages_by_id`, not N) and reads
`stored_len(chunk.size)` bytes at `chunk.offset` via `download_range`. A
standalone chunk is just the degenerate case (`offset 0`, length == the
whole document), so one code path covers both and the index never needs
to record document sizes. Decryption, the length check, and the
whole-file hash check are untouched — `DecryptingWriter` already consumes
exactly the sealed member stream. (Whole-document reads without a known
length — the index snapshots — keep using the streaming
`download_document`.)

Restoring a whole packed *directory* naturally issues one ranged read
per file against the same message; an easy later optimization is
sorting targets by `(msg_id, offset)` and coalescing adjacent members
into one ranged read, but it is not required for correctness.

## 7. Resume & failure model

- **Interrupted pack upload**: `upload_journal` records
  `(hash → telegram file_id, total_parts, done_parts)`; packs use it
  keyed by the pack hash. On the next push, files that completed earlier
  are skipped (fast-path or dedup), so the builder re-collects exactly
  the same still-pending small files in the same sorted-walk order and
  rebuilds the identical pack — same members, same pack hash. If
  `upload_journal` holds an entry for that pack hash with a matching part
  count, the part upload resumes from `done_parts`; deterministic
  per-member ciphertext guarantees identical bytes. If the pending set
  differs (a member changed, was added, or already landed elsewhere) the
  pack hash differs, no journal entry matches, and packing simply starts
  fresh — correctness never depends on resume. (As with chunks today,
  packs under the 10 MiB big-file threshold take `upload_source`'s
  one-shot path and never journal; a failure just re-uploads.)
- **Crash after upload, before index insert**: the pack message is
  orphaned in the channel; the next push re-uploads. Same exposure as
  today's per-chunk path, just bigger; bounded by `target_size`, and
  `tgfs prune` (future) can drop unreferenced pack messages.
- **`FLOOD_WAIT`**: one uploader actor accepts big-part and small-file jobs over
  an input channel and acknowledges each caller over a one-shot channel. It
  starts four jobs concurrently. Failed jobs move to the actor's private retry
  queue, which is always drained before new input; this avoids re-enqueueing
  into the bounded input channel while callers are waiting for acknowledgements.
  The first server flood wait pauses dispatch for the requested cooldown and
  reduces concurrency to one. After the cooldown, successful uploads raise the
  limit by one per throttle-free minute until it returns to four; another flood
  immediately resets it to one and restarts the gradual recovery. Packing
  reduces the number of `send_uploaded` calls by orders of magnitude, which is
  the point.

## 8. Garbage & future work

Messages are immutable, so deleting or modifying one member cannot
shrink its pack — the dead bytes stay in the channel until every
member is dead and `tgfs prune` deletes the whole message. Two future
extensions, both out of scope here:

- **Repack during prune**: rewrite packs whose live ratio drops below a
  watermark (download live members, re-pack, delete the old message) —
  the offline analogue of `git gc`.
- **Read coalescing** for whole-tree restores (§6).

## 9. Milestone

- **M6 — packing**: `[pack]` config + validation, the `offset` column,
  `PackSource` + the pack builder in push, `download_range` (via
  `DownloadIter`) in get, and pack-upload resume via `upload_journal`.
