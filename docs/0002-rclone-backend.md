# tgfs as an rclone backend — design

Status: **proposed**. Extends [SPEC.md](SPEC.md); builds on the packing
design in [0001-packing.md](0001-packing.md).

## 1. Goal

Let rclone treat a tgfs repo as a remote, so anything that speaks rclone
gets Telegram-backed storage for free. The motivating consumer is
**restic**:

```
restic -r rclone:tgfs:restic init
restic -r rclone:tgfs:restic backup ~/documents
```

restic then owns dedup, encryption, snapshots and pruning; tgfs is a dumb,
durable, free blob store underneath.

## 2. How to plug into rclone

Three ways were considered:

1. **Native rclone backend (Go).** rclone backends are compiled into
   rclone itself. This would mean reimplementing MTProto access, the
   chunk/pack storage format, and the index snapshot format in Go —
   two implementations of one on-wire format, drifting forever. Rejected.
2. **FUSE mount + rclone `local`.** SPEC §6's future mount could be
   pointed at by rclone's local backend. But the mount is read-only in
   its planned first version, FUSE is platform-fussy, and routing an
   object-store workload through POSIX semantics adds failure modes for
   nothing. Rejected.
3. **`tgfs serve` speaking a protocol rclone already has a backend
   for.** rclone's `webdav` backend needs only a small, well-defined
   HTTP subset, works over localhost, and supports everything restic's
   rclone backend requires (streamed PUT, ranged GET, listing, delete).
   **Chosen.**

So the deliverable is a long-running daemon:

```
tgfs serve webdav [--addr 127.0.0.1:8080] [--key <k> | --keyfile <path>]
```

run from inside a repo (or a bare clone, §7), with rclone configured as:

```ini
[tgfs]
type = webdav
url = http://127.0.0.1:8080
vendor = other
```

restic's `rclone:` backend then works unmodified: restic spawns
`rclone serve restic --stdio tgfs:restic`, rclone translates restic's
REST calls into WebDAV requests against the tgfs daemon.

A `tgfs serve restic` variant (restic's REST protocol directly, cutting
rclone out of the path) is a cheap later addition — it is a thinner
protocol than WebDAV over the same internal layer (§4) — but WebDAV is
the primary target because it serves every rclone consumer, not just
restic.

## 3. The architectural gap

tgfs today is a **folder mirror**: the local working tree is the source
of truth, `push` diffs it against the index, and every remote object
corresponds to a local file. An rclone backend inverts this: there is
**no local tree** — clients create, read, list and delete remote objects
directly, and the index namespace *is* the filesystem.

What transfers cleanly:

- **Chunk engine** (`file.rs`, `telegram_transfer.rs`): content-addressed
  chunks, packs, part-level resume, flood-wait handling, deterministic
  encryption — all reusable as-is. Uploads already read from a
  `PartSource` over a local file, which staging (§5) provides.
- **Ranged download** (`download_range`): restic reads blob ranges out
  of its pack files, which maps onto ranged GETs (§6).
- **Index + snapshot versioning**: the SQLite index and pinned-snapshot
  optimistic locking carry over; only *when* snapshots publish changes
  (§8).

What is genuinely new: a **direct-access layer** that mutates the index
without a working-tree diff, a staging area for incoming writes, and the
HTTP server itself.

## 4. New module: `vfs.rs`

One internal API both `serve webdav` and a future `serve restic` sit on.
It owns the index, the staging area, the pack buffer, and snapshot
publishing; the HTTP layer is a thin protocol translation.

```rust
pub struct Vfs { /* RepoContext + staging + pack buffer + publish state */ }

impl Vfs {
    /// Spool `body` to staging, hash, upload (or enqueue into the pack
    /// buffer), commit to the index. Returns once the object is durable
    /// per the contract in §8.
    pub async fn put(&self, path: &str, body: impl AsyncRead) -> Result<()>;

    /// Stream `len` bytes at `offset` (None = whole object). Serves
    /// staged-but-unflushed objects from staging.
    pub async fn get(&self, path: &str, range: Option<(u64, u64)>,
                     out: impl AsyncWrite) -> Result<()>;

    pub fn stat(&self, path: &str) -> Result<Option<FileEntry>>;
    pub fn list(&self, dir: &str) -> Result<Vec<DirEntry>>;   // one level
    pub async fn delete(&self, path: &str) -> Result<()>;
    pub async fn rename(&self, from: &str, to: &str) -> Result<()>;
    pub fn mkdir(&self, path: &str) -> Result<()>;

    /// Flush the pack buffer and publish a snapshot if dirty.
    pub async fn sync(&self) -> Result<()>;
}
```

Notes on the easy wins:

- **`rename` is a metadata operation.** Content addressing means a
  WebDAV `MOVE` never touches Telegram — it rewrites the `files` row.
  rclone gets server-side move essentially for free.
- **`stat`/`list`** are pure SQLite. `list` returns one directory level:
  files whose path has the prefix, collapsed at the next `/`, plus
  entries from the `dirs` table (§7).
- **`delete`** tombstones the entry (as `push` does for vanished files
  today). Message deletion is deferred to `tgfs prune` (§9).

## 5. Write path

WebDAV PUT bodies stream in with, at best, a `Content-Length`. tgfs
must know the content hash before it can dedup or name chunks, so every
PUT is first **spooled to a staging file** under `.tgfs/staging/`
(hashing while spooling), then handed to the existing pipeline:

- `size >= pack.threshold`: chunked and uploaded standalone via
  `file::upload`, exactly like push. restic's data packs default to
  ~16 MiB and are commonly tuned larger, so the bulk of a restic backup
  takes this path — one Telegram message per restic pack file, no
  amplification.
- `size < pack.threshold`: enqueued into a **live pack buffer** — the
  push pack builder, adapted to accumulate across PUTs instead of
  across one walk. For restic this catches `snapshots/*`, `keys/*`,
  `locks/*`, `config` and index files: exactly the many-tiny-files
  workload packing exists for.

The pack buffer flushes when: stored size reaches `pack.target_size`, a
configurable idle timeout expires (default 30 s), or `Vfs::sync` runs.
Until its pack lands, a staged member is fully readable and listable —
`get`/`stat` are served from the staging file and the index-pending
state — so a client that writes then immediately reads back (restic
verifies uploads this way) never observes a gap. Staging files are
deleted only after their chunk rows are committed.

Same-content PUTs dedup exactly as in push: if the chunk hash is
already in the index, nothing uploads and only the `files` row is
written.

Crash recovery: on startup, `serve` clears `.tgfs/staging/` — a PUT
that never returned success never happened, which is the contract every
rclone backend gives. In-flight pack/chunk part uploads resume via the
existing `upload_journal`, unchanged.

## 6. Read path

`get` with a range maps `(offset, len)` onto the file's chunk list
(chunk boundaries are every `chunk_size` bytes, so this is arithmetic),
then issues `download_range` per covered chunk — the same machinery
`tgfs get` uses for pack members.

One refinement is needed for **encrypted repos**: today
`DecryptingWriter` consumes a chunk's whole sealed stream.
Segments are sealed independently (1 MiB plaintext each, nonce =
(chunk hash, segment number)), so random access inside a chunk is
already possible in the format — the work is a
`decrypt_range` that fetches only the sealed segments covering the
requested range, decrypts each, and trims the edges. Worst-case
over-fetch is < 2 sealed segments (~2 MiB) per request. Until that
lands, ranged reads on encrypted repos fall back to
decrypt-from-chunk-start-and-discard, which is correct but wasteful;
`decrypt_range` is part of this milestone because restic restores lean
hard on ranged reads.

Whole-object GET is the degenerate range and needs nothing new.
Per-request `Document` handles are cached by `msg_id` (as `get` already
does) so N reads against one pack cost one message resolution;
`file_reference` expiry invalidates the cache entry and re-fetches.

## 7. Namespace, directories, and bare repos

The index's flat path list is the namespace. WebDAV needs directories
to exist as addressable resources (rclone `mkdir`s before writing;
restic creates `data/00`–`data/ff` at init), so the local index gains a
tiny `dirs` table recording explicitly created directories; PROPFIND
also synthesizes implicit directories from file path prefixes. The
`dirs` table is **local-only and not part of the snapshot**: an empty
directory that never receives a file does not survive
re-clone — the same documented behavior as rclone's bucket-style
backends (S3, GCS), which rclone and restic both tolerate. If this ever
bites, adding a `dirs` list to the snapshot format is a small, isolated
change.

Serving does not require a working tree, and mirroring one alongside
direct writes would reintroduce the diff model this design escapes. So
this milestone also adds **bare repos**:

```
tgfs clone --bare <name> [dir]   # .tgfs contents only, no working tree
tgfs init --bare [--encrypt ...] # new empty repo for serving
```

A bare repo is just the `.tgfs` directory contents at the top level
(config, index, staging); `push` and `pull`'s working-tree diffing are
disabled in it. `serve` runs in either kind of repo, but bare is the
recommended shape for a restic target. Running `serve` and `push`
against the *same* repo concurrently is rejected with a lock file
(`.tgfs/serve.lock`); against the same channel from different machines
it is caught by snapshot version locking, like concurrent pushes today.

## 8. Durability contract & snapshot publishing

The invariant: **when a PUT (or DELETE/MOVE) returns success, the
object's bytes are on Telegram** (or staged-and-journaled in a pack
whose flush is pending — see below) **and its metadata is committed to
the local SQLite index.** The pinned remote snapshot may lag.

Publishing a snapshot per operation is impossible (one message + pin
per PUT would drown in flood waits and version churn), so `serve`
publishes on:

- an idle timer: no mutation for `--publish-idle` (default 30 s, which
  also flushes the pack buffer first);
- a byte/operation high-water mark during sustained writes
  (default: every 1 GiB uploaded), bounding the replay window;
- graceful shutdown (SIGINT/SIGTERM drains staging, flushes packs,
  publishes, then exits).

Each publish bumps the version and re-checks the pinned message first —
the same optimistic locking as push; if another writer moved the pin,
`serve` refuses further writes and reports the conflict rather than
clobbering.

Failure windows, stated honestly:

- **Daemon or host crashes after PUTs, before publish**: chunks are on
  Telegram and the *local* index knows them; restarting `serve` (or
  running any tgfs command locally) still sees everything, and the next
  publish repairs the remote snapshot. Nothing is lost.
- **Local disk is destroyed before publish**: chunk messages uploaded
  since the last published snapshot are orphaned — unfindable without
  the index rows that named them. For restic this means pack files it
  believes it wrote are missing: `restic check` fails and the affected
  snapshots are damaged. The high-water publish bounds this to the
  configured window. Users who need tighter guarantees set
  `--publish-idle 0` (publish after every batch) and accept the flood
  cost. This is the honest price of metadata living outside Telegram's
  blob messages; it is the same class of risk as losing a local restic
  cache mid-upload, just with a larger blast radius, and it must be
  documented prominently.
- **Small files in the pack buffer at crash**: their PUTs already
  returned success, so buffer members must be durable *before* the pack
  flushes. Rule: a PUT into the pack buffer returns success only after
  the staging file is fsynced and recorded in a `staging_journal` table
  (path, hash, size, mtime). On restart, `serve` re-enqueues journaled
  staging entries instead of clearing them (§5's "clear staging" applies
  only to entries absent from the journal, i.e. half-received bodies).
  Locks and restic index files are tiny; the exposure is seconds of
  buffered writes surviving on local disk rather than Telegram, which
  the idle flush keeps short.

## 9. Interaction with restic's lifecycle

- **`restic backup`**: streams pack files (standalone messages) and
  index/snapshot files (pack buffer). Telegram's ~2 GiB document cap is
  far above restic's pack sizes; no interaction with tgfs chunking in
  practice (a >1 GiB restic pack would simply become two tgfs chunks,
  which is fine).
- **`restic check` / `restore`**: listing + ranged reads; no writes.
  `check --read-data` downloads everything — flood-wait throttling
  applies but correctness is unaffected.
- **Locks**: restic creates and deletes `locks/*` around every
  operation. Telegram messages are immutable and tgfs deletes are
  tombstones, so each lock file costs pack-buffer bytes that become
  dead weight in the channel until pruned. Negligible in size (a lock
  is <1 KiB) but it makes `tgfs prune` (already future work in
  0001 §8) more wanted, not less.
- **`restic prune`** rewrites packs and deletes the old ones. This
  works — deletes are tombstones — but reclaims **no Telegram storage**
  until `tgfs prune` exists to delete fully-dead messages and repack
  partially-dead ones. Since Telegram storage is free, the recommended
  initial posture is: run restic in append-only style (`forget` without
  `prune`, or `--no-prune`), and let a future `tgfs prune` do physical
  reclamation. Documented as a known limitation.
- **Concurrent restic clients** against one repo must go through the
  *same* `tgfs serve` daemon (restic's own lock files then coordinate
  them). Two daemons on two machines are caught by version locking, but
  only at publish time — the README must say: one serving daemon per
  repo.

## 10. HTTP server details

- Dependency: `axum` (or bare `hyper`) behind a `serve` cargo feature
  to keep the core CLI's build lean. tgfs already runs a tokio runtime.
- Verbs rclone's webdav backend needs: `OPTIONS`, `PROPFIND` (Depth 0
  and 1), `HEAD`, `GET` (with `Range`), `PUT`, `DELETE`, `MKCOL`,
  `MOVE`. No DAV locking (class 2) — rclone does not use it. PROPFIND
  responses carry `getcontentlength`, `getlastmodified` (from index
  mtime, set at PUT time), `resourcetype`.
- Binds `127.0.0.1` by default. `--addr` can widen it; non-loopback
  binds require `--htpasswd <file>` or `--user/--pass` (Basic auth),
  since the daemon holds the decrypted repo key in memory. TLS is out
  of scope — front with a reverse proxy if exposing beyond localhost
  (and think twice; the intended deployment is rclone on the same
  host).
- Writes are serialized internally (Telegram flood limits make write
  parallelism pointless); reads run concurrently. rclone's default
  4-way transfer parallelism therefore just queues on PUT, which is
  correct and self-throttling.

## 11. What this deliberately does not do

- **No Go code, no rclone patches** — everything ships in this repo.
- **No change to the storage format**: chunks, packs, encryption and
  snapshot format are untouched; a repo written through `serve` is
  readable by `pull`/`get` and vice versa. Only the snapshot cadence
  and the local-only `dirs`/`staging_journal` tables are new.
- **No multi-writer serving** — one daemon per repo (§9).
- **No physical deletion** — still `tgfs prune`'s job, unchanged.

## 12. Milestones

- **M7a — vfs core**: `vfs.rs` (put/get/stat/list/delete/rename over
  the index + chunk engine), staging area + `staging_journal`, live
  pack buffer with idle flush, bare repos (`init --bare`,
  `clone --bare`), `serve.lock`.
- **M7b — webdav server**: `tgfs serve webdav` behind the `serve`
  feature, verb handling in §10, publish policy in §8. Acceptance:
  `rclone check`/`copy`/`ls` round-trips, then
  `restic init/backup/check/restore` through `rclone:` against a real
  channel.
- **M7c — ranged decrypt**: `decrypt_range` (segment-granular reads on
  encrypted repos), replacing the decrypt-and-discard fallback.
- **M7d (optional) — `tgfs serve restic`**: restic's REST protocol on
  the same `Vfs`, removing rclone from the hot path for the restic
  use case.
