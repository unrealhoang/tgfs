# Streaming working-tree scan — design

Status: **proposed**. Changes `src/diff.rs`, `src/index.rs`, `src/sync.rs`.

tgfs is unreleased, so this design carries no migration or backward
compatibility concerns beyond one additive index query.

## 1. Problem

On a large working tree (the motivating case: an Immich library, hundreds
of thousands of photos/videos), `tgfs status` appears to hang, and `push`
inherits the same startup cost. Four independent causes:

1. **Collect-then-process traversal.** `walk_repo` (`src/diff.rs:33`)
   materializes the entire tree into a `Vec<LocalFile>` before anything is
   compared or printed. Nothing is shown until the full walk finishes.
2. **Per-file SQLite lookups.** `scan_changes` calls `index.get_file`
   for every file — two prepared statements each (`files` row plus a
   `file_chunks` query whose result status never uses). 500 k files ≈
   1 M+ point queries against a cold WAL database.
3. **No excludes.** Only `.tgfs` is skipped. `.git`, caches, thumbnails,
   editor litter are all statted and diffed.
4. **No progress, network first.** `status` resolves the Telegram peer
   and fetches the pinned index message *before* scanning
   (`src/sync.rs:104`), so the first visible output waits on the network;
   the scan itself prints nothing until done.

## 2. How restic does it (research summary)

restic's post-0.9 archiver (`internal/archiver`) is the reference design
for exactly this problem — stream a huge tree, compare against a previous
snapshot, report progress:

- **Streaming, deterministic traversal.** `saveTree`/`saveDir` recurse
  directory by directory, sorting entries lexicographically *within each
  directory*. Nothing is ever collected into a global list; each entry is
  handed to worker pools as a `futureNode` and resolved bottom-up.
- **Change detection against the parent snapshot, joined per directory.**
  While descending, the archiver loads the parent snapshot's tree node
  for the *same directory* and matches children by name — a merge join at
  directory granularity, never a global map and never per-file queries.
  `fileChanged()` compares size and mtime, plus ctime and inode unless
  `--ignore-ctime`/`--ignore-inode` is set. If unchanged (and all blobs
  are present in the repo index), the old node's blob list is reused
  without reading the file.
- **Progress via a concurrent Scanner.** A separate goroutine
  (`internal/archiver/scanner.go`) walks the same targets purely to count
  files/dirs/bytes (`ScanStats`), feeding the ETA denominator, while the
  archiver starts immediately and reports live numerators through
  `StartFile`/`CompleteItem`/`CompleteBlob` callbacks. The backup never
  waits for the scan.
- **Excludes, not gitignore.** restic uses `--exclude`/`--exclude-file`
  patterns and `CACHEDIR.TAG`, applied by `Select`/`SelectByName`
  callbacks — `SelectByName` runs *before* the `stat`, so excluded trees
  are never even touched.
- **Bounded parallelism.** `FileSaver` reads ~2 files concurrently;
  blob and tree saving are separate pools sized off `GOMAXPROCS`.

The transferable ideas, in order of impact for tgfs: (a) never build the
full file list; (b) replace per-file DB queries with one bulk read of the
prior state; (c) apply excludes before stat; (d) progress is a throttled
side channel, not a phase.

## 3. Design overview

Replace `walk_repo → Vec` + per-file `get_file` with:

> A streaming scanner that walks the tree in deterministic order,
> filters through ignore rules **before** stat, compares each entry
> against a **preloaded** snapshot of the index, and emits
> `ScanEvent`s to a callback. `status` and `push` are both consumers
> of the same event stream.

### 3.1 Traversal: the `ignore` crate

Swap `walkdir` for the `ignore` crate (ripgrep's walker):

```rust
ignore::WalkBuilder::new(&repo.root)
    .standard_filters(false)          // no implicit .gitignore/hidden rules
    .add_custom_ignore_filename(".tgfsignore")
    .filter_entry(|e| e.file_name() != REPO_DIR)
    .sort_by_file_name(|a, b| a.cmp(b))
    .build()
```

- The iterator is lazy: sorting happens per directory (same guarantee
  restic gives), so the first entries arrive immediately regardless of
  tree size.
- `standard_filters(false)` keeps tgfs's semantics explicit: tgfs is a
  backup tool, so `.gitignore` must **not** silently apply (a photo
  library is not a git repo; and in a git repo, ignored build outputs may
  still be worth backing up — but `.git` rarely is, see below). Instead:
  - `.tgfsignore` files (gitignore syntax, any directory level) are
    honored via `add_custom_ignore_filename`.
  - An optional `exclude = ["…"]` list in `.tgfs/config.toml` is compiled
    into an `ignore::overrides::Override` — the equivalent of restic's
    `--exclude`.
  - `.tgfs` itself remains hard-excluded.
- Like restic's `SelectByName`, ignore matching happens on the name/path
  before metadata is fetched, so an excluded `node_modules/` costs one
  readdir entry, not a subtree stat storm.

Default excludes ship in the generated `.tgfs/config.toml` on `init`
(`.git/`, `.DS_Store`, `Thumbs.db`) rather than being hardcoded, so the
policy is visible and editable.

`walk_repo` becomes an iterator (`fn walk_repo(&Repo) -> impl
Iterator<Item = Result<LocalFile>>`); the `Vec`-returning form is
deleted and both call sites consume the iterator directly.

### 3.2 Index comparison: one bulk preload

Add to `Index`:

```rust
/// path → (size, mtime) for all non-deleted files, in one query.
pub fn stat_map(&self) -> Result<HashMap<String, (u64, i64)>>
```

backed by `SELECT path, size, mtime FROM files WHERE deleted = 0`. The
scanner takes ownership of the map and *removes* each path as the walk
visits it:

- hit with equal `(size, mtime)` → `Unchanged`
- hit with different metadata → `Modified`
- miss → `New`
- entries left in the map after the walk → `Deleted`

This replaces ~2 N point queries with one table scan, and replaces the
separate `alive: HashSet` + `list_files` pass — deletion detection falls
out of the residue for free. Memory is ~100 bytes/entry: ~50 MB for
500 k files, acceptable for the target scale by a wide margin.

Why not restic's per-directory merge join? restic's parent snapshot *is*
a tree of per-directory nodes, so joining per directory is natural. tgfs's
index is a flat `path` table; slicing it per directory means either N
prefix queries (back where we started) or a global sorted merge — and a
sorted merge requires the walker's emission order to match SQLite's
`ORDER BY path` collation exactly, which byte-wise directory-recursion
order does *not* (e.g. `a.txt` vs `a/b`: flat ordering compares `.` with
`/` mid-path). The hash map is simpler, collation-proof, and cheap at
this scale. If tgfs ever targets tens of millions of files, revisit with
a merge join over a normalized sort key.

`get_file`'s unconditional `file_chunks` query also gets a targeted fix:
`scan_changes` and the `push` skip-path stop calling `get_file` entirely
(the stat map answers them), so the chunk query only runs where chunks
are actually needed (restore, upload dedup check).

### 3.3 The event stream

```rust
pub(crate) enum ScanEvent {
    New(LocalFile),
    Modified(LocalFile),
    Unchanged(LocalFile),
    Deleted(String),          // emitted after the walk completes
    Progress(ScanStats),      // throttled, see §3.4
}

pub(crate) struct ScanStats {
    pub scanned: u64,         // files visited
    pub bytes: u64,           // their cumulative size
}

pub(crate) fn scan(
    repo: &Repo,
    index: &Index,
    mut on_event: impl FnMut(ScanEvent) -> Result<()>,
) -> Result<Changes>          // summary counts, as today
```

A callback rather than an `Iterator`/`Stream` keeps borrows simple (the
walker, the stat map, and the SQLite connection all live in one stack
frame) and is sufficient for both consumers. `Changes` shrinks to
counters; the path lists live only in the events.

**`status`** prints changed paths *as they are discovered* (matching
today's output format, now incremental) and renders progress lines
between them. Deletions still print last — unavoidable, since they are
only known once the walk ends.

**`push`** replaces its `walk_repo` loop body (`src/sync.rs:164`) with a
match on events: `Unchanged` → `skipped += 1`; `New`/`Modified` → the
existing prepare/pack/upload logic, which begins uploading while the
walk is still running — restic's "don't wait for the scan" property.
`Deleted` events accumulate the tombstone list, replacing
`tombstone_missing`'s `alive` vector (which currently holds every path
in the repo). Uploads mutate the index (`upsert_file`, `insert_chunks`)
while the preloaded stat map is already in memory — safe, because the
map is a snapshot of *pre-push* state, which is exactly what the diff
must be computed against.

### 3.4 Progress reporting

restic runs a second concurrent walk to get a total for ETA. That is the
right call for a backup tool with hour-long runs; for `status` it doubles
I/O to decorate a command that should itself be fast. Split the
difference:

- **`status`**: no second walk. Emit `Progress` at most every 500 ms
  (checked cheaply every 256 files) and render to **stderr** as a
  carriage-return line: `scanning… 123456 files (487.3 GiB), 210 changed`
  — cleared before the summary. stderr keeps `status`'s stdout pipeable
  and the progress invisible to scripts. Only emitted when stderr is a
  TTY.
- **`push`**: same live counter. A restic-style concurrent
  scanner-for-ETA thread is possible later behind the same `ScanStats`
  type, but is out of scope: the pain today is absence of any signal,
  not absence of an ETA.

### 3.5 Ordering the network call

`status` currently blocks on `remote_index_info` before scanning. Since
the local scan and the version check are independent, run them
concurrently: spawn the scan on `tokio::task::spawn_blocking` (it is
synchronous SQLite + filesystem work, which also stops it from stalling
the async runtime) and `tokio::join!` it with the Telegram lookup. First
byte of output no longer waits on Telegram; if the network is slow the
scan results print first and the version line follows.

`push` keeps its current ordering — the version check is a guard that
must pass before any upload.

## 4. What deliberately does not change

- **Change detection stays `size + mtime`** (seconds). restic also
  checks ctime and inode; worth stealing eventually (catches
  mtime-restoring writes and same-mtime replacements), but it is a
  schema change (`files` gains `ctime`, `inode`) and orthogonal to the
  performance problem. Noted as follow-up, not done here.
- **No parallel stat/hash workers.** The `ignore` crate offers a
  parallel visitor and restic reads 2 files concurrently; tgfs's scan
  does no content hashing, so the walk is metadata-bound and a single
  thread saturates typical disks. Add parallelism only if profiling a
  real Immich-sized tree on the target hardware says otherwise.
- **Index schema** is untouched except for the new read path
  (`stat_map`). Snapshot format, packing, encryption: unaffected.

## 5. Implementation order

1. `Index::stat_map`; rewrite `scan_changes` on top of it (pure perf,
   no behavior change — existing tests must pass unchanged).
2. Convert `walk_repo` to a lazy iterator; introduce `scan` +
   `ScanEvent`; port `status` and `push` onto it.
3. Swap `walkdir` → `ignore`; add `.tgfsignore` + config `exclude`;
   tests for exclusion semantics (excluded-but-indexed files must report
   as `Deleted`, matching git's behavior when a tracked file becomes
   ignored — and `push` will tombstone them, which the docs must say
   loudly).
4. Progress rendering (stderr, TTY-gated, throttled).
5. Concurrent version-check in `status`.

Each step is independently shippable and testable; step 1 alone removes
the O(N) query storm, which is the dominant cost on a warm FS cache.
