//! Local SQLite metadata index: which files exist, which chunks make them
//! up, and where those chunks live in the storage channel.

use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

pub struct Index {
    conn: Connection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    pub path: String,
    pub size: u64,
    pub mtime: i64,
    pub hash: String,
    pub deleted: bool,
    /// Chunk hashes in order.
    pub chunks: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkEntry {
    pub hash: String,
    pub size: u64,
    pub msg_id: i32,
}

/// Serialized form uploaded to Telegram as the remote index snapshot.
#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub version: u32,
    pub created_at: i64,
    pub files: Vec<FileEntry>,
    pub chunks: Vec<ChunkEntry>,
}

impl Index {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)
            .with_context(|| format!("cannot open index at {}", path.display()))?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE IF NOT EXISTS files (
                 id      INTEGER PRIMARY KEY,
                 path    TEXT NOT NULL UNIQUE,
                 size    INTEGER NOT NULL,
                 mtime   INTEGER NOT NULL,
                 hash    TEXT NOT NULL,
                 deleted INTEGER NOT NULL DEFAULT 0
             );
             CREATE TABLE IF NOT EXISTS chunks (
                 hash   TEXT PRIMARY KEY,
                 size   INTEGER NOT NULL,
                 msg_id INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS file_chunks (
                 file_id    INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
                 seq        INTEGER NOT NULL,
                 chunk_hash TEXT NOT NULL REFERENCES chunks(hash),
                 PRIMARY KEY (file_id, seq)
             );
             CREATE TABLE IF NOT EXISTS snapshots (
                 id         INTEGER PRIMARY KEY,
                 created_at INTEGER NOT NULL,
                 msg_id     INTEGER NOT NULL
             );",
        )?;
        Ok(Self { conn })
    }

    pub fn get_file(&self, path: &str) -> Result<Option<FileEntry>> {
        let row = self
            .conn
            .query_row(
                "SELECT id, size, mtime, hash, deleted FROM files WHERE path = ?1",
                params![path],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, u64>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, bool>(4)?,
                    ))
                },
            )
            .optional()?;
        let Some((id, size, mtime, hash, deleted)) = row else {
            return Ok(None);
        };
        let mut stmt = self
            .conn
            .prepare("SELECT chunk_hash FROM file_chunks WHERE file_id = ?1 ORDER BY seq")?;
        let chunks = stmt
            .query_map(params![id], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(Some(FileEntry {
            path: path.to_string(),
            size,
            mtime,
            hash,
            deleted,
            chunks,
        }))
    }

    pub fn chunk(&self, hash: &str) -> Result<Option<ChunkEntry>> {
        Ok(self
            .conn
            .query_row(
                "SELECT size, msg_id FROM chunks WHERE hash = ?1",
                params![hash],
                |r| {
                    Ok(ChunkEntry {
                        hash: hash.to_string(),
                        size: r.get(0)?,
                        msg_id: r.get(1)?,
                    })
                },
            )
            .optional()?)
    }

    pub fn insert_chunk(&self, chunk: &ChunkEntry) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO chunks (hash, size, msg_id) VALUES (?1, ?2, ?3)",
            params![chunk.hash, chunk.size, chunk.msg_id],
        )?;
        Ok(())
    }

    /// Insert or replace a file record together with its chunk list.
    pub fn upsert_file(&mut self, entry: &FileEntry) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT INTO files (path, size, mtime, hash, deleted)
             VALUES (?1, ?2, ?3, ?4, 0)
             ON CONFLICT(path) DO UPDATE
               SET size = ?2, mtime = ?3, hash = ?4, deleted = 0",
            params![entry.path, entry.size, entry.mtime, entry.hash],
        )?;
        let id: i64 = tx.query_row(
            "SELECT id FROM files WHERE path = ?1",
            params![entry.path],
            |r| r.get(0),
        )?;
        tx.execute("DELETE FROM file_chunks WHERE file_id = ?1", params![id])?;
        for (seq, hash) in entry.chunks.iter().enumerate() {
            tx.execute(
                "INSERT INTO file_chunks (file_id, seq, chunk_hash) VALUES (?1, ?2, ?3)",
                params![id, seq as i64, hash],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Mark files under `prefix` that are not in `alive` as deleted
    /// (tombstoned). Returns the number of tombstones added.
    pub fn tombstone_missing(&self, prefix: &str, alive: &[String]) -> Result<usize> {
        let mut stmt = self
            .conn
            .prepare("SELECT path FROM files WHERE deleted = 0 AND path LIKE ?1 || '%'")?;
        let known = stmt
            .query_map(params![prefix], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let alive: std::collections::HashSet<&str> = alive.iter().map(|s| s.as_str()).collect();
        let mut count = 0;
        for path in known {
            if !alive.contains(path.as_str()) {
                self.conn.execute(
                    "UPDATE files SET deleted = 1 WHERE path = ?1",
                    params![path],
                )?;
                count += 1;
            }
        }
        Ok(count)
    }

    pub fn list_files(&self, prefix: Option<&str>, include_deleted: bool) -> Result<Vec<FileEntry>> {
        let mut stmt = self.conn.prepare(
            "SELECT path, size, mtime, hash, deleted FROM files
             WHERE path LIKE ?1 || '%' AND (deleted = 0 OR ?2)
             ORDER BY path",
        )?;
        let rows = stmt
            .query_map(params![prefix.unwrap_or(""), include_deleted], |r| {
                Ok(FileEntry {
                    path: r.get(0)?,
                    size: r.get(1)?,
                    mtime: r.get(2)?,
                    hash: r.get(3)?,
                    deleted: r.get(4)?,
                    chunks: Vec::new(),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn record_snapshot(&self, created_at: i64, msg_id: i32) -> Result<()> {
        self.conn.execute(
            "INSERT INTO snapshots (created_at, msg_id) VALUES (?1, ?2)",
            params![created_at, msg_id],
        )?;
        Ok(())
    }

    /// Full dump for the remote snapshot.
    pub fn export(&self) -> Result<Snapshot> {
        let mut files = Vec::new();
        for f in self.list_files(None, true)? {
            files.push(self.get_file(&f.path)?.expect("file just listed"));
        }
        let mut stmt = self.conn.prepare("SELECT hash, size, msg_id FROM chunks")?;
        let chunks = stmt
            .query_map([], |r| {
                Ok(ChunkEntry {
                    hash: r.get(0)?,
                    size: r.get(1)?,
                    msg_id: r.get(2)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(Snapshot {
            version: 1,
            created_at: now_unix(),
            files,
            chunks,
        })
    }
}

pub fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_index() -> (Index, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "tgfs-test-{}-{}.db",
            std::process::id(),
            now_unix()
        ));
        let _ = std::fs::remove_file(&path);
        (Index::open(&path).unwrap(), path)
    }

    #[test]
    fn file_roundtrip_and_dedup() {
        let (mut index, path) = temp_index();
        index
            .insert_chunk(&ChunkEntry {
                hash: "aa".into(),
                size: 10,
                msg_id: 1,
            })
            .unwrap();
        index
            .insert_chunk(&ChunkEntry {
                hash: "bb".into(),
                size: 5,
                msg_id: 2,
            })
            .unwrap();
        let entry = FileEntry {
            path: "photos/a.jpg".into(),
            size: 15,
            mtime: 100,
            hash: "filehash".into(),
            deleted: false,
            chunks: vec!["aa".into(), "bb".into()],
        };
        index.upsert_file(&entry).unwrap();

        let got = index.get_file("photos/a.jpg").unwrap().unwrap();
        assert_eq!(got.chunks, vec!["aa".to_string(), "bb".to_string()]);
        assert_eq!(got.size, 15);
        assert!(index.chunk("aa").unwrap().is_some());
        assert!(index.chunk("zz").unwrap().is_none());

        // Re-upserting with fewer chunks replaces the chunk list.
        let entry2 = FileEntry {
            chunks: vec!["bb".into()],
            mtime: 200,
            ..entry
        };
        index.upsert_file(&entry2).unwrap();
        let got = index.get_file("photos/a.jpg").unwrap().unwrap();
        assert_eq!(got.chunks, vec!["bb".to_string()]);
        assert_eq!(got.mtime, 200);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn tombstone_and_export() {
        let (mut index, path) = temp_index();
        for name in ["docs/a.txt", "docs/b.txt"] {
            index
                .upsert_file(&FileEntry {
                    path: name.into(),
                    size: 1,
                    mtime: 1,
                    hash: "h".into(),
                    deleted: false,
                    chunks: vec![],
                })
                .unwrap();
        }
        let n = index
            .tombstone_missing("docs/", &["docs/a.txt".to_string()])
            .unwrap();
        assert_eq!(n, 1);
        let visible = index.list_files(Some("docs/"), false).unwrap();
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].path, "docs/a.txt");

        let snapshot = index.export().unwrap();
        assert_eq!(snapshot.files.len(), 2); // export includes tombstones
        assert!(snapshot.files.iter().any(|f| f.deleted));

        let _ = std::fs::remove_file(path);
    }
}
