//! Persistent grammers session backed by the same `rusqlite` library as the
//! local tgfs index. Keeping a single SQLite implementation in the process
//! avoids the symbol and global-configuration clash between bundled sqlite3
//! copies from `rusqlite` and `libsql`.

use std::collections::HashMap;
use std::fmt;
use std::net::AddrParseError;
use std::path::Path;
use std::sync::{Mutex, MutexGuard};

use grammers_session::types::{
    ChannelKind, ChannelState, DcOption, PeerAuth, PeerId, PeerInfo, PeerKind, UpdateState,
    UpdatesState,
};
use grammers_session::{BoxFuture, Session, SessionData};
use rusqlite::{Connection, OptionalExtension, params};
use rusqlite_from_row::FromRow;

const VERSION: i64 = 1;

const USER_SELF: i64 = 1;
const USER_BOT: i64 = 2;
const MEGAGROUP: i64 = 4;
const BROADCAST: i64 = 8;
const GIGAGROUP: i64 = MEGAGROUP | BROADCAST;

struct Cache {
    home_dc: i32,
    dc_options: HashMap<i32, DcOption>,
}

#[derive(FromRow)]
struct DcOptionRow {
    #[from_row(rename = "dc_id")]
    id: i32,
    ipv4: String,
    ipv6: String,
    auth_key: Option<Vec<u8>>,
}

#[derive(FromRow)]
struct PeerRow {
    peer_id: i64,
    hash: Option<i64>,
    subtype: Option<i64>,
}

#[derive(FromRow)]
struct UpdatesStateRow {
    pts: i32,
    qts: i32,
    date: i32,
    seq: i32,
}

#[derive(FromRow)]
struct ChannelStateRow {
    #[from_row(rename = "peer_id")]
    id: i64,
    pts: i32,
}

/// A file-backed grammers session using tgfs's existing SQLite dependency.
pub struct FileSession {
    database: Mutex<Connection>,
    cache: Mutex<Cache>,
}

#[derive(Debug)]
pub enum FileSessionError {
    Poisoned,
    Address(AddrParseError),
    Sql(rusqlite::Error),
    InvalidAuthKeyLength(usize),
    UnsupportedVersion(i64),
}

impl fmt::Display for FileSessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Poisoned => write!(f, "session lock is poisoned"),
            Self::Address(_) => write!(f, "invalid datacenter address in session"),
            Self::Sql(error) => error.fmt(f),
            Self::InvalidAuthKeyLength(length) => {
                write!(f, "invalid auth key length: expected 256, got {length}")
            }
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported session database version {version}")
            }
        }
    }
}

impl std::error::Error for FileSessionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Address(error) => Some(error),
            Self::Sql(error) => Some(error),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for FileSessionError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sql(error)
    }
}

impl From<AddrParseError> for FileSessionError {
    fn from(error: AddrParseError) -> Self {
        Self::Address(error)
    }
}

impl FileSession {
    /// Opens an existing grammers SQLite session or creates a new one.
    ///
    /// The schema deliberately matches grammers-session 0.10's
    /// `SqliteSession`, so upgrading does not invalidate an existing login.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, FileSessionError> {
        let connection = Connection::open(path)?;
        Self::initialize(&connection)?;

        let defaults = SessionData::default();
        let home_dc = connection
            .query_row("SELECT dc_id FROM dc_home LIMIT 1", [], |row| row.get(0))
            .optional()?
            .unwrap_or(defaults.home_dc);
        let mut dc_options = defaults.dc_options;

        {
            let mut statement =
                connection.prepare("SELECT dc_id, ipv4, ipv6, auth_key FROM dc_option")?;
            let rows = statement
                .query_map([], DcOptionRow::try_from_row)?
                .collect::<Result<Vec<_>, _>>()?;
            for row in rows {
                let auth_key = row
                    .auth_key
                    .map(|key| {
                        let length = key.len();
                        key.try_into()
                            .map_err(|_| FileSessionError::InvalidAuthKeyLength(length))
                    })
                    .transpose()?;
                let option = DcOption {
                    id: row.id,
                    ipv4: row.ipv4.parse()?,
                    ipv6: row.ipv6.parse()?,
                    auth_key,
                };
                dc_options.insert(option.id, option);
            }
        }

        Ok(Self {
            database: Mutex::new(connection),
            cache: Mutex::new(Cache {
                home_dc,
                dc_options,
            }),
        })
    }

    fn initialize(connection: &Connection) -> Result<(), FileSessionError> {
        let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        match version {
            VERSION => Ok(()),
            0 => {
                connection.execute_batch(
                    "BEGIN;
                     CREATE TABLE dc_home (
                         dc_id INTEGER NOT NULL PRIMARY KEY
                     );
                     CREATE TABLE dc_option (
                         dc_id INTEGER NOT NULL PRIMARY KEY,
                         ipv4 TEXT NOT NULL,
                         ipv6 TEXT NOT NULL,
                         auth_key BLOB
                     );
                     CREATE TABLE peer_info (
                         peer_id INTEGER NOT NULL PRIMARY KEY,
                         hash INTEGER,
                         subtype INTEGER
                     );
                     CREATE TABLE update_state (
                         pts INTEGER NOT NULL,
                         qts INTEGER NOT NULL,
                         date INTEGER NOT NULL,
                         seq INTEGER NOT NULL
                     );
                     CREATE TABLE channel_state (
                         peer_id INTEGER NOT NULL PRIMARY KEY,
                         pts INTEGER NOT NULL
                     );
                     PRAGMA user_version = 1;
                     COMMIT;",
                )?;
                Ok(())
            }
            other => Err(FileSessionError::UnsupportedVersion(other)),
        }
    }

    fn database(&self) -> Result<MutexGuard<'_, Connection>, FileSessionError> {
        self.database.lock().map_err(|_| FileSessionError::Poisoned)
    }

    fn cache(&self) -> Result<MutexGuard<'_, Cache>, FileSessionError> {
        self.cache.lock().map_err(|_| FileSessionError::Poisoned)
    }

    fn query_peer(
        connection: &Connection,
        peer: PeerId,
    ) -> Result<Option<PeerInfo>, FileSessionError> {
        let map = |row: &rusqlite::Row<'_>| -> rusqlite::Result<PeerInfo> {
            let row = PeerRow::try_from_row(row)?;
            let auth = row.hash.map(PeerAuth::from_hash);
            Ok(match peer.kind() {
                PeerKind::User => PeerInfo::User {
                    id: PeerId::from_bot_api_dialog_id(row.peer_id)
                        .expect("stored peer ID was validated when cached")
                        .bare_id()
                        .expect("stored peer cannot be the self sentinel"),
                    auth,
                    bot: row.subtype.map(|value| value & USER_BOT != 0),
                    is_self: row.subtype.map(|value| value & USER_SELF != 0),
                },
                PeerKind::Chat => PeerInfo::Chat {
                    id: peer.bare_id().expect("chat peer has a bare ID"),
                },
                PeerKind::Channel => PeerInfo::Channel {
                    id: peer.bare_id().expect("channel peer has a bare ID"),
                    auth,
                    kind: row.subtype.and_then(|value| {
                        if value & GIGAGROUP == GIGAGROUP {
                            Some(ChannelKind::Gigagroup)
                        } else if value & BROADCAST != 0 {
                            Some(ChannelKind::Broadcast)
                        } else if value & MEGAGROUP != 0 {
                            Some(ChannelKind::Megagroup)
                        } else {
                            None
                        }
                    }),
                },
            })
        };

        if let Some(peer_id) = peer.bot_api_dialog_id() {
            connection
                .query_row(
                    "SELECT peer_id, hash, subtype FROM peer_info WHERE peer_id = ?1 LIMIT 1",
                    [peer_id],
                    map,
                )
                .optional()
                .map_err(Into::into)
        } else {
            connection
                .query_row(
                    "SELECT peer_id, hash, subtype FROM peer_info \
                     WHERE subtype & ?1 != 0 LIMIT 1",
                    [USER_SELF],
                    map,
                )
                .optional()
                .map_err(Into::into)
        }
    }
}

impl Session for FileSession {
    type Error = FileSessionError;

    fn home_dc_id(&self) -> Result<i32, Self::Error> {
        Ok(self.cache()?.home_dc)
    }

    fn set_home_dc_id(&self, dc_id: i32) -> BoxFuture<'_, Result<(), Self::Error>> {
        Box::pin(async move {
            let mut connection = self.database()?;
            let transaction = connection.transaction()?;
            transaction.execute("DELETE FROM dc_home", [])?;
            transaction.execute("INSERT INTO dc_home (dc_id) VALUES (?1)", [dc_id])?;
            transaction.commit()?;
            self.cache()?.home_dc = dc_id;
            Ok(())
        })
    }

    fn dc_option(&self, dc_id: i32) -> Result<Option<DcOption>, Self::Error> {
        Ok(self.cache()?.dc_options.get(&dc_id).cloned())
    }

    fn set_dc_option(&self, dc_option: &DcOption) -> BoxFuture<'_, Result<(), Self::Error>> {
        let dc_option = dc_option.clone();
        Box::pin(async move {
            self.database()?.execute(
                "INSERT OR REPLACE INTO dc_option (dc_id, ipv4, ipv6, auth_key) \
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    dc_option.id,
                    dc_option.ipv4.to_string(),
                    dc_option.ipv6.to_string(),
                    dc_option.auth_key.map(|key| key.to_vec()),
                ],
            )?;
            self.cache()?.dc_options.insert(dc_option.id, dc_option);
            Ok(())
        })
    }

    fn peer(&self, peer: PeerId) -> BoxFuture<'_, Result<Option<PeerInfo>, Self::Error>> {
        Box::pin(async move {
            let connection = self.database()?;
            Self::query_peer(&connection, peer)
        })
    }

    fn cache_peer(&self, peer: &PeerInfo) -> BoxFuture<'_, Result<(), Self::Error>> {
        let peer = peer.clone();
        Box::pin(async move {
            let connection = self.database()?;
            let mut merged = Self::query_peer(&connection, peer.id())?.unwrap_or(peer.clone());
            merged.extend_info(&peer);

            let (hash, subtype) = match &merged {
                PeerInfo::User {
                    auth, bot, is_self, ..
                } => {
                    let subtype = match (bot, is_self) {
                        (None, None) => None,
                        _ => Some(
                            (i64::from(bot.unwrap_or(false)) * USER_BOT)
                                | (i64::from(is_self.unwrap_or(false)) * USER_SELF),
                        ),
                    };
                    (auth.map(PeerAuth::hash), subtype)
                }
                PeerInfo::Chat { .. } => (None, None),
                PeerInfo::Channel { auth, kind, .. } => {
                    let subtype = kind.map(|kind| match kind {
                        ChannelKind::Megagroup => MEGAGROUP,
                        ChannelKind::Broadcast => BROADCAST,
                        ChannelKind::Gigagroup => GIGAGROUP,
                    });
                    (auth.map(PeerAuth::hash), subtype)
                }
            };
            connection.execute(
                "INSERT OR REPLACE INTO peer_info (peer_id, hash, subtype) VALUES (?1, ?2, ?3)",
                params![
                    merged
                        .id()
                        .bot_api_dialog_id()
                        .expect("cached peers always have a concrete ID"),
                    hash,
                    subtype,
                ],
            )?;
            Ok(())
        })
    }

    fn updates_state(&self) -> BoxFuture<'_, Result<UpdatesState, Self::Error>> {
        Box::pin(async move {
            let connection = self.database()?;
            let mut state = connection
                .query_row(
                    "SELECT pts, qts, date, seq FROM update_state LIMIT 1",
                    [],
                    |row| {
                        let row = UpdatesStateRow::try_from_row(row)?;
                        Ok(UpdatesState {
                            pts: row.pts,
                            qts: row.qts,
                            date: row.date,
                            seq: row.seq,
                            channels: Vec::new(),
                        })
                    },
                )
                .optional()?
                .unwrap_or_default();
            let mut statement = connection.prepare("SELECT peer_id, pts FROM channel_state")?;
            state.channels = statement
                .query_map([], ChannelStateRow::try_from_row)?
                .map(|row| {
                    row.map(|row| ChannelState {
                        id: row.id,
                        pts: row.pts,
                    })
                })
                .collect::<Result<_, _>>()?;
            Ok(state)
        })
    }

    fn set_update_state(&self, update: UpdateState) -> BoxFuture<'_, Result<(), Self::Error>> {
        Box::pin(async move {
            let mut connection = self.database()?;
            let transaction = connection.transaction()?;
            match update {
                UpdateState::All(state) => {
                    transaction.execute("DELETE FROM update_state", [])?;
                    transaction.execute(
                        "INSERT INTO update_state (pts, qts, date, seq) VALUES (?1, ?2, ?3, ?4)",
                        params![state.pts, state.qts, state.date, state.seq],
                    )?;
                    transaction.execute("DELETE FROM channel_state", [])?;
                    for channel in state.channels {
                        transaction.execute(
                            "INSERT INTO channel_state (peer_id, pts) VALUES (?1, ?2)",
                            params![channel.id, channel.pts],
                        )?;
                    }
                }
                UpdateState::Primary { pts, date, seq } => {
                    let changed = transaction.execute(
                        "UPDATE update_state SET pts = ?1, date = ?2, seq = ?3",
                        params![pts, date, seq],
                    )?;
                    if changed == 0 {
                        transaction.execute(
                            "INSERT INTO update_state (pts, qts, date, seq) VALUES (?1, 0, ?2, ?3)",
                            params![pts, date, seq],
                        )?;
                    }
                }
                UpdateState::Secondary { qts } => {
                    let changed = transaction.execute("UPDATE update_state SET qts = ?1", [qts])?;
                    if changed == 0 {
                        transaction.execute(
                            "INSERT INTO update_state (pts, qts, date, seq) VALUES (0, ?1, 0, 0)",
                            [qts],
                        )?;
                    }
                }
                UpdateState::Channel { id, pts } => {
                    transaction.execute(
                        "INSERT OR REPLACE INTO channel_state (peer_id, pts) VALUES (?1, ?2)",
                        params![id, pts],
                    )?;
                }
            }
            transaction.commit()?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn session_round_trip() {
        let session = FileSession::open(":memory:").unwrap();
        let default_dc = session.home_dc_id().unwrap();
        session.set_home_dc_id(default_dc + 1).await.unwrap();
        assert_eq!(session.home_dc_id().unwrap(), default_dc + 1);

        let me = PeerInfo::User {
            id: 42,
            auth: Some(PeerAuth::from_hash(1234)),
            bot: Some(false),
            is_self: Some(true),
        };
        session.cache_peer(&me).await.unwrap();
        assert_eq!(session.peer(PeerId::self_user()).await.unwrap(), Some(me));

        session
            .set_update_state(UpdateState::All(UpdatesState {
                pts: 1,
                qts: 2,
                date: 3,
                seq: 4,
                channels: vec![ChannelState { id: 5, pts: 6 }],
            }))
            .await
            .unwrap();
        assert_eq!(
            session.updates_state().await.unwrap(),
            UpdatesState {
                pts: 1,
                qts: 2,
                date: 3,
                seq: 4,
                channels: vec![ChannelState { id: 5, pts: 6 }],
            }
        );
    }
}
