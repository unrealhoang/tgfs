//! Remote index snapshot encoding, transport, and local import.

use anyhow::{Context, Result, bail};
use grammers_client::session::types::PeerRef;
use grammers_client::tl;

use crate::context::RepoContext;
use crate::crypto::Crypto;
use crate::index::{Index, Snapshot};
use crate::tg::Tg;

#[derive(Debug, Clone, Copy)]
pub struct RemoteIndexInfo {
    pub version: u64,
    pub msg_id: i32,
}

impl Tg {
    /// Read snapshot metadata from the channel's pinned message.
    pub async fn remote_index_info(&self, peer: PeerRef) -> Result<Option<RemoteIndexInfo>> {
        let pinned = match self.client.get_pinned_message(peer).await {
            Ok(pinned) => pinned,
            Err(grammers_client::InvocationError::Rpc(error)) if error.is("MESSAGE_IDS_EMPTY") => {
                None
            }
            Err(error) => return Err(error).context("failed to fetch pinned message"),
        };
        let Some(message) = pinned else {
            return Ok(None);
        };
        Ok(
            parse_caption(message.text()).map(|version| RemoteIndexInfo {
                version,
                msg_id: message.id(),
            }),
        )
    }

    /// Every currently pinned message in the channel that is a tgfs index
    /// snapshot. `remote_index_info` only sees the topmost pin, so this is
    /// what push uses to clean up the pins left by earlier versions.
    pub async fn pinned_indices(&self, peer: PeerRef) -> Result<Vec<RemoteIndexInfo>> {
        let mut search = self
            .client
            .search_messages(peer)
            .filter(tl::enums::MessagesFilter::InputMessagesFilterPinned);
        let mut found = Vec::new();
        while let Some(message) = search
            .next()
            .await
            .context("failed to list pinned messages")?
        {
            if let Some(version) = parse_caption(message.text()) {
                found.push(RemoteIndexInfo {
                    version,
                    msg_id: message.id(),
                });
            }
        }
        Ok(found)
    }
}

fn caption(version: u64, files: usize, chunks: usize, created_at: i64) -> String {
    format!("tgfs-index v{version} files={files} chunks={chunks} created={created_at}")
}

fn parse_caption(text: &str) -> Option<u64> {
    let rest = text.strip_prefix("tgfs-index v")?;
    let end = rest
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}

/// A downloaded snapshot, decrypted and ready to import.
pub struct FetchedSnapshot {
    pub info: RemoteIndexInfo,
    /// Zstd-compressed snapshot JSON (already decrypted).
    pub plain: Vec<u8>,
    pub encrypted: bool,
}

/// Serialize, compress, optionally encrypt, upload, and pin the local index.
pub async fn publish(context: &RepoContext, crypto: Option<&Crypto>) -> Result<()> {
    let peer = context.tg.peer(&context.repo.config)?;
    let dump = context.index.export()?;
    let json = serde_json::to_vec(&dump)?;
    let mut compressed = zstd::encode_all(json.as_slice(), 9)?;
    let mut name = format!("tgfs-index-v{}.json.zst", dump.version);
    if let Some(crypto) = crypto {
        let nonce_context = *blake3::hash(&compressed).as_bytes();
        compressed = crypto.seal_blob(&nonce_context, &compressed)?;
        name.push_str(".enc");
    }
    let caption = caption(
        dump.version,
        dump.files.len(),
        dump.chunks.len(),
        dump.created_at,
    );
    let mut cursor = std::io::Cursor::new(&compressed);
    let msg_id = context
        .tg
        .upload_document(peer, &mut cursor, compressed.len(), name, &caption)
        .await?;
    context.tg.pin(peer, msg_id).await?;
    context
        .index
        .record_snapshot(dump.version, dump.created_at, msg_id)?;
    println!(
        "index snapshot v{} pinned (message {msg_id}, {} bytes)",
        dump.version,
        compressed.len()
    );
    // Best effort: the snapshot is already published and recorded, so leftover
    // pins are cosmetic — never fail a finished push over them.
    match unpin_previous(&context.tg, peer, msg_id).await {
        Ok(0) => {}
        Ok(count) => println!("unpinned {count} previous index snapshot(s)"),
        Err(error) => eprintln!("warning: could not unpin previous index snapshots: {error:#}"),
    }
    Ok(())
}

/// Unpin the index snapshots of earlier pushes so the channel keeps exactly
/// one pinned index. The old messages themselves stay in the channel for
/// point-in-time restore. Pinning the new snapshot before unpinning the old
/// ones means an interruption can only leave extra pins behind, never none.
async fn unpin_previous(tg: &Tg, peer: PeerRef, keep: i32) -> Result<usize> {
    let mut unpinned = 0;
    for info in tg.pinned_indices(peer).await? {
        if info.msg_id == keep {
            continue;
        }
        tg.unpin(peer, info.msg_id).await?;
        unpinned += 1;
    }
    Ok(unpinned)
}

/// Download the pinned snapshot and decrypt it when necessary.
pub async fn fetch(
    tg: &Tg,
    peer: PeerRef,
    key: Option<&[u8; 32]>,
) -> Result<Option<FetchedSnapshot>> {
    let Some(info) = tg.remote_index_info(peer).await? else {
        return Ok(None);
    };
    Ok(Some(download(tg, peer, info, key).await?))
}

/// Download a known snapshot and decrypt it when necessary.
pub async fn download(
    tg: &Tg,
    peer: PeerRef,
    info: RemoteIndexInfo,
    key: Option<&[u8; 32]>,
) -> Result<FetchedSnapshot> {
    let mut raw = Vec::new();
    tg.download_document(peer, info.msg_id, &mut raw).await?;

    if !Crypto::is_sealed_blob(&raw) {
        if key.is_some() {
            println!("note: an encryption key was supplied but the snapshot is not encrypted");
        }
        return Ok(FetchedSnapshot {
            info,
            plain: raw,
            encrypted: false,
        });
    }

    let key = key.context("the remote snapshot is encrypted; supply --key or --keyfile")?;
    let plain = Crypto::new(key)
        .open_blob(&raw)
        .context("cannot decrypt the remote index snapshot")?;
    Ok(FetchedSnapshot {
        info,
        plain,
        encrypted: true,
    })
}

/// Decode and verify a snapshot, then replace the local index with it.
pub fn import(index: &mut Index, plain: &[u8], remote: RemoteIndexInfo) -> Result<()> {
    let json = zstd::decode_all(plain).context("snapshot is not valid zstd")?;
    let snapshot: Snapshot =
        serde_json::from_slice(&json).context("snapshot is not a valid tgfs index")?;
    if snapshot.version != remote.version {
        bail!(
            "pinned caption says v{} but snapshot contains v{} — refusing to import",
            remote.version,
            snapshot.version
        );
    }
    index.import(&snapshot)?;
    index.record_snapshot(snapshot.version, snapshot.created_at, remote.msg_id)?;
    println!(
        "pulled index v{} ({} files, {} chunks) — \
         local files are untouched; use `tgfs get` to restore",
        snapshot.version,
        snapshot.files.len(),
        snapshot.chunks.len()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caption_roundtrip() {
        let caption = caption(42, 10, 12, 1_752_000_000);
        assert_eq!(parse_caption(&caption), Some(42));
        assert_eq!(parse_caption("tgfs-index v7"), Some(7));
        assert_eq!(parse_caption("something else"), None);
        assert_eq!(parse_caption("tgfs-index vX"), None);
    }
}
