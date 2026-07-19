//! Thin wrapper around grammers: connect, log in, and move chunk-sized
//! documents in and out of the private storage channel.

use std::io::Write as _;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use grammers_client::session::Session as _;
use grammers_client::session::storages::SqliteSession;
use grammers_client::session::types::{PeerAuth, PeerId, PeerRef};
use grammers_client::message::InputMessage;
use grammers_client::{Client, SenderPool, SignInError, media::Media, tl};
use tokio::io::AsyncRead;

use crate::config::{RepoConfig, session_path};

/// Telegram upload part size. Must be a power of two ≤ 512 KiB.
pub const PART_SIZE: u64 = 512 * 1024;
/// Above this, Telegram requires the big-file upload path (which is also the
/// resumable one).
pub const BIG_FILE_THRESHOLD: u64 = 10 * 1024 * 1024;
const UPLOAD_WORKERS: usize = 4;

/// Retry policy tuned for unattended backups: sleep out flood waits up to
/// 30 minutes (Telegram tells us how long), retry a few times on transient
/// I/O errors with exponential backoff.
struct BackupRetry;

impl grammers_client::client::RetryPolicy for BackupRetry {
    fn should_retry(
        &self,
        ctx: &grammers_client::client::RetryContext,
    ) -> std::ops::ControlFlow<(), std::time::Duration> {
        use std::ops::ControlFlow;
        use std::time::Duration;
        match &ctx.error {
            grammers_client::InvocationError::Rpc(err) if err.code == 420 => {
                let secs = err.value.unwrap_or(1) as u64;
                if ctx.fail_count.get() <= 5 && secs <= 30 * 60 {
                    tracing::warn!("flood wait: sleeping {secs}s before retrying");
                    ControlFlow::Continue(Duration::from_secs(secs + 1))
                } else {
                    ControlFlow::Break(())
                }
            }
            grammers_client::InvocationError::Io(_) if ctx.fail_count.get() <= 5 => {
                ControlFlow::Continue(Duration::from_secs(1 << ctx.fail_count.get().min(5)))
            }
            _ => ControlFlow::Break(()),
        }
    }
}

/// Random-access byte source for the resumable uploader. Implementations
/// must be cheap to read from multiple tasks at once (pread-style).
pub trait PartSource: Send + Sync {
    /// Total number of bytes this source will upload.
    fn len(&self) -> u64;
    /// Fill `buf` exactly, starting at `offset`.
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()>;

    #[allow(dead_code)]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn read_part(&self, part: i32) -> Result<Vec<u8>> {
        let offset = part as u64 * PART_SIZE;
        let len = PART_SIZE.min(self.len() - offset) as usize;
        let mut buf = vec![0u8; len];
        self.read_at(offset, &mut buf)?;
        Ok(buf)
    }
}

pub fn total_parts(len: u64) -> i32 {
    len.div_ceil(PART_SIZE) as i32
}

pub struct Tg {
    pub client: Client,
    session: Arc<SqliteSession>,
}

impl Tg {
    /// Connect using the stored session. Does not log in by itself.
    pub async fn connect(api_id: i32) -> Result<Self> {
        let session = Arc::new(
            SqliteSession::open(session_path()?)
                .await
                .map_err(|e| anyhow::anyhow!("cannot open session store: {e}"))?,
        );
        let pool = SenderPool::new(Arc::clone(&session), api_id);
        let client = Client::with_configuration(
            pool.handle,
            grammers_client::client::ClientConfiguration {
                retry_policy: Box::new(BackupRetry),
                ..Default::default()
            },
        );
        tokio::spawn(pool.runner.run());
        Ok(Self { client, session })
    }

    /// Interactive login: phone → code → optional 2FA password.
    pub async fn login_interactive(&self, api_hash: &str) -> Result<()> {
        if self.client.is_authorized().await? {
            let me = self.client.get_me().await?;
            println!(
                "already logged in as {}",
                me.full_name()
            );
            return Ok(());
        }
        let phone = prompt("Phone number (international format, e.g. +84...): ")?;
        let token = self
            .client
            .request_login_code(phone.trim(), api_hash)
            .await
            .context("failed to request login code")?;
        let code = prompt("Login code: ")?;
        match self.client.sign_in(&token, code.trim()).await {
            Ok(user) => {
                println!("signed in as {}", user.full_name());
            }
            Err(SignInError::PasswordRequired(password_token)) => {
                let hint = password_token.hint().unwrap_or("none");
                let password =
                    rpassword::prompt_password(format!("2FA password (hint: {hint}): "))?;
                let user = self
                    .client
                    .check_password(password_token, password.trim())
                    .await
                    .map_err(|e| anyhow::anyhow!("2FA check failed: {e}"))?;
                println!("signed in as {}", user.full_name());
            }
            Err(e) => bail!("sign in failed: {e}"),
        }
        Ok(())
    }

    /// List channels in the account's dialogs whose title starts with
    /// `prefix` (pass `"tgfs-"` for tgfs storage channels).
    pub async fn list_channels(&self, prefix: &str) -> Result<Vec<ChannelInfo>> {
        let mut found = Vec::new();
        let mut dialogs = self.client.iter_dialogs();
        while let Some(dialog) = dialogs.next().await? {
            if let grammers_client::peer::Peer::Channel(channel) = dialog.peer()
                && channel.title().starts_with(prefix)
            {
                let id = channel.id().bare_id().context("channel id")?;
                let access_hash = self
                    .session
                    .peer_ref(channel.id())
                    .await
                    .map_err(|e| anyhow::anyhow!("session error: {e}"))?
                    .map(|r| r.auth.hash())
                    .unwrap_or_default();
                found.push(ChannelInfo {
                    id,
                    access_hash,
                    title: channel.title().to_string(),
                });
            }
        }
        Ok(found)
    }

    /// Find a channel by exact title among the account's dialogs.
    pub async fn find_channel(&self, title: &str) -> Result<Option<ChannelInfo>> {
        Ok(self
            .list_channels(title)
            .await?
            .into_iter()
            .find(|c| c.title == title))
    }

    /// Find the storage channel among the dialogs, or create a new private
    /// broadcast channel. Returns `(channel_id, access_hash, existed)`.
    pub async fn ensure_channel(&self, title: &str) -> Result<(i64, i64, bool)> {
        if let Some(existing) = self.find_channel(title).await? {
            println!("using existing channel {title:?} ({})", existing.id);
            return Ok((existing.id, existing.access_hash, true));
        }

        let updates = self
            .client
            .invoke(&tl::functions::channels::CreateChannel {
                broadcast: true,
                megagroup: false,
                for_import: false,
                forum: false,
                title: title.to_string(),
                about: "tgfs backup storage — do not delete messages".to_string(),
                geo_point: None,
                address: None,
                ttl_period: None,
            })
            .await
            .context("failed to create storage channel")?;
        let chats = match updates {
            tl::enums::Updates::Updates(u) => u.chats,
            tl::enums::Updates::Combined(u) => u.chats,
            _ => bail!("unexpected response to CreateChannel"),
        };
        for chat in chats {
            if let tl::enums::Chat::Channel(c) = chat {
                println!("created private channel {title:?} ({})", c.id);
                return Ok((c.id, c.access_hash.unwrap_or_default(), false));
            }
        }
        bail!("CreateChannel response contained no channel")
    }

    pub fn peer(&self, config: &RepoConfig) -> Result<PeerRef> {
        self.peer_from(config.channel_id, config.channel_access_hash)
    }

    pub fn peer_from(&self, channel_id: i64, access_hash: i64) -> Result<PeerRef> {
        Ok(PeerRef {
            id: PeerId::channel(channel_id).context("invalid channel id")?,
            auth: PeerAuth::from_hash(access_hash),
        })
    }

    /// Upload `size` bytes from `stream` as a document named `name`, with
    /// `caption` for human browsing. Returns the message id.
    pub async fn upload_document<S: AsyncRead + Unpin>(
        &self,
        peer: PeerRef,
        stream: &mut S,
        size: usize,
        name: String,
        caption: &str,
    ) -> Result<i32> {
        let uploaded = self
            .client
            .upload_stream(stream, size, name)
            .await
            .context("upload failed")?;
        self.send_uploaded(peer, uploaded, caption).await
    }

    async fn send_uploaded(
        &self,
        peer: PeerRef,
        uploaded: grammers_client::media::Uploaded,
        caption: &str,
    ) -> Result<i32> {
        let msg = self
            .client
            .send_message(
                peer,
                InputMessage::new()
                    .text(caption)
                    .mime_type("application/octet-stream")
                    .document(uploaded),
            )
            .await
            .context("failed to send document message")?;
        Ok(msg.id())
    }

    /// Upload a [`PartSource`] as a document and send it. Files above
    /// [`BIG_FILE_THRESHOLD`] go through Telegram's big-file path with
    /// [`UPLOAD_WORKERS`] parallel part uploads and are resumable:
    /// `resume` continues a previous attempt (same Telegram `file_id`), and
    /// `on_progress(file_id, done_parts)` is called as the contiguous prefix
    /// of confirmed parts grows, so the caller can journal it.
    pub async fn upload_source(
        &self,
        peer: PeerRef,
        source: Arc<dyn PartSource>,
        name: String,
        caption: &str,
        resume: Option<(i64, i32)>,
        mut on_progress: impl FnMut(i64, i32) -> Result<()>,
    ) -> Result<i32> {
        let len = source.len();
        if len == 0 {
            bail!("refusing to upload an empty document");
        }

        if len <= BIG_FILE_THRESHOLD {
            let mut buf = vec![0u8; len as usize];
            source.read_at(0, &mut buf)?;
            let mut cursor = std::io::Cursor::new(buf);
            let uploaded = self
                .client
                .upload_stream(&mut cursor, len as usize, name)
                .await
                .context("upload failed")?;
            return self.send_uploaded(peer, uploaded, caption).await;
        }

        let total = total_parts(len);
        let (file_id, start) = match resume {
            Some((id, done)) => {
                println!("    resuming upload at part {done}/{total}");
                (id, done)
            }
            None => (rand::random::<i64>(), 0),
        };
        on_progress(file_id, start)?;

        // Workers pull part numbers from a shared counter and report each
        // confirmed part back; this task tracks the contiguous prefix.
        let next = Arc::new(std::sync::atomic::AtomicI32::new(start));
        let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut workers = tokio::task::JoinSet::new();
        for _ in 0..UPLOAD_WORKERS {
            let client = self.client.clone();
            let source = Arc::clone(&source);
            let next = Arc::clone(&next);
            let done_tx = done_tx.clone();
            workers.spawn(async move {
                loop {
                    let part = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if part >= total {
                        return Ok(());
                    }
                    let bytes = source.read_part(part)?;
                    let ok = client
                        .invoke(&tl::functions::upload::SaveBigFilePart {
                            file_id,
                            file_part: part,
                            file_total_parts: total,
                            bytes,
                        })
                        .await
                        .with_context(|| format!("failed to upload part {part}/{total}"))?;
                    if !ok {
                        bail!("Telegram rejected part {part}/{total}");
                    }
                    if done_tx.send(part).is_err() {
                        return Ok(());
                    }
                }
            });
        }
        drop(done_tx);

        let mut confirmed = std::collections::BTreeSet::new();
        let mut watermark = start;
        while let Some(part) = done_rx.recv().await {
            confirmed.insert(part);
            let mut advanced = false;
            while confirmed.remove(&watermark) {
                watermark += 1;
                advanced = true;
            }
            if advanced {
                on_progress(file_id, watermark)?;
            }
        }
        while let Some(result) = workers.join_next().await {
            result.context("upload worker panicked")??;
        }
        if watermark != total {
            bail!("upload incomplete: {watermark}/{total} parts confirmed");
        }

        let uploaded = grammers_client::media::Uploaded {
            raw: tl::enums::InputFile::Big(tl::types::InputFileBig {
                id: file_id,
                parts: total,
                name,
            }),
        };
        self.send_uploaded(peer, uploaded, caption).await
    }

    /// Stream the document in message `msg_id` into `out`, returning the
    /// number of bytes written.
    pub async fn download_document<W: std::io::Write>(
        &self,
        peer: PeerRef,
        msg_id: i32,
        out: &mut W,
    ) -> Result<u64> {
        let msgs = self.client.get_messages_by_id(peer, &[msg_id]).await?;
        let msg = msgs
            .into_iter()
            .next()
            .flatten()
            .with_context(|| format!("message {msg_id} not found in storage channel"))?;
        let media = msg
            .media()
            .with_context(|| format!("message {msg_id} has no media"))?;
        let doc = match media {
            Media::Document(doc) => doc,
            other => bail!("message {msg_id} is not a document: {other:?}"),
        };
        let mut total = 0u64;
        let mut download = self.client.iter_download(&doc);
        while let Some(chunk) = download
            .next()
            .await
            .with_context(|| format!("download of message {msg_id} failed"))?
        {
            total += chunk.len() as u64;
            out.write_all(&chunk)?;
        }
        Ok(total)
    }

    pub async fn pin(&self, peer: PeerRef, msg_id: i32) -> Result<()> {
        self.client
            .pin_message(peer, msg_id)
            .await
            .context("failed to pin message")
    }

    /// Read the remote index version from the pinned snapshot's caption
    /// (`tgfs-index v<N> ...`) without downloading the snapshot itself.
    /// `None` means the channel has no pinned tgfs-index yet.
    pub async fn remote_index_info(&self, peer: PeerRef) -> Result<Option<RemoteIndexInfo>> {
        let Some(msg) = self
            .client
            .get_pinned_message(peer)
            .await
            .context("failed to fetch pinned message")?
        else {
            return Ok(None);
        };
        Ok(parse_index_caption(msg.text()).map(|version| RemoteIndexInfo {
            version,
            msg_id: msg.id(),
        }))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RemoteIndexInfo {
    pub version: u64,
    pub msg_id: i32,
}

#[derive(Debug, Clone)]
pub struct ChannelInfo {
    pub id: i64,
    pub access_hash: i64,
    pub title: String,
}

/// Caption format written by snapshot uploads: `tgfs-index v<N> ...`.
pub fn index_caption(version: u64, files: usize, chunks: usize, created_at: i64) -> String {
    format!("tgfs-index v{version} files={files} chunks={chunks} created={created_at}")
}

fn parse_index_caption(text: &str) -> Option<u64> {
    let rest = text.strip_prefix("tgfs-index v")?;
    let end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caption_roundtrip() {
        let caption = index_caption(42, 10, 12, 1_752_000_000);
        assert_eq!(parse_index_caption(&caption), Some(42));
        assert_eq!(parse_index_caption("tgfs-index v7"), Some(7));
        assert_eq!(parse_index_caption("something else"), None);
        assert_eq!(parse_index_caption("tgfs-index vX"), None);
    }
}

fn prompt(msg: &str) -> Result<String> {
    print!("{msg}");
    std::io::stdout().flush()?;
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    Ok(line)
}
