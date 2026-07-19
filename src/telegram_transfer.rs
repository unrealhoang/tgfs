//! Telegram document upload and download transport.

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use grammers_client::media::{Document, Media};
use grammers_client::message::InputMessage;
use grammers_client::session::types::PeerRef;
use grammers_client::tl;
use tokio::io::AsyncRead;

use crate::tg::Tg;

/// Telegram upload part size. Must be a power of two ≤ 512 KiB.
pub const PART_SIZE: u64 = 512 * 1024;
/// Above this, Telegram requires the resumable big-file upload path.
pub const BIG_FILE_THRESHOLD: u64 = 10 * 1024 * 1024;
const UPLOAD_WORKERS: usize = 4;

/// Random-access byte source for the resumable uploader.
pub trait PartSource: Send + Sync {
    fn len(&self) -> u64;
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

impl Tg {
    /// Upload `size` bytes as a named document. Returns the message id.
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
        let message = self
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
        Ok(message.id())
    }

    /// Upload a source, using resumable parallel parts for large files.
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

    /// Resolve one document message. Callers restoring packed files can cache
    /// this value so all members of a pack share one message lookup.
    pub async fn document(&self, peer: PeerRef, msg_id: i32) -> Result<Document> {
        let messages = self.client.get_messages_by_id(peer, &[msg_id]).await?;
        let message = messages
            .into_iter()
            .next()
            .flatten()
            .with_context(|| format!("message {msg_id} not found in storage channel"))?;
        let media = message
            .media()
            .with_context(|| format!("message {msg_id} has no media"))?;
        match media {
            Media::Document(document) => Ok(document),
            other => bail!("message {msg_id} is not a document: {other:?}"),
        }
    }

    /// Write exactly `len` stored bytes beginning at `offset` in `document`.
    /// Telegram requests remain aligned to grammers' default 512 KiB chunks.
    pub async fn download_range<W: std::io::Write>(
        &self,
        document: &Document,
        offset: u64,
        len: u64,
        out: &mut W,
    ) -> Result<u64> {
        offset
            .checked_add(len)
            .context("ranged download offset overflow")?;
        let first_chunk = i32::try_from(offset / PART_SIZE)
            .context("ranged download offset exceeds Telegram's limits")?;
        let mut leading = (offset % PART_SIZE) as usize;
        let mut remaining = len;
        let mut written = 0u64;
        let mut download = self.client.iter_download(document).skip_chunks(first_chunk);
        while remaining > 0 {
            let Some(chunk) = download.next().await.context("ranged download failed")? else {
                break;
            };
            if leading >= chunk.len() {
                leading -= chunk.len();
                continue;
            }
            let available = &chunk[leading..];
            leading = 0;
            let take = remaining.min(available.len() as u64) as usize;
            out.write_all(&available[..take])?;
            remaining -= take as u64;
            written += take as u64;
        }
        Ok(written)
    }

    /// Stream a whole document into `out`, returning the number of bytes written.
    pub async fn download_document<W: std::io::Write>(
        &self,
        peer: PeerRef,
        msg_id: i32,
        out: &mut W,
    ) -> Result<u64> {
        let document = self.document(peer, msg_id).await?;
        let mut total = 0u64;
        let mut download = self.client.iter_download(&document);
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
}
