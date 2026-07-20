//! Telegram document upload and download transport.

use std::collections::VecDeque;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use grammers_client::media::{Document, Media, Uploaded};
use grammers_client::message::InputMessage;
use grammers_client::session::types::PeerRef;
use grammers_client::tl;
use tokio::io::{AsyncRead, AsyncReadExt as _};

use crate::tg::Tg;

/// Telegram upload part size. Must be a power of two ≤ 512 KiB.
pub const PART_SIZE: u64 = 512 * 1024;
/// Above this, Telegram requires the resumable big-file upload path.
pub const BIG_FILE_THRESHOLD: u64 = 10 * 1024 * 1024;
const INITIAL_UPLOAD_CONCURRENCY: usize = 4;
const UPLOAD_CONCURRENCY_RECOVERY_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(60);
const UPLOAD_QUEUE_CAPACITY: usize = 64;
const MAX_UPLOAD_RETRIES: u32 = 5;
const MAX_FLOOD_WAIT: u64 = 30 * 60;

/// Handle to one background upload scheduler. Callers submit work through the
/// bounded channel and await a per-job acknowledgement; only the scheduler
/// owns retries and concurrency decisions.
#[derive(Clone)]
pub(crate) struct Uploader {
    tx: tokio::sync::mpsc::Sender<UploadJob>,
}

struct UploadJob {
    kind: UploadJobKind,
    failures: u32,
}

enum UploadJobKind {
    BigPart {
        request: tl::functions::upload::SaveBigFilePart,
        ack: tokio::sync::oneshot::Sender<Result<bool>>,
    },
    SmallFile {
        bytes: Vec<u8>,
        name: String,
        ack: tokio::sync::oneshot::Sender<Result<Uploaded>>,
    },
}

enum UploadSuccess {
    BigPart(bool),
    SmallFile(Uploaded),
}

struct UploadFailure {
    error: anyhow::Error,
    flood_wait: Option<u64>,
}

struct UploadControl {
    concurrency: usize,
    resume_at: Option<tokio::time::Instant>,
    recover_at: Option<tokio::time::Instant>,
}

impl UploadControl {
    fn new() -> Self {
        Self {
            concurrency: INITIAL_UPLOAD_CONCURRENCY,
            resume_at: None,
            recover_at: None,
        }
    }

    fn pause_until(&mut self, deadline: tokio::time::Instant) {
        self.resume_at = Some(
            self.resume_at
                .map_or(deadline, |current| current.max(deadline)),
        );
    }

    fn throttle_until(&mut self, deadline: tokio::time::Instant) {
        self.concurrency = 1;
        self.pause_until(deadline);
        self.recover_at = self
            .resume_at
            .map(|resume_at| resume_at + UPLOAD_CONCURRENCY_RECOVERY_INTERVAL);
    }

    fn clear_expired_pause(&mut self, now: tokio::time::Instant) {
        if self.resume_at.is_some_and(|deadline| deadline <= now) {
            self.resume_at = None;
        }
    }

    /// Add one worker after a quiet interval and a successful request. Basing
    /// the next interval on `now` prevents a long idle period from restoring
    /// all four workers at once.
    fn recover_after_success(&mut self, now: tokio::time::Instant) -> Option<usize> {
        if self.concurrency >= INITIAL_UPLOAD_CONCURRENCY
            || self.recover_at.is_none_or(|deadline| deadline > now)
        {
            return None;
        }

        self.concurrency += 1;
        self.recover_at = (self.concurrency < INITIAL_UPLOAD_CONCURRENCY)
            .then_some(now + UPLOAD_CONCURRENCY_RECOVERY_INTERVAL);
        Some(self.concurrency)
    }
}

impl UploadJob {
    fn is_cancelled(&self) -> bool {
        match &self.kind {
            UploadJobKind::BigPart { ack, .. } => ack.is_closed(),
            UploadJobKind::SmallFile { ack, .. } => ack.is_closed(),
        }
    }

    fn succeed(self, success: UploadSuccess) {
        match (self.kind, success) {
            (UploadJobKind::BigPart { ack, .. }, UploadSuccess::BigPart(ok)) => {
                let _ = ack.send(Ok(ok));
            }
            (UploadJobKind::SmallFile { ack, .. }, UploadSuccess::SmallFile(uploaded)) => {
                let _ = ack.send(Ok(uploaded));
            }
            _ => unreachable!("upload job returned the wrong result kind"),
        }
    }

    fn fail(self, error: anyhow::Error) {
        match self.kind {
            UploadJobKind::BigPart { ack, .. } => {
                let _ = ack.send(Err(error));
            }
            UploadJobKind::SmallFile { ack, .. } => {
                let _ = ack.send(Err(error));
            }
        }
    }
}

impl Uploader {
    pub(crate) fn spawn(client: grammers_client::Client) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(UPLOAD_QUEUE_CAPACITY);
        tokio::spawn(run_uploader(client, rx));
        Self { tx }
    }

    async fn big_part(&self, request: tl::functions::upload::SaveBigFilePart) -> Result<bool> {
        let (ack, done) = tokio::sync::oneshot::channel();
        self.tx
            .send(UploadJob {
                kind: UploadJobKind::BigPart { request, ack },
                failures: 0,
            })
            .await
            .map_err(|_| anyhow::anyhow!("uploader stopped before accepting big-file part"))?;
        done.await
            .context("uploader stopped before finishing big-file part")?
    }

    async fn small_file(&self, bytes: Vec<u8>, name: String) -> Result<Uploaded> {
        if bytes.len() as u64 > BIG_FILE_THRESHOLD {
            bail!("small-file upload exceeds Telegram's 10 MiB threshold");
        }
        let (ack, done) = tokio::sync::oneshot::channel();
        self.tx
            .send(UploadJob {
                kind: UploadJobKind::SmallFile { bytes, name, ack },
                failures: 0,
            })
            .await
            .map_err(|_| anyhow::anyhow!("uploader stopped before accepting small file"))?;
        done.await
            .context("uploader stopped before finishing small file")?
    }
}

fn flood_wait(error: &grammers_client::InvocationError) -> Option<u64> {
    match error {
        grammers_client::InvocationError::Rpc(rpc) if rpc.code == 420 => {
            Some(rpc.value.unwrap_or(1) as u64)
        }
        _ => None,
    }
}

fn invocation_failure(error: grammers_client::InvocationError) -> UploadFailure {
    let wait = flood_wait(&error);
    UploadFailure {
        error: anyhow::Error::new(error),
        flood_wait: wait,
    }
}

fn stream_failure(error: std::io::Error) -> UploadFailure {
    let wait = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<grammers_client::InvocationError>())
        .and_then(flood_wait);
    UploadFailure {
        error: anyhow::Error::new(error),
        flood_wait: wait,
    }
}

async fn execute_upload_job(
    client: grammers_client::Client,
    mut job: UploadJob,
) -> (UploadJob, std::result::Result<UploadSuccess, UploadFailure>) {
    let result = match &mut job.kind {
        UploadJobKind::BigPart { request, .. } => client
            .invoke(request)
            .await
            .map(UploadSuccess::BigPart)
            .map_err(invocation_failure),
        UploadJobKind::SmallFile { bytes, name, .. } => {
            let mut cursor = std::io::Cursor::new(bytes.as_slice());
            client
                .upload_stream(&mut cursor, bytes.len(), name.clone())
                .await
                .map(UploadSuccess::SmallFile)
                .map_err(stream_failure)
        }
    };
    (job, result)
}

fn spawn_upload_job(
    in_flight: &mut tokio::task::JoinSet<(
        UploadJob,
        std::result::Result<UploadSuccess, UploadFailure>,
    )>,
    client: &grammers_client::Client,
    job: UploadJob,
) {
    let client = client.clone();
    in_flight.spawn(execute_upload_job(client, job));
}

fn finish_upload_job(
    result: std::result::Result<
        (UploadJob, std::result::Result<UploadSuccess, UploadFailure>),
        tokio::task::JoinError,
    >,
    retries: &mut VecDeque<UploadJob>,
    control: &mut UploadControl,
) {
    let (mut job, result) = match result {
        Ok(result) => result,
        Err(error) => {
            tracing::error!(%error, "uploader job panicked");
            return;
        }
    };
    if job.is_cancelled() {
        return;
    }
    match result {
        Ok(success) => {
            if let Some(concurrency) = control.recover_after_success(tokio::time::Instant::now()) {
                tracing::info!(
                    operation = "upload",
                    concurrency,
                    "Telegram upload concurrency increased after a throttle-free interval"
                );
            }
            job.succeed(success);
        }
        Err(failure) => {
            job.failures += 1;
            if job.failures > MAX_UPLOAD_RETRIES
                || failure.flood_wait.is_some_and(|wait| wait > MAX_FLOOD_WAIT)
            {
                let reason = if failure.flood_wait.is_some_and(|wait| wait > MAX_FLOOD_WAIT) {
                    "server wait exceeds the 30-minute limit"
                } else {
                    "retry-attempt limit exceeded"
                };
                job.fail(failure.error.context(reason));
                return;
            }

            let delay = match failure.flood_wait {
                Some(server_wait) => {
                    let wait = server_wait.saturating_add(1);
                    crate::tg::clear_progress_line();
                    tracing::warn!(
                        operation = "upload",
                        server_wait_seconds = server_wait,
                        wait_seconds = wait,
                        "Telegram throttled upload; pausing uploader for {wait}s and reducing concurrency to one"
                    );
                    std::time::Duration::from_secs(wait)
                }
                None => std::time::Duration::from_secs(1 << job.failures.min(5)),
            };
            let retry_at = tokio::time::Instant::now() + delay;
            if failure.flood_wait.is_some() {
                control.throttle_until(retry_at);
            } else {
                control.pause_until(retry_at);
            }
            retries.push_back(job);
        }
    }
}

fn next_upload_job(
    retries: &mut VecDeque<UploadJob>,
    input: &mut tokio::sync::mpsc::Receiver<UploadJob>,
    input_closed: &mut bool,
) -> Option<UploadJob> {
    loop {
        let next = if let Some(job) = retries.pop_front() {
            Some(job)
        } else if *input_closed {
            None
        } else {
            match input.try_recv() {
                Ok(job) => Some(job),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => None,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    *input_closed = true;
                    None
                }
            }
        };
        match next {
            Some(job) if job.is_cancelled() => continue,
            other => return other,
        }
    }
}

async fn run_uploader(
    client: grammers_client::Client,
    mut input: tokio::sync::mpsc::Receiver<UploadJob>,
) {
    let mut retries = VecDeque::new();
    let mut in_flight = tokio::task::JoinSet::new();
    let mut control = UploadControl::new();
    let mut input_closed = false;

    loop {
        control.clear_expired_pause(tokio::time::Instant::now());

        if control.resume_at.is_none() {
            while in_flight.len() < control.concurrency {
                let Some(job) = next_upload_job(&mut retries, &mut input, &mut input_closed) else {
                    break;
                };
                spawn_upload_job(&mut in_flight, &client, job);
            }
        }

        if input_closed && retries.is_empty() && in_flight.is_empty() {
            return;
        }

        if let Some(deadline) = control.resume_at {
            if in_flight.is_empty() {
                tokio::time::sleep_until(deadline).await;
                control.resume_at = None;
            } else {
                tokio::select! {
                    result = in_flight.join_next() => {
                        finish_upload_job(
                            result.expect("join set was non-empty"),
                            &mut retries,
                            &mut control,
                        );
                    }
                    _ = tokio::time::sleep_until(deadline) => control.resume_at = None,
                }
            }
            continue;
        }

        if !in_flight.is_empty()
            && (!retries.is_empty() || in_flight.len() >= control.concurrency || input_closed)
        {
            let result = in_flight.join_next().await.expect("join set was non-empty");
            finish_upload_job(result, &mut retries, &mut control);
            continue;
        }

        if in_flight.is_empty() {
            match input.recv().await {
                Some(job) if !job.is_cancelled() => {
                    spawn_upload_job(&mut in_flight, &client, job);
                }
                Some(_) => {}
                None => input_closed = true,
            }
        } else {
            tokio::select! {
                result = in_flight.join_next() => {
                    finish_upload_job(
                        result.expect("join set was non-empty"),
                        &mut retries,
                        &mut control,
                    );
                }
                job = input.recv() => match job {
                    Some(job) if !job.is_cancelled() => {
                        spawn_upload_job(&mut in_flight, &client, job);
                    }
                    Some(_) => {}
                    None => input_closed = true,
                }
            }
        }
    }
}

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

struct MemorySource(Vec<u8>);

impl PartSource for MemorySource {
    fn len(&self) -> u64 {
        self.0.len() as u64
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let offset = usize::try_from(offset).context("memory upload offset overflow")?;
        let end = offset
            .checked_add(buf.len())
            .context("memory upload range overflow")?;
        let source = self
            .0
            .get(offset..end)
            .context("memory upload read beyond end")?;
        buf.copy_from_slice(source);
        Ok(())
    }
}

pub fn total_parts(len: u64) -> i32 {
    len.div_ceil(PART_SIZE) as i32
}

fn confirm_part(
    part: i32,
    confirmed: &mut std::collections::BTreeSet<i32>,
    watermark: &mut i32,
) -> bool {
    confirmed.insert(part);
    let mut advanced = false;
    while confirmed.remove(watermark) {
        *watermark += 1;
        advanced = true;
    }
    advanced
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
        let mut bytes = vec![0u8; size];
        stream
            .read_exact(&mut bytes)
            .await
            .context("cannot read small upload")?;
        if bytes.len() as u64 > BIG_FILE_THRESHOLD {
            return self
                .upload_source(
                    peer,
                    Arc::new(MemorySource(bytes)),
                    name,
                    caption,
                    None,
                    |_, _| Ok(()),
                )
                .await;
        }
        let _transfer = self.begin_upload();
        let uploaded = self.uploader.small_file(bytes, name).await?;
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
        let _transfer = self.begin_upload();
        let len = source.len();
        if len == 0 {
            bail!("refusing to upload an empty document");
        }

        if len <= BIG_FILE_THRESHOLD {
            let mut buf = vec![0u8; len as usize];
            source.read_at(0, &mut buf)?;
            let uploaded = self.uploader.small_file(buf, name).await?;
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
        let mut producers = tokio::task::JoinSet::new();
        for _ in 0..INITIAL_UPLOAD_CONCURRENCY {
            let uploader = self.uploader.clone();
            let source = Arc::clone(&source);
            let next = Arc::clone(&next);
            let done_tx = done_tx.clone();
            producers.spawn(async move {
                loop {
                    let part = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if part >= total {
                        return Ok(());
                    }
                    let bytes = source.read_part(part)?;
                    let request = tl::functions::upload::SaveBigFilePart {
                        file_id,
                        file_part: part,
                        file_total_parts: total,
                        bytes,
                    };
                    let ok = uploader
                        .big_part(request)
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
        let mut done_open = true;
        while !producers.is_empty() {
            tokio::select! {
                biased;
                result = producers.join_next() => {
                    match result.expect("join set was non-empty") {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            producers.abort_all();
                            return Err(error);
                        }
                        Err(error) => {
                            producers.abort_all();
                            return Err(anyhow::Error::new(error).context("upload producer panicked"));
                        }
                    }
                }
                maybe_part = done_rx.recv(), if done_open => {
                    match maybe_part {
                        Some(part) if confirm_part(part, &mut confirmed, &mut watermark) => {
                            on_progress(file_id, watermark)?;
                        }
                        Some(_) => {}
                        None => done_open = false,
                    }
                }
            }
        }
        while let Ok(part) = done_rx.try_recv() {
            if confirm_part(part, &mut confirmed, &mut watermark) {
                on_progress(file_id, watermark)?;
            }
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
        let _transfer = self.begin_download();
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
        let _transfer = self.begin_download();
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
        let _transfer = self.begin_download();
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

#[cfg(test)]
mod tests {
    use super::*;

    fn big_part_job(part: i32) -> (UploadJob, tokio::sync::oneshot::Receiver<Result<bool>>) {
        let (ack, done) = tokio::sync::oneshot::channel();
        (
            UploadJob {
                kind: UploadJobKind::BigPart {
                    request: tl::functions::upload::SaveBigFilePart {
                        file_id: 7,
                        file_part: part,
                        file_total_parts: 10,
                        bytes: vec![part as u8],
                    },
                    ack,
                },
                failures: 0,
            },
            done,
        )
    }

    fn job_part(job: &UploadJob) -> i32 {
        match &job.kind {
            UploadJobKind::BigPart { request, .. } => request.file_part,
            UploadJobKind::SmallFile { .. } => panic!("expected big-part job"),
        }
    }

    fn rpc_error(code: i32, name: &str, value: Option<u32>) -> grammers_client::InvocationError {
        grammers_client::InvocationError::Rpc(grammers_client::sender::RpcError {
            code,
            name: name.to_string(),
            value,
            caused_by: None,
        })
    }

    #[test]
    fn recognizes_server_flood_wait() {
        assert_eq!(
            flood_wait(&rpc_error(420, "FLOOD_PREMIUM_WAIT", Some(14))),
            Some(14)
        );
        assert_eq!(flood_wait(&rpc_error(500, "INTERNAL", None)), None);
    }

    #[test]
    fn retry_queue_has_priority_over_input_channel() {
        let (retry, _retry_done) = big_part_job(1);
        let (input_job, _input_done) = big_part_job(2);
        let mut retries = VecDeque::from([retry]);
        let (input_tx, mut input_rx) = tokio::sync::mpsc::channel(2);
        assert!(input_tx.try_send(input_job).is_ok());
        let mut input_closed = false;

        let first = next_upload_job(&mut retries, &mut input_rx, &mut input_closed).unwrap();
        let second = next_upload_job(&mut retries, &mut input_rx, &mut input_closed).unwrap();
        assert_eq!(job_part(&first), 1);
        assert_eq!(job_part(&second), 2);
    }

    #[test]
    fn cancelled_retry_is_discarded_before_new_input() {
        let (cancelled, cancelled_done) = big_part_job(1);
        drop(cancelled_done);
        let (input_job, _input_done) = big_part_job(2);
        let mut retries = VecDeque::from([cancelled]);
        let (input_tx, mut input_rx) = tokio::sync::mpsc::channel(2);
        assert!(input_tx.try_send(input_job).is_ok());
        let mut input_closed = false;

        let selected = next_upload_job(&mut retries, &mut input_rx, &mut input_closed).unwrap();
        assert_eq!(job_part(&selected), 2);
        assert!(retries.is_empty());
    }

    #[test]
    fn flood_failure_requeues_job_and_reduces_concurrency() {
        let (job, mut done) = big_part_job(3);
        let mut retries = VecDeque::new();
        let mut control = UploadControl::new();
        let before = tokio::time::Instant::now();

        finish_upload_job(
            Ok((
                job,
                Err(UploadFailure {
                    error: anyhow::anyhow!("throttled"),
                    flood_wait: Some(14),
                }),
            )),
            &mut retries,
            &mut control,
        );

        assert_eq!(control.concurrency, 1);
        assert_eq!(retries.len(), 1);
        assert_eq!(retries.front().unwrap().failures, 1);
        let resume_at = control.resume_at.unwrap();
        assert!(resume_at >= before + std::time::Duration::from_secs(15));
        assert_eq!(
            control.recover_at,
            Some(resume_at + UPLOAD_CONCURRENCY_RECOVERY_INTERVAL)
        );
        assert!(matches!(
            done.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn upload_concurrency_recovers_one_step_per_successful_interval() {
        let now = tokio::time::Instant::now();
        let mut control = UploadControl::new();
        control.throttle_until(now);
        let first_recovery = now + UPLOAD_CONCURRENCY_RECOVERY_INTERVAL;

        assert_eq!(
            control.recover_after_success(first_recovery - std::time::Duration::from_secs(1)),
            None
        );
        assert_eq!(control.recover_after_success(first_recovery), Some(2));
        assert_eq!(control.recover_after_success(first_recovery), None);

        let second_recovery = first_recovery + UPLOAD_CONCURRENCY_RECOVERY_INTERVAL;
        assert_eq!(control.recover_after_success(second_recovery), Some(3));
        let third_recovery = second_recovery + UPLOAD_CONCURRENCY_RECOVERY_INTERVAL;
        assert_eq!(control.recover_after_success(third_recovery), Some(4));
        assert_eq!(control.recover_at, None);
        assert_eq!(
            control.recover_after_success(third_recovery + UPLOAD_CONCURRENCY_RECOVERY_INTERVAL),
            None
        );
    }

    #[test]
    fn another_flood_resets_concurrency_recovery() {
        let now = tokio::time::Instant::now();
        let mut control = UploadControl::new();
        control.throttle_until(now);
        assert_eq!(
            control.recover_after_success(now + UPLOAD_CONCURRENCY_RECOVERY_INTERVAL),
            Some(2)
        );

        let next_flood_resume = now + std::time::Duration::from_secs(90);
        control.throttle_until(next_flood_resume);
        assert_eq!(control.concurrency, 1);
        assert_eq!(
            control.recover_at,
            Some(next_flood_resume + UPLOAD_CONCURRENCY_RECOVERY_INTERVAL)
        );
    }

    #[test]
    fn upload_watermark_only_advances_over_contiguous_parts() {
        let mut confirmed = std::collections::BTreeSet::new();
        let mut watermark = 10;
        assert!(!confirm_part(12, &mut confirmed, &mut watermark));
        assert_eq!(watermark, 10);
        assert!(confirm_part(10, &mut confirmed, &mut watermark));
        assert_eq!(watermark, 11);
        assert!(confirm_part(11, &mut confirmed, &mut watermark));
        assert_eq!(watermark, 13);
    }
}
