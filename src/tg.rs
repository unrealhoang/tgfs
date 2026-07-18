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

use crate::config::{Config, DEFAULT_CHANNEL_TITLE, session_path};

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
        let client = Client::new(pool.handle);
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

    /// Find the storage channel among the dialogs, or create a new private
    /// broadcast channel. Returns `(channel_id, access_hash)`.
    pub async fn ensure_channel(&self) -> Result<(i64, i64)> {
        let mut dialogs = self.client.iter_dialogs();
        while let Some(dialog) = dialogs.next().await? {
            if let grammers_client::peer::Peer::Channel(channel) = dialog.peer()
                && channel.title() == DEFAULT_CHANNEL_TITLE
            {
                let id = channel.id().bare_id().context("channel id")?;
                let auth = self
                    .session
                    .peer_ref(channel.id())
                    .await
                    .map_err(|e| anyhow::anyhow!("session error: {e}"))?
                    .map(|r| r.auth.hash())
                    .unwrap_or_default();
                println!("using existing channel \"{DEFAULT_CHANNEL_TITLE}\" ({id})");
                return Ok((id, auth));
            }
        }

        let updates = self
            .client
            .invoke(&tl::functions::channels::CreateChannel {
                broadcast: true,
                megagroup: false,
                for_import: false,
                forum: false,
                title: DEFAULT_CHANNEL_TITLE.to_string(),
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
                println!("created private channel \"{DEFAULT_CHANNEL_TITLE}\" ({})", c.id);
                return Ok((c.id, c.access_hash.unwrap_or_default()));
            }
        }
        bail!("CreateChannel response contained no channel")
    }

    pub fn peer(&self, config: &Config) -> Result<PeerRef> {
        Ok(PeerRef {
            id: PeerId::channel(config.channel_id).context("invalid channel id in config")?,
            auth: PeerAuth::from_hash(config.channel_access_hash),
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
}

fn prompt(msg: &str) -> Result<String> {
    print!("{msg}");
    std::io::stdout().flush()?;
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    Ok(line)
}
