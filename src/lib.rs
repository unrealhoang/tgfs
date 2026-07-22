//! tgfs — use Telegram as a file storage / backup backend.
//!
//! This crate exposes the building blocks behind the `tgfs` CLI so other
//! programs can drive them directly: repository discovery and configuration
//! ([`config`]), the local content index ([`index`]), synchronization with
//! Telegram ([`sync`], [`snapshot`]), optional client-side encryption
//! ([`crypto`], [`key`]), and the Telegram session and transport layers
//! ([`tg`], [`session`], [`telegram_transfer`]).
//!
//! Working-tree scanning and comparison with the index live in [`diff`], the
//! chunk-level file engine in [`mod@file`], file restoration in [`restore`], and
//! the shared per-repo context in [`context`]. The CLI argument schema is
//! defined in [`cli`].
//!
//! The CLI-specific command wiring lives in [`commands`]; the binary in
//! `src/main.rs` is a thin shell over it.

pub mod cli;
pub mod commands;
pub mod config;
pub mod context;
pub mod crypto;
pub mod diff;
pub mod file;
pub mod index;
pub mod key;
pub mod restore;
pub mod session;
pub mod snapshot;
pub mod sync;
pub mod telegram_transfer;
pub mod tg;
