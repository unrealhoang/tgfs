//! Repository-scoped dependencies shared by application services.

use crate::config::Repo;
use crate::index::Index;
use crate::tg::Tg;

/// Runtime context for operations on an opened repository.
pub struct RepoContext {
    pub(crate) tg: Tg,
    pub(crate) index: Index,
    pub(crate) repo: Repo,
}

impl RepoContext {
    pub fn new(tg: Tg, index: Index, repo: Repo) -> Self {
        Self { tg, index, repo }
    }
}
