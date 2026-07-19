//! Selection and restoration of files from the remote index.

use std::path::PathBuf;

use anyhow::{Result, bail};

use crate::context::RepoContext;
use crate::crypto::Crypto;
use crate::file;

/// Restore an exact file or folder prefix under `dest`.
pub async fn get(
    context: &RepoContext,
    remote: &str,
    dest: Option<PathBuf>,
    key: Option<&[u8; 32]>,
) -> Result<()> {
    let dest = dest.unwrap_or_else(|| context.repo.root.clone());
    let targets = if let Some(file) = context.index.get_file(remote)?.filter(|file| !file.deleted) {
        vec![file]
    } else {
        let prefix = format!("{}/", remote.trim_end_matches('/'));
        let listed = context.index.list_files(Some(&prefix), false)?;
        if listed.is_empty() {
            bail!("no file or folder named {remote:?} in the index — try `tgfs ls`");
        }
        listed
            .into_iter()
            .map(|file| {
                context
                    .index
                    .get_file(&file.path)
                    .map(|entry| entry.expect("just listed"))
            })
            .collect::<Result<Vec<_>>>()?
    };
    let peer = context.tg.peer(&context.repo.config)?;
    let crypto = key.map(Crypto::new);
    let mut documents = std::collections::HashMap::new();

    for target in targets {
        let out_path = file::download(
            &context.tg,
            &context.index,
            peer,
            &target,
            &dest,
            crypto.as_ref(),
            &mut documents,
        )
        .await?;
        println!("✓ {} → {}", target.path, out_path.display());
    }
    Ok(())
}
