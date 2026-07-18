use clap::{Parser, Subcommand};

/// tgfs — use Telegram as a file storage / backup backend.
///
/// See docs/SPEC.md for the design.
#[derive(Parser)]
#[command(name = "tgfs", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Log in to Telegram and create/select the storage channel
    Init,
    /// Sync a local folder to Telegram (one-way backup by default)
    Sync {
        /// Folder to back up
        path: std::path::PathBuf,
    },
    /// List files stored in the remote index
    Ls {
        /// Remote path prefix to list
        prefix: Option<String>,
    },
    /// Download a file (or folder) from the remote index
    Get {
        /// Remote path to download
        remote: String,
        /// Local destination (defaults to current directory)
        dest: Option<std::path::PathBuf>,
    },
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    match cli.command {
        Command::Init => anyhow::bail!("not implemented yet — see docs/SPEC.md milestone M1"),
        Command::Sync { .. } => anyhow::bail!("not implemented yet — see docs/SPEC.md milestone M3"),
        Command::Ls { .. } => anyhow::bail!("not implemented yet — see docs/SPEC.md milestone M3"),
        Command::Get { .. } => anyhow::bail!("not implemented yet — see docs/SPEC.md milestone M3"),
    }
}
