use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};

use crate::config::AppConfig;
use crate::docker;
use crate::error::Hfs3Error;
use crate::pipeline;
use crate::types::{parse_repo_url, RunResult};

#[derive(Parser)]
#[command(name = "hfs3", about = "Mirror HuggingFace repos to S3")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Stream HuggingFace repo to S3
    Mirror {
        /// HuggingFace repo ID or URL (e.g. "meta-llama/Llama-2-7b")
        repo: String,
    },
    /// Download from S3 to local directory
    Pull {
        /// HuggingFace repo ID or URL
        repo: String,
        /// Local destination directory
        #[arg(long, default_value = "./repo")]
        dest: PathBuf,
    },
    /// Pull from S3, build and run Docker image
    Run {
        /// HuggingFace repo ID or URL
        repo: String,
        /// Local destination directory
        #[arg(long, default_value = "./repo")]
        dest: PathBuf,
        /// Port to expose from the container
        #[arg(long, default_value_t = 7860)]
        port: u16,
        /// Force re-pull even if dest already exists
        #[arg(long)]
        force: bool,
    },
}

/// Entry point for the hfs3 CLI. Called from main().
pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    // Init tracing: human-readable to stderr, controlled by RUST_LOG
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "hfs3=info".parse().unwrap()),
        )
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    match cli.command {
        Commands::Mirror { repo } => cmd_mirror(&repo).await?,
        Commands::Pull { repo, dest } => cmd_pull(&repo, &dest).await?,
        Commands::Run {
            repo,
            dest,
            port,
            force,
        } => cmd_run(&repo, &dest, port, force).await?,
    };
    Ok(())
}

/// Mirror: parse repo -> load config -> stream HF to S3 -> print JSON result
async fn cmd_mirror(repo_str: &str) -> Result<(), Hfs3Error> {
    let repo = parse_repo_url(repo_str)?;
    let config = AppConfig::from_env()?;
    let result = pipeline::mirror_repo(&config, &repo).await?;
    let json = serde_json::to_string_pretty(&result)
        .map_err(|e| Hfs3Error::Parse(format!("JSON serialization failed: {e}")))?;
    println!("{json}");
    // Exit non-zero when some files failed so callers can detect partial mirrors.
    if result.files_failed > 0 {
        std::process::exit(2);
    }
    Ok(())
}

/// Pull: parse repo -> load config -> download S3 to local -> print JSON result
async fn cmd_pull(repo_str: &str, dest: &Path) -> Result<(), Hfs3Error> {
    let repo = parse_repo_url(repo_str)?;
    let config = AppConfig::from_env()?;
    let result = pipeline::pull_repo(&config, &repo, dest).await?;
    let json = serde_json::to_string_pretty(&result)
        .map_err(|e| Hfs3Error::Parse(format!("JSON serialization failed: {e}")))?;
    println!("{json}");
    Ok(())
}

/// Run: parse repo -> pull if needed -> docker build -> docker run -> print JSON result
async fn cmd_run(repo_str: &str, dest: &Path, port: u16, force: bool) -> Result<(), Hfs3Error> {
    let repo = parse_repo_url(repo_str)?;
    let config = AppConfig::from_env()?;

    // Pull from S3 if dest doesn't exist or force is set
    if !dest.exists() || force {
        eprintln!("Pulling repo to {} ...", dest.display());
        pipeline::pull_repo(&config, &repo, dest).await?;
    } else {
        eprintln!(
            "Destination {} already exists, skipping pull (use --force to re-pull)",
            dest.display()
        );
    }

    // Build Docker image
    let image_tag = format!("hfs3-{}", repo.repo_id.replace('/', "-").to_lowercase());
    docker::build_image(dest, &image_tag).await?;

    // Run Docker image
    docker::run_image(&image_tag, port, None).await?;

    // Print JSON result
    let result = RunResult {
        repo_id: repo.repo_id,
        repo_type: repo.repo_type.to_string(),
        image_tag,
        port,
    };
    let json = serde_json::to_string_pretty(&result)
        .map_err(|e| Hfs3Error::Parse(format!("JSON serialization failed: {e}")))?;
    println!("{json}");
    Ok(())
}
