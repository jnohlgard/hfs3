//! hfs3 binary entrypoint.
//! All logic lives in the library crate (src/lib.rs).
//! CLI wiring is in src/cli.rs.

#[tokio::main]
async fn main() {
    let _ = dotenvy::dotenv(); // load .env if present, ignore if missing
    if let Err(e) = hfs3::cli::run().await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
