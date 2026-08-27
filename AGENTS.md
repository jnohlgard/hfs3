# AGENTS.md

## What this is

HF-to-S3 mirror. Streams HuggingFace repos/spaces directly to S3 (zero intermediate files) so they can be deployed on air-gapped servers that have S3 access but no HF access. Single Rust binary with three subcommands: mirror, pull, run.

## Commands

```
just build            # cargo build
just build-release    # cargo build --release
just test             # cargo test
just test -v          # cargo test with verbose output
just check            # cargo check
just clippy           # cargo clippy -- -D warnings
just fmt              # cargo fmt
just lint             # fmt-check + clippy
just mirror <repo>    # HF -> S3 (needs HFS3_S3_BUCKET)
just pull <repo>      # S3 -> local
just run <repo>       # S3 -> local -> docker build+run
```

## Architecture

Rust modules in `src/`, each independently testable:

- `types.rs` — domain types: RepoRef, RepoType, HfFileEntry, MirrorResult, PullResult
- `error.rs` — Hfs3Error enum (thiserror) with Config, HfApi, S3, Io, Docker, Parse variants
- `config.rs` — env var parsing (HFS3_S3_BUCKET, HFS3_S3_PREFIX, HF_TOKEN, AWS_REGION)
- `concurrency.rs` — memory-aware concurrency: reads /proc/meminfo, adaptive chunk sizing
- `hf.rs` — HuggingFace REST API client (reqwest, streaming responses)
- `s3.rs` — S3 multipart upload/download (aws-sdk-s3, streaming)
- `pipeline.rs` — orchestrates HF->S3 streaming pipeline (zero-copy pipe)
- `docker.rs` — build and run Docker images from pulled repos
- `cli.rs` — clap-based CLI with mirror/pull/run subcommands

## Env vars

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `HFS3_S3_BUCKET` | yes | — | S3 bucket for mirrored repos |
| `HFS3_S3_PREFIX` | no | `hfs3-mirror` | Key prefix within the bucket |
| `HF_TOKEN` | no | — | HuggingFace auth token for gated repos |
| `AWS_REGION` | no | — | AWS region for S3 client |
| `HFS3_MAX_CHUNK_MB` | no | — | MiB cap on multipart chunk size (clamped to S3 5 MiB part minimum and the 10,000-part floor) |

## Testing conventions

- Unit tests in each module with `#[cfg(test)]` + `#[tokio::test]`
- HF API tests mock HTTP responses with wiremock
- No mock S3 clients: `s3.rs` unit tests cover pure logic (chunk sizing, key building, path safety); real S3 is covered by `just e2e` (mirror -> pull -> per-file sha256 compare against a reachable endpoint; `just s3-up` starts a throwaway MinIO, or point `S3_ENDPOINT` at your own bucket)
- `tests/smoke.rs` exercises the real binary via `CARGO_BIN_EXE_hfs3` (CLI parsing, exit codes)
- Docker tests check preconditions only; no real Docker calls in tests
- All modules take dependencies as explicit arguments for testability
- `tests/*.py` hold the original Python behavioral spec, kept for reference (not run)

## Tooling

- **Rust 2021 edition** with tokio async runtime
- **just** as task runner — all commands go through the justfile
- **cargo** for build/test/check
- **clap** (derive) for CLI parsing
- **reqwest** for HTTP (with streaming)
- **aws-sdk-s3** for S3 operations
- **tracing** + **tracing-subscriber** for structured logging
- **thiserror** for error types
- **serde** + **serde_json** for serialization
