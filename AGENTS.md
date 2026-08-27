# AGENTS.md

## What this is

HF-to-S3 mirror. Streams HuggingFace repos/spaces directly to S3 (zero intermediate files) so they can be deployed on air-gapped servers that have S3 access but no HF access. Single Rust binary with three subcommands: mirror, pull, run.

**Streaming architecture** -- zero-copy pipe from HF REST API response chunks directly into S3 multipart upload. No intermediate files. Memory-aware concurrency reads `/proc/meminfo` to limit parallel transfers.

## Stack

| Layer      | Choice                         |
|------------|--------------------------------|
| Language   | Rust 2021                      |
| Runtime    | tokio (multi-threaded)         |
| HTTP       | reqwest (stream + json)        |
| S3         | aws-sdk-s3                     |
| CLI        | clap (derive)                  |
| Serialization | serde + serde_json         |
| Logging    | tracing + tracing-subscriber   |
| Errors     | thiserror                      |
| Task runner| just                           |
| CI         | GitHub Actions                 |
| Container  | devcontainer (Rust + just + awscli) |

## Commands

```
just build            # cargo build
just build-release    # cargo build --release
just test             # cargo test
just test -v          # cargo test with verbose output
just check            # cargo check
just clippy           # cargo clippy -- -D warnings
just fmt              # cargo fmt
just fmt-check        # cargo fmt -- --check
just lint             # fmt-check + clippy
just mirror <repo>    # HF -> S3 (needs HFS3_S3_BUCKET)
just pull <repo>      # S3 -> local
just run <repo>       # S3 -> local -> docker build+run
just e2e              # mirror -> pull -> per-file sha256 compare against a reachable endpoint
just s3-up            # start a throwaway MinIO for e2e (or point S3_ENDPOINT at your own bucket)
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
- `stats.rs` — transfer stats and progress
- `cli.rs` — clap-based CLI with mirror/pull/run subcommands

## Env vars

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `HFS3_S3_BUCKET` | yes | — | S3 bucket for mirrored repos |
| `HFS3_S3_PREFIX` | no | `hfs3-mirror` | Key prefix within the bucket |
| `HF_TOKEN` | no | — | HuggingFace auth token for gated repos |
| `AWS_REGION` | no | — | AWS region for S3 client |
| `HFS3_MAX_CHUNK_MB` | no | — | MiB cap on multipart chunk size (clamped to S3 5 MiB part minimum and the 10,000-part floor) |

## Output format

- **stderr**: Human-readable progress (file listings, transfer plans, per-file progress)
- **stdout**: JSON summary on completion

```json
{
  "repo_id": "meta-llama/Llama-2-7b",
  "repo_type": "model",
  "bucket": "my-bucket",
  "prefix": "hfs3-mirror/model/meta-llama--Llama-2-7b",
  "files_transferred": 12,
  "files_failed": 0,
  "bytes_transferred": 13456789,
  "failed_files": [],
  "duration_secs": 45.2
}
```

Exit codes: 0 = success, 1 = error, 2 = `mirror` with one or more failed files (JSON is still printed).

## S3 key layout

Objects are stored at: `s3://{bucket}/{prefix}/{repo_type}/{owner}--{name}/{file_path}`

Example: `s3://my-bucket/hfs3-mirror/model/meta-llama--Llama-2-7b/config.json`

## Adaptive chunk sizing

| File size | Chunk size |
|-----------|------------|
| < 1 GB    | 8 MB       |
| < 5 GB    | 64 MB      |
| >= 5 GB   | 128 MB     |

Files < 8 MB use `put_object` instead of multipart.

## Testing conventions

- Unit tests in each module with `#[cfg(test)]` + `#[tokio::test]`
- HF API tests mock HTTP responses with wiremock
- No mock S3 clients: `s3.rs` unit tests cover pure logic (chunk sizing, key building, path safety); real S3 is covered by `just e2e` (mirror -> pull -> per-file sha256 compare against a reachable endpoint; `just s3-up` starts a throwaway MinIO, or point `S3_ENDPOINT` at your own bucket)
- `tests/smoke.rs` exercises the real binary via `CARGO_BIN_EXE_hfs3` (CLI parsing, exit codes)
- Config tests use a mutex since they mutate env vars
- Docker tests check preconditions only; no real Docker calls in tests
- All modules take dependencies as explicit arguments for testability
- `tests/*.py` hold the original Python behavioral spec, kept for reference (not run)

## Execution Plan

This project has a DAG-based execution plan at `spec/plan.yaml`.
Proto contracts defining agent I/O boundaries are in `spec/contracts/`.
To execute: read `spec/plan.yaml`, launch worker agents by level (level 0 first, in parallel),
validate output protos at each boundary, gate on tests passing before advancing to the next level.

### DAG summary

```
Level 0: A0 (scaffold), A1 (types), A2 (config), A5 (concurrency), A7 (docker)
Level 1: A3 (hf_client), A4 (s3_client), A10 (ci)
Level 2: A6 (pipeline)
Level 3: A8 (cli_wiring)
Level 4: A9 (integration_test)

Critical path: A0 -> A3 -> A6 -> A8 -> A9
```

### What is NOT being built

- Auth / multi-user
- Web UI
- Windows support
- Caching / incremental sync
- S3 server-side encryption configuration
- Any Python code remaining (beyond test reference)

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
