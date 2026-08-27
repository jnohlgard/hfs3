# CLAUDE.md -- hfs3

> HuggingFace-to-S3 mirror tool. Rust CLI rewrite from Python.

---

## What this is

A single Rust binary that mirrors HuggingFace repos to S3 for air-gapped deployment. Three subcommands: `mirror` (HF -> S3), `pull` (S3 -> local), `run` (pull + docker build + run).

**Streaming architecture** -- zero-copy pipe from HF REST API response chunks directly into S3 multipart upload. No intermediate files. Memory-aware concurrency reads `/proc/meminfo` to limit parallel transfers.

---

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

---

## Commands

```
just build            # cargo build
just build-release    # cargo build --release
just test             # cargo test (unit tests only)
just test-v           # cargo test -- --nocapture
just test-integration # cargo test --test integration_test -- --ignored
just clippy           # cargo clippy -- -D warnings
just fmt              # cargo fmt
just fmt-check        # cargo fmt -- --check
just lint             # fmt-check + clippy
just mirror <repo>    # cargo run -- mirror <repo>
just pull <repo>      # cargo run -- pull <repo>
just run <repo>       # cargo run -- run <repo>
```

---

## Architecture

Six modules in `src/`, each independently testable:

- `types.rs` -- RepoRef, RepoType, HfFileEntry, MirrorResult, PullResult, RunResult, parse_repo_url
- `error.rs` -- Hfs3Error enum (Config, HfApi, S3, Io, Docker, Parse, Http)
- `config.rs` -- AppConfig::from_env() reads HFS3_S3_BUCKET, HFS3_S3_PREFIX, HF_TOKEN, AWS_REGION
- `hf.rs` -- list_repo_files (HF REST API), download_file_stream (byte stream)
- `s3.rs` -- S3Ops (multipart upload, download, list), chunk_size_for_file (adaptive)
- `concurrency.rs` -- available_memory_bytes, calculate_max_concurrency, plan_transfer
- `pipeline.rs` -- mirror_repo, pull_repo (orchestration layer)
- `docker.rs` -- build_image, run_image (shell out to docker CLI)
- `main.rs` -- clap CLI wiring, tracing init, JSON output

---

## Env vars

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `HFS3_S3_BUCKET` | yes | -- | S3 bucket for mirrored repos |
| `HFS3_S3_PREFIX` | no | `hfs3-mirror` | Key prefix within the bucket |
| `HF_TOKEN` | no | -- | HuggingFace auth token for gated repos |
| `AWS_REGION` | no | -- | AWS region for S3 client |

---

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

---

## S3 key layout

Objects are stored at: `s3://{bucket}/{prefix}/{repo_type}/{owner}--{name}/{file_path}`

Example: `s3://my-bucket/hfs3-mirror/model/meta-llama--Llama-2-7b/config.json`

---

## Adaptive chunk sizing

| File size | Chunk size |
|-----------|------------|
| < 1 GB    | 8 MB       |
| < 5 GB    | 64 MB      |
| >= 5 GB   | 128 MB     |

Files < 8 MB use `put_object` instead of multipart.

---

## Testing conventions

- Unit tests: `#[cfg(test)] mod tests` in each module, run with `just test`
- Integration tests: `tests/integration_test.rs`, `#[ignore]`, requires network, run with `just test-integration`
- Python tests in `tests/` are kept as behavioral spec reference (not run)
- Config tests use a mutex since they mutate env vars
- No mock S3 in Rust tests (unit tests cover pure logic, integration tests cover HF API)

---

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
