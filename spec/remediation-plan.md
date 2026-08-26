# hfs3 Remediation Plan (2026-08-26)

Step-by-step plan addressing the 2026-08-26 code review findings.
Each step is a small, independently reviewable commit, in execution order.

## Local verification loop

No CI is in use yet; run these locally after every step:

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

(Use bare cargo, not the justfile — `just` recipes route through the devcontainer.)

## End-to-end loop (local S3)

Acceptance test for the implementation: mirror a small HuggingFace repo to a
local S3 server, pull it back, and verify byte-for-byte identity.

```sh
just s3-up                 # docker run MinIO on :9000 (fixed dev creds)
# create bucket once: aws --endpoint-url http://localhost:9000 \
#   s3api create-bucket --bucket hfs3-e2e
HFS3_S3_BUCKET=hfs3-e2e HFS3_S3_ENDPOINT=http://localhost:9000 \
  cargo run -- mirror hf-internal-testing/tiny-random-bert
HFS3_S3_BUCKET=hfs3-e2e HFS3_S3_ENDPOINT=http://localhost:9000 \
  cargo run -- pull hf-internal-testing/tiny-random-bert --dest /tmp/hfs3-e2e/repo
# then sha256-compare pulled files against the originals fetched from HF
just s3-down
```

The `e2e` justfile recipe automates the above. If the working machine has no
Docker, use the standalone MinIO server binary or a real S3 bucket instead —
the harness only needs an S3-compatible API plus `HFS3_S3_ENDPOINT`.

## Steps

### 1. Green baseline: fmt + clippy
- `cargo fmt` fixes drift in `src/concurrency.rs` (rustfmt 1.9 wraps lines differently).
- `src/stats.rs:762`: `std::io::Error::new(ErrorKind::Other, ...)` -> `std::io::Error::other(...)`.
- Verify: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test`.
- Commit: `fix: make fmt and clippy pass on current toolchain`

### 2. Local S3 endpoint support
- `config.rs`: new optional `s3_endpoint` field, read from `HFS3_S3_ENDPOINT`.
- `s3.rs`: `S3Ops::new` accepts the endpoint and sets `endpoint_url` on the SDK config.
- `pipeline.rs`: thread the config value through.
- Unit test for config parsing (env mutex helper already exists in `config.rs`).
- Commit: `feat: support custom S3 endpoint via HFS3_S3_ENDPOINT`

### 3. E2E harness
- justfile recipes: `s3-up` / `s3-down` (MinIO via docker, fixed dev creds,
  non-interactive), `e2e` (mirror + pull + sha256 compare, exit non-zero on mismatch).
- README: "Local S3 testing" section.
- Run it once against `hf-internal-testing/tiny-random-bert`; confirm all files identical.
- Commit: `test: add local MinIO e2e harness for mirror and pull`

### 4. Mirror: verify uploaded bytes match listed size
- `s3.rs upload_multipart`: actually use the `file_size` parameter; after the
  final part is uploaded, if `total_bytes != file_size`, abort the multipart
  upload and return an error (currently a silently truncated stream is stored
  as a complete object).
- Unit test: stream shorter than declared size -> error; exact size -> ok.
- Commit: `fix: reject uploads whose byte count differs from the listed size`

### 5. Mirror: report failures, exit non-zero on partial mirror
- `types.rs`: `MirrorResult` gains `files_failed: usize` and
  `failed_files: Vec<String>`.
- `pipeline.rs`: collect the names of failed files.
- `cli.rs cmd_mirror`: print the JSON summary, then exit with code 2 when
  `files_failed > 0`.
- Docs: update the JSON contract in README/CLAUDE.md/AGENTS.md; document
  exit codes (0 ok, 1 error, 2 partial).
- E2E: confirm happy path still exits 0 with `files_failed: 0`.
- Commit: `feat: report failed files in mirror JSON and exit non-zero on partial mirrors`

### 6. Hermetic detection tests with real HF 401 semantics
- `pipeline.rs`: add `resolve_repo_type_with_base` (mirrors the existing
  `detect_repo_type_with_base` pattern); production path uses `HF_BASE`.
- Rewire `test_resolve_falls_back_to_detection_failure` to a wiremock server —
  it currently makes real network calls to huggingface.co.
- Update detection mocks to encode real HF behavior: nonexistent repos return
  401 (not 404) unauthenticated; per-repo space/dataset endpoints require auth.
  Detection only trusts 200 as a positive match.
- Commit: `test: make repo-type detection hermetic and match real HF 401 semantics`

### 7. HF error messaging for 401/403/404
- `hf.rs`: map statuses to actionable messages:
  - 401/403 -> repo requires auth (gated/private) — set `HF_TOKEN`; if the repo
    ID was given bare, also suggest including the type (`datasets/`, `spaces/`)
    in the URL, since unauthenticated per-repo API probes cannot distinguish
    types.
  - 404 -> repo not found.
- Unit tests for each status.
- Commit: `fix: give actionable HF error messages for auth and not-found`

### 8. Pull: atomic writes + path sanitization
- `s3.rs download_to_file`: write to `<dest>.hfs3-tmp` then rename over the
  final path (no more partial files left at the destination).
- `download_all`: sanitize the relative key before joining to the dest dir —
  reject absolute paths and `..` components, and verify the normalized path
  stays under the destination (defense against path traversal via hostile keys).
- Unit tests for both.
- Commit: `fix: make pull writes atomic and block path traversal`

### 9. Pull: concurrent downloads
- `s3.rs download_all`: bounded concurrency with a semaphore + JoinSet, sized
  from `plan_transfer` (same memory-aware model as mirror).
- E2E: pull timing improves; integrity still verified.
- Commit: `perf: download pull files concurrently`

### 10. Pull/run: skip existing files, fix stale-dir heuristic
- `download_all`: skip an object when a local file of the same size exists.
- `cli.rs cmd_run`: only skip the pull when the destination is non-empty
  (an empty leftover dir from a failed pull must not be treated as complete),
  with a warning; `--force` still re-pulls.
- Commit: `fix: skip already-downloaded files and stop building from partial pull dirs`

### 11. Stop blocking the async runtime on /proc reads
- `stats.rs sample_memory`, `concurrency.rs available_memory_bytes`,
  `process_rss_bytes`: use `tokio::fs` (or `spawn_blocking`).
- Commit: `fix: use non-blocking /proc reads in async context`

### 12. Documentation alignment
- AGENTS.md: align the "Testing conventions" section with reality (wiremock
  for HF, no S3 mocks; e2e via local MinIO).
- `spec/plan.yaml`: fix stale local repo path; move "cross-compiled releases"
  out of scope (not implemented).
- README: document exit codes, `HFS3_S3_ENDPOINT`, and the known limitation
  that Xet-based repos are downloaded via `/resolve/` (no chunked-CAS/resume).
- devcontainer: remove the personal dotfiles repo clone from `initializeCommand`.
- Dedupe the duplicated 8 MB put-object threshold (`s3.rs` / `stats.rs`).
- Fix the stale comment in `main.rs` (logic lives in `cli.rs`).
- Commit: `docs: align agent docs, spec, README, and devcontainer with implementation`

## Completion criteria

- All 12 commits land in order; local lint + tests green after each.
- `just e2e` passes end-to-end (mirror -> S3 -> pull -> hashes match).
- A partial mirror (e.g. a nonexistent file) yields `files_failed > 0` and
  exit code 2; a full mirror yields exit 0.
