# hfs3

Mirror HuggingFace repos to S3 for air-gapped deployment.

Single Rust binary. Streams files directly from the HuggingFace REST API into S3 multipart uploads — no intermediate files, no disk required. Memory-aware concurrency reads `/proc/meminfo` to limit parallel transfers.

## Install

```bash
just build-release
# binary at target/release/hfs3
```

## Setup

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `HFS3_S3_BUCKET` | yes | — | S3 bucket for mirrored repos |
| `HFS3_S3_PREFIX` | no | `hfs3-mirror` | Key prefix within the bucket |
| `HF_TOKEN` | no | — | HuggingFace auth token (for gated repos) |
| `AWS_REGION` | no | — | AWS region for S3 client |
| `HFS3_S3_ENDPOINT` | no | — | S3 endpoint URL override (e.g. a local MinIO) |
| `HFS3_MAX_CHUNK_MB` | no | — | Cap on multipart chunk size in MiB (clamped to S3's 5 MiB part minimum and the 10,000-part-per-file floor) |

AWS credentials are resolved via the standard SDK chain (env vars, `~/.aws/credentials`, IAM role, etc).

## Usage

### Mirror a repo from HuggingFace to S3

```bash
hfs3 mirror meta-llama/Llama-2-7b
```

Accepts bare repo IDs or full URLs:

```bash
hfs3 mirror https://huggingface.co/meta-llama/Llama-2-7b
hfs3 mirror https://huggingface.co/datasets/user/my-dataset
hfs3 mirror https://huggingface.co/spaces/user/my-space
```

### Pull a mirrored repo from S3 to local disk

```bash
hfs3 pull meta-llama/Llama-2-7b --dest ./my-model
```

### Pull, build, and run as a Docker container

```bash
hfs3 run user/my-space --port 7860
```

### Quick test

```bash
just example
```

Mirrors [`hf-internal-testing/tiny-random-bert`](https://huggingface.co/hf-internal-testing/tiny-random-bert) (~1MB) to your S3 bucket.

## Testing against S3

Point hfs3 at any S3-compatible endpoint with `HFS3_S3_ENDPOINT` (not needed for real AWS).

The `e2e` recipe is the acceptance test: it mirrors [`hf-internal-testing/tiny-random-bert`](https://huggingface.co/hf-internal-testing/tiny-random-bert) (~1MB) into a bucket, pulls it back, and sha256-compares every file against the HuggingFace original. It exits non-zero on any mismatch.

Against a cluster S3 (using your ambient AWS credentials):

```bash
just e2e S3_ENDPOINT=https://s3.cluster.example:9000 E2E_BUCKET=hfs3-e2e E2E_AWS_REGION=us-east-1
```

Add `E2E_S3_USER=... E2E_S3_PASS=...` to use explicit keys instead of the ambient credential chain.

On a machine with no real S3, `just s3-up` starts a throwaway MinIO on `:9000` (docker; fixed dev creds `hfs3test`/`hfs3testpassword`) and `just s3-down` stops it. The harness only needs a reachable endpoint, so a standalone MinIO server binary works just as well.

## Output

Progress goes to stderr. A JSON summary prints to stdout on completion:

```json
{
  "repo_id": "hf-internal-testing/tiny-random-bert",
  "repo_type": "model",
  "bucket": "my-bucket",
  "prefix": "hfs3-mirror/model/hf-internal-testing--tiny-random-bert",
  "files_transferred": 10,
  "files_failed": 0,
  "bytes_transferred": 28183891,
  "failed_files": [],
  "duration_secs": 8.3
}
```

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Error (config, network, S3, etc.) |
| 2 | `mirror` completed with one or more failed files (`files_failed > 0`); the JSON summary is still printed |

## S3 key layout

```
s3://{bucket}/{prefix}/{repo_type}/{owner}--{name}/{file_path}
```

Example: `s3://my-bucket/hfs3-mirror/model/meta-llama--Llama-2-7b/config.json`

## Adaptive chunk sizing

| File size | Chunk size |
|-----------|------------|
| < 1 GB | 8 MB |
| < 5 GB | 64 MB |
| >= 5 GB | 128 MB |

Files under 8 MB skip multipart and use a single `PutObject`.

Set `HFS3_MAX_CHUNK_MB` to cap the chunk size (useful on low-memory hosts); the effective chunk never drops below S3's 5 MiB part minimum or below what 10,000 parts would allow.

## Resuming interrupted transfers

If a mirror run dies mid-transfer (network drop, Ctrl-C, power loss), re-running `hfs3 mirror <repo>` picks up where it left off instead of restarting:

- **Large files** (multipart): hfs3 looks up the surviving S3 multipart upload and resumes it. Parts already in S3 are skipped; the HuggingFace download restarts from the first missing part using an HTTP `Range` request (hfs3 hard-errors if the server ignores the range, since resuming from a full-body response would corrupt the object). A small manifest object `<key>.hfs3-resume.json` pins the chunk layout (file size, chunk size, upload id) so resumption works even if the memory-based chunk sizing would pick a different chunk size on the next run. The manifest is deleted once the upload completes.
- **Small files** (single `PutObject`): uploaded atomically, so an interrupted small file simply re-uploads in full.
- **Error handling**: transport failures (timeouts, connection drops) leave the multipart upload and manifest in place for the next run; integrity failures (size or part-layout mismatches) abort the upload and start fresh.
- The JSON summary's `bytes_transferred` only counts bytes moved during the current run (resumed parts are excluded).

## Known Limitations

- **Xet repos**: work via plain `/resolve/` URLs, but hfs3 does not use Xet's chunked-CAS protocol. Very large Xet-hosted files are downloaded as plain HTTP streams (transfer resumption still works at the S3 multipart level).
- **Custom S3 endpoints** (`HFS3_S3_ENDPOINT`): always use path-style addressing (`endpoint/bucket/key`). Endpoints that require virtual-host style (`bucket.endpoint`) are not supported.

## Development

Requires a devcontainer (Rust toolchain, just, awscli). All `just` recipes run inside the container automatically:

```bash
just dev        # rebuild the devcontainer
just build      # build
just test       # run tests
just clippy     # lint
just fmt        # format
just example    # mirror a tiny test repo
```

## License

MIT
