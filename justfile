# hfs3 — Mirror HuggingFace to S3

default:
    @just --list

# Ensure devcontainer is running, then exec a command with host AWS creds
[private]
dc +cmd:
    #!/usr/bin/env bash
    set -euo pipefail
    devcontainer up --workspace-folder .
    # Resolve host AWS credentials and forward into the container
    eval "$(aws configure export-credentials --format env 2>/dev/null || true)"
    remote_env=()
    [ -n "${AWS_ACCESS_KEY_ID:-}" ] && remote_env+=(--remote-env "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID")
    [ -n "${AWS_SECRET_ACCESS_KEY:-}" ] && remote_env+=(--remote-env "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY")
    [ -n "${AWS_SESSION_TOKEN:-}" ] && remote_env+=(--remote-env "AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN")
    [ -n "${AWS_REGION:-}" ] && remote_env+=(--remote-env "AWS_REGION=$AWS_REGION")
    devcontainer exec --workspace-folder . "${remote_env[@]}" {{cmd}}

# Spin up the devcontainer (force rebuild)
dev:
    devcontainer up --workspace-folder . --remove-existing-container

# Run an arbitrary command inside the devcontainer
exec +cmd:
    just dc {{cmd}}

build:
    just dc cargo build

build-release:
    just dc cargo build --release

test *args='':
    just dc cargo test {{args}}

test-v *args='':
    just dc cargo test {{args}} -- --nocapture

check:
    just dc cargo check

clippy:
    just dc cargo clippy -- -D warnings

fmt:
    just dc cargo fmt

fmt-check:
    just dc cargo fmt -- --check

lint: fmt-check clippy

clean:
    just dc cargo clean

# Main workflows
# Mirror a tiny test repo (~1MB) to verify setup
example:
    just mirror hf-internal-testing/tiny-random-bert

mirror repo:
    just dc cargo run -- mirror "{{repo}}"

pull repo dest='./repo':
    just dc cargo run -- pull "{{repo}}" --dest "{{dest}}"

run repo dest='./repo' port='7860':
    just dc cargo run -- run "{{repo}}" --dest "{{dest}}" --port "{{port}}"

# === S3 end-to-end testing ===
# e2e runs on the host with a bare cargo install (not inside the devcontainer).
# It only needs a reachable S3 endpoint (real AWS, a cluster S3, or a local
# MinIO), the aws-cli, and python3. Override with:
#   just e2e S3_ENDPOINT=https://s3.example.com:9000 E2E_BUCKET=my-bucket
# Credentials: set E2E_S3_USER/E2E_S3_PASS for explicit keys, or leave them
# empty to use the ambient AWS credential chain (env vars / ~/.aws).

E2E_S3_USER := ''
E2E_S3_PASS := ''
S3_ENDPOINT := 'http://localhost:9000'
E2E_BUCKET := 'hfs3-e2e'
E2E_REPO := 'hf-internal-testing/tiny-random-bert'
E2E_AWS_REGION := 'us-east-1'

# Start a throwaway MinIO with fixed dev creds (http://localhost:9000).
# Optional helper for machines without any real S3; not needed when
# S3_ENDPOINT points at an existing server.
s3-up:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -n "$(docker ps --filter name=^hfs3-minio$ -q)" ]; then
      echo "hfs3-minio is already running at {{S3_ENDPOINT}}"
      exit 0
    fi
    docker rm -f hfs3-minio >/dev/null 2>&1 || true
    # Pin a specific release tag if you want reproducible e2e runs.
    docker run -d --name hfs3-minio \
      -e MINIO_ROOT_USER=hfs3test \
      -e MINIO_ROOT_PASSWORD=hfs3testpassword \
      -p 9000:9000 \
      minio/minio:latest \
      server /data --address :9000 --console-address :9001
    endpoint="{{S3_ENDPOINT}}"
    for i in $(seq 1 30); do
      if python3 scripts/s3-probe.py "$endpoint" >/dev/null; then
        echo "MinIO ready at $endpoint (console on :9001, creds hfs3test/hfs3testpassword)"
        exit 0
      fi
      sleep 1
    done
    echo "MinIO did not become ready within 30s (docker logs hfs3-minio)" >&2
    exit 1

# Stop and remove the throwaway MinIO
s3-down:
    #!/usr/bin/env bash
    set -euo pipefail
    docker rm -f hfs3-minio >/dev/null 2>&1 || true
    echo "MinIO stopped"

# End-to-end: mirror a tiny HF repo to S3, pull it back, and verify every
# file byte-for-byte against the HuggingFace original.
e2e:
    #!/usr/bin/env bash
    set -euo pipefail
    export HFS3_S3_BUCKET="{{E2E_BUCKET}}"
    export HFS3_S3_ENDPOINT="{{S3_ENDPOINT}}"
    if [ -n "{{E2E_S3_USER}}" ]; then
      export AWS_ACCESS_KEY_ID="{{E2E_S3_USER}}"
      export AWS_SECRET_ACCESS_KEY="{{E2E_S3_PASS}}"
    fi
    export AWS_REGION="{{E2E_AWS_REGION}}"
    endpoint="{{S3_ENDPOINT}}"
    python3 scripts/s3-probe.py "$endpoint"
    if ! aws --endpoint-url "$endpoint" s3api head-bucket --bucket "$HFS3_S3_BUCKET" >/dev/null 2>&1; then
      echo "Creating bucket $HFS3_S3_BUCKET"
      aws --endpoint-url "$endpoint" s3api create-bucket --bucket "$HFS3_S3_BUCKET"
    fi
    repo="{{E2E_REPO}}"
    dest="/tmp/hfs3-e2e/${repo//\//_}"
    rm -rf "$dest"
    mkdir -p "$dest"
    echo "=== mirror $repo -> s3://$HFS3_S3_BUCKET ==="
    cargo run --quiet -- mirror "$repo"
    echo "=== pull s3://$HFS3_S3_BUCKET -> $dest ==="
    cargo run --quiet -- pull "$repo" --dest "$dest"
    echo "=== verifying pulled files against HuggingFace originals ==="
    python3 scripts/e2e-verify.py "$repo" "$dest"
