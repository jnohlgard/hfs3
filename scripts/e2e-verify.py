#!/usr/bin/env python3
"""Verify pulled files match HuggingFace originals byte-for-byte.

Usage: python3 scripts/e2e-verify.py <repo-id> <dest-dir>

Lists every file in the repo (via the HF API), downloads each original,
and compares its sha256 against the locally pulled copy. Exits non-zero
if any file is missing or differs.
"""
import hashlib
import json
import os
import sys
import urllib.parse
import urllib.request


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    repo, dest = sys.argv[1], sys.argv[2]
    tree_url = f"https://huggingface.co/api/models/{repo}/tree/main?recursive=true"
    entries = json.load(urllib.request.urlopen(tree_url, timeout=60))
    files = sorted(e["path"] for e in entries if e["type"] == "file")
    if not files:
        print(f"error: HuggingFace returned no files for {repo}", file=sys.stderr)
        return 1
    failures = []
    for index, path in enumerate(files, start=1):
        url = f"https://huggingface.co/{repo}/resolve/main/{urllib.parse.quote(path)}"
        print(f"[{index}/{len(files)}] downloading {path} ...", flush=True)
        original = b""
        last_report = -1
        with urllib.request.urlopen(url, timeout=300) as resp:
            while True:
                chunk = resp.read(1 << 20)
                if not chunk:
                    break
                original += chunk
                done = len(original) >> 20
                if done > last_report:
                    last_report = done
                    print(f"    {last_report} MiB / {path}", file=sys.stderr, flush=True)
        print(f"    done: {len(original)} bytes", flush=True)
        local = os.path.join(dest, path)
        if not os.path.isfile(local):
            print(f"FAIL {path}: missing locally")
            failures.append(path)
            continue
        with open(local, "rb") as fh:
            pulled = fh.read()
        h_o = hashlib.sha256(original).hexdigest()
        h_p = hashlib.sha256(pulled).hexdigest()
        if h_o == h_p:
            print(f"OK   {path}  sha256:{h_o[:16]}")
        else:
            print(f"FAIL {path}  sha256:{h_o[:16]} vs {h_p[:16]}")
            failures.append(path)
    print(f"{len(files) - len(failures)}/{len(files)} files identical")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
