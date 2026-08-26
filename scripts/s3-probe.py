#!/usr/bin/env python3
"""Check that an S3 endpoint URL is reachable (TCP connect).

Usage: python3 scripts/s3-probe.py <url>
"""
import socket
import sys
import urllib.parse


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    url = sys.argv[1]
    host, _, port = urllib.parse.urlparse(url).netloc.partition(":")
    port = int(port or (443 if url.startswith("https") else 80))
    try:
        socket.create_connection((host, port), timeout=3)
    except OSError as exc:
        print(f"S3 endpoint {url} is not reachable: {exc}", file=sys.stderr)
        print(
            "Point S3_ENDPOINT at a running S3 server "
            "(see README, Testing against S3).",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
