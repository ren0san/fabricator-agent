#!/usr/bin/env python3
"""Show running fabricator-agent version payload."""

from __future__ import annotations

import argparse
import json
import urllib.request


def main() -> int:
    parser = argparse.ArgumentParser(description="Show fabricator-agent version.")
    parser.add_argument("--url", default="http://127.0.0.1:8010", help="Agent base URL")
    args = parser.parse_args()

    url = args.url.rstrip("/") + "/version"
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=5) as resp:  # noqa: S310
        payload = json.loads(resp.read().decode("utf-8", errors="replace") or "{}")

    print(
        f"[agent] version={payload.get('version')} "
        f"tag={payload.get('tag')} commit={payload.get('commit')} dirty={payload.get('dirty')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
