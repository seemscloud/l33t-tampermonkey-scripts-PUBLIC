#!/usr/bin/env python3
"""
Static secret/key scan for Airflow code directory.

Default target: airflow/code/
Usage:
  python3 analyze_static.py            # scans airflow/code
  python3 analyze_static.py other/path # scan custom path(s)
"""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path
from typing import Iterable, List, Tuple

# Regex patterns for common secret/token/password markers
PATTERNS: List[Tuple[str, re.Pattern[str]]] = [
    ("api_key", re.compile(r"(?i)api[_-]?key\s*[:=]\s*['\"]?([A-Za-z0-9._-]{16,})")),
    ("access_key", re.compile(r"(?i)access[_-]?key\s*[:=]\s*['\"]?([A-Za-z0-9._-]{16,})")),
    ("access_token", re.compile(r"(?i)access[_-]?token\s*[:=]\s*['\"]?([A-Za-z0-9._-]{16,})")),
    ("refresh_token", re.compile(r"(?i)refresh[_-]?token\s*[:=]\s*['\"]?([A-Za-z0-9._-]{16,})")),
    ("secret", re.compile(r"(?i)secret\s*[:=]\s*['\"]?([A-Za-z0-9./+=_-]{8,})")),
    ("password", re.compile(r"(?i)pass(word)?\s*[:=]\s*['\"]?([^\s'\"]{6,})")),
    ("token", re.compile(r"(?i)token\s*[:=]\s*['\"]?([A-Za-z0-9._-]{10,})")),
    ("bearer_header", re.compile(r"(?i)bearer\s+[A-Za-z0-9._-]{20,}")),
    ("authorization_header", re.compile(r"(?i)authorization\s*[:=]\s*['\"]?\w+\s+[A-Za-z0-9._-]{10,}")),
    ("aws_access_key_id", re.compile(r"AKIA[0-9A-Z]{16}")),
    ("aws_secret_access_key", re.compile(r"(?<![A-Za-z0-9])[A-Za-z0-9/+=]{40}(?![A-Za-z0-9/+=])")),
    ("uri_basic_auth", re.compile(r"[a-zA-Z][a-zA-Z0-9+.-]*://[^\s'\"<>]+?:[^\s'\"<>]+?@")),
    ("client_secret", re.compile(r"(?i)client_secret\s*[:=]\s*['\"]?([^\s'\"]{8,})")),
    ("client_id", re.compile(r"(?i)client_id\s*[:=]\s*['\"]?([^\s'\"]{6,})")),
    ("private_key_pem", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
    ("private_key_rsa", re.compile(r"-----BEGIN RSA PRIVATE KEY-----")),
    ("private_key_ec", re.compile(r"-----BEGIN EC PRIVATE KEY-----")),
    ("private_key_dsa", re.compile(r"-----BEGIN DSA PRIVATE KEY-----")),
    ("private_key_pgp", re.compile(r"-----BEGIN PGP PRIVATE KEY BLOCK-----")),
    ("ssh_private_key", re.compile(r"-----BEGIN OPENSSH PRIVATE KEY-----")),
    ("ssh_rsa", re.compile(r"ssh-rsa\s+[A-Za-z0-9+/=]{20,}")),
    ("slack_token", re.compile(r"xox[baprs]-[0-9A-Za-z-]{20,}")),
    ("github_token", re.compile(r"gh[pous]_[A-Za-z0-9]{36}")),
    ("gitlab_pat", re.compile(r"glpat-[A-Za-z0-9_-]{20,}")),
    ("stripe_key", re.compile(r"sk_(live|test)_[A-Za-z0-9]{20,}")),
    ("sendgrid_key", re.compile(r"SG\.[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}")),
    ("google_api_key", re.compile(r"AIza[0-9A-Za-z-_]{35}")),
    ("telegram_bot_token", re.compile(r"\d{8,10}:AA[A-Za-z0-9_-]{33}")),
    ("twilio_sid", re.compile(r"AC[0-9a-fA-F]{32}")),
    ("twilio_token", re.compile(r"(?i)twilio(_auth)?_?token\s*[:=]\s*['\"]?([0-9a-f]{32})")),
    ("jwt", re.compile(r"[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}")),
]


def iter_files(paths: Iterable[str]) -> Iterable[Path]:
    for p in paths:
        path = Path(p)
        if path.is_dir():
            for root, _dirs, files in os.walk(path):
                for fname in files:
                    yield Path(root) / fname
        elif path.is_file():
            yield path


def scan_file(path: Path, context: int) -> List[Tuple[int, str, str, List[str]]]:
    matches: List[Tuple[int, str, str, List[str]]] = []
    try:
        if path.stat().st_size > 5 * 1024 * 1024:
            return matches
        text = path.read_text(errors="ignore")
    except (OSError, UnicodeDecodeError):
        return matches

    lines = text.splitlines()
    for idx, line in enumerate(lines, start=1):
        for name, pat in PATTERNS:
            m = pat.search(line)
            if m:
                snippet = m.group(0)
                start = max(0, idx - (context + 1))
                end = min(len(lines), idx + context)
                ctx_lines = lines[start:end]
                matches.append((idx, name, snippet.strip(), ctx_lines))
    return matches


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan files for likely secrets/keys.")
    parser.add_argument(
        "paths",
        nargs="*",
        help="Files or directories to scan (default: airflow/code)",
    )
    parser.add_argument(
        "-C",
        "--context",
        type=int,
        default=3,
        help="Number of context lines before/after a match (default: 3)",
    )
    args = parser.parse_args()
    targets = args.paths or [Path(__file__).resolve().parent / "code"]
    ctx = max(0, args.context)

    total = 0
    for path in iter_files(targets):
        hits = scan_file(path, ctx)
        for line_no, kind, snippet, context in hits:
            total += 1
            print(f"{path}:{line_no}: [{kind}] {snippet}")
            # show 3 lines before and after
            for rel_idx, ctx_line in enumerate(context):
                ctx_no = line_no - (len(context) - rel_idx) + (len(context) - 1)
                print(f"    {ctx_no:>5}: {ctx_line}")

    if total == 0:
        print("No obvious secrets found.")
    else:
        print(f"Found {total} potential secret(s).")
    return 0 if total == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

