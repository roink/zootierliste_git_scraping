#!/usr/bin/env python3
"""
fetch_zoo_arts.py — fetch list of 'art' IDs for zoos from Zootierliste and
write CSV files named <zoo_id>.csv with a single column: art

Modes:
  # 1) Single zoo:
  python fetch_zoo_arts.py --zoo-id 10003612

  # 2) All IDs from zoo_locations.csv (expects a 'zoo_id' column):
  python fetch_zoo_arts.py --locations zoo_locations.csv

  # 3) Windowed subset from zoo_locations.csv:
  python fetch_zoo_arts.py --locations zoo_locations.csv --offset 200 --limit 50

Options:
  --out-dir DIR           Where to write CSVs (default: current directory)
  --haltung {0,1}         0 = current holdings (default), 1 = former
  --sleep SEC             Delay between requests (default 1.0)
  --max-retries N         HTTP retries per request (default 3)
  --timeout CONN READ     Timeouts (connect, read) in seconds (default 5 20)
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import time
from typing import Iterable, List, Set

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

AJAX_URL = "https://www.zootierliste.de/ajax.php"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Python-requests",
}
AJAX_HEADERS = {"X-Requested-With": "XMLHttpRequest"}


def session_with_retries(max_retries: int) -> requests.Session:
    """Create a Session with retry/backoff for robustness."""
    # Retries/backoff handled by urllib3 Retry.
    s = requests.Session()
    retries = Retry(
        total=max_retries,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset({"GET", "POST", "HEAD"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update(HEADERS)
    return s


_ART_QS_RE = re.compile(r"(?:^|[?&])art=(\d+)(?:&|$)")


def extract_art_ids_from_html(html_text: str) -> List[int]:
    """Parse HTML, find all <a href="...&art=####">, return sorted unique ints."""
    soup = BeautifulSoup(html_text, "html.parser")
    arts: Set[int] = set()
    for a in soup.find_all("a", href=True):  # finding links with href
        m = _ART_QS_RE.search(a["href"])
        if m:
            arts.add(int(m.group(1)))
    return sorted(arts)


def fetch_art_ids_for_zoo(
    sess: requests.Session,
    zoo_id: int,
    haltung: int,
    timeout: tuple[float, float],
) -> List[int]:
    """POST to ajax.php to get holdings HTML, extract 'art' IDs."""
    data = {
        "id": str(zoo_id),
        "haltung": str(haltung),         # 0 current, 1 former
        "aktion": "getarten",
        "sender": "zoosmap.php",
        "height": "530px",
    }
    r = sess.post(AJAX_URL, data=data, headers=AJAX_HEADERS, timeout=timeout)
    r.raise_for_status()
    return extract_art_ids_from_html(r.text.lstrip("\ufeff"))


def read_zoo_ids_from_locations(path: str) -> List[int]:
    """Read a CSV that contains a 'zoo_id' column (plus optional other columns)."""
    ids: List[int] = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "zoo_id" not in reader.fieldnames:
            raise ValueError(f"{path} has no 'zoo_id' column")
        for row in reader:
            v = row.get("zoo_id", "").strip()
            if not v:
                continue
            try:
                ids.append(int(v))
            except ValueError:
                continue
    return ids


def write_art_csv(out_dir: str, zoo_id: int, arts: Iterable[int]) -> str:
    """Write <out_dir>/<zoo_id>.csv with one column 'art'."""
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{zoo_id}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["art"])
        for a in arts:
            w.writerow([a])
    return path


def run_for_ids(
    ids: Iterable[int],
    out_dir: str,
    haltung: int,
    sleep_s: float,
    max_retries: int,
    timeout: tuple[float, float],
) -> None:
    sess = session_with_retries(max_retries)
    count = 0
    for zoo_id in ids:
        try:
            arts = fetch_art_ids_for_zoo(sess, zoo_id, haltung, timeout)
            path = write_art_csv(out_dir, zoo_id, arts)
            print(f"[OK] {zoo_id}: {len(arts)} arts → {path}")
            
        except requests.HTTPError as e:
            print(f"[HTTP] {zoo_id}: {e}", file=sys.stderr)
        except Exception as e:
            print(f"[ERR] {zoo_id}: {e}", file=sys.stderr)
        count += 1
        if sleep_s > 0 and count < 999999:
            time.sleep(sleep_s)


def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch Zootierliste 'art' IDs per zoo and write per-zoo CSV files.")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--zoo-id", type=int, help="Fetch a single zoo by ID")
    g.add_argument("--locations", metavar="CSV", help="CSV with a 'zoo_id' column (e.g., zoo_locations.csv)")
    ap.add_argument("--offset", type=int, default=0, help="Offset into locations list (default 0)")
    ap.add_argument("--limit", type=int, default=None, help="Max IDs to process from locations list")
    ap.add_argument("--haltung", type=int, choices=[0, 1], default=0, help="0=current (default), 1=former")
    ap.add_argument("--out-dir", default=".", help="Directory to write <zoo_id>.csv files")
    ap.add_argument("--sleep", type=float, default=1.0, help="Sleep between requests (seconds)")
    ap.add_argument("--max-retries", type=int, default=3, help="HTTP retries per request")
    ap.add_argument("--timeout", nargs=2, type=float, default=[5.0, 20.0],
                    metavar=("CONNECT", "READ"), help="Timeouts in seconds")
    args = ap.parse_args()

    if args.zoo_id is not None:
        ids = [args.zoo_id]
    else:
        all_ids = read_zoo_ids_from_locations(args.locations)
        if args.offset or args.limit is not None:
            start = max(args.offset, 0)
            end = start + args.limit if args.limit is not None else None
            ids = all_ids[start:end]
        else:
            ids = all_ids

    if not ids:
        print("No zoo IDs to process.", file=sys.stderr)
        return 2

    run_for_ids(
        ids=ids,
        out_dir=args.out_dir,
        haltung=args.haltung,
        sleep_s=args.sleep,
        max_retries=args.max_retries,
        timeout=(args.timeout[0], args.timeout[1]),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

