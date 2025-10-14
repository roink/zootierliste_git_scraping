#!/usr/bin/env python3
"""
zoo_locations.py — fetch a list of zoos with coordinates from Zootierliste and
write a CSV with columns: zoo_id, latitude, longitude (sorted by zoo_id).

Usage:
  python zoo_locations.py --out zoos.csv
  python zoo_locations.py --out zoos.csv --url https://www.zootierliste.de/map_zoos.php

Notes:
- This expects Zootierliste's 'map_zoos.php' to return a tab-separated payload
  with a header row and a first column "lat,lon" and a second column "zoo_id".
- We commit only (zoo_id, lat, lon). If multiple rows share a zoo_id, the first
  occurrence wins (you can change that behavior below if needed).
"""

import argparse
import csv
import sys
import time
from typing import Dict, Tuple, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_URL = "https://www.zootierliste.de/map_zoos.php"
DEFAULT_OUT = "zoo_locations.csv"
SLEEP_SECONDS = 1.0  # be polite to the remote server

HEADERS = {
    "User-Agent": "Mozilla/5.0 (+https://github.com/)",
}
AJAX_HEADERS = {
    "X-Requested-With": "XMLHttpRequest",
}


def session_with_retries() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "HEAD"]),
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update(HEADERS)
    return s


def fetch_map_tsv(url: str, sess: requests.Session) -> str:
    """
    Fetch the raw (tab-separated) text from map_zoos.php.
    If the endpoint requires AJAX, we add the header; harmless if not required.
    """
    r = sess.get(url, headers=AJAX_HEADERS, timeout=(10, 30))
    r.raise_for_status()
    time.sleep(SLEEP_SECONDS)
    # Some pages may prepend BOM; strip common BOMs.
    text = r.text.lstrip("\ufeff").lstrip("\ufffe")
    return text


def parse_to_locations(tsv_text: str) -> List[Tuple[int, float, float]]:
    """
    Parse the TSV from Zootierliste and return a list of (zoo_id, lat, lon).
    The expected format is:
      header...
      "<lat>,<lon>\t<zoo_id>\t..."
    """
    out: List[Tuple[int, float, float]] = []
    reader = csv.reader(tsv_text.splitlines(), delimiter="\t")
    # Skip header row
    header = next(reader, None)
    for row in reader:
        if not row or len(row) < 2:
            continue
        latlon = row[0].strip()
        zoo_id_str = row[1].strip()
        if not latlon or not zoo_id_str:
            continue
        try:
            lat_str, lon_str = latlon.split(",", 1)
            lat = float(lat_str)
            lon = float(lon_str)
            zoo_id = int(zoo_id_str)
            out.append((zoo_id, lat, lon))
        except Exception:
            # Skip malformed rows
            continue
    return out


def dedupe_by_zoo_id(locs: List[Tuple[int, float, float]]) -> Dict[int, Tuple[float, float]]:
    """
    Keep the first occurrence of each zoo_id.
    If you prefer 'last occurrence wins', just assign unconditionally.
    """
    result: Dict[int, Tuple[float, float]] = {}
    for zoo_id, lat, lon in locs:
        if zoo_id not in result:
            result[zoo_id] = (lat, lon)
    return result

def _fmt_sig6(x: float) -> str:
    """
    Format with up to 6 significant digits (no scientific notation for normal lat/lon),
    trim trailing zeros, and ensure at least one decimal point.
    """
    s = format(x, ".6g")  # 6 significant digits
    if "e" in s or "E" in s:
        s = f"{x:.6f}".rstrip("0").rstrip(".")
    if "." not in s:  # keep a decimal point to make it clear it's a float
        s += ".0"
    return s

def write_csv(path: str, items: List[Tuple[int, float, float]]) -> None:
     with open(path, "w", newline="", encoding="utf-8") as f:
         w = csv.writer(f)
         w.writerow(["zoo_id", "latitude", "longitude"])
         for zoo_id, lat, lon in items:
            w.writerow([zoo_id, _fmt_sig6(lat), _fmt_sig6(lon)])


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate CSV of zoo_id, latitude, longitude from Zootierliste.")
    ap.add_argument("--url", default=DEFAULT_URL, help="map_zoos.php URL")
    ap.add_argument("--out", default=DEFAULT_OUT, help="Output CSV path")
    args = ap.parse_args()

    sess = session_with_retries()
    try:
        tsv_text = fetch_map_tsv(args.url, sess)
        locs = parse_to_locations(tsv_text)
        if not locs:
            print("No locations parsed. Is the endpoint reachable and format as expected?", file=sys.stderr)
            return 2
        by_id = dedupe_by_zoo_id(locs)
        sorted_rows = sorted(((zid, lat, lon) for zid, (lat, lon) in by_id.items()), key=lambda x: x[0])
        write_csv(args.out, sorted_rows)
        print(f"Wrote {len(sorted_rows)} rows to {args.out}")
        return 0
    except requests.HTTPError as e:
        print(f"HTTP error: {e}", file=sys.stderr)
        return 3
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

