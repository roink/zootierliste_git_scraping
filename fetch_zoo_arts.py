#!/usr/bin/env python3
"""
fetch_zoo_arts.py — fetch list of 'art' IDs for zoos from Zootierliste and
write CSV files named <zoo_id>.csv with columns: art, klasse, ordnung, familie

Since the 2.0 migration this script additionally:

* appends a backwards-compatible ``behind_scenes`` column;
* preserves an existing value for that column while refreshing inventories;
* records Zootierliste's canonical zoo name and country in zoo_identities.csv.

The identity file is used by enrich_behind_scenes.py to join species-page
holding notes back to stable zoo IDs without fuzzy matching.

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
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple
from urllib.parse import parse_qs, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

AJAX_URL = "https://www.zootierliste.de/ajax.php"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Python-requests",
}
AJAX_HEADERS = {"X-Requested-With": "XMLHttpRequest"}
CSV_HEADER = ["art", "klasse", "ordnung", "familie", "behind_scenes"]


@dataclass(frozen=True)
class ZooIdentity:
    zoo_id: int
    zoo_name: str
    country: str


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


def _parse_int_param(params: dict[str, list[str]], key: str) -> int | None:
    values = params.get(key)
    if not values:
        return None
    try:
        return int(values[0])
    except ValueError:
        return None


def extract_zoo_identity(soup: BeautifulSoup, zoo_id: int) -> ZooIdentity:
    """
    Parse e.g.:
      Aktueller Bestand
      von: Nashville (Zoo at Grassmere) [Vereinigte Staaten von Amerika]
    """
    first_cell = soup.select_one("#zootitle td")
    if first_cell is None:
        raise ValueError("getarten response has no #zootitle first cell")

    text = " ".join(first_cell.get_text(" ", strip=True).split())
    match = re.search(r"\bvon:\s*(.*?)\s*\[([^\[\]]+)\]\s*$", text)
    if not match:
        raise ValueError(f"could not parse zoo identity from: {text!r}")

    return ZooIdentity(
        zoo_id=zoo_id,
        zoo_name=match.group(1).strip(),
        country=match.group(2).strip(),
    )


def extract_art_rows_from_html(html_text: str) -> List[Tuple[int, int, int, int]]:
    """Parse HTML, find all <a href="...&art=####">, return sorted unique rows."""
    soup = BeautifulSoup(html_text, "html.parser")
    rows: dict[int, Tuple[int, int, int]] = {}
    for a in soup.find_all("a", href=True):  # finding links with href
        href = a["href"]
        if "art=" not in href:
            continue
        params = parse_qs(urlparse(href).query)
        art = _parse_int_param(params, "art")
        klasse = _parse_int_param(params, "klasse")
        ordnung = _parse_int_param(params, "ordnung")
        familie = _parse_int_param(params, "familie")
        if art is None or klasse is None or ordnung is None or familie is None:
            continue
        rows[art] = (klasse, ordnung, familie)
    return [(art, *rows[art]) for art in sorted(rows)]


def parse_inventory(
    html_text: str,
    zoo_id: int,
) -> Tuple[List[Tuple[int, int, int, int]], ZooIdentity]:
    soup = BeautifulSoup(html_text, "html.parser")
    return extract_art_rows_from_html(str(soup)), extract_zoo_identity(soup, zoo_id)


def fetch_art_rows_for_zoo(
    sess: requests.Session,
    zoo_id: int,
    haltung: int,
    timeout: tuple[float, float],
) -> Tuple[List[Tuple[int, int, int, int]], ZooIdentity]:
    """POST to ajax.php to get holdings HTML, extract art rows and zoo identity."""
    data = {
        "id": str(zoo_id),
        "haltung": str(haltung),         # 0 current, 1 former
        "aktion": "getarten",
        "sender": "zoosmap.php",
        "height": "530px",
    }
    r = sess.post(AJAX_URL, data=data, headers=AJAX_HEADERS, timeout=timeout)
    r.raise_for_status()
    return parse_inventory(r.text.lstrip("\ufeff"), zoo_id)


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


def read_existing_statuses(path: Path) -> dict[int, str]:
    """Retain enrichment while the inventory itself is refreshed."""
    if not path.exists():
        return {}

    statuses: dict[int, str] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or "art" not in reader.fieldnames:
            return {}
        for row in reader:
            try:
                art = int((row.get("art") or "").strip())
            except ValueError:
                continue
            value = (row.get("behind_scenes") or "").strip()
            if value in {"0", "1"}:
                statuses[art] = value
    return statuses


def write_art_csv(out_dir: str, zoo_id: int, rows: Iterable[tuple[int, int, int, int]]) -> str:
    """Write <out_dir>/<zoo_id>.csv with art/klasse/ordnung/familie/behind_scenes.

    Existing ``behind_scenes`` values are preserved when the inventory is
    refreshed; the write is atomic (tmp file + rename).
    """
    os.makedirs(out_dir, exist_ok=True)
    path = Path(out_dir) / f"{zoo_id}.csv"
    existing_statuses = read_existing_statuses(path)
    temporary = path.with_suffix(path.suffix + ".tmp")

    with temporary.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(CSV_HEADER)
        for art, klasse, ordnung, familie in rows:
            w.writerow(
                [art, klasse, ordnung, familie, existing_statuses.get(art, "")]
            )
    temporary.replace(path)
    return str(path)


def read_identities(path: Path) -> dict[int, ZooIdentity]:
    if not path.exists():
        return {}

    result: dict[int, ZooIdentity] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {"zoo_id", "zoo_name", "country"}
        if not reader.fieldnames or not required.issubset(reader.fieldnames):
            raise ValueError(f"{path} has an unexpected header")
        for row in reader:
            try:
                zoo_id = int((row.get("zoo_id") or "").strip())
            except ValueError:
                continue
            result[zoo_id] = ZooIdentity(
                zoo_id=zoo_id,
                zoo_name=(row.get("zoo_name") or "").strip(),
                country=(row.get("country") or "").strip(),
            )
    return result


def atomic_write_identities(
    path: Path,
    identities: dict[int, ZooIdentity],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["zoo_id", "zoo_name", "country"])
        for zoo_id in sorted(identities):
            identity = identities[zoo_id]
            writer.writerow([identity.zoo_id, identity.zoo_name, identity.country])
    temporary.replace(path)


def run_for_ids(
    ids: Iterable[int],
    out_dir: str,
    identity_path: Path,
    haltung: int,
    sleep_s: float,
    max_retries: int,
    timeout: tuple[float, float],
) -> None:
    sess = session_with_retries(max_retries)
    identities = read_identities(identity_path)
    ids_list = list(ids)
    count = 0
    for zoo_id in ids_list:
        count += 1
        try:
            rows, identity = fetch_art_rows_for_zoo(sess, zoo_id, haltung, timeout)
            path = write_art_csv(out_dir, zoo_id, rows)
            identities[zoo_id] = identity
            # Checkpoint identity data after every successful zoo.
            atomic_write_identities(identity_path, identities)
            print(
                f"[OK] {count}/{len(ids_list)} {zoo_id}: {len(rows)} arts "
                f"→ {path}; {identity.zoo_name!r}, {identity.country!r}"
            )
        except requests.HTTPError as e:
            print(f"[HTTP] {zoo_id}: {e}", file=sys.stderr)
        except Exception as e:
            print(f"[ERR] {zoo_id}: {e}", file=sys.stderr)
        if sleep_s > 0 and count < len(ids_list):
            time.sleep(sleep_s)


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Fetch Zootierliste 'art' IDs per zoo and write per-zoo CSV files "
            "with art/klasse/ordnung/familie columns."
        )
    )
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--zoo-id", type=int, help="Fetch a single zoo by ID")
    g.add_argument("--locations", metavar="CSV", help="CSV with a 'zoo_id' column (e.g., zoo_locations.csv)")
    ap.add_argument("--offset", type=int, default=0, help="Offset into locations list (default 0)")
    ap.add_argument("--limit", type=int, default=None, help="Max IDs to process from locations list")
    ap.add_argument("--haltung", type=int, choices=[0, 1], default=0, help="0=current (default), 1=former")
    ap.add_argument("--out-dir", default=".", help="Directory to write <zoo_id>.csv files")
    ap.add_argument(
        "--identity-out",
        type=Path,
        default=Path("zoo_identities.csv"),
        help="CSV to write/update zoo_id,zoo_name,country identities (default zoo_identities.csv)",
    )
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
        identity_path=args.identity_out,
        haltung=args.haltung,
        sleep_s=args.sleep,
        max_retries=args.max_retries,
        timeout=(args.timeout[0], args.timeout[1]),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
