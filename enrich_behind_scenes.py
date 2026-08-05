#!/usr/bin/env python3
"""
Enrich per-zoo holdings CSVs with Zootierliste's "hinter den Kulissen" status.

Request strategy:

* cap page requests per invocation with --max-pages;
* persist a compact cache in Git;
* join holders to stable zoo IDs using exact normalized (country, zoo_name)
  values collected from the existing getarten responses.

CSV semantics:

* 1 = a recognized behind-the-scenes/off-show phrase occurs in the holding note;
* 0 = the species page was checked, the holder matched, and no such phrase occurs;
* empty = species not checked yet or holder could not be mapped reliably.

A 0 therefore means "no Zootierliste behind-scenes phrase found", not a
guarantee that the animal is publicly visible.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import os
import re
import sys
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SPECIES_URL = "https://www.zootierliste.de/"
HEADERS = {
    "User-Agent": (
        "ZooTracker-Zootierliste-scraper/2.0 "
        "(https://github.com/roink/zootierliste_git_scraping)"
    ),
}
CSV_HEADER = ["art", "klasse", "ordnung", "familie", "behind_scenes"]

BEHIND_SCENES_PATTERNS = (
    re.compile(r"\bhinter\s+den\s+kulissen\b", re.IGNORECASE),
    re.compile(r"\bbehind\s+the\s+scenes\b", re.IGNORECASE),
    re.compile(r"\boff[\s-]*(?:show|exhibit(?:ion)?)\b", re.IGNORECASE),
    re.compile(
        r"\bnot\s+(?:currently\s+)?on\s+(?:show|display|exhibit)\b",
        re.IGNORECASE,
    ),
)


@dataclass(frozen=True)
class TaxonRef:
    art: int
    klasse: int
    ordnung: int
    familie: int


@dataclass(frozen=True)
class ZooIdentity:
    zoo_id: int
    zoo_name: str
    country: str


@dataclass(frozen=True)
class ParsedHolder:
    country: str
    zoo_name: str
    behind_scenes: bool
    note: str


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def isoformat_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def normalize_key(value: str) -> str:
    value = html.unescape(value)
    value = unicodedata.normalize("NFKC", value)
    value = value.replace("\u00a0", " ")
    return " ".join(value.split()).casefold()


def normalized_note(value: str) -> str:
    value = html.unescape(value)
    value = unicodedata.normalize("NFKC", value)
    value = value.replace("\u00a0", " ")
    return " ".join(value.split())


def is_behind_scenes(note: str) -> bool:
    normalized = normalized_note(note)
    return any(pattern.search(normalized) for pattern in BEHIND_SCENES_PATTERNS)


def extract_overlib_note(onclick: str) -> str:
    """
    Convert the simple string expression used in Zootierliste's overlib call
    into readable text. This is deliberately not a JavaScript evaluator.
    """
    if not onclick:
        return ""

    expression = onclick
    if "overlib(" in expression:
        expression = expression.split("overlib(", 1)[1]
    if ", STICKY" in expression:
        expression = expression.rsplit(", STICKY", 1)[0]

    expression = re.sub(
        r"""['"]?\s*\+\s*String\.fromCharCode\(\s*34\s*\)\s*\+\s*['"]?""",
        '"',
        expression,
    )
    expression = re.sub(
        r"""['"]?\s*\+\s*String\.fromCharCode\(\s*39\s*\)\s*\+\s*['"]?""",
        "'",
        expression,
    )
    expression = expression.strip().strip(";")
    if len(expression) >= 2 and expression[0] == expression[-1] and expression[0] in {
        "'",
        '"',
    }:
        expression = expression[1:-1]

    expression = expression.replace(r"\'", "'").replace(r'\"', '"')
    expression = expression.replace(r"\n", "\n").replace(r"\r", "")
    soup = BeautifulSoup(html.unescape(expression), "html.parser")
    return normalized_note(soup.get_text(" ", strip=True))


def country_from_container(container: Tag) -> str | None:
    container_id = str(container.get("id") or "")
    marker = "_land_"
    suffix = "_zoos"
    if marker not in container_id or not container_id.endswith(suffix):
        return None
    return container_id.split(marker, 1)[1][: -len(suffix)]


def parse_species_holders(html_text: str) -> list[ParsedHolder]:
    soup = BeautifulSoup(html_text, "html.parser")
    holders: list[ParsedHolder] = []

    containers = soup.find_all(
        "div",
        id=lambda value: isinstance(value, str)
        and "_land_" in value
        and value.endswith("_zoos"),
    )
    for container in containers:
        country = country_from_container(container)
        if not country:
            continue

        for anchor in container.find_all("a", recursive=True):
            # Holder anchors use javascript:void(0). Source links only appear
            # inside an onclick string and are therefore not parsed as children.
            href = normalize_key(str(anchor.get("href") or ""))
            if href != "javascript:void(0)":
                continue

            zoo_name = normalized_note(anchor.get_text(" ", strip=True))
            if not zoo_name:
                continue

            onclick = str(anchor.get("onclick") or "")
            note = extract_overlib_note(onclick)
            holders.append(
                ParsedHolder(
                    country=normalized_note(country),
                    zoo_name=zoo_name,
                    behind_scenes=is_behind_scenes(onclick) or is_behind_scenes(note),
                    note=note,
                )
            )

    return holders


def session_with_retries(max_retries: int) -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=max_retries,
        connect=max_retries,
        read=max_retries,
        status=max_retries,
        backoff_factor=0.75,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session


def read_taxa_and_relations(
    holdings_dir: Path,
) -> tuple[dict[int, TaxonRef], dict[int, set[int]]]:
    taxa: dict[int, TaxonRef] = {}
    relations: dict[int, set[int]] = {}

    for path in sorted(holdings_dir.glob("*.csv")):
        try:
            zoo_id = int(path.stem)
        except ValueError:
            continue

        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            required = {"art", "klasse", "ordnung", "familie"}
            if not reader.fieldnames or not required.issubset(reader.fieldnames):
                print(f"[WARN] skipping malformed holdings file {path}", file=sys.stderr)
                continue

            for row in reader:
                try:
                    taxon = TaxonRef(
                        art=int((row.get("art") or "").strip()),
                        klasse=int((row.get("klasse") or "").strip()),
                        ordnung=int((row.get("ordnung") or "").strip()),
                        familie=int((row.get("familie") or "").strip()),
                    )
                except ValueError:
                    continue

                previous = taxa.get(taxon.art)
                if previous is not None and previous != taxon:
                    raise ValueError(
                        f"conflicting taxonomy for art={taxon.art}: "
                        f"{previous!r} versus {taxon!r}"
                    )
                taxa[taxon.art] = taxon
                relations.setdefault(taxon.art, set()).add(zoo_id)

    return taxa, relations


def read_identities(
    path: Path,
) -> tuple[dict[tuple[str, str], int], set[tuple[str, str]]]:
    candidates: dict[tuple[str, str], set[int]] = {}

    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {"zoo_id", "zoo_name", "country"}
        if not reader.fieldnames or not required.issubset(reader.fieldnames):
            raise ValueError(f"{path} has an unexpected header")

        for row in reader:
            try:
                identity = ZooIdentity(
                    zoo_id=int((row.get("zoo_id") or "").strip()),
                    zoo_name=(row.get("zoo_name") or "").strip(),
                    country=(row.get("country") or "").strip(),
                )
            except ValueError:
                continue

            key = (
                normalize_key(identity.country),
                normalize_key(identity.zoo_name),
            )
            candidates.setdefault(key, set()).add(identity.zoo_id)

    unique: dict[tuple[str, str], int] = {}
    ambiguous: set[tuple[str, str]] = set()
    for key, zoo_ids in candidates.items():
        if len(zoo_ids) == 1:
            unique[key] = next(iter(zoo_ids))
        else:
            ambiguous.add(key)

    return unique, ambiguous


def default_cache() -> dict[str, Any]:
    return {"schema_version": 1, "species": {}}


def load_cache(path: Path) -> dict[str, Any]:
    if not path.exists():
        return default_cache()
    with path.open(encoding="utf-8") as handle:
        data = json.load(handle)
    if data.get("schema_version") != 1 or not isinstance(data.get("species"), dict):
        raise ValueError(f"unsupported cache schema in {path}")
    return data


def atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
    temporary.replace(path)


def species_needs_fetch(
    cache_entry: dict[str, Any] | None,
    stale_before: datetime,
) -> bool:
    if not cache_entry:
        return True
    fetched_at = parse_timestamp(cache_entry.get("fetched_at"))
    return fetched_at is None or fetched_at < stale_before


def fetch_species_page(
    session: requests.Session,
    taxon: TaxonRef,
    timeout: tuple[float, float],
) -> str:
    response = session.get(
        SPECIES_URL,
        params={
            "klasse": taxon.klasse,
            "ordnung": taxon.ordnung,
            "familie": taxon.familie,
            "art": taxon.art,
        },
        timeout=timeout,
    )
    response.raise_for_status()
    return response.text.lstrip("\ufeff")


def build_cache_entry(
    taxon: TaxonRef,
    parsed_holders: list[ParsedHolder],
    identity_map: dict[tuple[str, str], int],
    ambiguous_keys: set[tuple[str, str]],
    expected_zoo_ids: set[int],
) -> dict[str, Any]:
    holders: dict[str, int] = {}
    positive_evidence: dict[str, str] = {}
    unmatched: list[dict[str, Any]] = []

    for holder in parsed_holders:
        key = (normalize_key(holder.country), normalize_key(holder.zoo_name))
        zoo_id = identity_map.get(key)

        if zoo_id is None:
            unmatched.append(
                {
                    "country": holder.country,
                    "zoo_name": holder.zoo_name,
                    "behind_scenes": int(holder.behind_scenes),
                    "reason": "ambiguous identity" if key in ambiguous_keys else "no identity",
                }
            )
            continue

        holders[str(zoo_id)] = int(holder.behind_scenes)
        if holder.behind_scenes and holder.note:
            positive_evidence[str(zoo_id)] = holder.note[:2000]

    missing_expected = sorted(expected_zoo_ids - {int(value) for value in holders})

    return {
        "art": taxon.art,
        "klasse": taxon.klasse,
        "ordnung": taxon.ordnung,
        "familie": taxon.familie,
        "fetched_at": isoformat_z(utc_now()),
        "holders": dict(sorted(holders.items(), key=lambda item: int(item[0]))),
        "positive_evidence": dict(
            sorted(positive_evidence.items(), key=lambda item: int(item[0]))
        ),
        "unmatched": unmatched,
        "missing_expected_zoo_ids": missing_expected,
    }


def status_from_cache(
    cache: dict[str, Any],
    art: int,
    zoo_id: int,
) -> str:
    entry = cache["species"].get(str(art))
    if not entry:
        return ""
    value = entry.get("holders", {}).get(str(zoo_id))
    if value in (0, 1):
        return str(value)
    return ""


def rewrite_holdings_csvs(holdings_dir: Path, cache: dict[str, Any]) -> tuple[int, int]:
    changed_files = 0
    changed_rows = 0

    for path in sorted(holdings_dir.glob("*.csv")):
        try:
            zoo_id = int(path.stem)
        except ValueError:
            continue

        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                continue
            rows = list(reader)

        output_rows: list[list[str]] = []
        file_changed = reader.fieldnames != CSV_HEADER

        for row in rows:
            try:
                art = int((row.get("art") or "").strip())
            except ValueError:
                continue

            old_status = (row.get("behind_scenes") or "").strip()
            new_status = status_from_cache(cache, art, zoo_id)
            if old_status != new_status:
                file_changed = True
                changed_rows += 1

            output_rows.append(
                [
                    (row.get("art") or "").strip(),
                    (row.get("klasse") or "").strip(),
                    (row.get("ordnung") or "").strip(),
                    (row.get("familie") or "").strip(),
                    new_status,
                ]
            )

        if not file_changed:
            continue

        temporary = path.with_suffix(path.suffix + ".tmp")
        with temporary.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(CSV_HEADER)
            writer.writerows(output_rows)
        temporary.replace(path)
        changed_files += 1

    return changed_files, changed_rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--holdings-dir", type=Path, default=Path("holdings"))
    parser.add_argument(
        "--identities",
        type=Path,
        default=Path("zoo_identities.csv"),
    )
    parser.add_argument(
        "--cache",
        type=Path,
        default=Path("species_display_status.json"),
    )
    parser.add_argument(
        "--art",
        type=int,
        action="append",
        default=[],
        help=(
            "Restrict processing to one art ID. May be supplied repeatedly; "
            "useful for diagnostics and targeted refreshes."
        ),
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=500,
        help="Maximum species-page requests in this invocation.",
    )
    parser.add_argument(
        "--refresh-days",
        type=int,
        default=90,
        help="Refetch a species page after this many days.",
    )
    parser.add_argument("--sleep", type=float, default=1.0)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument(
        "--timeout",
        nargs=2,
        type=float,
        default=(5.0, 30.0),
        metavar=("CONNECT", "READ"),
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=25,
        help="Persist the cache after this many successful page fetches.",
    )
    args = parser.parse_args()

    if args.max_pages < 0:
        parser.error("--max-pages must be non-negative")
    if args.refresh_days < 0:
        parser.error("--refresh-days must be non-negative")

    taxa, relations = read_taxa_and_relations(args.holdings_dir)
    identity_map, ambiguous_keys = read_identities(args.identities)
    cache = load_cache(args.cache)

    selected_arts = sorted(set(args.art)) if args.art else sorted(taxa)
    unknown_arts = [art for art in selected_arts if art not in taxa]
    if unknown_arts:
        parser.error(
            "art IDs are not present in the holdings CSVs: "
            + ", ".join(str(art) for art in unknown_arts)
        )

    stale_before = utc_now() - timedelta(days=args.refresh_days)
    missing: list[TaxonRef] = []
    stale: list[TaxonRef] = []

    for art in selected_arts:
        taxon = taxa[art]
        entry = cache["species"].get(str(art))
        if not entry:
            missing.append(taxon)
        elif species_needs_fetch(entry, stale_before):
            stale.append(taxon)

    candidates = (missing + stale)[: args.max_pages]
    print(
        f"Taxa={len(taxa)}; cache={len(cache['species'])}; "
        f"missing={len(missing)}; stale={len(stale)}; "
        f"requesting={len(candidates)}"
    )
    if ambiguous_keys:
        print(
            f"[WARN] {len(ambiguous_keys)} duplicate (country, zoo_name) "
            "identity keys will not be matched.",
            file=sys.stderr,
        )

    session = session_with_retries(args.max_retries)
    successful = 0
    for index, taxon in enumerate(candidates, start=1):
        try:
            page = fetch_species_page(
                session,
                taxon,
                (args.timeout[0], args.timeout[1]),
            )
            parsed_holders = parse_species_holders(page)
            cache["species"][str(taxon.art)] = build_cache_entry(
                taxon=taxon,
                parsed_holders=parsed_holders,
                identity_map=identity_map,
                ambiguous_keys=ambiguous_keys,
                expected_zoo_ids=relations.get(taxon.art, set()),
            )
            successful += 1
            entry = cache["species"][str(taxon.art)]
            positive = sum(entry["holders"].values())
            print(
                f"[OK] {index}/{len(candidates)} art={taxon.art}: "
                f"holders={len(entry['holders'])}, "
                f"behind_scenes={positive}, "
                f"unmatched={len(entry['unmatched'])}, "
                f"missing_expected={len(entry['missing_expected_zoo_ids'])}"
            )

            if (
                args.checkpoint_every > 0
                and successful % args.checkpoint_every == 0
            ):
                atomic_write_json(args.cache, cache)
        except requests.HTTPError as exc:
            print(f"[HTTP] art={taxon.art}: {exc}", file=sys.stderr)
        except Exception as exc:
            print(f"[ERR] art={taxon.art}: {exc}", file=sys.stderr)

        if args.sleep > 0 and index < len(candidates):
            time.sleep(args.sleep)

    atomic_write_json(args.cache, cache)
    changed_files, changed_rows = rewrite_holdings_csvs(args.holdings_dir, cache)
    print(
        f"Cache saved to {args.cache}; successful requests={successful}; "
        f"holdings files changed={changed_files}; rows changed={changed_rows}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
