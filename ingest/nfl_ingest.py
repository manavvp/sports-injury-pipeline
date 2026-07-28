"""
NFL raw-layer ingest from nflverse GitHub Releases into S3.

nflverse publishes each dataset as a GitHub Release; each year is a CSV asset
attached to that release. There is no REST API — the "endpoint" is a stable
download URL. This script wraps that pattern with retry, idempotent S3 uploads,
and a CLI so backfills and future single-year top-ups are the same code path.

Layout on S3 mirrors the source filename exactly, so the existing Glue crawlers
pick new years up with no config change:

    s3://<bucket>/raw/nfl/<dataset>/<source-filename>.csv

Usage:
    # Backfill everything from 2018 through 2024
    python -m ingest.nfl_ingest --years 2018-2024

    # Single dataset, single year
    python -m ingest.nfl_ingest --datasets injuries --years 2024

    # Dry run — print planned uploads without touching S3
    python -m ingest.nfl_ingest --years 2024 --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass
from typing import Iterable

import boto3
import requests
from botocore.exceptions import BotoCoreError, ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("nfl_ingest")

BUCKET = "sports-injury-pipeline-manav"
S3_PREFIX = "raw/nfl"
NFLVERSE_BASE = "https://github.com/nflverse/nflverse-data/releases/download"

# Timeout is generous — GitHub Releases occasionally 302-redirect through
# release-assets host that can be slow on the first hit.
HTTP_TIMEOUT_SECONDS = 60
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5


@dataclass(frozen=True)
class Dataset:
    """
    A nflverse dataset. `year_partitioned=False` means one aggregate file for
    all history (currently just `combine`), which ignores --years.
    """
    name: str                # local identifier ("injuries", "rosters", ...)
    release_tag: str         # GitHub release tag ("injuries", "rosters", ...)
    filename_template: str   # "{year}" placeholder if year-partitioned
    year_partitioned: bool = True


DATASETS: dict[str, Dataset] = {
    "injuries":     Dataset("injuries",     "injuries",     "injuries_{year}.csv"),
    "rosters":      Dataset("rosters",      "rosters",      "roster_{year}.csv"),
    # DEPRECATED 2025-08-01: 2024 still resolves via redirect, 2025+ requires
    # migration to the `stats_player` release tag with a different schema.
    "player_stats": Dataset("player_stats", "player_stats", "player_stats_{year}.csv"),
    # Single aggregate file — re-download replaces prior years too.
    "combine":      Dataset("combine",      "combine",      "combine.csv", year_partitioned=False),
}


def parse_years(spec: str) -> list[int]:
    """Accepts '2024', '2018-2024', or '2019,2021,2024'."""
    spec = spec.strip()
    if "-" in spec:
        start, end = spec.split("-", 1)
        return list(range(int(start), int(end) + 1))
    return [int(y) for y in spec.split(",")]


def download_with_retry(url: str) -> bytes:
    """GET with linear backoff. Raises on final failure."""
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=HTTP_TIMEOUT_SECONDS, allow_redirects=True)
            resp.raise_for_status()
            return resp.content
        except requests.RequestException as exc:
            last_exc = exc
            log.warning("attempt %d/%d failed for %s: %s", attempt, MAX_RETRIES, url, exc)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    assert last_exc is not None
    raise last_exc


def upload_to_s3(s3, key: str, body: bytes) -> None:
    """Overwrites unconditionally — makes re-runs idempotent."""
    try:
        s3.put_object(Bucket=BUCKET, Key=key, Body=body)
    except (BotoCoreError, ClientError) as exc:
        log.error("s3 put_object failed for s3://%s/%s: %s", BUCKET, key, exc)
        raise


def plan(datasets: Iterable[Dataset], years: list[int]) -> list[tuple[Dataset, int | None]]:
    """Expand (datasets × years) but only cross-multiply year-partitioned ones."""
    items: list[tuple[Dataset, int | None]] = []
    for ds in datasets:
        if ds.year_partitioned:
            items.extend((ds, y) for y in years)
        else:
            items.append((ds, None))
    return items


def run(datasets: list[Dataset], years: list[int], dry_run: bool) -> int:
    s3 = boto3.client("s3") if not dry_run else None
    failed = 0

    for ds, year in plan(datasets, years):
        filename = ds.filename_template.format(year=year) if year else ds.filename_template
        url = f"{NFLVERSE_BASE}/{ds.release_tag}/{filename}"
        key = f"{S3_PREFIX}/{ds.name}/{filename}"

        if dry_run:
            log.info("[dry-run] %s -> s3://%s/%s", url, BUCKET, key)
            continue

        try:
            log.info("downloading %s", url)
            body = download_with_retry(url)
            log.info("uploading %d bytes to s3://%s/%s", len(body), BUCKET, key)
            upload_to_s3(s3, key, body)
        except Exception as exc:
            log.error("FAILED %s: %s", filename, exc)
            failed += 1

    if failed:
        log.error("%d file(s) failed", failed)
    else:
        log.info("all uploads succeeded")
    return failed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest nflverse raw CSVs into S3.")
    parser.add_argument(
        "--datasets",
        default=",".join(DATASETS),
        help=f"Comma-separated. Choices: {','.join(DATASETS)}. Default: all.",
    )
    parser.add_argument(
        "--years",
        required=True,
        help="Year, range ('2018-2024'), or list ('2019,2021,2024'). "
             "Ignored for non-year-partitioned datasets (combine).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Log planned actions only.")
    args = parser.parse_args(argv)

    try:
        selected = [DATASETS[name.strip()] for name in args.datasets.split(",")]
    except KeyError as exc:
        parser.error(f"unknown dataset {exc}. valid: {list(DATASETS)}")

    years = parse_years(args.years)
    log.info("datasets=%s years=%s dry_run=%s",
             [d.name for d in selected], years, args.dry_run)

    return run(selected, years, args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
