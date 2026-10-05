#!/usr/bin/env python3
"""Download air-quality data from the XKDR India Air Quality Database.

Two modes:

  --mode dev     Small API slice for development and schema validation.
                 Honours the demo key's 10,000-row cap.

  --mode bulk    Monthly Parquet partition files from /v1/files.
                 Needs a personal API key (the demo key can only read 2024).

Usage:
    python scripts/download_data.py --mode dev --year 2024 --month 11
    python scripts/download_data.py --mode bulk --year 2024 --months 1-12

The API key is read from the XKDR_API_KEY environment variable, or from a
.env file in the project root. Never commit that file.
"""

import argparse
import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

BASE_URL = "https://airquality.xkdr.org"
RAW_DIR = PROJECT_ROOT / "data" / "raw"

# Public demo key from the project homepage. Capped at 10,000 rows per query
# and can only read 2024 bulk files.
DEMO_KEY = "aqi_demo_wbf92Qx21zX-Wa_Tg8Dx1nXe"


def get_api_key() -> str:
    """Return the API key, falling back to the public demo key."""
    key = os.environ.get("XKDR_API_KEY", "").strip()
    if key and key != "your_key_here":
        return key
    return DEMO_KEY


def using_demo_key(key: str) -> bool:
    return key == DEMO_KEY


def auth_headers(key: str) -> dict:
    return {"Authorization": f"Bearer {key}"}


def check_api(key: str) -> dict:
    """Fetch /v1/meta so we fail early with a clear message if the key is bad."""
    response = requests.get(
        f"{BASE_URL}/v1/meta", headers=auth_headers(key), timeout=60
    )
    if response.status_code != 200:
        print(f"ERROR: /v1/meta returned {response.status_code}", file=sys.stderr)
        print(f"       {response.text[:300]}", file=sys.stderr)
        sys.exit(1)

    meta = response.json()
    print("XKDR India Air Quality Database")
    print(f"  tier            : {meta.get('your_tier')}")
    print(f"  exported at     : {meta.get('exported_at')}")
    print(f"  total rows      : {meta.get('measurements', {}).get('rows', 0):,}")
    print(f"  total size      : {meta.get('measurements', {}).get('bytes', 0) / 1e6:.0f} MB")
    print(f"  stations        : {meta.get('stations')}")
    print(f"  parameters      : {meta.get('parameters')}")
    months = meta.get("measurements", {})
    print(f"  coverage        : {months.get('first_month')} .. {months.get('last_month')}")
    print(f"  max rows/query  : {meta.get('max_rows_per_query', 0):,}")
    return meta


def parse_months(spec: str) -> list[int]:
    """Parse '1-12', '1,2,3' or a single '7' into a list of month numbers."""
    months: list[int] = []
    for part in spec.split(","):
        part = part.strip()
        if "-" in part:
            start, end = part.split("-", 1)
            months.extend(range(int(start), int(end) + 1))
        else:
            months.append(int(part))
    for month in months:
        if not 1 <= month <= 12:
            raise ValueError(f"month out of range: {month}")
    return sorted(set(months))


def download_dev_slice(key: str, year: int, months: list[int], station: str | None) -> None:
    """Fetch one month as CSV via /v1/measurements. Small, capped, easy to debug."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    for month in months:
        start = f"{year}-{month:02d}-01"
        if month == 12:
            end = f"{year}-12-31"
        else:
            end = f"{year}-{month + 1:02d}-01"

        params = {
            "start": start,
            "end": end,
            "format": "csv",
            "agg": "raw",
        }
        if station:
            params["station"] = station

        target = RAW_DIR / f"dev_{year}_{month:02d}.csv"
        print(f"  {year}-{month:02d} -> {target.name}")
        response = requests.get(
            f"{BASE_URL}/v1/measurements",
            headers=auth_headers(key),
            params=params,
            timeout=120,
        )
        if response.status_code != 200:
            print(f"    ERROR {response.status_code}: {response.text[:200]}", file=sys.stderr)
            continue

        target.write_text(response.text, encoding="utf-8")

        truncated = response.headers.get("X-Truncated", "false").lower()
        row_count = response.headers.get("X-Row-Count", "?")
        size_mb = target.stat().st_size / 1e6
        note = "  [TRUNCATED - demo key row cap]" if truncated == "true" else ""
        print(f"    {row_count} rows, {size_mb:.1f} MB{note}")


def download_bulk(key: str, year: int, months: list[int]) -> None:
    """Fetch monthly Parquet partition files from /v1/files/<key>."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    for month in months:
        rel = f"v1/measurements/year={year}/month={month:02d}/data.parquet"
        target = RAW_DIR / "measurements" / f"year={year}" / f"month={month:02d}" / "data.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)

        if target.exists() and target.stat().st_size > 0:
            print(f"  {year}-{month:02d} already present ({target.stat().st_size / 1e6:.1f} MB), skipping")
            continue

        print(f"  {year}-{month:02d} -> {target}")
        response = requests.get(
            f"{BASE_URL}/v1/files/{rel}",
            headers=auth_headers(key),
            timeout=600,
            stream=True,
        )

        if response.status_code == 403:
            print(f"    SKIPPED: demo key cannot read {year} (403)", file=sys.stderr)
            continue
        if response.status_code != 200:
            print(f"    ERROR {response.status_code}: {response.text[:200]}", file=sys.stderr)
            continue

        # Write to a .part file first so an interrupted download is never
        # mistaken for a complete one on the next run.
        part = target.with_suffix(".parquet.part")
        written = 0
        with part.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                handle.write(chunk)
                written += len(chunk)
        part.rename(target)
        print(f"    {written / 1e6:.1f} MB")


def download_reference(key: str) -> None:
    """Fetch the station and parameter reference tables."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    for name in ("stations", "parameters"):
        target = RAW_DIR / f"{name}.parquet"
        if target.exists():
            print(f"  {name}.parquet already present, skipping")
            continue
        print(f"  {name}.parquet")
        response = requests.get(
            f"{BASE_URL}/v1/files/v1/{name}.parquet",
            headers=auth_headers(key),
            timeout=300,
        )
        if response.status_code != 200:
            print(f"    ERROR {response.status_code}: {response.text[:200]}", file=sys.stderr)
            continue
        target.write_bytes(response.content)
        print(f"    {target.stat().st_size / 1e6:.1f} MB")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=["dev", "bulk", "meta"], default="meta",
                        help="meta: just show dataset info. dev: CSV slice. bulk: monthly Parquet.")
    parser.add_argument("--year", type=int, default=2024, help="year to download")
    parser.add_argument("--months", default="11", help="months: '1-12', '1,2,3', or '7'")
    parser.add_argument("--station", default=None, help="restrict dev slice to one station_id")
    parser.add_argument("--no-reference", action="store_true",
                        help="skip stations.parquet / parameters.parquet in bulk mode")
    args = parser.parse_args()

    key = get_api_key()
    print("Checking API access...")
    check_api(key)

    if using_demo_key(key):
        print("\nUsing the PUBLIC DEMO KEY (10,000 row cap, 2024 bulk files only).")
        print("For the full dataset, get your own key at https://airquality.xkdr.org/signup")
        print("and put it in .env as XKDR_API_KEY=aqi_...\n")

    if args.mode == "meta":
        return

    months = parse_months(args.months)

    if args.mode == "dev":
        print(f"Downloading dev slice for {args.year}, months: {months}")
        download_dev_slice(key, args.year, months, args.station)
    else:
        print(f"Downloading bulk Parquet for {args.year}, months: {months}")
        if not args.no_reference:
            download_reference(key)
        download_bulk(key, args.year, months)

    print("\nDone. Next: python scripts/inspect_data.py")


if __name__ == "__main__":
    main()